# S3 Bucket Types and Layers — Bronze, Silver, Gold and Archive

## What This Document Covers

This document explains the S3 storage design for the music streaming pipeline: the difference
between the **Bronze**, **Silver**, **Gold**, and **Archive** layers, why S3 is a *flat*
key–value store (and how this project simulates a folder structure on top of it), and when each
layer is written, read, and cleaned up during a pipeline run. Every claim below maps to concrete
resources in `terraform/main.tf` and to the Glue jobs in `glue_jobs/` that move data between
layers.

---

## 1. Why a Layered Storage Model At All

Raw streaming files arriving from an upstream system cannot be trusted, cannot be queried
efficiently, and should never be overwritten. At the same time, the dashboards and the DynamoDB
load step need small, clean, query-ready datasets. A single bucket holding "the data" would force
every job to re-read and re-clean the same files repeatedly, and would make it impossible to tell
the difference between *what arrived*, *what was cleaned*, and *what was computed*.

The **medallion architecture** (Bronze → Silver → Gold) solves this by giving each stage of the
data's life its own home. Data only ever flows in one direction, and each layer has exactly one
responsibility:

| Layer | S3 location in this project | Format | Written by | Responsibility |
|---|---|---|---|---|
| **Bronze** (Raw) | `raw` bucket — `songs/`, `users/`, `streams/` prefixes | CSV | Upstream producers / manual upload | Untouched landing zone for incoming files |
| **Silver** | `curated` bucket — `silver/` prefix | Parquet | `etl_transform_job.py` | Cleansed, joined, deduplicated records |
| **Gold** | `curated` bucket — `gold/` prefix | Parquet | `kpi_aggregation_job.py` | Business-level KPI aggregates |
| **Archive** | `archive` bucket — `streams/` prefix | CSV | `archive_job.py` | Processed raw files, removed from Bronze |

There are **three physical buckets**, not four. Silver and Gold share the single `curated` bucket
(separated by prefix) because they have the same access controls, the same lifecycle needs, and
are always written and read by the same trusted Glue jobs. Bronze and Archive are isolated into
their own buckets because they have fundamentally different trust levels and lifecycle rules.

---

## 2. The Bronze Layer — Raw Landing Zone

**Bucket:** `aws_s3_bucket.raw` (`terraform/main.tf:10`)
**Tag:** `Layer = "bronze"`

The Bronze bucket is the entry point of the whole pipeline. Upstream producers drop CSV files into
one of three prefixes:

- `songs/` — the song catalogue (`track_id`, `track_name`, `track_genre`, `duration_ms`)
- `users/` — the user catalogue (`user_id`, `user_name`, `user_country`)
- `streams/` — the streaming events (`user_id`, `track_id`, `listen_time`)

**Defining characteristics of Bronze:**

- **It is never modified in place.** Files are read, never edited. This is the immutable record of
  "what actually arrived."
- **Versioning is enabled** (`aws_s3_bucket_versioning.raw`, `main.tf:20`) so that even an
  accidental overwrite of a key keeps the prior version recoverable.
- **It is the event source.** `aws_s3_bucket_notification.raw_eventbridge` (`main.tf:28`) turns on
  EventBridge notifications so that a new `streams/` file landing in this bucket can trigger the
  Step Functions pipeline.
- **Server-side encryption** (AES256) is applied at rest (`main.tf:33`).

**When to use Bronze:** only as a write target for incoming data and a read source for the first
Glue job (validation / transform). Nothing downstream should ever query Bronze directly for
analytics — the data is unvalidated, CSV-formatted, and slow to scan.

---

## 3. The Silver Layer — Cleansed and Enriched

**Location:** `aws_s3_bucket.curated` bucket, `silver/` prefix (`main.tf:64`, `main.tf:90`)
**Written by:** `glue_jobs/etl_transform_job.py`

The Silver layer is where raw CSV becomes trustworthy, query-ready data. The transform job
(`etl_transform_job.py`) does the **Bronze → Silver** step:

1. Reads the raw `streams` and `songs` data (via the Glue Data Catalog tables the crawler built
   over the Bronze bucket).
2. Joins streams to songs on `track_id` and derives a `stream_date` column
   (`build_enriched_streams`, `etl_transform_job.py:64`).
3. Merges with existing partitions and **deduplicates**, so re-runs do not double-count events
   (`merge_and_deduplicate`).
4. Writes the result as **Parquet**, partitioned by `stream_date`, to
   `s3://<curated_bucket>/silver/enriched_streams` (`write_silver`, `main.tf` curated bucket).

**Why Parquet and not CSV here:** Parquet is columnar, compressed, and carries a schema. It is
dramatically cheaper and faster for the downstream Spark aggregation to read than raw CSV, and it
removes the type ambiguity inherent in CSV.

**Why partition by `stream_date`:** the job uses *dynamic partition overwrite*
(`spark.sql.sources.partitionOverwriteMode = dynamic`, `etl_transform_job.py:124`) so that
re-processing one day's data replaces only that day's partition rather than rewriting the entire
Silver dataset.

**When to use Silver:** as the single clean source of truth for any aggregation. The Gold job
reads exclusively from Silver. Silver is detailed (one row per stream event) — use it when you
need record-level granularity.

---

## 4. The Gold Layer — Business Aggregates

**Location:** `aws_s3_bucket.curated` bucket, `gold/` prefix (`main.tf:96`)
**Written by:** `glue_jobs/kpi_aggregation_job.py`

The Gold layer holds the final, business-ready aggregates that the DynamoDB load step and any
BI tool consume. The KPI job (`kpi_aggregation_job.py`) does the **Silver → Gold** step, reading
`silver/enriched_streams` and producing three datasets:

- `gold/genre_kpis` — listen count, unique listeners, total and average listen time per genre per
  day (partitioned by `stream_date`).
- `gold/top_songs` — the top 3 songs per genre per day (partitioned by `stream_date`).
- `gold/top_genres` — the top 5 genres globally per day (partitioned by `date`).

These three Gold datasets map one-to-one onto the three DynamoDB tables described in
[DynamoDB_Key_Design.md](DynamoDB_Key_Design.md). The `dynamodb_loader.py` job reads from
`s3://<curated_bucket>/gold/...` and writes each dataset into its matching table.

**Defining characteristics of Gold:**

- **Small and pre-aggregated** — a handful of rows per genre per day, not millions of events.
- **Directly serves the application** — these are exactly the numbers a dashboard shows; no
  further computation is needed.
- **Disposable and reproducible** — Gold can always be rebuilt from Silver, which can be rebuilt
  from Bronze. That is precisely why the directional flow matters.

**When to use Gold:** for serving dashboards, leaderboards, and the DynamoDB load. Never query
Gold for record-level detail — it has already been collapsed to aggregates.

---

## 5. The Archive Layer — Processed-File Graveyard

**Bucket:** `aws_s3_bucket.archive` (`main.tf:104`)
**Tag:** `Layer = "archive"`
**Written by:** `glue_jobs/archive_job.py`

Once a stream file has been successfully transformed into Silver, it must not be processed again
on the next run — otherwise the same events would be re-ingested. The Archive layer solves this by
*moving* processed files out of the Bronze `streams/` prefix.

The archive job (`archive_job.py`) does a **copy-then-delete**, deliberately in that order
(`archive_processed_streams`, `archive_job.py:53`):

1. **Copy** every object under the raw bucket's `streams/` prefix to the archive bucket
   (`copy_objects`).
2. **Delete** those same objects from the raw bucket only after every copy succeeds
   (`bulk_delete_objects`, batched at the 1000-key S3 limit).

The ordering is a safety guarantee, documented in the job itself:

- If the **copy** fails, the raw files are untouched and the next run reprocesses them safely.
- If the **delete** fails after a successful copy, the file briefly exists in both buckets —
  harmless, because Silver-layer deduplication keeps the result correct.

**Lifecycle / cost management:** the archive bucket has a lifecycle rule
(`aws_s3_bucket_lifecycle_configuration.archive`, `main.tf:124`) that transitions all objects to
the **GLACIER** storage class after **90 days**. Archived files are kept for audit and recovery
but moved to the cheapest tier since they are rarely read.

**When to use Archive:** never as a pipeline input. It exists purely to (a) keep the Bronze
`streams/` prefix clean so the crawler and transform job only ever see *new* files, and (b)
preserve a cheap, durable history of every raw file ever processed.

---

## 6. Why S3 Is Flat — and How the Folder Structure Is Simulated

This is one of the most commonly misunderstood aspects of S3, and it directly shaped how the
buckets in this project are laid out.

### S3 has no folders

S3 is **not a filesystem**. It is a flat key–value object store. A bucket is a single namespace
that maps a **key** (a string) to an **object** (the bytes). There is no directory tree, no
`mkdir`, no concept of a folder containing files. When you store an object with the key
`streams/streams1.csv`, the key is literally the entire string `"streams/streams1.csv"` — the `/`
is just an ordinary character in that string, not a directory separator.

### How "folders" appear anyway

What looks like a folder is really a **key prefix** plus a UI convention:

- The AWS console (and the SDKs, via the `Delimiter` parameter) groups keys that share a common
  prefix up to the next `/` and *renders* them as if they were folders. So all keys beginning with
  `streams/` are displayed under a "streams" folder that does not actually exist as an object.
- Listing operations like `list_objects_v2(Prefix="streams/")` — exactly what the archive job uses
  (`archive_job.py:14`) — filter by that prefix string. This is how the job finds "all files in
  the streams folder" without there being a folder at all.

### Why this project creates empty placeholder objects

Because folders don't really exist, an "empty folder" cannot exist either — there is nothing to
list until a real file is uploaded. To make the intended structure visible in the console *before*
any data arrives, the Terraform explicitly creates **zero-byte placeholder objects** whose keys
end in `/`:

```hcl
# main.tf:43 — Bronze bucket folder placeholders
resource "aws_s3_object" "songs_folder" {
  bucket  = aws_s3_bucket.raw.id
  key     = "songs/"
  content = ""
}

resource "aws_s3_object" "users_folder" {
  bucket  = aws_s3_bucket.raw.id
  key     = "users/"
  content = ""
}
```

```hcl
# main.tf:90 — curated bucket layer placeholders
resource "aws_s3_object" "silver_folder" {
  bucket  = aws_s3_bucket.curated.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold_folder" {
  bucket  = aws_s3_bucket.curated.id
  key     = "gold/"
  content = ""
}
```

Each of these is a real object with an empty body whose key is just the prefix followed by `/`.
The console interprets a zero-byte key ending in `/` as an empty folder and renders it. This is
purely cosmetic and organizational — it documents the intended layout (`songs/`, `users/` in
Bronze; `silver/`, `gold/` in curated) so that anyone browsing the bucket immediately understands
the structure even before the first file lands.

> Note: the `streams/` prefix is *not* given a placeholder object because that prefix is created
> implicitly the moment the first real `streams/...csv` file is uploaded, and it is continuously
> emptied by the archive job. The `songs/` and `users/` reference data, by contrast, benefits from
> a visible placeholder.

---

## 7. The End-to-End Flow Across Layers

Putting the four layers together, a single pipeline run moves data strictly in one direction:

```
 Upstream producers
        │  (CSV upload)
        ▼
 ┌──────────────┐     etl_transform_job.py      ┌──────────────────────┐
 │   BRONZE      │  ── join + dedup + Parquet ─▶ │  SILVER (curated)     │
 │ raw bucket    │                               │  silver/enriched_…    │
 │ songs/ users/ │                               └──────────┬───────────┘
 │ streams/      │                                          │ kpi_aggregation_job.py
 └──────┬────────┘                                          ▼
        │                                        ┌──────────────────────┐
        │ archive_job.py                         │  GOLD (curated)       │
        │ (copy → delete)                        │  genre_kpis/ top_…    │
        ▼                                        └──────────┬───────────┘
 ┌──────────────┐                                          │ dynamodb_loader.py
 │  ARCHIVE      │                                          ▼
 │ archive bucket│                                  DynamoDB (3 tables)
 │ streams/      │
 │ → GLACIER 90d │
 └──────────────┘
```

1. **Bronze → Silver:** `etl_transform_job.py` reads raw CSV, joins streams to songs,
   deduplicates, and writes partitioned Parquet to `silver/`.
2. **Silver → Gold:** `kpi_aggregation_job.py` reads `silver/enriched_streams` and writes the
   three aggregate datasets to `gold/`.
3. **Gold → DynamoDB:** `dynamodb_loader.py` reads each Gold dataset and loads it into its
   matching table.
4. **Bronze → Archive:** `archive_job.py` copies processed `streams/` files to the archive bucket
   and deletes them from Bronze so the next run starts clean.

---

## 8. When to Use Each Layer — Quick Reference

| If you need to… | Use this layer |
|---|---|
| Ingest a new raw file from upstream | **Bronze** (`raw` bucket, correct prefix) |
| Recover the exact bytes that arrived | **Bronze** (versioned) or **Archive** (if already processed) |
| Run record-level analysis on clean stream events | **Silver** (`curated/silver/`) |
| Serve a dashboard, leaderboard, or load DynamoDB | **Gold** (`curated/gold/`) |
| Audit historical raw files cheaply | **Archive** (Glacier after 90 days) |
| Re-run the whole pipeline from scratch | Start at **Bronze**; Silver and Gold are fully reproducible |

The guiding rule: **data flows in one direction, Bronze → Silver → Gold**, with **Archive** as a
side exit for processed raw files. Each layer trades detail for query-readiness — Bronze is the
most detailed and least usable, Gold is the most refined and immediately consumable.
