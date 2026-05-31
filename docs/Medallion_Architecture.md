# The Medallion Architecture — Bronze, Silver, Gold

## What This Document Covers

This document explains the **medallion architecture** (also called the multi-hop or Bronze/Silver/
Gold pattern) from the ground up: what it is, *why* it exists in data engineering, and exactly how
this pipeline implements it across its S3 layers and Glue jobs. It is written for a data engineer
new to the cloud, so it builds the idea up from the underlying problem before showing the concrete
implementation.

This is the *conceptual* companion to two more implementation-focused docs:
[S3_Bucket_Layers.md](S3_Bucket_Layers.md) (the bucket/prefix mechanics) and
[Glue_Transformation_Code.md](Glue_Transformation_Code.md) (the job-by-job transformations).

---

## 1. The Problem the Medallion Architecture Solves

Imagine you skip the pattern entirely. Raw CSV files arrive, and a single job reads them, cleans
them, joins them, aggregates them, and writes the final numbers — all in one pass, overwriting the
source. Now ask some ordinary questions:

- *"Yesterday's numbers look wrong. Can I see the exact raw file that produced them?"* — Gone. The
  job consumed and overwrote it.
- *"I found a bug in the aggregation logic. Can I recompute last month without re-ingesting?"* — No,
  because the cleaned, intermediate data was never kept; only the final output exists.
- *"A new analyst wants the detailed per-event data, not the daily summary."* — It doesn't exist as
  a queryable dataset; only the summary survived.
- *"Half the pipeline failed. What state is my data in?"* — Unknown, because everything happened in
  one indivisible step with no checkpoints.

Every one of these problems comes from collapsing *what arrived*, *what was cleaned*, and *what was
computed* into a single stage. The medallion architecture exists to **separate those three
concerns into three durable, queryable layers**, so each can be inspected, reprocessed, and trusted
independently.

---

## 2. What the Medallion Architecture Is

The medallion architecture organizes a data lake into three progressively refined layers, named
after medal tiers to signal increasing value and quality:

| Layer | Also called | Contains | Trust level | Granularity |
|---|---|---|---|---|
| **Bronze** | Raw / Landing | Data exactly as it arrived | Untrusted | Most detailed, messiest |
| **Silver** | Cleansed / Enriched | Validated, cleaned, joined records | Trusted | Detailed, one row per event |
| **Gold** | Curated / Aggregated | Business-level aggregates ready to serve | Authoritative | Summarized, few rows |

The core rules of the pattern are:

1. **Data flows in one direction only:** Bronze → Silver → Gold. A later layer is always *derived*
   from an earlier one, never the other way around.
2. **Each layer is durable and queryable.** You don't throw away Bronze when you produce Silver.
   Every layer persists.
3. **Each layer has exactly one responsibility.** Bronze preserves, Silver cleans, Gold aggregates.
4. **Quality increases as you move up; volume and detail decrease.** Bronze has every messy raw
   row; Gold has a handful of polished numbers.

The payoff is **reproducibility and isolation**. Because each layer is preserved and derived from
the one below, you can always rebuild Gold from Silver, or rebuild Silver from Bronze, after fixing
a bug — without re-ingesting anything from the source. And because the layers are separate, a
failure or a bad transformation is contained to one hop instead of corrupting everything.

---

## 3. Why It Exists — The Engineering Benefits

The pattern is popular because it directly buys you several properties that production data
pipelines need:

- **Auditability / lineage.** Bronze is an immutable record of "what actually arrived." When a
  number looks wrong, you can trace it back through Silver to the exact raw bytes in Bronze.
- **Reprocessability.** Fixed a bug in the KPI logic? Re-run the Silver→Gold job over existing
  Silver data. No need to ask the upstream system to resend files.
- **Separation of trust.** Downstream consumers (dashboards, ML, analysts) read from Silver/Gold,
  which are clean and typed. Nobody queries the messy, unvalidated Bronze directly.
- **Performance and cost.** Raw CSV is slow and expensive to scan repeatedly. Converting to
  columnar Parquet at the Silver stage means every downstream read is cheaper and faster.
- **Incremental processing.** Partitioning each layer (here, by date) means you reprocess only the
  affected slice, not the entire history.
- **Failure isolation.** Each hop is a checkpoint. If Silver→Gold fails, Silver is still intact and
  you simply re-run that one hop.

---

## 4. How This Pipeline Implements the Medallion Architecture

This project maps the three layers onto S3 locations and the Glue jobs that move data between them.
There are **three physical S3 buckets**, with Silver and Gold sharing the `curated` bucket by
prefix:

| Layer | Physical location | Format | Produced by | Partitioned by |
|---|---|---|---|---|
| **Bronze** | `raw` bucket — `songs/`, `users/`, `streams/` | CSV | Upstream upload | n/a |
| **Silver** | `curated` bucket — `silver/enriched_streams` | Parquet | `etl_transform_job.py` | `stream_date` |
| **Gold** | `curated` bucket — `gold/genre_kpis`, `gold/top_songs`, `gold/top_genres` | Parquet | `kpi_aggregation_job.py` | `stream_date` / `date` |

The directional flow Bronze → Silver → Gold is enforced by the Step Functions state machine, which
runs the transform job before the aggregation job before the load (see
[Step_Functions.md](Step_Functions.md)).

### 4a. Bronze — the immutable landing zone

**Where:** `aws_s3_bucket.raw`, prefixes `songs/`, `users/`, `streams/`.

Raw CSV files land here exactly as the upstream producer sends them. Three rules make this a proper
Bronze layer in this project:

- **It is never edited in place.** Files are read, never modified.
- **Versioning is enabled**, so even an accidental overwrite is recoverable.
- **It is the trigger source** — a new file under `streams/` emits an S3 event that starts the whole
  pipeline (see [Event_Driven_Architecture.md](Event_Driven_Architecture.md)).

Because it is untrusted CSV, nothing downstream queries Bronze directly for analytics. Its only
consumers are the crawler (which catalogs its schema) and the first Glue jobs.

### 4b. Silver — cleansed and enriched

**Where:** `curated` bucket, `silver/enriched_streams`. **Job:** `etl_transform_job.py`.

This is the Bronze → Silver hop, where raw CSV becomes trustworthy Parquet. The job:

1. Reads `streams` and `songs` from the catalog.
2. **Joins** them on `track_id` so every stream event is *enriched* with its song's
   `track_name`, `track_genre`, and `duration_ms`, and derives a typed `stream_date`.
3. **Deduplicates** on the natural key `["user_id", "track_id", "listen_time"]` so reprocessing a
   file can never double-count an event.
4. Writes **Parquet partitioned by `stream_date`**, using dynamic partition overwrite so only
   affected days are rewritten.

The shape change is the essence of the Silver layer: same "one row per stream event" granularity,
but now joined, typed, deduplicated, and stored in an efficient columnar format. This is the single
clean source of truth that all aggregation reads from.

### 4c. Gold — business aggregates

**Where:** `curated` bucket, `gold/`. **Job:** `kpi_aggregation_job.py`.

This is the Silver → Gold hop, collapsing millions of detailed events into a handful of business
metrics. From the single Silver dataset it produces three Gold datasets:

- `gold/genre_kpis` — listen count, unique listeners, total and average listen time per genre per
  day.
- `gold/top_songs` — the top 3 songs per genre per day.
- `gold/top_genres` — the top 5 genres globally per day.

These are small, pre-aggregated, and directly serve dashboards — they map one-to-one onto the three
DynamoDB tables (see [KPI_Design_and_Computation.md](KPI_Design_and_Computation.md) and
[DynamoDB_Data_Modeling.md](DynamoDB_Data_Modeling.md)). Crucially, Gold is **disposable and
reproducible**: it can always be rebuilt from Silver, which can always be rebuilt from Bronze.

---

## 5. A Note on the Archive — Not a Fourth Medallion Layer

This pipeline also has an **Archive** bucket, but it is *not* a fourth tier of the medallion
pattern. Archive holds processed Bronze files moved out of the way after a successful run, to keep
the `streams/` prefix containing only new data. It is a housekeeping/retention concern, not a
refinement stage. The medallion pattern is strictly Bronze → Silver → Gold; Archive sits to the
side. See [Archival_Strategy.md](Archival_Strategy.md) for that mechanism.

---

## 6. How the Layers Map to Reprocessing Scenarios

To make the value concrete, here is how each layer pays off when something needs to change:

| Scenario | Which layer you start from | Why |
|---|---|---|
| KPI formula was wrong | **Silver** | Re-run only `kpi_aggregation_job` over existing Silver — no re-ingest |
| Join logic / dedup bug | **Bronze** | Re-run `etl_transform_job` to rebuild Silver from raw |
| Audit a suspicious number | **Bronze** | Trace the final figure back to the exact raw rows |
| New analyst wants per-event detail | **Silver** | Already exists as a clean, queryable Parquet dataset |
| Rebuild everything from scratch | **Bronze** | Bronze is the immutable origin; Silver and Gold regenerate from it |

---

## 7. Summary

| Principle of the pattern | How this pipeline honors it |
|---|---|
| Three layers, increasing quality | Bronze (raw CSV) → Silver (clean Parquet) → Gold (KPIs) |
| One-directional flow | `etl_transform` (B→S) then `kpi_aggregation` (S→G), enforced by Step Functions |
| Each layer durable & queryable | All three persist in S3; Silver/Gold catalogued for Athena |
| One responsibility per layer | Bronze preserves, Silver cleans+enriches, Gold aggregates |
| Quality up, volume down | Millions of raw rows → a few KPI rows per genre per day |
| Reproducibility | Gold rebuilds from Silver; Silver rebuilds from Bronze |
| Efficiency | CSV → columnar Parquet + date partitioning at the Silver hop |

The medallion architecture is what turns a pile of raw CSV uploads into a trustworthy, auditable,
reproducible analytics pipeline. In this project it is realized as three S3 layers connected by two
Glue transformation hops, flowing strictly in one direction from the immutable Bronze landing zone
to the polished Gold metrics that the application serves.
