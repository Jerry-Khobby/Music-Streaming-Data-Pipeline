# Glue Transformation Code — The Five Jobs and How Data Shape Changes

## What This Document Covers

This document explains, in detail, the five AWS Glue jobs that make up the music streaming
pipeline. For each job it describes **what it does**, **what it receives as input**, **what it
returns/writes as output**, and how the **shape of the data changes** as it flows from raw CSV all
the way to the three DynamoDB tables. Every claim maps to concrete code in
[glue_jobs/](../glue_jobs/) and the wiring in
[terraform/glue_jobs.tf](../terraform/glue_jobs.tf).

The five jobs run strictly in this order, chained by Glue triggers:

```
1. validation  →  2. etl_transform  →  3. kpi_aggregation  →  4. dynamodb_loader  →  5. archive
```

A Glue **crawler** runs before validation to catalog the raw CSV files into the Glue Data Catalog
(the `streams`, `songs`, and `users` tables) so the Spark jobs can read them by table name.

---

## 1. How Data Shape Changes Across the Whole Pipeline

Before the per-job detail, here is the single most important idea — the data physically changes
shape and format at every stage:

| Stage | Format | Granularity | Example shape |
|---|---|---|---|
| **Bronze** (raw input) | CSV | One row per raw event / catalogue entry | `streams`: `user_id, track_id, listen_time` |
| After **validation** | (unchanged) | Same — validation reads but does not rewrite | Schema/non-empty guaranteed |
| **Silver** (`etl_transform`) | Parquet | One row per *enriched, deduplicated* stream | `user_id, track_id, listen_time, track_name, track_genre, duration_ms, stream_date` |
| **Gold** (`kpi_aggregation`) | Parquet | One row per *aggregate* (genre-day, song-rank, genre-rank) | `genre_kpis`, `top_songs`, `top_genres` |
| **DynamoDB** (`dynamodb_loader`) | DynamoDB items | Same as Gold, keyed for lookup | 3 tables keyed by `genre_date` / `date` + `rank` |
| **Archive** (`archive`) | CSV (moved) | Raw files relocated, not reshaped | original `streams/*.csv` in archive bucket |

The pipeline is a funnel: it starts with many detailed raw rows and ends with a handful of
query-ready aggregate items, enriching and collapsing the data at each step.

---

## 2. Job 1 — `validation_job.py`

**Source:** [glue_jobs/validation_job.py](../glue_jobs/validation_job.py)
**Glue job:** `aws_glue_job.validation` ([glue_jobs.tf:71](../terraform/glue_jobs.tf#L71))
**Arguments received:** `--JOB_NAME`, `--glue_database`

### What it does

This is the pipeline's gatekeeper. It confirms that the three catalog tables the crawler built —
`streams`, `songs`, `users` — actually exist, are non-empty, and contain the columns the
downstream jobs depend on. It **transforms nothing**; it only decides whether the pipeline is
allowed to proceed.

### Input

The Glue Data Catalog tables (read via `create_dynamic_frame.from_catalog(...).toDF()` in
`loadTable`, [validation_job.py:40](../glue_jobs/validation_job.py#L40)). The required columns per
table are declared up front ([validation_job.py:12](../glue_jobs/validation_job.py#L12)):

```python
REQUIRED_COLUMNS = {
    "streams": {"user_id", "track_id", "listen_time"},
    "songs":   {"track_id", "track_name", "track_genre", "duration_ms"},
    "users":   {"user_id", "user_name", "user_country"},
}
```

### What it checks

For each table, `validateTable` ([validation_job.py:79](../glue_jobs/validation_job.py#L79))
runs two checks:

- **`checkNonEmpty`** — fails the pipeline if a table is empty, **except** for `streams`, which is
  allowed to be empty (it raises the custom `NoNewStreams` exception instead). An empty `streams`
  table simply means no new files arrived — a normal, clean exit, not an error.
- **`checkMissingColumns`** — fails if any required column is absent.

### Resilience: retry with exponential backoff

Because the crawler and this job run close together, the catalog table may not be registered yet
when validation first looks. `validateTable` retries up to 3 times with exponential backoff
(10 → 20 → 40 seconds, [validation_job.py:95](../glue_jobs/validation_job.py#L95)) on a
`TableNotFound`, then gives a clear error pointing at the crawler if it still isn't there.

### Output

**No data output.** Its "return value" is a decision:

- All tables valid → `job.commit()` and continue the pipeline.
- `streams` empty (`NoNewStreams`) → log, `job.commit()`, `sys.exit(0)` — clean stop, no
  downstream work ([validation_job.py:143](../glue_jobs/validation_job.py#L143)).
- Missing columns / empty `songs`/`users` (`ValueError`) → re-raise to **fail** the pipeline
  ([validation_job.py:148](../glue_jobs/validation_job.py#L148)).

**Shape change: none.** Validation is a pass/fail gate.

---

## 3. Job 2 — `etl_transform_job.py` (Bronze → Silver)

**Source:** [glue_jobs/etl_transform_job.py](../glue_jobs/etl_transform_job.py)
**Glue job:** `aws_glue_job.etl_transform` ([glue_jobs.tf:105](../terraform/glue_jobs.tf#L105))
**Arguments received:** `--JOB_NAME`, `--glue_database`, `--curated_bucket`

### What it does

This is the core transformation. It turns raw, untyped CSV stream events into clean, enriched,
deduplicated Parquet — the **Silver** layer. This is the step where the data's shape changes most.

### Input

Two catalog tables read into DataFrames ([etl_transform_job.py:129](../glue_jobs/etl_transform_job.py#L129)):

- `streams` → `user_id, track_id, listen_time`
- `songs` → `track_id, track_name, track_genre, duration_ms`

### Processing steps

1. **Stale-catalog / empty guard** (`check_streams_have_data`,
   [etl_transform_job.py:36](../glue_jobs/etl_transform_job.py#L36)). If the `streams` table has
   *no columns* (a stale catalog left behind when the crawler ran over an empty prefix) or zero
   rows, the job commits and exits cleanly rather than crashing — a direct consequence of the
   archive job emptying `streams/` between runs.
2. **Column validation** (`validate_columns`) on both inputs as a second safety net.
3. **The join — enrichment** (`build_enriched_streams`,
   [etl_transform_job.py:64](../glue_jobs/etl_transform_job.py#L64)):

   ```python
   streams_df
     .join(songs_df.select(SONGS_COLUMNS), on="track_id", how="inner")
     .withColumn("stream_date", F.to_date(F.col("listen_time")))
   ```

   This **inner join** attaches each song's `track_name`, `track_genre`, and `duration_ms` to
   every stream event, then derives a typed `stream_date` from the `listen_time` timestamp. The
   inner join also acts as a filter: stream events whose `track_id` isn't in the song catalogue are
   dropped (an empty result triggers a clean skip,
   [etl_transform_job.py:151](../glue_jobs/etl_transform_job.py#L151)).
4. **Incremental merge + deduplication** (`merge_and_deduplicate`,
   [etl_transform_job.py:83](../glue_jobs/etl_transform_job.py#L83)). It finds which
   `stream_date` partitions the new data touches, loads only those existing Silver partitions,
   unions old + new, and deduplicates on the natural key
   `["user_id", "track_id", "listen_time"]`. This is the **safety net that guarantees correctness
   even if a file is reprocessed** (see [Archival_Strategy.md](Archival_Strategy.md)).

### Output

Parquet written to `s3://<curated_bucket>/silver/enriched_streams`, **partitioned by
`stream_date`** (`write_silver`, [etl_transform_job.py:103](../glue_jobs/etl_transform_job.py#L103)).
Crucially it uses **dynamic partition overwrite**
(`spark.sql.sources.partitionOverwriteMode = dynamic`,
[etl_transform_job.py:124](../glue_jobs/etl_transform_job.py#L124)) so only the affected days'
partitions are rewritten, never the entire Silver dataset.

### Shape change

```
streams (CSV):  user_id, track_id, listen_time
songs   (CSV):  track_id, track_name, track_genre, duration_ms
                              │  inner join on track_id + derive stream_date + dedup
                              ▼
silver (Parquet, partitioned by stream_date):
   user_id, track_id, listen_time, track_name, track_genre, duration_ms, stream_date
```

Format CSV → Parquet; granularity stays "one row per stream event" but rows are now **enriched**
(genre/name/duration attached), **typed**, and **deduplicated**.

---

## 4. Job 3 — `kpi_aggregation_job.py` (Silver → Gold)

**Source:** [glue_jobs/kpi_aggregation_job.py](../glue_jobs/kpi_aggregation_job.py)
**Glue job:** `aws_glue_job.kpi_aggregation` ([glue_jobs.tf:212](../terraform/glue_jobs.tf#L212))
**Arguments received:** `--JOB_NAME`, `--curated_bucket`

### What it does

It collapses the detailed Silver stream events into three business-level aggregate datasets — the
**Gold** layer. This is where granularity changes from "one row per event" to "one row per
metric".

### Input

A single Silver dataset read from `s3://<curated_bucket>/silver/enriched_streams`
([kpi_aggregation_job.py:115](../glue_jobs/kpi_aggregation_job.py#L115)), cached because it is
scanned multiple times.

### Processing — three aggregates

1. **`genre_kpis`** (`assembleGenreKpis`,
   [kpi_aggregation_job.py:50](../glue_jobs/kpi_aggregation_job.py#L50)) — groups by
   `stream_date, track_genre` and computes four metrics from three helper functions:
   - `listen_count` = `count(*)`
   - `unique_listeners` = `countDistinct(user_id)`
   - `total_listen_time_ms` = `sum(duration_ms)`
   - `avg_listen_time_ms_per_user` = `sum(duration_ms) / countDistinct(user_id)`

   It then builds the composite key `genre_date = concat_ws("#", track_genre, stream_date)` →
   e.g. `"Afrobeats#2026-05-17"`.
2. **`top_songs`** (`computeTopSongsPerGenre`,
   [kpi_aggregation_job.py:66](../glue_jobs/kpi_aggregation_job.py#L66)) — counts plays per
   `(stream_date, track_genre, track_id, track_name)`, then ranks within each genre-day using
   `row_number()` over a window ordered by `desc(play_count), track_id`, and keeps `rank <= 3`.
   `row_number()` (not `rank()`) guarantees **unique** ranks even when play counts tie. It also
   builds the `genre_date` composite key.
3. **`top_genres`** (`computeTopGenresPerDay`,
   [kpi_aggregation_job.py:82](../glue_jobs/kpi_aggregation_job.py#L82)) — takes `genre_kpis`,
   ranks genres within each day by `desc(listen_count), track_genre`, keeps `rank <= 5`, and
   **renames `stream_date` → `date`** so the key is self-describing for the daily leaderboard
   table.

The thresholds are named constants: `TOP_SONGS_RANK = 3`, `TOP_GENRES_RANK = 5`
([kpi_aggregation_job.py:13](../glue_jobs/kpi_aggregation_job.py#L13)).

### Output

Three Parquet datasets under `s3://<curated_bucket>/gold/`
([kpi_aggregation_job.py:122](../glue_jobs/kpi_aggregation_job.py#L122)):

- `gold/genre_kpis` — partitioned by `stream_date`
- `gold/top_songs` — partitioned by `stream_date`
- `gold/top_genres` — partitioned by `date`

It also enables dynamic partition overwrite so re-runs replace only affected day-partitions.

### Shape change

```
silver (one row per stream event)
        │  groupBy + aggregate + window-rank
        ▼
gold/genre_kpis : genre_date, stream_date, track_genre, listen_count,
                  unique_listeners, total_listen_time_ms, avg_listen_time_ms_per_user
gold/top_songs  : genre_date, stream_date, track_genre, track_id, track_name,
                  play_count, rank        (≤ 3 rows per genre-day)
gold/top_genres : date, track_genre, listen_count, rank   (≤ 5 rows per day)
```

Granularity collapses from millions of events to a few rows per genre per day. These three Gold
datasets map one-to-one onto the three DynamoDB tables (see
[DynamoDB_Key_Design.md](DynamoDB_Key_Design.md)).

---

## 5. Job 4 — `dynamodb_loader.py` (Gold → DynamoDB)

**Source:** [glue_jobs/dynamodb_loader.py](../glue_jobs/dynamodb_loader.py)
**Glue job:** `aws_glue_job.dynamodb_loader` ([glue_jobs.tf:140](../terraform/glue_jobs.tf#L140))
**Arguments received:** `--JOB_NAME`, `--curated_bucket`, `--aws_region`

### What it does

It loads each Gold Parquet dataset into its matching DynamoDB table so the application can query
the metrics with single-digit-millisecond key lookups. It does not aggregate; it reshapes each row
into a DynamoDB item and writes it.

### Input

Three Gold datasets, each read and deduplicated on its primary key
([dynamodb_loader.py:92](../glue_jobs/dynamodb_loader.py#L92)):

```python
genreKpisDF = loadParquet(spark, f"{goldBase}/genre_kpis").dropDuplicates(["genre_date"])
topSongsDF  = loadParquet(spark, f"{goldBase}/top_songs").dropDuplicates(["genre_date", "rank"])
topGenresDF = loadParquet(spark, f"{goldBase}/top_genres").dropDuplicates(["date", "rank"])
```

The `dropDuplicates` keys **mirror the DynamoDB primary keys exactly**, which makes the load
idempotent: the same key always overwrites itself with the same data.

### Processing

- **Type conversion for DynamoDB.** DynamoDB has no float type, so floating-point metrics are
  converted to `Decimal` via `toDecimal` ([dynamodb_loader.py:18](../glue_jobs/dynamodb_loader.py#L18)),
  and counts are cast to `int`. The three `build*Item` functions
  ([dynamodb_loader.py:32](../glue_jobs/dynamodb_loader.py#L32)) define the exact item schema per
  table.
- **Distributed batch writes.** `loadToDynamo`
  ([dynamodb_loader.py:69](../glue_jobs/dynamodb_loader.py#L69)) uses `df.foreachPartition` so each
  Spark partition writes its own rows in parallel, each opening a `batch_writer()`
  ([dynamodb_loader.py:24](../glue_jobs/dynamodb_loader.py#L24)) that batches `put_item` calls.

### Output

Items written to the three DynamoDB tables: `genre_kpis`, `top_songs`, `top_genres`.

### Shape change

```
gold Parquet rows  ──build*Item + Decimal/int coercion──▶  DynamoDB items
   (same fields, now typed for DynamoDB and keyed by the table's partition/sort key)
```

Format Parquet → DynamoDB items; granularity unchanged; floats become `Decimal`.

---

## 6. Job 5 — `archive_job.py` (Bronze cleanup)

**Source:** [glue_jobs/archive_job.py](../glue_jobs/archive_job.py)
**Glue job:** `aws_glue_job.archive` ([glue_jobs.tf:177](../terraform/glue_jobs.tf#L177))
**Arguments received:** `--JOB_NAME`, `--raw_bucket`, `--archive_bucket`, `--aws_region`

### What it does

It is the only job that touches **no Spark DataFrame** — it is a pure boto3 S3 housekeeping job.
It moves the processed `streams/*.csv` files out of the raw bucket and into the archive bucket so
the next pipeline run only sees *new* files. This is the mechanism that keeps the pipeline
incremental; it is documented fully in [Archival_Strategy.md](Archival_Strategy.md).

### Input

The keys under the raw bucket's `streams/` prefix, discovered with a paginated `list_objects_v2`
(`list_stream_objects`, [archive_job.py:13](../glue_jobs/archive_job.py#L13)), skipping the folder
placeholder.

### Processing

A deliberate **copy-then-delete**: `copy_objects` then `bulk_delete_objects`
([archive_job.py:53](../glue_jobs/archive_job.py#L53)). Copy first means a failure never loses
data; if the post-copy delete half-fails, the ETL job's deduplication corrects any reprocessing.

### Output

No data transformation — files relocated from `s3://<raw_bucket>/streams/` to
`s3://<archive_bucket>/streams/`, and removed from raw. (The archive bucket later transitions these
to Glacier after 90 days via its lifecycle rule.)

### Shape change

**None.** The CSV files move untouched; only their location changes.

---

## 7. End-to-End Summary

| # | Job | Input | Output | Format change | Shape change |
|---|---|---|---|---|---|
| 1 | `validation_job` | catalog tables `streams`/`songs`/`users` | pass/fail decision | none | none (gate) |
| 2 | `etl_transform_job` | `streams` + `songs` (CSV) | `silver/enriched_streams` | CSV → Parquet | join-enriched + typed + deduped |
| 3 | `kpi_aggregation_job` | `silver/enriched_streams` | `gold/genre_kpis`, `top_songs`, `top_genres` | Parquet → Parquet | events → aggregates |
| 4 | `dynamodb_loader` | the three Gold datasets | 3 DynamoDB tables | Parquet → DynamoDB items | floats → Decimal, keyed |
| 5 | `archive_job` | `streams/*.csv` in raw bucket | same files in archive bucket | none | none (relocation) |

The pipeline reads raw CSV, validates it, enriches and deduplicates it into typed Parquet,
aggregates it into a handful of business KPIs, serves those KPIs from DynamoDB, and finally
archives the consumed raw files — each job doing exactly one job, in order.
