# Partitioning Strategy in S3

## What This Document Covers

This document explains why the curated data is **partitioned by date**, how partitioning improves
Athena query performance and cost, and what the partition **folder structure** actually looks like in
this pipeline. It is written for a data engineer new to the cloud. Everything maps to the
`partitionBy` calls in [glue_jobs/etl_transform_job.py](../glue_jobs/etl_transform_job.py) and
[glue_jobs/kpi_aggregation_job.py](../glue_jobs/kpi_aggregation_job.py).

> **Accuracy note up front:** the topic title mentions a `year=2026/month=05/day=17` folder layout.
> This pipeline actually partitions by a **single date column**, producing folders like
> `stream_date=2026-05-17/`. Both are valid Hive-style partitioning; Section 4 explains the
> difference honestly, so you understand what this project does *and* what the multi-level
> year/month/day pattern would mean.

---

## 1. What Partitioning Is

Recall that S3 is a flat key-value store and "folders" are just key prefixes (see
[S3_Bucket_Layers.md](S3_Bucket_Layers.md)). **Partitioning** is the practice of organizing a
dataset's files into prefixes based on the value of a column — most commonly a date — so that the
column's value is encoded in the file path itself.

Instead of dumping every day's data into one flat pile:

```
gold/genre_kpis/part-0001.parquet
gold/genre_kpis/part-0002.parquet      ← all dates mixed together
```

partitioning splits it by date into separate prefixes:

```
gold/genre_kpis/stream_date=2026-05-17/part-0001.parquet
gold/genre_kpis/stream_date=2026-05-18/part-0001.parquet   ← each day in its own prefix
```

The key idea: **the partition column's value lives in the path**, so a query engine can find a
specific day's data by looking at the path alone — without opening any files.

---

## 2. How This Pipeline Partitions

Both Glue jobs that write to the curated bucket partition their output by date.

**Silver** — the transform job writes enriched streams partitioned by `stream_date`
([etl_transform_job.py:103](../glue_jobs/etl_transform_job.py#L103)):

```python
df.write.mode("overwrite").partitionBy("stream_date").parquet(path)
# → silver/enriched_streams/stream_date=2026-05-17/...
```

**Gold** — the KPI job writes each dataset partitioned by date
([kpi_aggregation_job.py:122](../glue_jobs/kpi_aggregation_job.py#L122)):

```python
writeParquet(genreKpisDF, f"{goldBase}/genre_kpis", partitionCols=["stream_date"])
writeParquet(topSongsDF,  f"{goldBase}/top_songs",  partitionCols=["stream_date"])
writeParquet(topGenresDF, f"{goldBase}/top_genres",  partitionCols=["date"])
```

So the on-disk layout in the curated bucket is, for example:

```
gold/genre_kpis/stream_date=2026-05-17/part-….parquet
gold/top_songs/stream_date=2026-05-17/part-….parquet
gold/top_genres/date=2026-05-17/part-….parquet
```

This is **Hive-style partitioning** — the `column=value` naming convention that Spark, the Glue
crawler, and Athena all understand automatically.

---

## 3. Why Partition by Date — Performance and Cost

Date is the natural partition key here because **every access pattern is scoped to a day**. The
DynamoDB tables answer "for this day" questions, and analysts ask "what happened on 2026-05-17?"
Partitioning by date pays off in four ways:

### 3a. Partition pruning — read only what you need

When a query filters on the partition column, the engine **skips entire partitions that can't
match**. A query for `WHERE stream_date = '2026-05-17'` reads only the
`stream_date=2026-05-17/` prefix and ignores every other day's files. This is called **partition
pruning**, and it is the single biggest performance lever.

Without partitioning, the same query would have to scan *every* file in the dataset and filter
row-by-row — reading (and paying for) all of history to answer a one-day question.

### 3b. Athena cost — you pay for bytes scanned

Athena charges **per amount of data scanned**. Partition pruning directly reduces that: scanning one
day's partition instead of the whole dataset means scanning a tiny fraction of the bytes, so the
query costs a tiny fraction as much. As the dataset grows over months and years, this difference
grows from "nice" to "enormous" — an unpartitioned query gets linearly slower and more expensive
every day, while a partitioned one stays roughly constant for a fixed date range.

### 3c. Faster writes and safe reprocessing

Partitioning also helps the *write* side. Both jobs use **dynamic partition overwrite**
(`spark.sql.sources.partitionOverwriteMode = dynamic`,
[etl_transform_job.py:124](../glue_jobs/etl_transform_job.py#L124)). This means re-running the
pipeline for one date **overwrites only that date's partition** and leaves all other days untouched —
instead of rewriting the entire dataset. That makes reprocessing a single day cheap and safe, and
contributes to the pipeline's idempotency (see
[Idempotency_in_Data_Pipelines.md](Idempotency_in_Data_Pipelines.md)).

### 3d. Parallelism

Splitting data into many partition files lets Spark and Athena read them **in parallel**, improving
throughput on large scans.

---

## 4. `stream_date=2026-05-17/` vs `year=2026/month=05/day=17/`

The topic referenced a multi-level `year=2026/month=05/day=17/` structure. Here is the honest
comparison, because both are legitimate and the distinction is worth understanding:

| | Single-level (this project) | Multi-level (year/month/day) |
|---|---|---|
| Folder layout | `stream_date=2026-05-17/` | `year=2026/month=05/day=17/` |
| Partition columns | one: `stream_date` | three: `year`, `month`, `day` |
| Query a single day | `WHERE stream_date='2026-05-17'` | `WHERE year=2026 AND month=05 AND day=17` |
| Query a whole month | range filter on `stream_date` | `WHERE year=2026 AND month=05` (prunes to the month folder) |
| Best when | Most queries target specific days (this pipeline's case) | Queries often aggregate by month/year, or partition counts get very large |

**Why single-level `stream_date` is the right choice here:** the pipeline's queries and the DynamoDB
access patterns are all **day-scoped**, so one date partition column gives clean per-day pruning with
the simplest possible layout. The multi-level `year/month/day` scheme shines when you frequently query
*ranges* like "all of May" or "all of 2026" — the nested folders let the engine prune to a whole month
or year by matching a higher folder, and it keeps the number of sub-partitions per level small. This
project doesn't need that, so it uses the simpler single-level scheme. (If month/year roll-up queries
became common, switching to `year/month/day` would be the natural evolution.)

Either way, the *principle* is identical: encode the date in the path so the engine reads only the
relevant folders.

---

## 5. How Athena Learns the Partitions

Writing partitioned files is only half the story — Athena must *know* the partitions exist. That's the
**curated crawler's** job: after the KPI job writes new `gold/` partitions, the crawler scans the
curated bucket and **registers the new partitions in the Glue Data Catalog** (see
[Schema_Management_and_Glue_Catalog.md](Schema_Management_and_Glue_Catalog.md) and
[Amazon_Athena.md](Amazon_Athena.md)). The crawler config inherits partition schema from the table
(`Partitions = { AddOrUpdateBehavior = "InheritFromTable" }`), and the Step Functions step that runs
it is non-fatal — if it fails, Athena simply misses the newest day until the next run.

---

## 6. Summary

| Aspect | This pipeline |
|---|---|
| **What is partitioned** | Silver (`enriched_streams`) and all three Gold datasets |
| **Partition column** | `stream_date` (Silver, `genre_kpis`, `top_songs`); `date` (`top_genres`) |
| **Folder layout** | Hive-style `stream_date=2026-05-17/` (single-level) |
| **Performance benefit** | Partition pruning — read only the relevant day(s), not all history |
| **Cost benefit** | Athena bills per byte scanned; pruning slashes bytes scanned, and keeps cost flat as data grows |
| **Write benefit** | Dynamic partition overwrite rewrites only the affected day → cheap, safe reprocessing |
| **vs year/month/day** | Both are Hive partitioning; single-level fits day-scoped queries; multi-level suits month/year roll-ups |
| **How Athena sees it** | The curated crawler registers partitions into the Glue Catalog |

Partitioning by date turns "scan everything and filter" into "open only the right folder." For a
pipeline whose every question is about a specific day, that means fast, cheap, parallel queries today
and — crucially — queries that stay fast and cheap as years of data accumulate.
