# Glue Crawlers and Glue Jobs — Catalog, Crawlers, Job Types, and Ordering

## What This Document Covers

This document explains the difference between a **Glue Crawler** and a **Glue Job**, why the
crawler **must** run before the jobs, the difference between **Python Shell** and **PySpark
(`glueetl`)** job types, and the central role of the **Glue Data Catalog** that ties everything
together. Every claim maps to concrete code in [terraform/main.tf](../terraform/main.tf),
[terraform/glue_jobs.tf](../terraform/glue_jobs.tf), and
[terraform/step_functions.tf](../terraform/step_functions.tf).

---

## 1. Crawler vs Job — Two Different Tools

This is the single most important distinction in AWS Glue, and the two are constantly confused:

| | **Glue Crawler** | **Glue Job** |
|---|---|---|
| **Purpose** | Discover *what data exists and what shape it has* | *Process* the data |
| **Reads** | Raw files in S3 (their structure) | Data **via** the catalog the crawler built, or directly from S3 |
| **Writes** | Table definitions (schema/metadata) into the Glue **Data Catalog** | Transformed data back to S3 / DynamoDB |
| **Output** | **Metadata** — column names, types, partitions. *No data is moved.* | **Data** — new files, new rows, moved objects |
| **Runs** | Before jobs, to register/refresh schema | After the crawler, to transform |
| **In this project** | `raw_crawler`, `curated_crawler` | 5 jobs: validation, etl_transform, kpi_aggregation, dynamodb_loader, archive |

A crawler is a **schema discovery tool**. A job is a **data processing program**. The crawler
produces the *map*; the jobs follow the map to do the work.

### The two crawlers in this project

**`raw_crawler`** ([main.tf:277](../terraform/main.tf#L277)) points at the three raw prefixes and
registers one catalog table per prefix:

```hcl
resource "aws_glue_crawler" "raw_crawler" {
  database_name = aws_glue_catalog_database.music_db.name
  s3_target { path = "s3://${aws_s3_bucket.raw.id}/songs/" }
  s3_target { path = "s3://${aws_s3_bucket.raw.id}/streams/" }
  s3_target { path = "s3://${aws_s3_bucket.raw.id}/users/" }
  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }
}
```

It scans the CSV files, **infers each column's name and type**, and creates the `songs`, `streams`,
and `users` tables in the `music_db` catalog database. `UPDATE_IN_DATABASE` means re-runs refresh
the schema in place if the file structure changes.

**`curated_crawler`** ([main.tf:322](../terraform/main.tf#L322)) points at `gold/` and registers
the aggregate datasets so they become queryable in **Amazon Athena**, keeping the partition list
current after each KPI run.

---

## 2. The Glue Data Catalog — the Shared Metastore

**`aws_glue_catalog_database.music_db`** ([main.tf:268](../terraform/main.tf#L268)) is the
central piece that makes the crawler and the jobs work together. The Data Catalog is a **schema
registry / metastore**: a central place that stores *table definitions* (names, columns, types,
S3 locations, partitions) separately from the data itself, which stays in S3.

Its role in this project:

- **Decouples storage from schema.** The actual bytes live in S3 as CSV/Parquet. The catalog holds
  the *description* of those bytes. A Glue job never has to hard-code "the streams file has columns
  X, Y, Z at path …" — it just asks the catalog for the `streams` table.
- **It is the hand-off point between crawler and jobs.** The crawler **writes** table definitions
  into the catalog; the jobs **read** from it. They never talk to each other directly — the catalog
  is the contract between them.
- **It is what the jobs actually read.** Both `validation_job.py` and `etl_transform_job.py` load
  data with `create_dynamic_frame.from_catalog(database=..., table_name=...)` — i.e. *by catalog
  table name*, not by S3 path. The database name is injected via the `--glue_database` argument
  ([glue_jobs.tf:91](../terraform/glue_jobs.tf#L91)).
- **It powers Athena.** Because the `gold/` tables are registered, analysts can run SQL over the
  KPIs in Athena without any extra setup — that is the entire reason `curated_crawler` exists.

So the data flow of *metadata* is: **Crawler → Data Catalog → Jobs (and Athena)**.

---

## 3. Why the Crawler Must Run Before the Jobs

This is a hard ordering requirement, not a preference. The jobs read their input **by catalog
table name**, and those tables **do not exist until the crawler creates them**.

Concretely, `validation_job.py` and `etl_transform_job.py` both call:

```python
glue_context.create_dynamic_frame.from_catalog(database=database, table_name="streams")
```

If the crawler has not run, there is no `streams` table in `music_db`, and the call fails with
"Table not found". This is exactly why `validation_job.py` wraps the load in retry-with-backoff and
raises a `TableNotFound` pointing at the crawler — it is defending against being run too early.

The Step Functions state machine enforces the ordering explicitly. Its documented execution order
([step_functions.tf:1](../terraform/step_functions.tf#L1)) is:

```
1. StartRawCrawler   → fires the raw crawler
   WaitForCrawler    → waits 45s between polls
   CheckCrawlerStatus→ reads crawler state via the AWS SDK
   IsCrawlerReady    → loops until state == READY, only then proceeds
   CheckStreamsExist → confirms real CSV files exist under streams/
2. ValidateData      → validation_job
3. TransformData     → etl_transform_job
4. AggregateKPIs     → kpi_aggregation_job
5. LoadDynamoDB      → dynamodb_loader
6. StartCuratedCrawler → refresh gold/ partitions for Athena
7. ArchiveFiles      → archive_job
```

The machine **starts the crawler, polls until it reports `READY`, and only then runs the jobs.**
The curated crawler runs again near the end (step 6) so the freshly written `gold/` partitions are
registered for Athena.

### The stale-schema edge case this ordering creates

There is a subtle interaction with archival worth documenting. The crawler registers the `streams`
table schema from **whatever is currently in `streams/`**. If the archive job from a previous run
emptied that prefix and the crawler then runs over an empty folder, it registers the table with
**zero columns**. The pipeline handles this gracefully: the state machine's `CheckStreamsExist`
step short-circuits to a clean success when there are no files, and `etl_transform_job.py`'s
`check_streams_have_data` guard exits cleanly if the catalog table has no columns. So the
crawler-first ordering is safe even when there is nothing new to process.

---

## 4. Python Shell vs PySpark (`glueetl`) Job Types

AWS Glue offers different **job types**, set by the `command { name = ... }` field:

| Job type | `command.name` | Runtime | Best for | Startup / cost |
|---|---|---|---|---|
| **PySpark (Spark ETL)** | `"glueetl"` | Distributed Apache Spark cluster | Large-scale joins, aggregations, partitioned reads/writes | Slower start (minutes), more cost |
| **Python Shell** | `"pythonshell"` | Single node, plain Python | Lightweight orchestration, small boto3/SQL tasks, no Spark | Fast start (seconds), ~cheaper |

**The rule of thumb:** if the work needs distributed data processing (Spark DataFrames), use
`glueetl`. If it is small, single-node "glue code" — calling an API, moving files, a quick
metadata operation — Python Shell is faster to start and cheaper to run.

### How this maps onto the five jobs in this project

Four of the five jobs are genuinely Spark workloads and are correctly configured as PySpark
(`glueetl`):

- **validation_job** — reads catalog tables into Spark DataFrames to check schema/row counts.
- **etl_transform_job** — a distributed join of streams × songs plus dedup. Classic Spark.
- **kpi_aggregation_job** — groupBy aggregations and windowed ranking over all events. Classic
  Spark.
- **dynamodb_loader** — uses `df.foreachPartition` to write to DynamoDB in parallel across Spark
  partitions.

All four use `command { name = "glueetl" }`, `glue_version = "4.0"`, `worker_type = "G.1X"`, and
`number_of_workers = 2` ([glue_jobs.tf](../terraform/glue_jobs.tf)).

### The archive job — intended Python Shell, an important note

The **archive_job** is a pure boto3 S3 operation: list, copy, delete. It uses **no Spark at all**.
It is therefore the textbook candidate for a **Python Shell** job, and the code comment says
exactly that ([glue_jobs.tf:173](../terraform/glue_jobs.tf#L173)):

```hcl
# ── GLUE JOB 4 — archive_job (Python Shell) ──
# Pure boto3 S3 operations — no Spark needed. Python Shell starts in seconds
# vs. minutes for a Spark cluster, and costs ~4x less per run.
```

**However**, the actual resource is still configured with `command { name = "glueetl" }`
([glue_jobs.tf:182](../terraform/glue_jobs.tf#L182)) — i.e. it currently deploys as a *PySpark*
job, spinning up a 2-worker Spark cluster it never uses. To realize the documented speed and cost
benefit, the archive job's command should be changed to a Python Shell command:

```hcl
command {
  name            = "pythonshell"
  script_location = "s3://${aws_s3_bucket.curated.id}/scripts/archive_job.py"
  python_version  = "3"
}
# and replace worker_type/number_of_workers with: max_capacity = 0.0625  (1/16 DPU)
```

This is the one place where the project's *intent* (Python Shell, per the comment and the job's
Spark-free nature) and its *current configuration* (`glueetl`) diverge — flagged here so the doc
reflects reality, not just intent.

---

## 5. How a Run Ties It All Together

```
        ┌─────────────┐   writes schema    ┌──────────────────────┐
        │ raw_crawler │ ─────────────────▶ │  Glue Data Catalog    │
        │ (discovery) │                    │  music_db: streams,   │
        └─────────────┘                    │  songs, users         │
                                           └──────────┬────────────┘
                                                      │ from_catalog(table_name)
            reads via catalog ┌───────────────────────┘
                              ▼
   validation → etl_transform → kpi_aggregation → dynamodb_loader → archive
     (glueetl)    (glueetl)        (glueetl)         (glueetl)     (glueetl*, should be pythonshell)
                                           │ writes gold/
                                           ▼
        ┌────────────────┐  registers gold/ partitions  ┌──────────┐
        │ curated_crawler│ ───────────────────────────▶ │  Athena   │
        └────────────────┘                              └──────────┘
```

1. The **raw crawler** discovers the raw CSV schemas and writes `streams`/`songs`/`users` into the
   **Data Catalog**.
2. The **jobs** read those tables *by name* from the catalog and process the data — Spark for the
   four data-heavy jobs, a boto3 task for archive.
3. The **curated crawler** registers the resulting `gold/` tables so **Athena** can query the KPIs.

The crawler builds the map; the catalog holds the map; the jobs follow it. That is why the crawler
runs first, and why the catalog sits at the center of the whole pipeline.

---

## 6. Summary

| Question | Answer for this project |
|---|---|
| Crawler vs Job? | Crawler discovers schema and writes it to the Data Catalog (no data moved); Jobs process data. |
| Why crawler first? | Jobs read input via `from_catalog(table_name)`; those tables don't exist until the raw crawler creates them. Step Functions starts the crawler and polls until `READY` before any job runs. |
| Data Catalog's role? | Central metastore decoupling S3 storage from schema; the hand-off contract between crawler (writes) and jobs/Athena (read). |
| Python Shell vs PySpark? | PySpark (`glueetl`) for distributed Spark work — used by validation, etl_transform, kpi_aggregation, dynamodb_loader; Python Shell (`pythonshell`) for lightweight single-node boto3 work — the intended type for archive (currently still `glueetl`). |
