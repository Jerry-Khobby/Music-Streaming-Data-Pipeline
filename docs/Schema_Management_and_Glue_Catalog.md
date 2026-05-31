# Schema Management and the Glue Data Catalog

## What This Document Covers

This document explains what a **schema** is, how the **Glue Data Catalog** stores it, what happens
when a source file changes its columns, and how the crawler setting
`update_behavior = "UPDATE_IN_DATABASE"` protects this pipeline. It is written for a data engineer
new to the cloud, building each idea up before showing the implementation. It is the schema-focused
companion to [Glue_Crawlers_and_Jobs.md](Glue_Crawlers_and_Jobs.md). Everything maps to
[terraform/main.tf](../terraform/main.tf).

---

## 1. What a Schema Is

A **schema** is the *description of the shape of your data* — the list of columns, their names,
their data types, and their order. For a CSV like `streams`, the schema is something like:

```
user_id    : string
track_id   : string
listen_time: timestamp
```

The data itself is the rows of values; the schema is the contract that says what those values
*mean* and what type each one is. Without a schema, a file is just undifferentiated text — you can't
ask for "the `user_id` column" because nothing knows which column that is or that it should be read
as a string.

Every processing step in this pipeline depends on a schema: the validation job checks required
*columns*, the transform job joins on the `track_id` *column*, the KPI job sums the `duration_ms`
*column*. All of that requires something, somewhere, to know the shape of each dataset. That
"somewhere" is the Glue Data Catalog.

---

## 2. What the Glue Data Catalog Is and How It Stores Schema

The **Glue Data Catalog** is a central **metastore** — a managed registry that stores **table
definitions** separately from the data. The data stays in S3 as files; the catalog stores the
*metadata about* those files: the table name, its columns and types (the schema), its S3 location,
its partitions, and its file format.

In this project the catalog database is created here ([main.tf:268](../terraform/main.tf#L268)):

```hcl
resource "aws_glue_catalog_database" "music_db" {
  name        = var.glue_database_name
  description = "Glue Data Catalog database for the music streaming pipeline"
}
```

Think of `music_db` as a library catalogue. The books (data) are on the S3 shelves; the catalogue
(Data Catalog) records the title, location, and contents of each book so anyone can find and read it
without rummaging through every shelf. Crucially:

- The catalog holds **only metadata** — registering a table moves no data.
- It **decouples storage from schema.** Jobs ask the catalog "what does the `streams` table look
  like and where is it?" instead of hard-coding file paths and column lists.
- It is the **shared contract**: the crawler *writes* schema into it; the Glue jobs and Athena
  *read* schema from it. They never talk directly — the catalog sits between them.

The Glue jobs read by catalog table name, e.g.
`create_dynamic_frame.from_catalog(database=..., table_name="streams")` — so if the catalog's idea
of the schema is wrong, every job is affected. That is why managing the schema correctly matters so
much.

---

## 3. How the Schema Gets Into the Catalog — the Crawler

Nobody writes these schemas by hand. The **Glue Crawler** discovers them automatically. The raw
crawler ([main.tf:277](../terraform/main.tf#L277)) scans the three raw S3 prefixes, *infers* each
file's columns and types by sampling the data, and registers `songs`, `streams`, and `users` as
tables in `music_db`:

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
  ...
}
```

Because the crawler *infers* the schema rather than having it hard-coded, the catalog stays in sync
with the real files automatically — which is exactly what makes the next part, schema *changes*,
manageable.

---

## 4. What Happens When a Source File Changes Its Columns

Source data drifts over time. An upstream team might add a `device_type` column to stream files,
rename `listen_time`, or change a column's type. The question every pipeline must answer is: *what
happens to the catalog when the incoming files no longer match the registered schema?*

The answer is governed by the crawler's **`schema_change_policy`**, which has two settings:

### `update_behavior = "UPDATE_IN_DATABASE"` — adapt the catalog to the new shape

When the crawler next runs and detects that the files' schema has changed, `UPDATE_IN_DATABASE` tells
it to **update the existing table definition in place** to reflect the new schema — for example,
adding the newly-appeared `device_type` column to the `streams` table. The table keeps its name,
location, and history; only its column definitions are refreshed.

The practical effects:

- **New columns are picked up automatically.** If upstream adds a column, the catalog learns about it
  on the next crawl with no manual intervention and no code change.
- **The table is not torn down and recreated.** Updating *in place* means anything referencing the
  table (the jobs, Athena queries, partition metadata) continues to point at the same table — it just
  sees the refreshed schema. There is no window where the table disappears.
- **Existing partitions are preserved.** The crawler config also sets
  `Partitions = { AddOrUpdateBehavior = "InheritFromTable" }`, so partitions inherit the table's
  updated schema rather than being orphaned.

The alternatives illustrate why this matters. If the policy instead *replaced* the table on every
change, you would lose the table's identity, its partition registrations, and any manual tweaks each
time the schema drifted — and downstream references could break mid-run. `UPDATE_IN_DATABASE` is the
gentle, additive option: evolve the catalog to match reality without disrupting the consumers.

### `delete_behavior = "LOG"` — never silently drop data

The second setting controls what happens when files or columns *disappear*. `delete_behavior = "LOG"`
tells the crawler to **only log** that something is gone, rather than deleting the table or partition
from the catalog. This is a deliberately conservative, data-protective choice:

- If a prefix is temporarily empty (e.g. the archive job moved files out), the crawler does **not**
  delete the table — it just notes it. The table definition survives, so the next run still works.
- It prevents the crawler from destructively removing catalog entries based on a transient empty
  state. The alternative (`DELETE_FROM_DATABASE`) would remove tables/partitions when their files
  vanish — risky in a pipeline where files are *intentionally* moved out after processing.

Together, `UPDATE_IN_DATABASE` + `LOG` give the catalog a safe evolution policy: **add and update
schema as data changes, but never destroy catalog entries automatically.**

---

## 5. The Stale-Schema Edge Case This Pipeline Defends Against

There is one important interaction between schema management and the rest of the pipeline that this
project handles explicitly. Because the archive job moves processed files *out* of the `streams/`
prefix after each run, the crawler can sometimes run over an **empty** `streams/` folder. When it
does, it registers the `streams` table with **zero columns** — a technically-valid but useless
schema.

This is exactly the kind of stale-metadata problem schema management has to anticipate. The pipeline
guards against it in two places (also covered in [Data_Validation.md](Data_Validation.md)):

- **Step Functions `CheckStreamsExist`** queries S3 directly for files under `streams/` and exits
  cleanly if there are none — so the jobs never run against a stale zero-column table in the first
  place.
- **The transform job's `check_streams_have_data`** guard exits cleanly if the loaded DataFrame has
  no columns, rather than crashing with a confusing "missing required columns" error.

The lesson: the catalog reflects *whatever the crawler last saw*, which is not always *what you
intend to process*. Robust pipelines treat the catalog as a cache of metadata that can be stale, and
verify against the source of truth (S3) when it matters.

---

## 6. The Curated Crawler — Schema Management for the Output Side

Schema management is not only about inputs. After the KPI job writes Gold Parquet, the
**curated crawler** ([main.tf:322](../terraform/main.tf#L322)) crawls `gold/` and registers those
datasets (and their new partitions) in the same `music_db`, with the same `UPDATE_IN_DATABASE` / `LOG`
policy. This keeps the catalog's view of the Gold tables current so **Amazon Athena** can run SQL
over the latest KPIs immediately. As the Step Functions definition notes, this step is non-fatal — if
it fails, Athena just misses the newest partition until the next run, but the pipeline still succeeds.

---

## 7. Summary

| Concept | How this pipeline handles it |
|---|---|
| **Schema** | The column names/types describing each dataset's shape |
| **Where schema lives** | The Glue Data Catalog database `music_db` — metadata only, separate from S3 data |
| **How schema is discovered** | The crawler infers it from the files and registers tables automatically |
| **When source columns change** | `UPDATE_IN_DATABASE` refreshes the table in place — new columns picked up, table identity and partitions preserved |
| **When files/columns disappear** | `delete_behavior = LOG` only logs it — never auto-deletes catalog entries |
| **Stale zero-column schema** | Guarded by Step Functions `CheckStreamsExist` and the transform job's column check |
| **Output schema** | The curated crawler keeps `gold/` tables current for Athena (non-fatal step) |

The Glue Data Catalog is the single place that knows the shape of every dataset, and the crawler's
`UPDATE_IN_DATABASE` + `LOG` policy lets that knowledge **evolve safely** as source files change —
adapting to new columns automatically while never destructively dropping a table just because its
files were moved or temporarily empty.
