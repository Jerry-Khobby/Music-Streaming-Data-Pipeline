# Data Validation in Pipelines — Failing Fast Before Transformation

## What This Document Covers

This document explains why **validation is the very first step** of this pipeline, what counts as a
*valid* dataset here, what happens when validation fails, and why failing early **saves money**. It
is written for a data engineer new to building production pipelines, so it explains the reasoning,
not just the code. Everything maps to [glue_jobs/validation_job.py](../glue_jobs/validation_job.py)
and the guards in [terraform/step_functions.tf](../terraform/step_functions.tf) and
[glue_jobs/etl_transform_job.py](../glue_jobs/etl_transform_job.py).

---

## 1. Why Validate First — The "Garbage In, Garbage Out" Problem

A data pipeline is a chain: each step consumes the previous step's output. If bad data enters at the
top, every downstream step faithfully processes the garbage and produces garbage — but now you have
*paid* for all that processing, and the bad results may have already been served to users before
anyone notices.

The principle is **fail fast**: detect a problem at the earliest possible moment, before any
expensive or irreversible work happens. In this pipeline that means a dedicated **validation job
runs before any transformation, aggregation, or loading.** It is a gate: only data that passes is
allowed through to the costly Spark stages.

Concretely, the order is ([step_functions.tf:14](../terraform/step_functions.tf#L14)):

```
... crawler ready ...
→ ValidateData    (validation_job — the gate)
→ TransformData   (Bronze → Silver — expensive Spark join)
→ AggregateKPIs   (Silver → Gold — expensive Spark aggregation)
→ LoadDynamoDB    (writes to the serving database)
→ ArchiveFiles
```

Validation sits in front of the three expensive, data-mutating steps. If the input is malformed, the
pipeline stops at the gate and never spends money on the rest.

---

## 2. What Counts as "Valid" in This Pipeline

The validation job checks the three source datasets — `streams`, `songs`, `users` — against three
concrete rules. A dataset is valid only if it passes all three.

### Rule 1 — The table must exist in the catalog

The job reads each dataset through the Glue Data Catalog (`loadTable`,
[validation_job.py:40](../glue_jobs/validation_job.py#L40)). If the crawler has not yet registered a
table, the load raises a custom `TableNotFound`. This is treated as a *transient* condition (the
crawler may still be finishing), so it is **retried with exponential backoff** —
10s, 20s, 40s, up to 3 attempts ([validation_job.py:79](../glue_jobs/validation_job.py#L79)):

```python
for attempt in range(max_retries):
    try:
        df = loadTable(...)
        checkNonEmpty(df, tableName)
        checkMissingColumns(df, tableName)
        return
    except TableNotFound:
        wait_time = retry_delay * (2 ** attempt)   # 10, 20, 40
        time.sleep(wait_time)
```

If the table still isn't there after 3 tries, that's a real failure (the crawler likely failed), and
the job raises a clear `ValueError` pointing the operator at the crawler logs.

### Rule 2 — The dataset must not be empty

`checkNonEmpty` ([validation_job.py:67](../glue_jobs/validation_job.py#L67)) fails the pipeline if a
table has zero rows — *with one deliberate exception*:

```python
def checkNonEmpty(df, tableName):
    if df.rdd.isEmpty():
        if tableName == "streams":
            raise NoNewStreams()        # not an error — just nothing new to do
        raise ValueError(f"[{tableName}] Dataset is empty — pipeline cannot proceed.")
```

An empty `songs` or `users` table is a genuine error — you cannot enrich or process without the
reference data. But an empty `streams` table is **normal**: it simply means no new files arrived. So
it raises `NoNewStreams`, which the pipeline treats as a **clean exit**, not a failure (see Section
4). This distinction matters — it prevents false alarms when there's simply nothing to process.

### Rule 3 — The required columns must be present

`checkMissingColumns` ([validation_job.py:55](../glue_jobs/validation_job.py#L55)) verifies each
dataset contains the exact columns the downstream jobs depend on. The contract is declared up front
([validation_job.py:12](../glue_jobs/validation_job.py#L12)):

```python
REQUIRED_COLUMNS = {
    "streams": {"user_id", "track_id", "listen_time"},
    "songs":   {"track_id", "track_name", "track_genre", "duration_ms"},
    "users":   {"user_id", "user_name", "user_country"},
}
```

If any required column is missing, it raises a `ValueError` naming exactly which columns are absent.
This catches a renamed or dropped column at the gate — before the Spark join would crash deep inside
the transform job with a far more confusing error.

So a **valid record/dataset** in this pipeline is one whose table exists in the catalog, is non-empty
(or is an empty `streams` table, handled as "nothing to do"), and contains every required column.

---

## 3. A Second Line of Defense — Checks Beyond the Validation Job

Validation is not *only* the validation job. The pipeline layers two more guards around the same
concern, because catalog metadata can be stale:

- **`CheckStreamsExist` in Step Functions** ([step_functions.tf:286](../terraform/step_functions.tf#L286)) —
  after the crawler runs, the state machine queries S3 *directly* for objects under `streams/`. If
  there are none, it routes to `NoStreamsToProcess` (a `Succeed` state) and exits before spending any
  Glue compute. This is the authoritative check, because S3 is the source of truth, not the catalog.
- **`check_streams_have_data` in the transform job** ([etl_transform_job.py:36](../glue_jobs/etl_transform_job.py#L36)) —
  guards against a *stale zero-column* catalog entry (which happens when the crawler ran over an empty
  prefix). If the streams DataFrame has no columns, it exits cleanly rather than crashing.

Together these mean the pipeline validates both the **schema** (validation job) and the **actual
presence of data** (Step Functions S3 check), defending against both bad data and stale metadata.

---

## 4. What Happens When Validation Fails

There are two distinct outcomes, and the difference is the whole point of careful validation:

### Outcome A — A real failure (stops the pipeline)

A missing column, an empty `songs`/`users` table, or a table that never appears raises a
`ValueError`, which is **not caught** — it propagates and fails the Glue job
([validation_job.py:148](../glue_jobs/validation_job.py#L148)):

```python
except ValueError as error:
    logger.error(f"❌ Validation failed — aborting pipeline: {error}")
    raise   # let it fail the job
```

Because the job fails, the Step Functions `Catch` on `ValidateData` fires, routes to `NotifyFailure`
(which publishes a formatted SNS alert to Slack/email), and ends at `PipelineFailed` (see
[Step_Functions.md](Step_Functions.md)). **None of the expensive downstream steps run.** The operator
gets a precise message — *which* table, *which* missing columns — and no bad data reaches Silver,
Gold, or DynamoDB.

### Outcome B — A clean "nothing to do" exit

If `streams` is empty (`NoNewStreams`), the job logs it, commits, and exits with success
([validation_job.py:143](../glue_jobs/validation_job.py#L143)):

```python
except NoNewStreams:
    logger.info("[streams] No new stream files — exiting cleanly.")
    job.commit()
    sys.exit(0)
```

This is a *success*, not a failure — so no alarm fires and no false alert is sent. Distinguishing "no
data" from "bad data" is exactly what keeps the monitoring trustworthy.

---

## 5. Why Failing Early Saves Money

This is the economic argument for putting validation first, and it is concrete in this project:

1. **Spark compute is the expensive part.** The transform and aggregation jobs spin up Glue Spark
   workers (`G.1X`, 2 workers each) to perform a distributed join and aggregation. That compute is
   billed for the duration of the job. The validation job is small and fast (a 10-minute timeout vs
   30 for the others) — checking schema and row counts is cheap.
2. **Failing at the gate avoids paying for doomed work.** If a column is missing, a validation-first
   design spends a few cheap seconds detecting it. A validation-*last* (or validation-never) design
   would spin up the full Spark cluster, run the join, and only then crash — paying for minutes of
   distributed compute to produce nothing.
3. **It avoids cascading waste.** Without the gate, bad data could pass through transform *and*
   aggregation *and* the DynamoDB load before failing — three expensive stages billed, plus
   potentially corrupt data written to the serving database that then has to be cleaned up.
4. **It avoids the most expensive failure of all: serving wrong data.** Catching a problem before
   DynamoDB means users never see incorrect KPIs. Fixing data after it has been served — and
   rebuilding trust — costs far more than any compute bill.
5. **The S3 existence check avoids empty runs entirely.** `CheckStreamsExist` ends the run before any
   Glue job starts when there are no new files, so the pipeline spends *zero* compute on a no-op
   trigger.

In short: validation is cheap, the work it guards is expensive, and the consequences of skipping it
(wasted compute, corrupt serving data, eroded trust) are far costlier than the gate itself.

---

## 6. Summary

| Aspect | How this pipeline handles it |
|---|---|
| **When validation runs** | First — before transform, aggregate, or load |
| **What is validated** | Table exists (catalog), non-empty, has all required columns |
| **The required-column contract** | `REQUIRED_COLUMNS` for `streams`, `songs`, `users` |
| **Transient catalog gaps** | `TableNotFound` retried with exponential backoff (10/20/40s) |
| **Empty `streams` handling** | `NoNewStreams` → clean success exit, no alarm |
| **Bad data handling** | `ValueError` → job fails → SNS alert → pipeline stops; no downstream work |
| **Extra guards** | Step Functions `CheckStreamsExist` (S3 truth) + transform-job stale-schema guard |
| **Why fail early** | Validation is cheap; Spark compute and serving wrong data are expensive |

Validation is the inexpensive gate that protects the expensive pipeline behind it. By checking
schema and presence first — and cleanly distinguishing "no new data" from "broken data" — the
pipeline avoids wasting compute on doomed runs, never writes garbage to the serving layer, and only
ever alerts a human when something is genuinely wrong.
