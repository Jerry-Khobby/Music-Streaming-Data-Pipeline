# Idempotency in Data Pipelines

## What This Document Covers

This document explains what **idempotency** means, why **every job in this pipeline must be safe to
re-run**, how DynamoDB's `PutItem` "upsert" behavior enforces idempotency at the serving layer, and
exactly **what would break** if the pipeline were not idempotent. It is written for a data engineer
new to the concept. Everything maps to the Glue jobs in [glue_jobs/](../glue_jobs/) and the design
choices around them.

---

## 1. What Idempotency Means

**Idempotency** means: *performing an operation many times produces the same result as performing it
once.* Running it twice, or five times, leaves the system in exactly the state it would be in after a
single run — no duplicates, no double-counting, no drift.

A everyday analogy: pressing the "floor 3" button in an elevator is idempotent — pressing it five
times still takes you to floor 3 once. Adding "+1" to a counter is **not** idempotent — doing it five
times gives +5.

In data pipelines, the operation that matters is **"process this batch of data."** An idempotent
pipeline can process the same input twice and the final tables look identical to processing it once.
A non-idempotent pipeline would, on the second run, double its counts, create duplicate rows, or
corrupt its aggregates.

---

## 2. Why Re-Runs Are Inevitable (So Idempotency Is Mandatory)

You might think "I'll just make sure each batch runs exactly once" — but in a distributed,
event-driven system, **re-runs are not an edge case, they are guaranteed to happen eventually**.
Here are the concrete ways this pipeline can see the same data twice:

- **Retries.** If a Glue job fails partway and is retried, the early part of its work may run again.
- **Dead-letter replays.** A message that landed in the DLQ and is later redriven re-enters the
  pipeline (see [SQS_and_Dead_Letter_Queue.md](SQS_and_Dead_Letter_Queue.md)).
- **Duplicate triggers.** SQS guarantees *at-least-once* delivery, so a single upload can occasionally
  produce two messages and two executions.
- **Partial archival failure.** If the archive job copies files but fails before deleting them from
  the raw bucket, the next run re-reads those same files (see
  [Archival_Strategy.md](Archival_Strategy.md)).
- **Manual reprocessing.** An engineer re-runs the pipeline for a date to recover from a bug.

Because re-runs *will* happen, the only safe design is one where re-running is **harmless**. That is
why idempotency is a hard requirement here, not a nice-to-have. Every stage is built so that seeing
the same data again converges on the same correct result.

---

## 3. How Each Stage Is Made Idempotent

This pipeline enforces idempotency with **two complementary techniques** applied at the right layers:
**deduplication on a natural key** (so identical records collapse) and **key-based overwrites** (so
re-writing produces the same item). Together they make the whole chain re-runnable.

### Stage 1 — Silver: deduplicate on the natural key

The transform job, when it merges new data into the existing Silver layer, deduplicates on the
columns that uniquely identify a stream event ([etl_transform_job.py:14](../glue_jobs/etl_transform_job.py#L14)
and [:83](../glue_jobs/etl_transform_job.py#L83)):

```python
STREAM_DEDUP_KEY = ["user_id", "track_id", "listen_time"]
...
deduped_df = combined_df.dropDuplicates(STREAM_DEDUP_KEY)
```

If the same stream file is processed twice, the duplicate rows have identical
`(user_id, track_id, listen_time)` values, so `dropDuplicates` collapses them back to one. The Silver
layer therefore holds each real event exactly once **regardless of how many times the source file was
read** — the foundation that makes everything downstream idempotent too.

It also uses **dynamic partition overwrite** so reprocessing a date *replaces* that date's partition
rather than appending to it ([etl_transform_job.py:124](../glue_jobs/etl_transform_job.py#L124)) —
another form of "re-running produces the same state, not more state."

### Stage 2 — Gold: aggregates are recomputed, not accumulated

The KPI job computes each metric with `groupBy` aggregations over the (already-deduplicated) Silver
data and writes with `mode("overwrite")`. Aggregations like `count(*)` and `sum(...)` are computed
fresh from the current Silver state each run — they don't *add to* a previous total, they *recompute*
it. So re-running the KPI job over the same Silver data yields identical Gold numbers.

### Stage 3 — DynamoDB: key-based upserts (the main mechanism)

This is the most important enforcement point and the one called out in the topic. The loader writes
each Gold row to DynamoDB with `put_item`, and crucially **deduplicates each dataset on exactly its
table's primary key first** ([dynamodb_loader.py:92](../glue_jobs/dynamodb_loader.py#L92)):

```python
genreKpisDF = loadParquet(...).dropDuplicates(["genre_date"])
topSongsDF  = loadParquet(...).dropDuplicates(["genre_date", "rank"])
topGenresDF = loadParquet(...).dropDuplicates(["date", "rank"])
```

The next section explains *why* `put_item` on a key makes this idempotent.

---

## 4. How DynamoDB `PutItem` Enforces Idempotency (Upserts)

DynamoDB's `PutItem` is an **upsert**: "update if the key exists, insert if it doesn't." The rule is
simple and powerful:

> **In DynamoDB, every item is uniquely identified by its primary key. Writing an item whose key
> already exists completely overwrites the existing item.** There is no "duplicate row" — the key *is*
> the identity.

This is fundamentally different from a relational `INSERT`, which would happily add a second row with
the same logical key and create a duplicate. In DynamoDB:

- `put_item` with `genre_date = "Afrobeats#2026-05-17"` the first time **inserts** the item.
- `put_item` with the *same* `genre_date` a second time **replaces** it with the new (identical) data.

So if the pipeline runs twice for the same day, the second run's writes land on the **same keys** as
the first run's and simply overwrite them with the same values. The table ends up exactly as it would
after one run — **the definition of idempotency.**

The loader's `dropDuplicates` on each table's primary key (`["genre_date"]`,
`["genre_date","rank"]`, `["date","rank"]`) makes this airtight by ensuring even *within a single
run* there's only one row per key to write. Because those dedup keys are **identical** to the DynamoDB
primary keys, the loader and the database agree on what "the same item" means.

This is precisely why a DLQ replay or a manual re-run is safe: replaying produces `put_item` calls on
keys that already hold the correct data, overwriting them with the same data — a no-op in effect.

---

## 5. What Breaks If a Job Is Not Idempotent

To see why all this matters, imagine removing each safeguard:

| If this were missing… | What would break |
|---|---|
| **Silver dedup** (`dropDuplicates` on the natural key) | Reprocessing a file would store each event twice in Silver. Every downstream count and sum would be inflated — `listen_count` doubled, `unique_listeners` possibly skewed, totals wrong. |
| **Overwrite semantics in Gold** | If Gold *appended* instead of overwriting, a re-run would stack a second day's worth of aggregates on top of the first, corrupting the KPI history. |
| **Key-based writes in DynamoDB** (if it were an append-style store) | A re-run would create duplicate KPI rows per genre-day, and the dashboard would show two conflicting "top song #1" entries or doubled metrics. |
| **Matching dedup/primary keys** | If the loader deduped on different columns than the table's key, multiple rows could map to one key (last-write-wins, silently losing data) or one logical record could split across keys. |

The unifying failure mode of a non-idempotent pipeline is **silent data corruption on retry**: nothing
errors, but the numbers quietly become wrong, and because the pipeline "succeeded," nobody notices
until a business user questions an impossible metric. Idempotency is what lets this pipeline retry,
replay, and reprocess freely **without ever risking that corruption**.

---

## 6. Summary

| Question | This pipeline's answer |
|---|---|
| **What is idempotency?** | Running an operation many times = running it once; no duplication or drift |
| **Why required here?** | Retries, DLQ replays, at-least-once delivery, partial archival, and manual re-runs all cause re-processing |
| **Silver idempotency** | `dropDuplicates(["user_id","track_id","listen_time"])` + dynamic partition overwrite |
| **Gold idempotency** | Aggregates recomputed from Silver and written with `overwrite`, not accumulated |
| **DynamoDB idempotency** | `put_item` is a key-based upsert; same key overwrites; loader dedups on the exact primary key |
| **What breaks without it** | Silent double-counting and duplicate KPI rows on every retry — corruption with no error |

Idempotency is the property that makes this pipeline **safe to re-run by design**. Deduplication on
natural keys collapses repeated records, recomputed aggregates never accumulate, and DynamoDB's
key-based `PutItem` upserts guarantee that re-writing a KPI just overwrites it with the same value —
so a retry, a replay, or a manual reprocess always lands on the same correct result.
