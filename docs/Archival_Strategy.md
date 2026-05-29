# Archival Strategy — Moving Processed Files Out of the Pipeline

## What This Document Covers

This document explains the archival strategy of the music streaming pipeline in detail: what
archival means in data engineering and why it matters, why processed files **must** be physically
moved out of the ingestion path, how the project uses an S3 Glacier lifecycle rule to control
long-term storage cost, and — most importantly — how archival is the mechanism that prevents the
same stream events from being processed twice. Every claim below maps to concrete code in
[glue_jobs/archive_job.py](../glue_jobs/archive_job.py) and infrastructure in
[terraform/main.tf](../terraform/main.tf).

---

## 1. What "Archival" Means in Data Engineering

In data engineering, **archival** is the deliberate act of taking data that has already been
processed and relocating it to a separate, cheaper, longer-term store — *out of the path that
active processing reads from*. Archival is not the same as backup and not the same as deletion:

- **Backup** keeps a *copy* of live data for disaster recovery; the original stays in place.
- **Deletion** removes data permanently with no recovery.
- **Archival** *moves* the data: it disappears from the active working set but is preserved
  intact somewhere it can be retrieved later if needed (audit, replay, compliance).

The defining property of an archive is that it answers the question *"what did we already
process, and can we prove it?"* without that historical data interfering with *"what is new and
still needs processing?"*. In this project, archival is what cleanly separates **new stream files
that still need to be ingested** from **stream files that have already been turned into Silver and
Gold data**.

---

## 2. Why Files Must Be Moved After Processing

This pipeline is **incremental and event-driven**. New `streams/*.csv` files land in the raw
(Bronze) bucket, a Glue crawler catalogs whatever is currently in the `streams/` prefix, and the
ETL job reads that catalog table and transforms *everything it finds*.

That design creates a hard requirement: **the `streams/` prefix must contain only files that have
not yet been processed.** If processed files were left in place, the next pipeline run's crawler
would re-catalog them, and the ETL job would read and re-transform the same events again. The
consequences would be:

- **Inflated KPIs.** A stream counted today would be counted again tomorrow, and again the day
  after, doubling and tripling `listen_count`, `unique_listeners`, and listen-time totals.
- **Ever-growing scan cost.** Every run would re-read the entire history of stream files, so
  processing time and cost would grow linearly with the age of the pipeline instead of with the
  volume of *new* data.
- **A stale, bloated catalog.** The crawler would keep re-scanning files that contribute nothing
  new.

Moving processed files out of `streams/` is therefore not housekeeping — it is the core mechanism
that keeps the pipeline incremental and its numbers correct. The archive job
([archive_job.py](../glue_jobs/archive_job.py)) is the **last step** of the pipeline, running only
after validation, transform, KPI aggregation, and the DynamoDB load have all succeeded. By the
time a file is archived, every byte of value has already been extracted from it.

---

## 3. How Archival Prevents Duplicate Processing

The archive job implements a deliberate **copy-then-delete** sequence, and the *ordering* is the
entire safety argument. From `archive_processed_streams` ([archive_job.py:53](../glue_jobs/archive_job.py#L53)):

```python
def archive_processed_streams(s3_client, raw_bucket, archive_bucket):
    keys = list_stream_objects(s3_client, raw_bucket)
    if not keys:
        logger.info("No stream files found to archive.")
        return

    # Copy all files first; only delete after all copies succeed.
    copy_objects(s3_client, raw_bucket, archive_bucket, keys)
    bulk_delete_objects(s3_client, raw_bucket, keys)
```

### Step 1 — Discover what to archive

`list_stream_objects` ([archive_job.py:13](../glue_jobs/archive_job.py#L13)) paginates over the
raw bucket's `streams/` prefix and returns every real object key, skipping the zero-byte folder
placeholder (`if not obj["Key"].endswith("/")`). Pagination matters because a bucket may hold more
keys than a single `list_objects_v2` call returns.

### Step 2 — Copy every file to the archive bucket

`copy_objects` ([archive_job.py:24](../glue_jobs/archive_job.py#L24)) issues a server-side
`copy_object` for each key, preserving the same key in the archive bucket so the original path is
recoverable.

### Step 3 — Delete from raw only after all copies succeed

`bulk_delete_objects` ([archive_job.py:34](../glue_jobs/archive_job.py#L34)) removes the files
from the raw bucket in batches of up to 1000 keys (`S3_DELETE_LIMIT`, the hard limit of the S3
`delete_objects` API). It inspects the response for partial failures and raises if any key failed.

### Why copy-then-delete is safe under failure

The job is built so that **no failure mode can lose data or silently double-count**:

| Failure point | What state results | Why it is safe |
|---|---|---|
| Copy fails | Files remain in raw, nothing deleted | Next run simply reprocesses them — no data lost |
| Delete fails after copy | Files exist in **both** raw and archive | Files get reprocessed next run, but **Silver-layer deduplication removes the duplicates**, so KPIs stay correct |

This is exactly what the job's own comments and the `RuntimeError` message state
([archive_job.py:45](../glue_jobs/archive_job.py#L45)):

```python
raise RuntimeError(
    f"S3 bulk delete partially failed for {len(failed_keys)} key(s): {failed_keys}. "
    "These files remain in the raw bucket and will be reprocessed next run. "
    "Deduplication in the ETL job will prevent duplicate KPIs."
)
```

### The two-layer defense against duplicates

Duplicate prevention in this project is therefore **two independent layers**, and archival is the
first:

1. **Archival (primary):** moving files out of `streams/` means the next run normally never even
   sees an already-processed file.
2. **Deduplication (safety net):** even if archival half-fails and a file lingers, the ETL job
   deduplicates on the natural key `["user_id", "track_id", "listen_time"]` before writing Silver
   (see [Glue_Transformation_Code.md](Glue_Transformation_Code.md)). So a re-read file produces
   identical rows that collapse to one.

Together these guarantee **idempotency**: running the pipeline twice over the same data yields the
same result as running it once.

---

## 4. S3 Glacier Lifecycle Rules — Controlling Long-Term Cost

Archived files are kept for audit and potential replay, but they are read rarely, if ever. Keeping
them in S3 Standard storage indefinitely would waste money. The project handles this with an S3
**lifecycle rule** on the archive bucket that automatically transitions objects to the
**GLACIER** storage class after 90 days.

From [terraform/main.tf:124](../terraform/main.tf#L124):

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "move-to-glacier"
    status = "Enabled"

    filter {} # applies to all objects in the bucket

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}
```

### What this rule does, line by line

- **`filter {}`** — an empty filter means the rule applies to *every* object in the archive
  bucket, regardless of prefix or tag.
- **`transition { days = 90 ... }`** — 90 days after an object's creation, S3 automatically moves
  it from the default Standard tier to the **Glacier** tier. No code runs; S3 enforces this on a
  schedule.
- **`storage_class = "GLACIER"`** — Glacier is an archival tier priced for long-term retention.
  Storage is dramatically cheaper than Standard, in exchange for higher retrieval latency and a
  per-retrieval cost.

### Why 90 days, and why Glacier

The 90-day window is a deliberate trade-off. For the first 90 days, a file is recent enough that
an operator might need to replay it quickly (e.g. to re-run the pipeline after fixing a bug), so
it stays in Standard with instant, cheap access. After 90 days the probability of needing it drops
sharply, so the cost optimization (Glacier) outweighs the slower retrieval. The data is never
deleted — it remains fully durable and recoverable, just colder and cheaper.

### Why the rule lives only on the archive bucket

Note that **only the archive bucket has a lifecycle rule.** The raw (Bronze) bucket should never
have a Glacier rule because its files are short-lived — the archive job removes them within one
pipeline run. The curated (Silver/Gold) bucket holds active, frequently-read query data that must
stay instantly available. Putting any of that on Glacier would cripple the pipeline. Lifecycle
management is correctly scoped to the one place where data is genuinely cold: the archive.

---

## 5. The Archive Bucket Itself

From [terraform/main.tf:104](../terraform/main.tf#L104):

```hcl
resource "aws_s3_bucket" "archive" {
  bucket        = "${var.archive_bucket_name}-${var.environment}"
  force_destroy = true

  tags = {
    Layer = "archive"
    Usage = "Processed files moved here to prevent reprocessing"
  }
}
```

It is a separate physical bucket (not just a prefix) because its trust level and lifecycle are
fundamentally different from Bronze and curated data. Like every bucket in the project it is
encrypted at rest with AES256 (`aws_s3_bucket_server_side_encryption_configuration.archive`,
[main.tf:114](../terraform/main.tf#L114)). The bucket tag `Usage = "Processed files moved here to
prevent reprocessing"` documents the archival intent directly in the infrastructure.

---

## 6. Where Archival Sits in the Pipeline

The archive job is wired as the **final** Glue trigger in the workflow — it only runs after the
DynamoDB load completes successfully (`aws_glue_trigger.after_dynamodb_loader` in
[terraform/glue_jobs.tf](../terraform/glue_jobs.tf)):

```
validation → etl_transform → kpi_aggregation → dynamodb_loader → archive
```

This ordering is essential to the strategy. Because archival is last, a file is only ever removed
from `streams/` **after** all of its value has been:

1. Validated (schema and non-empty checks),
2. Transformed and merged into Silver,
3. Aggregated into Gold KPIs,
4. Loaded into DynamoDB.

If any earlier step fails, the pipeline stops and the files are **not** archived — they stay in
`streams/` and are safely reprocessed on the next run. Archival as the last step is what makes the
whole pipeline safe to retry.

---

## 7. Summary

| Concern | How this project handles it |
|---|---|
| What archival is | Moving processed `streams/` files to a dedicated archive bucket — preserve, don't delete |
| Why move files at all | Keeps the `streams/` prefix to *new* data only, so the pipeline stays incremental |
| Duplicate prevention (primary) | Files leave `streams/` after processing, so they aren't seen again |
| Duplicate prevention (safety net) | ETL deduplicates on `user_id + track_id + listen_time` if a file is ever re-read |
| Failure safety | Copy-then-delete: copy failure → reprocess; delete failure → dedup corrects it |
| Long-term cost | Lifecycle rule transitions archive objects to Glacier after 90 days |
| Ordering | Archive runs last, only after DynamoDB load succeeds — safe to retry the whole pipeline |

The strategy makes the pipeline **idempotent and incremental**: each stream file is processed
exactly once under normal operation, at most a bounded number of times under failure (with dedup
guaranteeing correctness regardless), and is preserved cheaply forever afterward.
