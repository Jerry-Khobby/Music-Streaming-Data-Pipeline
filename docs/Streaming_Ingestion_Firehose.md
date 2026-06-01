# Streaming Ingestion — Kinesis Data Firehose (and Why Not Kinesis Data Streams)

## What This Document Covers

The original pipeline started with a **manual CSV upload** to S3 — fine for a demo, but not how a
real music service feeds data. This document explains the **automated ingestion front end** that
replaces it:

```
 producer script  →  Kinesis Data Firehose (Direct PUT)  →  S3 raw/streams/  →  existing pipeline (unchanged)
```

It explains what each piece does, the buffer settings that make it handle both **bursty** and
**sparse** traffic, and — most importantly — **why Firehose was chosen over Kinesis Data Streams**.
Code references map to [producer/stream_producer.py](../producer/stream_producer.py) and
[terraform/ingestion.tf](../terraform/ingestion.tf).

---

## 1. The Problem This Solves

The brief says stream data *"arrives at unpredictable intervals."* Manually uploading a file does
not model that, and it makes the pipeline impossible to demonstrate realistically. We need a source
that:

- emits events **continuously and automatically**, with no human in the loop;
- arrives at **irregular intervals** — sometimes a burst of activity, sometimes near silence;
- lands the data in S3 as **batch files** so the existing event-driven pipeline triggers unchanged.

The combination of *streaming ingestion* + *batched S3 delivery* is exactly what **Kinesis Data
Firehose** is built for.

---

## 2. The Guiding Principle — Choose Components by the Problem, Not the Traffic Shape

This is the single most important idea behind the design, and the strongest point to make when
defending it:

> **Choose components by the problem they solve, not by the shape of the traffic.**
> Variable traffic (bursty + sparse) is a **buffering and cost** problem → that is **Firehose**.
> Multi-consumer / replay / strict ordering is a **distribution** problem → that is **Kinesis Data
> Streams (KDS)**. This pipeline has the former, not the latter.

It is tempting to reason "the traffic is variable and spiky, therefore I need a heavyweight
streaming service like KDS." But variability is not what KDS is *for*. Firehose already absorbs
variability natively. KDS exists to *distribute* a stream to multiple independent readers and to
*retain* it for replay — neither of which this pipeline needs. Picking the tool by the problem it
solves (not by how the traffic happens to look) is what keeps the design simple and cheap.

---

## 3. Two Traffic Shapes, Both Handled by Firehose Alone

The two scenarios that motivated this design are exactly the two Firehose handles best.

### Bursty — "lots of data within ~4 minutes"

Firehose **buffers** incoming records and writes them to S3 only when a threshold trips (see §5). A
short burst is therefore **consolidated into one (or a few) batch files**, not scattered across
hundreds of tiny objects. This matters because the naive alternative — the producer writing one S3
object per event — would turn a burst into hundreds of pipeline triggers and the classic **small-files
problem**. Firehose's buffer is precisely the thing that protects against bursts. KDS adds nothing
here; Firehose absorbs the spike on its own (its Direct PUT quotas are far above demo volumes).

### Sparse — "data uploaded just twice a day"

Firehose flushes on a buffer **size** *or* an **interval**, whichever comes first. So even when only
a handful of records arrive at noon, the interval timer (set to **60 s** here, the AWS minimum)
forces a flush and the file lands promptly. And because Firehose Direct PUT bills **per GB ingested**, a quiet
day costs almost nothing. KDS would be the *wrong* choice here: it bills per **shard-hour** whether
data flows or not, so you would pay for mostly-idle shards on every quiet day.

| Traffic shape | What it really needs | How Firehose delivers it |
|---|---|---|
| **Bursty** (many in minutes) | Consolidate into batch files; absorb the spike | Buffer batches the burst into one file; auto-scales |
| **Sparse** (twice a day) | Flush promptly; cost ~nothing when idle | Interval timer forces a flush; pay-per-GB, no idle cost |

---

## 4. Why Firehose and Not Kinesis Data Streams — The Full Comparison

KDS is the right tool when you have at least one of the needs in the left column. This pipeline has
none of them:

| KDS is justified when you need… | Does this pipeline need it? |
|---|---|
| **Multiple independent consumers** of the same stream (e.g. a live dashboard *and* S3 archival at once) | ❌ One consumer: S3 → the batch pipeline |
| **Replay / re-read** of the stream (KDS retains 24h–365d) | ❌ Not needed; S3 is the durable record and the ETL dedups on reprocess |
| **Strict per-key ordering / sharding** | ❌ KPIs are daily aggregates — order-independent |
| **Throughput beyond Firehose Direct PUT limits** | ❌ Demo-scale, far below Firehose quotas |
| **Sub-second, per-event processing** | ❌ Metrics are daily roll-ups, computed in batch |

Zero of five. Adding KDS would mean provisioning shards, managing capacity, extra IAM, extra
Terraform, and a **standing per-shard cost** — all to solve problems we don't have. That is a direct
violation of **KISS** and **YAGNI**.

**It is also not a one-way door.** If a genuine second consumer or a replay requirement ever appears,
the producer swaps one API call (`firehose.put_record_batch` → `kinesis.put_record`) and a delivery
stream is pointed at the KDS stream. So there is no future-proofing argument for building KDS *now*.

---

## 5. The Firehose Configuration

Defined in [terraform/ingestion.tf](../terraform/ingestion.tf) as a **Direct PUT** delivery stream
(no `kinesis_source_configuration` block — producers call the Firehose API directly):

```hcl
resource "aws_kinesis_firehose_delivery_stream" "streams_ingestion" {
  name        = "${var.project_name}-streams-ingestion"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.raw.arn

    prefix              = "streams/"                  # lands where the pipeline already looks
    error_output_prefix = "streams-errors/!{firehose:error-output-type}/"

    buffering_size     = var.firehose_buffer_size_mb          # default 5 MB
    buffering_interval = var.firehose_buffer_interval_seconds # default 60 s (AWS minimum)
    compression_format = "UNCOMPRESSED"
    # ...cloudwatch_logging_options...
  }
}
```

### The buffer thresholds are the heart of it

Firehose writes a file when **either** threshold trips first:

- **`buffering_size` (default 5 MB)** — flush once this much data has accumulated. Caps how large a
  burst-file gets.
- **`buffering_interval` (default 60 s — the AWS minimum)** — flush at most this long after the first
  buffered record. This is what guarantees **sparse** data still lands promptly. Firehose **cannot**
  deliver to S3 faster than 60 s; that floor is a deliberate part of how the service batches.

Both are exposed as Terraform variables (`firehose_buffer_size_mb`,
`firehose_buffer_interval_seconds`) so you can tune the latency-vs-consolidation trade-off without
editing resources:

- **The default 60 s** is the AWS minimum → fastest delivery Firehose allows, best for demos.
- **Raise it** (toward 900 s) → fewer, larger files and fewer Glue runs, at the cost of latency, when
  latency stops mattering.

### IAM is least-privilege

The Firehose role ([ingestion.tf](../terraform/ingestion.tf)) can write **only** to the raw bucket
and log **only** to its own CloudWatch group — nothing else. This matches the project's per-service,
scoped-role pattern (see [iam-roles-and-policies.md](iam-roles-and-policies.md)).

---

## 6. The Producer Script

[producer/stream_producer.py](../producer/stream_producer.py) simulates the music app. It reads the
sample files in [data/streams/](../data/streams/) and sends them to Firehose **one file at a time**,
each as JSON records via `put_record_batch`, waiting a jittered interval between files. This models
the brief's *"batch files that arrive at irregular intervals"* directly: each source file becomes one
S3 batch object and one pipeline run.

```
 send streams1.csv  →  (wait 90–150 s)  →  send streams2.csv  →  (wait)  →  send streams3.csv
      │                                         │                              │
      ▼ Firehose flush                          ▼ Firehose flush               ▼ Firehose flush
 1 S3 object + 1 pipeline run             1 S3 object + 1 pipeline run    1 S3 object + 1 pipeline run
```

```bash
# Get the stream name from terraform output, then:
python producer/stream_producer.py --stream-name music-streaming-streams-ingestion
# Tune the gap between files (defaults: 90–150 s):
python producer/stream_producer.py --stream-name music-streaming-streams-ingestion --min-delay 75 --max-delay 120
```

**Why the wait between files must exceed 60 s.** Firehose flushes its buffer at most every 60 s
(§5). If the producer sent the next file sooner, both files would share one buffer window and **merge
into a single S3 object** — collapsing three arrivals into one. The default 90–150 s jittered wait
keeps each file landing as its own object while keeping arrival times irregular. (The script warns if
`--min-delay` is set at or below 60 s.)

It only needs `boto3` and credentials with `firehose:PutRecordBatch` on the stream. Failed records
in a batch are retried positionally before the script gives up — so a transient throttle doesn't
silently drop events.

---

## 7. Why JSON, Not CSV — A Decision That Matters

Firehose **concatenates** the records it receives into each S3 file; it does not add a CSV header per
file. If the producer sent raw CSV rows, the delivered files would be **headerless**, and the Glue
crawler would name the columns `col0`, `col1`, `col2` — breaking the validation job, which expects
`user_id`, `track_id`, `listen_time`.

Sending **newline-delimited JSON (JSONL)** avoids this entirely: each record carries its own field
names, so the crawler infers a clean, named schema. The producer appends `"\n"` to every JSON record
to produce valid JSONL files. (This is why the streams catalog table is JSON-classified, while the
static `songs`/`users` reference data remains CSV — different tables, different classifiers, both
correct.)

---

## 8. How It Plugs Into the Existing Pipeline — Zero Downstream Changes

Firehose simply does the **"a file landed in `streams/`"** step that a human used to do manually.
Everything after that is untouched:

```
producer → Firehose → S3 raw/streams/...   ◀── NEW (this document)
                          │  ObjectCreated
                          ▼
        EventBridge → SQS → Pipe → Step Functions → Glue jobs → DynamoDB   ◀── UNCHANGED
```

The EventBridge rule already filters on the `streams/` prefix, so Firehose's objects (which land at
`streams/YYYY/MM/DD/HH/...`) match and trigger the pipeline exactly as a manual upload did. See
[Event_Driven_Architecture.md](Event_Driven_Architecture.md) for the trigger chain.

### One detail to be aware of

Firehose appends a UTC date path (`streams/2024/06/25/17/...`) that is **not** Hive-style
(`key=value`), so the Glue crawler may register harmless extra `partition_0…partition_N` columns on
the `streams` table. This does **not** break anything: validation checks that required columns are a
*subset* of what exists (extra columns are ignored), and the downstream jobs select only the columns
they need. If a perfectly clean layout is ever wanted, enable Firehose **dynamic partitioning** with a
Hive-style custom prefix — but that is unnecessary for this pipeline.

---

## 9. Summary

| Question | Answer |
|---|---|
| What replaced manual upload? | An automated producer → **Kinesis Data Firehose (Direct PUT)** → S3 |
| Why Firehose? | It **buffers variable arrivals into batch files** cheaply — exactly the bursty+sparse problem |
| Why not Kinesis Data Streams? | KDS solves **distribution** (multi-consumer, replay, ordering) — needs this pipeline doesn't have |
| How are bursts handled? | The buffer **consolidates** them into one file and absorbs the spike |
| How is sparse data handled? | The **interval timer** forces a prompt flush; pay-per-GB means ~no idle cost |
| What changed downstream? | **Nothing** — Firehose lands files under `streams/`, and the existing pipeline triggers as before |
| Could we move to KDS later? | Yes, with a one-line producer change — so there's no reason to build it now |

The governing rule throughout: **choose components by the problem they solve, not by the shape of the
traffic.** The problem here is buffering irregular arrivals into batch files at low cost — and that is
Firehose, not Kinesis Data Streams.
