# "Real-Time" vs Batch — Why This Pipeline Is Event-Driven Batch (and Why That's Correct)

## What This Document Covers

The project brief is titled around a **"real-time data pipeline,"** yet this implementation
processes data in **batches** using AWS Glue jobs orchestrated by Step Functions. At first glance
that looks like a contradiction. This document explains why it is **not** — why event-driven batch
is the *correct* reading of the brief, why true stream processing would actually have been the
*wrong* choice here, and how to defend that decision confidently.

Use this as a direct answer to the question: *"The brief says real-time — why did you build batch?"*

---

## 1. The Apparent Contradiction

The headline of the brief says **real-time**. But the body of the same brief describes something
that is unambiguously **batch** in nature. Both statements come from the requirements:

> *"The incoming streaming data is stored in Amazon S3 in **batch files** that arrive at irregular
> intervals."*

> *"Unlike batch processing, this pipeline must handle data arriving at **unpredictable intervals**,
> ensuring **timely** computation of key metrics."*

> Required tooling: **Step Functions for orchestration** and **AWS Glue** (PySpark + Python Shell).

So the brief simultaneously says "real-time," "batch files," "unpredictable intervals," and mandates
**batch-oriented tools**. The resolution is that the word *"real-time"* in this brief does **not**
mean *stream processing*. It means **low-latency and event-driven** — react to data the moment it
arrives, rather than waiting for a fixed schedule. Those are very different things, and conflating
them is the single most common misreading of this project.

---

## 2. The Three Meanings of "Real-Time"

"Real-time" is an overloaded term. In data engineering it usually collapses three distinct
architectures into one word. Knowing the difference is the heart of the defence.

| Architecture | What triggers work | Typical tools | Latency | Unit of data |
|---|---|---|---|---|
| **True stream processing** | Each individual event, continuously | Kinesis, Kafka, Flink, Spark Structured Streaming | milliseconds–seconds | one event/record |
| **Event-driven batch** ← *this project* | The arrival of a **file** | S3 events → EventBridge → SQS → Step Functions → Glue | seconds–minutes | one file (a batch of records) |
| **Scheduled batch** | A clock / cron timer | Cron, scheduled EventBridge, Airflow timers | minutes–hours | whatever accumulated since last run |

The brief is asking us to move from the **bottom row** (scheduled batch) **up to the middle row**
(event-driven batch). It is **not** asking us to jump to the **top row** (true streaming) — and the
data format plus the mandated tools make that jump impossible anyway.

---

> **A note on the Firehose ingestion layer.** This project uses **Kinesis Data Firehose** to ingest
> events (see [Streaming_Ingestion_Firehose.md](Streaming_Ingestion_Firehose.md)). That is **stream
> *ingestion / transport*** — how records *travel* and get *batched into files* — which is a different
> thing from **stream *processing***. The argument below is against stream *processing* (computing
> results on every event in motion). It is **not** against streaming *ingestion*. "Streaming ingest →
> batch process" is a deliberate, standard pattern; §3a explains why the two coexist without
> contradiction.

---

## 3. Why True Stream Processing Would Have Been the *Wrong* Choice

It is worth being explicit: choosing Kinesis/Kafka/Flink here would not have been "more advanced" —
it would have been **incorrect engineering** that fights the brief on three fronts.

### a) The data is files, not a stream
The source is **CSV files landing in an S3 prefix**. A stream processor consumes an unbounded,
continuous feed of individual records from a streaming source (a Kafka topic, a Kinesis shard). We
do not have that. We have discrete objects appearing at random times. Forcing a stream processor
onto a file-based source means building an artificial adapter (e.g. tailing S3 into Kinesis) that
adds cost and failure modes for **zero benefit** — the data was never a stream to begin with.

### b) The mandated tools are batch tools
The brief explicitly requires **AWS Glue (PySpark and Python Shell jobs)** and **Step Functions**.
Glue batch jobs and Step Functions are designed to process **bounded datasets** — a file, a
partition, a table snapshot. They are not stream processors. You **cannot** implement Kinesis-style
per-event processing inside a Glue batch job. Honoring the required toolset *necessitates* a batch
design.

### c) The KPIs are daily aggregates, not per-event reactions
Every required metric is a **daily roll-up**: *daily* listen count, *daily* unique listeners,
*top 3 songs per genre per day*, *top 5 genres per day*. These are inherently **batch
aggregations** over a day's worth of data. There is no business value in recomputing "top 5 genres
of the day" on every single play event in true real time — the metric is defined per day, so it is
naturally computed per batch. The KPI definitions themselves point to batch.

> **One-line version:** True streaming solves a problem this project does not have (an unbounded
> per-event feed needing sub-second reaction) while breaking the constraints it *does* have (file
> sources, Glue/Step Functions, daily aggregates).

---

## 4. Why Plain Scheduled Batch Would *Also* Have Been Wrong

If true streaming is over-engineering, the opposite mistake is the naive version: *"just run a Glue
job on a cron every hour."* The brief explicitly rules this out:

> *"...data arriving at **unpredictable intervals**, ensuring **timely** computation..."*
> *"**Unlike batch processing**..."*

A fixed schedule fails this requirement because:

- **Latency is bounded by the interval.** A file arriving at 2:05 AM waits until the next scheduled
  run — that is not "timely."
- **It runs for nothing.** When no file arrives in an interval, the scheduled job still wakes up and
  bills compute for zero work.
- **It cannot adapt to "unpredictable."** You would have to *guess* the arrival cadence and tune the
  cron to it — which is exactly the prediction the brief says to avoid.

So the brief brackets the answer from both sides: **not** continuous streaming (the data and tools
forbid it), and **not** scheduled batch (the latency and "unpredictable" wording forbid it). What
sits precisely in the middle is **event-driven batch** — and that is what this project implements.

---

## 5. What This Project Actually Built — Event-Driven Batch

The pipeline reacts to **the event of a file arriving**, and then processes that file as a batch.
The trigger chain is fully event-driven, with no schedule and no manual start:

```
 S3 ObjectCreated (streams/)
   → EventBridge rule        (filters: only streams/ uploads)
     → SQS main queue          (durably buffers; DLQ catches poison messages)
       → EventBridge Pipe        (starts one execution per file)
         → Step Functions          (validate → transform → aggregate → load → archive)
           → Glue batch jobs           (PySpark + Python Shell process the batch)
```

This gives the **low latency and responsiveness** the brief calls "real-time" while using the
**batch tooling** the brief mandates:

- **Triggered by data, not a clock** — uploading a file is the only thing needed to run the pipeline;
  processing begins seconds after arrival.
- **No empty runs** — a `CheckStreamsExist` state exits cleanly when there is nothing to process, so
  no Glue compute is billed for nothing.
- **Bursts are absorbed** — SQS buffers simultaneous uploads; a concurrency guard in the state
  machine serializes overlapping runs instead of dropping or colliding.
- **Nothing is lost** — events sit durably in SQS, and failures land in a dead-letter queue for
  inspection.

The full hop-by-hop walkthrough of this chain lives in
[Event_Driven_Architecture.md](Event_Driven_Architecture.md).

---

## 6. Mapping Each Brief Phrase to a Design Decision

This table is the quickest way to defend the architecture line-by-line against the wording of the
brief.

| Phrase in the brief | What it actually requires | How this design satisfies it |
|---|---|---|
| *"real-time data pipeline"* | Low latency, react on arrival | Event-driven trigger; runs seconds after a file lands |
| *"batch files...in Amazon S3"* | The source is files, not a stream | S3-prefix-triggered batch processing |
| *"arrive at unpredictable intervals"* | No fixed schedule | Pure event trigger — no cron anywhere |
| *"timely computation"* | Minimise delay from arrival → result | FIRE_AND_FORGET pipe starts the run immediately |
| *"Unlike batch processing"* | Don't use a naive scheduled batch | Event-driven, not time-driven |
| *"Step Functions...AWS Glue (PySpark + Python Shell)"* | Use batch orchestration + batch jobs | Exactly these services, in this order |
| *"daily KPIs...per day"* | Aggregate over a day's data | Batch aggregation keyed by `stream_date` |

Every phrase resolves cleanly to event-driven batch. None of them resolves to true streaming.

---

## 7. The Defence — Say It Like This

When challenged on *"why isn't this real-time streaming?"*, the strongest answer is short, confident,
and shows you understood the trade-off rather than missing it:

> *"The source data arrives as discrete batch files in S3 at irregular intervals, and the brief
> mandates a batch toolset — AWS Glue and Step Functions. 'Real-time' is therefore satisfied through
> an **event-driven** trigger: each file launches the pipeline the moment it lands, via
> S3 → EventBridge → SQS → Step Functions, achieving near-real-time latency with no polling schedule.
> I deliberately did **not** use Kinesis or Spark Structured Streaming, because there is no
> continuous event stream to consume — the data is files, the required tools are batch tools, and the
> KPIs are daily aggregates. True streaming would have added cost and complexity while violating the
> constraints of the brief. I also did not use a cron schedule, because the brief calls for timely
> processing of unpredictable arrivals, which a fixed interval cannot provide. Event-driven batch is
> the precise middle ground the brief describes."*

### Three sentences if you only have a moment
1. The data is **files**, not a continuous stream, and the required tools (Glue, Step Functions) are
   **batch** tools — so true streaming was never on the table.
2. "Real-time" here means **event-driven**: the pipeline fires the instant a file lands, not on a
   schedule — that's the low latency the brief wants.
3. The KPIs are **daily aggregates**, which are inherently batch computations — so processing per
   file/per day is correct by design, not a compromise.

---

## 8. Summary

| Question | Answer |
|---|---|
| Did the brief say "real-time"? | Yes — but it means **event-driven / low-latency**, not stream processing. |
| Is this project batch? | Yes — **event-driven batch**: it processes a file-batch the moment the file arrives. |
| Was batch the right choice? | **Yes.** The data is files, the mandated tools are batch tools, and the KPIs are daily aggregates. |
| Would true streaming have been better? | **No** — it would fight the data format, the required tooling, and the metric definitions. |
| Would scheduled batch have been acceptable? | **No** — it cannot deliver timely processing of unpredictable arrivals. |
| What is the correct sweet spot? | **Event-driven batch** — exactly what this pipeline implements. |

This pipeline **reacts to data instead of predicting it**, using the batch tools the brief requires,
to compute the daily aggregates the brief defines — which is precisely what "real-time" means in this
context.
