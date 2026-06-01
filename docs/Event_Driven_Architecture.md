# Event-Driven Architecture — How a File Upload Starts the Pipeline

## What This Document Covers

This document explains what makes this pipeline **event-driven** rather than **scheduled**, walks
through the exact trigger chain `S3 → EventBridge → SQS → EventBridge Pipes → Step Functions`
one hop at a time, and explains *why* this design matters specifically when data arrives
irregularly. It is written for a data engineer new to cloud event systems, so it defines each
concept before showing how this project wires it. Everything maps to
[terraform/messaging.tf](../terraform/messaging.tf) and
[terraform/step_functions.tf](../terraform/step_functions.tf).

---

## 1. Scheduled vs Event-Driven — Two Ways to Start Work

There are two fundamental ways to decide *when* a data pipeline runs.

### The scheduled (cron) approach

A scheduler fires the pipeline at fixed times — "run every night at 2 AM." This is simple, but it
has built-in problems when data does not arrive on a neat timetable:

- **Latency.** A file that arrives at 2:05 AM waits ~24 hours until the next 2 AM run.
- **Wasted runs.** If no new files arrived, the 2 AM run still spins up compute, scans for nothing,
  and costs money for zero work.
- **Missed or doubled data.** If files sometimes arrive in bursts and sometimes not at all, a fixed
  schedule either processes stale data or races against late arrivals.
- **Guesswork.** You must *predict* when data will be ready, rather than *react* to it actually
  being ready.

### The event-driven approach

Instead of asking "what time is it?", an event-driven pipeline asks "did something happen?" The
arrival of a file **is** the trigger. The moment a new stream file lands in S3, the pipeline starts
— no sooner, no later, and never for nothing.

This project is **event-driven**. Nobody schedules it and nobody runs it by hand. Uploading a CSV
to the `streams/` prefix is the only thing required to make the entire pipeline execute.

---

## 2. What "Event-Driven" Means Here

An **event** is a small notification that *something happened* — in this case, "an object was
created in S3." An event-driven architecture is a set of services that **produce**, **route**, and
**react to** these events, with each service decoupled from the others.

The key property is **decoupling**: the thing that produces the event (S3) knows nothing about the
thing that ultimately reacts to it (Step Functions). Between them sit routing and buffering
services that can be changed independently. You can swap, scale, or add consumers without touching
the producer.

This pipeline's trigger chain has four links between the file landing and the work:

```
 [producer → Kinesis Data Firehose]   (ingestion — lands a batch file in streams/)
   → S3 (ObjectCreated on streams/)
     → EventBridge rule          (detects & filters the event)
       → SQS main queue           (buffers it durably; DLQ catches failures)
         → EventBridge Pipe        (polls the queue, reshapes the message)
           → Step Functions        (StartExecution — the pipeline runs)
```

Each link does exactly one job. The next sections walk through them in order.

> **How files arrive.** A file lands in `streams/` either from a manual upload *or* from the
> automated **Kinesis Data Firehose** ingestion layer that batches producer events into S3 files.
> Either way, the trigger chain below is identical — it reacts to *a file appearing*, regardless of
> who put it there. See [Streaming_Ingestion_Firehose.md](Streaming_Ingestion_Firehose.md) for the
> ingestion front end.

---

## 3. The Trigger Chain, Hop by Hop

### Hop 0 — Firehose lands the file (ingestion)

Before the trigger chain begins, a file has to *arrive*. In production that is the job of the
**Kinesis Data Firehose** ingestion layer: a producer sends play events to a Firehose Direct PUT
delivery stream, Firehose buffers them, and writes a batch file into `streams/`. This is **stream
ingestion**, deliberately separate from the **batch processing** that follows — see
[Streaming_Ingestion_Firehose.md](Streaming_Ingestion_Firehose.md) for why Firehose (not Kinesis
Data Streams) is the right tool, and [Real_Time_vs_Batch_Justification.md](Real_Time_vs_Batch_Justification.md)
for why ingestion-streaming and batch-processing coexist by design. A manual upload produces the
exact same `streams/` object, so everything from Hop 1 onward is identical either way.

### Hop 1 — S3 emits an event

When a file is written to the raw bucket — by Firehose or by a manual upload — S3 can emit an
`Object Created` event. This project turns that on by enabling EventBridge notifications on the raw
bucket
(`aws_s3_bucket_notification.raw_eventbridge`, in [terraform/main.tf](../terraform/main.tf)):

```hcl
resource "aws_s3_bucket_notification" "raw_eventbridge" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
}
```

From this point on, **every** object created anywhere in the raw bucket is announced to EventBridge.
The *filtering* (we only care about `streams/`) happens at the next hop.

### Hop 2 — EventBridge rule detects and filters

EventBridge is a serverless **event bus**: services publish events to it, and **rules** match
events by pattern and route the matches to **targets**. The rule here
([messaging.tf:119](../terraform/messaging.tf#L119)) listens only for new files under `streams/`:

```hcl
resource "aws_cloudwatch_event_rule" "streams_uploaded" {
  name           = "${var.project_name}-streams-uploaded"
  event_bus_name = "default"
  event_pattern  = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.raw.id] }
      object = { key  = [{ prefix = "streams/" }] }   # <-- only streams/
    }
  })
}
```

This **content-based filtering** is why a `songs/` or `users/` reference-data upload does *not*
kick off a stream-processing run — only new stream events do. The rule's target is the SQS queue
([messaging.tf:142](../terraform/messaging.tf#L142)), and it is configured with a retry policy and a
dead-letter fallback so a delivery failure to the queue is itself handled.

### Hop 3 — SQS buffers the event (with a safety net)

SQS (Simple Queue Service) is a durable **message queue** — a buffer that holds events until
something is ready to process them. This project uses two queues
([messaging.tf:65](../terraform/messaging.tf#L65)):

```hcl
resource "aws_sqs_queue" "pipeline_events" {            # main queue
  visibility_timeout_seconds = 300                      # ≥ Step Functions startup time
  message_retention_seconds  = 86400                    # 1 day
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.pipeline_dlq.arn
    maxReceiveCount     = 3                              # try 3 times, then give up
  })
}

resource "aws_sqs_queue" "pipeline_dlq" {               # dead-letter queue
  message_retention_seconds = 1209600                   # 14 days — keep failures for inspection
}
```

Why put a queue here at all, instead of triggering Step Functions directly?

- **Buffering / back-pressure.** If ten files are uploaded at once, ten events queue up and are
  processed in an orderly fashion rather than overwhelming the system.
- **Durability.** An event sitting in the queue survives even if the downstream pipeline is
  momentarily unavailable. It is not lost.
- **Poison-message handling.** If an event fails to process 3 times (`maxReceiveCount = 3`), SQS
  moves it to the **dead-letter queue** (DLQ), where it is retained for 14 days for a human to
  inspect. One malformed event can never block the whole pipeline forever — and a CloudWatch alarm
  fires when anything lands in the DLQ (see [CloudWatch_Monitoring.md](CloudWatch_Monitoring.md)).

### Hop 4 — EventBridge Pipes connects the queue to Step Functions

EventBridge **Pipes** is a managed point-to-point connector: it polls a source (SQS), optionally
reshapes each message, and delivers it to a target (Step Functions) — with **no code to write or
host**. The pipe ([messaging.tf:212](../terraform/messaging.tf#L212)):

```hcl
resource "aws_pipes_pipe" "sqs_to_sfn" {
  source = aws_sqs_queue.pipeline_events.arn
  source_parameters {
    sqs_queue_parameters {
      batch_size                         = 1   # one file → one pipeline execution
      maximum_batching_window_in_seconds = 0
    }
  }
  target = aws_sfn_state_machine.pipeline.arn
  target_parameters {
    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"       # start the run and go back to polling
    }
  }
}
```

Two settings matter:

- **`batch_size = 1`** — each SQS message starts exactly one Step Functions execution, so one
  uploaded file maps to one pipeline run.
- **`FIRE_AND_FORGET`** — the pipe starts the execution and immediately returns to polling SQS; it
  does not block waiting for the multi-minute pipeline to finish.

The pipe also exists to **reshape the message**. As the comment in the code explains, the raw SQS
payload is an array like `[{"messageId":"...","body":"..."}]`. If Step Functions received that
array as its input, its very first `ResultPath` write would crash with
`States.ReferencePathConflict`. The state machine's first state (`NormalizeInput`) defends against
this by replacing the input with a clean `{}` (see [Step_Functions.md](Step_Functions.md)).

### Hop 5 — Step Functions runs the pipeline

The pipe calls `StartExecution` on the state machine, and the orchestration takes over: crawler →
validate → transform → aggregate → load → archive, with all the branching and error handling
described in [Step_Functions.md](Step_Functions.md).

---

## 4. Handling Bursts — The Concurrency Guard

Event-driven systems must answer a question schedulers never face: *what if two events arrive
seconds apart?* With `batch_size = 1`, two quick uploads would start two executions at once. Running
two full pipelines simultaneously wastes compute and risks data races writing to Silver/Gold.

This pipeline handles it inside the state machine, not by dropping events. The first thing each
execution does is check whether another execution is already running, and if so, **wait and
re-check** rather than abort ([step_functions.tf:171](../terraform/step_functions.tf#L171)):

```
CheckAlreadyRunning (list RUNNING executions)
  → IsAnotherRunning (Choice)
       ├─ another active → WaitForPreviousRun (60s) → back to CheckAlreadyRunning
       └─ clear → proceed
```

The newer execution politely yields until the previous one finishes, then runs — so **every**
uploaded file is still processed, just serialized into orderly runs instead of colliding. This is
the event-driven counterpart to a scheduler's implicit "only one run at a time."

---

## 5. Why This Matters for Irregular Data Arrival

The whole design pays off precisely because stream files **do not arrive on a predictable
schedule**. Here is what event-driven buys over a cron schedule in that situation:

| Concern | Scheduled (cron) | This event-driven pipeline |
|---|---|---|
| **Latency** | Up to one full interval of delay | Processing starts seconds after upload |
| **Empty runs** | Runs (and bills) even with no new data | `CheckStreamsExist` exits cleanly; no Glue compute spent |
| **Bursts** | A burst between runs piles up until the next tick | Each file queues and triggers its own run; concurrency guard serializes them |
| **Late / irregular arrivals** | Easily missed or processed stale | Reacts to actual arrival, whenever it happens |
| **Lost events** | A missed schedule = missed data | SQS durably holds events; DLQ catches failures |
| **Cost** | Pay for every scheduled wake-up | Pay only when data actually arrives |

In short, the pipeline **reacts to data instead of predicting it**. When uploads are sporadic, that
means no wasted runs and no stale results; when uploads burst, the queue absorbs the spike and the
concurrency guard keeps runs orderly; and when something goes wrong, the DLQ and CloudWatch alarms
make sure nothing is silently lost.

---

## 6. Why Each Service Instead of a Simpler Wiring

A reasonable question: S3 can trigger a Lambda or an SNS topic directly — why the four-hop chain?
Each hop earns its place:

- **EventBridge** (not direct S3→target) gives rich **content filtering** (only `streams/`) and
  decouples producer from consumer.
- **SQS** (not EventBridge→Step Functions directly) adds **durability, buffering, and a DLQ** so
  bursts are absorbed and failures are retained, not dropped.
- **Pipes** (not a custom Lambda poller) connects SQS to Step Functions and reshapes the message
  with **no code to maintain**.
- **Step Functions** provides the orchestration, branching, and error handling a single Lambda
  could not cleanly express.

(See [All_Services_Used.md](All_Services_Used.md) for the full per-service rationale.)

---

## 7. Summary

| Piece | Role in the trigger chain |
|---|---|
| **S3 + bucket notification** | Emits an `Object Created` event when a file lands in the raw bucket |
| **EventBridge rule** | Matches only `streams/` uploads and routes them to SQS |
| **SQS main queue** | Durably buffers events; smooths bursts (back-pressure) |
| **SQS dead-letter queue** | Catches events that fail 3 times; retains them 14 days |
| **EventBridge Pipe** | Polls SQS, reshapes the message, starts one Step Functions execution per file |
| **Step Functions** | Runs the orchestrated pipeline; concurrency guard serializes overlapping runs |

The result is a pipeline that starts the instant data arrives, never runs for nothing, absorbs
bursts without losing events, and serializes overlapping work — exactly the properties you want when
data arrival is irregular and you cannot predict it in advance.
