# Amazon EventBridge — The Event Bus Between S3 and the Pipeline

## What This Document Covers

This document explains what an **event bus** is, how **EventBridge rules** filter the flood of S3
notifications down to just the events this pipeline cares about, and **why EventBridge sits between
S3 and SQS** rather than having S3 trigger Step Functions directly. It is written for a data engineer
new to event-driven systems. Everything maps to [terraform/messaging.tf](../terraform/messaging.tf)
and [terraform/main.tf](../terraform/main.tf). For the full trigger chain, see
[Event_Driven_Architecture.md](Event_Driven_Architecture.md).

---

## 1. What an Event Bus Is

An **event** is a small message announcing that *something happened* — "an object was created in
S3," "an EC2 instance stopped," "a pipeline execution succeeded." Across a large AWS account, dozens
of services emit thousands of these events constantly.

An **event bus** is a central channel that receives events from many sources and routes them to many
destinations. Think of it like a busy mail-sorting room: every department drops mail onto one
conveyor, and sorting rules decide which envelopes get forwarded to which recipients. Producers don't
need to know who (if anyone) will consume their events; consumers don't need to know who produced
them. The bus decouples the two.

**Amazon EventBridge** is AWS's managed event bus. This project uses the **`default` event bus** —
the account-wide bus that AWS services automatically publish to. The pipeline doesn't create a custom
bus; it just attaches a **rule** to the default bus to listen for the one kind of event it cares
about.

The three pieces of EventBridge to know:

- **Event** — the JSON describing what happened (e.g. an S3 `Object Created` event, including the
  bucket and object key).
- **Rule** — a filter (an "event pattern") plus a list of targets. If an event matches the pattern,
  EventBridge forwards it to the targets.
- **Target** — where a matched event is sent (here, the SQS queue).

---

## 2. How S3 Events Reach EventBridge

By default, S3 does **not** announce object creations to EventBridge — you must turn it on per
bucket. This project enables it on the raw bucket
(`aws_s3_bucket_notification.raw_eventbridge`, in [main.tf](../terraform/main.tf)):

```hcl
resource "aws_s3_bucket_notification" "raw_eventbridge" {
  bucket      = aws_s3_bucket.raw.id
  eventbridge = true
}
```

Once enabled, **every** object created anywhere in the raw bucket emits an `Object Created` event to
the default event bus — uploads under `songs/`, `users/`, and `streams/` alike. That's a lot of
noise, and most of it shouldn't start the pipeline. Filtering that noise is the rule's job.

---

## 3. How the Rule Filters S3 Notifications

The pipeline should run only when a **new stream file** arrives — not when reference data (`songs/`,
`users/`) is uploaded. The EventBridge rule expresses exactly that with an **event pattern**
([messaging.tf:119](../terraform/messaging.tf#L119)):

```hcl
resource "aws_cloudwatch_event_rule" "streams_uploaded" {
  name           = "${var.project_name}-streams-uploaded"
  event_bus_name = "default"
  event_pattern  = jsonencode({
    source        = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = { name = [aws_s3_bucket.raw.id] }
      object = { key  = [{ prefix = "streams/" }] }   # ← only keys starting with streams/
    }
  })
}
```

An **event pattern** is a template that an incoming event must match. EventBridge compares each event
against it field by field; only events that match **all** specified fields are forwarded. Reading this
pattern:

- `source = ["aws.s3"]` — only S3 events.
- `"detail-type" = ["Object Created"]` — only object-creation events (ignore deletes, etc.).
- `bucket.name = [the raw bucket]` — only *this* bucket.
- `object.key = [{ prefix = "streams/" }]` — **the key filter** — only objects whose key begins with
  `streams/`.

That last line is **content-based filtering**, and it is the whole point. A `songs/songs.csv` upload
emits an event, but it fails the `prefix = "streams/"` test, so the rule ignores it and the pipeline
does **not** run. Only a genuine new stream file matches and proceeds. This filtering happens *in the
bus*, before any compute is involved — so non-stream uploads cost nothing.

The rule's **target** is the SQS queue ([messaging.tf:142](../terraform/messaging.tf#L142)), and it's
configured defensively: a `retry_policy` (retry delivery to the queue up to 3 times over an hour) and
a `dead_letter_config` (if delivery to the queue ultimately fails, the event itself is sent to the
DLQ). So even the *handoff from EventBridge to SQS* is protected.

---

## 4. Why EventBridge Sits Between S3 and SQS (Not S3 → Step Functions Directly)

This is the key design question. S3 can notify several targets, and you could imagine wiring the
pipeline more directly. EventBridge earns its place for specific reasons:

### Reason 1 — Rich, content-based filtering

S3's own bucket notifications support only coarse prefix/suffix filters and can get tangled when
multiple notification configs target the same bucket. EventBridge offers a full **event-pattern**
language — match on source, detail-type, bucket, key prefix, and more — in one declarative rule. The
clean "only `streams/` uploads" filter is far easier and more expressive in EventBridge.

### Reason 2 — Decoupling producer from consumer

EventBridge separates "S3 announced something" from "this specific pipeline reacts." If tomorrow you
want a second consumer (say, an audit logger) to also react to stream uploads, you add another rule or
target — **without touching S3 or the existing pipeline**. The producer (S3) stays oblivious to who
consumes its events.

### Reason 3 — Why not S3 → Step Functions directly?

Even if S3 could start the state machine directly, you would lose the buffering and failure-handling
that the pipeline needs. Specifically:

- **No buffering / back-pressure.** A burst of uploads would start a burst of executions with nothing
  to smooth the spike. The **SQS** layer (which EventBridge feeds) absorbs bursts.
- **No durable retry / dead-lettering for the trigger itself.** If starting the pipeline failed,
  there'd be nowhere for the event to wait or be quarantined. SQS + its DLQ provide exactly that (see
  [SQS_and_Dead_Letter_Queue.md](SQS_and_Dead_Letter_Queue.md)).
- **Envelope reshaping is still needed.** The message has to be cleaned before Step Functions can use
  it, which the EventBridge **Pipe** handles between SQS and the state machine.

So the chain is deliberately **S3 → EventBridge → SQS → Pipe → Step Functions**, where EventBridge is
the *smart filter/router* at the front and SQS is the *durable buffer* behind it. Each layer does one
job: EventBridge decides *whether* an event is relevant; SQS decides *when* it gets processed and what
happens if it can't be.

### A second EventBridge use in this project

EventBridge isn't only at the front of the pipeline. The monitoring layer also uses EventBridge rules
to react to pipeline outcomes — catching Step Functions `SUCCEEDED` events and CloudWatch alarm
state-changes, then reshaping them into human-readable notifications
([monitoring.tf](../terraform/monitoring.tf)). This shows the same event-bus pattern reused for
observability, not just ingestion. (See [CloudWatch_Monitoring.md](CloudWatch_Monitoring.md).)

---

## 5. Summary

| Concept | This pipeline's implementation |
|---|---|
| **Event bus** | The account `default` bus; the pipeline attaches a rule to it |
| **Enabling S3 events** | `aws_s3_bucket_notification.raw_eventbridge` turns on EventBridge for the raw bucket |
| **Rule + event pattern** | `streams_uploaded` matches `aws.s3` / `Object Created` / raw bucket / key prefix `streams/` |
| **Content filtering** | Only `streams/` uploads trigger the pipeline; `songs/`/`users/` uploads are ignored — in the bus, before any compute |
| **Target** | The SQS main queue, with a delivery retry policy and DLQ fallback |
| **Why EventBridge over direct S3→SFN** | Rich filtering + producer/consumer decoupling, with SQS behind it for buffering and durable failure handling |
| **Reused for observability** | EventBridge rules also reshape success/alarm events into readable alerts |

EventBridge is the intelligent front door of the pipeline: it listens to everything S3 announces,
forwards only the events that genuinely matter (new stream files), and hands them to the durable SQS
buffer — keeping the producer (S3) and the consumer (Step Functions) cleanly decoupled.
