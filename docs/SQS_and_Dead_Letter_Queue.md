# SQS and the Dead-Letter Queue

## What This Document Covers

This document explains how **message queuing** works, the difference between the **main queue** and
the **dead-letter queue (DLQ)** in this pipeline, what the **retry count** means, and what you
actually do with a message that lands in the DLQ. It is written for a data engineer new to queues.
Everything maps to [terraform/messaging.tf](../terraform/messaging.tf). For how the queue fits the
larger trigger chain, see [Event_Driven_Architecture.md](Event_Driven_Architecture.md).

---

## 1. What a Message Queue Is and Why You Need One

Imagine the producer of events (here, EventBridge reacting to S3 uploads) and the consumer of those
events (here, the pipeline that does the work) talking **directly**. Two problems appear immediately:

- **Speed mismatch.** Files might arrive in a sudden burst — ten uploads in two seconds — but the
  pipeline takes minutes to process each. If the producer hands events straight to the consumer, the
  consumer is overwhelmed or events are dropped.
- **Fragility.** If the consumer is briefly unavailable when an event arrives, the event is simply
  lost — there is nowhere for it to wait.

A **message queue** solves both by sitting *between* producer and consumer as a durable buffer. The
producer drops a **message** into the queue and moves on. The message waits safely in the queue until
a consumer is ready to pick it up. This **decouples** the two sides: they no longer need to be
available at the same time or run at the same speed.

**Amazon SQS** (Simple Queue Service) is AWS's managed queue. In this pipeline, each message is an
S3 `Object Created` event meaning "a new stream file arrived."

### Key queue behaviors to know

- **Visibility timeout.** When a consumer picks up a message, SQS *hides* it from other consumers for
  a set time (so two consumers don't process the same message). If the consumer finishes and deletes
  the message, it's gone. If the consumer crashes without deleting it, the timeout expires and the
  message **reappears** for another attempt. This pipeline sets the main queue's visibility timeout to
  300 seconds, deliberately ≥ the time it takes Step Functions to start
  ([messaging.tf:80](../terraform/messaging.tf#L80)).
- **Retention.** A message that is never successfully processed doesn't live forever — each queue has
  a retention period after which old messages are discarded.

---

## 2. The Two Queues in This Pipeline

This project uses **two** SQS queues that work as a pair: a **main queue** and a **dead-letter
queue**.

### The main queue — `pipeline_events`

This is the working queue ([messaging.tf:78](../terraform/messaging.tf#L78)). EventBridge sends every
new-stream-file event here; the EventBridge Pipe polls it and starts one Step Functions execution per
message.

```hcl
resource "aws_sqs_queue" "pipeline_events" {
  name                       = "${var.project_name}-pipeline-events"
  visibility_timeout_seconds = 300     # ≥ Step Functions startup time
  message_retention_seconds  = 86400   # keep messages up to 1 day

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.pipeline_dlq.arn
    maxReceiveCount     = 3            # after 3 failed receives, send to the DLQ
  })
}
```

The crucial part is the **`redrive_policy`**: it links the main queue to the DLQ and sets the retry
threshold (`maxReceiveCount = 3`). This is what turns the two queues into a safety system.

### The dead-letter queue — `pipeline_dlq`

A DLQ is a *second* queue whose only job is to **catch messages the main queue could not process
successfully** ([messaging.tf:65](../terraform/messaging.tf#L65)):

```hcl
resource "aws_sqs_queue" "pipeline_dlq" {
  name                      = "${var.project_name}-pipeline-dlq"
  message_retention_seconds = 1209600   # 14 days — keep failures long enough to investigate
}
```

Notice its retention is **14 days**, far longer than the main queue's 1 day. That is deliberate: a
failed message needs to sit safely long enough for a human to notice and investigate it.

---

## 3. What "Retry Count" Means — `maxReceiveCount`

This is the heart of how the two queues cooperate. Every time a consumer **receives** a message from
the main queue but fails to process and delete it (so the visibility timeout expires and the message
reappears), SQS increments that message's **receive count**.

`maxReceiveCount = 3` means: *give this message up to 3 attempts.* The flow for a problematic message
is:

```
Message arrives in main queue
  → received (attempt 1) → processing fails → message reappears   (receive count = 1)
  → received (attempt 2) → processing fails → message reappears   (receive count = 2)
  → received (attempt 3) → processing fails → message reappears   (receive count = 3)
  → receive count now exceeds maxReceiveCount
  → SQS automatically MOVES the message to the dead-letter queue
```

Why retry at all? Because many failures are **transient** — a momentary throttle, a brief service
hiccup. Retrying 3 times gives a temporarily-failing message a fair chance to succeed on its own.

Why a limit? Because some failures are **permanent** — a genuinely malformed ("poison") message will
fail every single time. Without a limit, that poison message would be retried forever, blocking the
queue and burning resources. `maxReceiveCount` draws the line: *try a few times, then set it aside
instead of letting it jam the pipeline.* This is the queue's way of distinguishing "bad luck" from
"bad message."

---

## 4. Why a Message Ends Up in the DLQ

A message lands in the DLQ only after it has failed `maxReceiveCount` (3) times. In practice that
points to one of a few root causes:

- **A malformed / poison event** — the event payload is structurally wrong, so the Pipe or the state
  machine rejects it every time.
- **A persistent downstream problem** — e.g. the state machine cannot be started due to a permissions
  or configuration fault, so every delivery attempt fails identically.
- **A repeated processing error** that is not actually transient.

The DLQ is, in effect, the pipeline's **"failed mail" tray**: messages that could not be delivered no
matter how many times they were tried, kept aside so they neither block healthy traffic nor vanish
silently.

---

## 5. How You Find Out — DLQ Alarm

A message sitting in a DLQ is useless if nobody notices it. This pipeline wires a CloudWatch alarm
directly to DLQ depth ([monitoring.tf:63](../terraform/monitoring.tf#L63)):

```hcl
resource "aws_cloudwatch_metric_alarm" "sqs_dlq_has_messages" {
  metric_name        = "ApproximateNumberOfMessagesVisible"
  namespace          = "AWS/SQS"
  threshold          = 1
  dimensions         = { QueueName = aws_sqs_queue.pipeline_dlq.name }
  alarm_description   = "Messages landed in the dead-letter queue. Inspect with: aws sqs receive-message --queue-url <dlq-url>"
  alarm_actions      = [aws_sns_topic.pipeline_alerts.arn]
}
```

The moment **one** message lands in the DLQ, the alarm fires and SNS sends an alert (to email and
Slack). The alarm description even embeds the command to inspect the queue. This matters because, as
the monitoring design notes, a DLQ filling up often means **the pipeline never even started** — a
failure that produces no Step Functions activity at all, and so would otherwise be invisible. (See
[CloudWatch_Monitoring.md](CloudWatch_Monitoring.md).)

---

## 6. What You Do With Messages in the DLQ

When the DLQ alarm fires, the operational playbook is:

1. **Inspect the message.** Read the failed message(s) to see the actual event payload:
   ```
   aws sqs receive-message --queue-url <pipeline_dlq_url>
   ```
   (The `sqs_pipeline_dlq_url` is a Terraform output, [outputs.tf:140](../terraform/outputs.tf#L140).)
2. **Diagnose the root cause.** Is the event malformed? Was there a permissions/config problem that
   caused every start to fail? The payload usually reveals which.
3. **Fix the underlying issue.** Correct the bad data source, fix the IAM/config problem, or patch the
   code that couldn't handle the event.
4. **Decide what to do with the message:**
   - **Redrive / replay** — once the cause is fixed, the message can be moved back to the main queue
     (SQS has a built-in "DLQ redrive" feature) so the now-fixed pipeline reprocesses it. Because the
     pipeline is **idempotent** (see [Idempotency_in_Data_Pipelines.md](Idempotency_in_Data_Pipelines.md)),
     replaying a message is safe — it won't double-count data.
   - **Discard** — if the message was genuinely garbage and represents no real data, delete it.
5. **Confirm the DLQ drains** and the alarm returns to OK.

The 14-day retention exists precisely to give you time to do all this before the failed messages
expire.

---

## 7. Why This Design — Decoupling, Durability, Safety

Putting SQS (with a DLQ) between EventBridge and Step Functions, rather than wiring them directly,
buys three things:

- **Decoupling & back-pressure** — bursts of uploads queue up and are processed in orderly fashion
  instead of overwhelming the pipeline.
- **Durability** — an event survives in the queue even if the consumer is briefly unavailable; it is
  not lost.
- **Failure isolation** — a single poison message is retried a bounded number of times, then set
  aside in the DLQ where it neither blocks healthy events nor disappears silently, and triggers an
  alert so a human can act.

---

## 8. Summary

| Concept | This pipeline's implementation |
|---|---|
| **Message queue** | SQS buffers S3-event messages between EventBridge and Step Functions |
| **Main queue** (`pipeline_events`) | Working queue; 300s visibility timeout, 1-day retention |
| **Dead-letter queue** (`pipeline_dlq`) | Catches messages that fail repeatedly; 14-day retention |
| **`maxReceiveCount = 3`** | Retry a message up to 3 times before moving it to the DLQ |
| **Why retry then stop** | Retries beat transient failures; the limit stops poison messages from jamming the queue |
| **How you're notified** | CloudWatch alarm on DLQ depth ≥ 1 → SNS → Slack/email |
| **What you do** | Inspect → diagnose → fix → redrive (safe, because idempotent) or discard |

The main queue keeps healthy events flowing and absorbs bursts; the dead-letter queue quarantines the
events that can't be processed, alerts a human, and holds them long enough to investigate and replay —
so nothing is lost and nothing poison ever blocks the pipeline.
