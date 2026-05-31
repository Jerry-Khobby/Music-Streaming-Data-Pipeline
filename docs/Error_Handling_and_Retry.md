# Error Handling and Retry Strategies

## What This Document Covers

This document explains how this pipeline handles failures end to end: how **Step Functions `Catch`
blocks** work, what **exponential backoff** is and why retries use it, what gets **logged** when a job
fails, and how **SNS** fits into the alerting chain. It is written for a data engineer new to building
resilient pipelines. It draws together threads covered individually in
[Step_Functions.md](Step_Functions.md), [Data_Validation.md](Data_Validation.md), and
[CloudWatch_Monitoring.md](CloudWatch_Monitoring.md), and focuses on the cross-cutting error story.

---

## 1. The Philosophy — Expect Failure, Contain It, Announce It

Distributed pipelines fail. Networks blip, services throttle, an upstream file is malformed, a
crawler runs a moment too early. A robust pipeline does not try to *prevent* all failure — it is
designed to **expect failure, contain it so it doesn't corrupt data, and announce it so a human can
act.** This pipeline does that with three layers working together:

1. **Retry** transient failures automatically (they often fix themselves).
2. **Catch** genuine failures and route them to a safe, consistent failure path (don't run downstream
   steps on bad data).
3. **Alert** a human via logs + SNS so the failure is never silent.

The rest of this document walks through each layer.

---

## 2. Step Functions `Catch` Blocks — Containing Failure

A **`Catch`** block attached to a Step Functions state says: *"if this state fails, don't crash the
whole execution with a raw error — instead jump to a named recovery state."* It is the pipeline's
primary containment mechanism.

Every working step in the state machine carries the same Catch
([step_functions.tf:332](../terraform/step_functions.tf#L332)):

```hcl
Catch = [{
  ErrorEquals = ["States.ALL"]    # catch any error
  ResultPath  = "$.error"          # save the error onto the execution's data
  Next        = "NotifyFailure"    # jump to the failure-notification state
}]
```

Three things happen on failure:

- **`ErrorEquals = ["States.ALL"]`** — match *any* error type.
- **`ResultPath = "$.error"`** — store the error's details (`Error` and `Cause`) on the execution
  state so the next step can include them in the alert.
- **`Next = "NotifyFailure"`** — route to the single, shared failure handler.

Because **every** task points its Catch at the same `NotifyFailure` state, a failure *anywhere* —
crawler, validation, transform, aggregation, load, archive — converges on one consistent path. This
is what guarantees that a failure at step 3 cleanly **stops** steps 4–6 instead of running them on
incomplete or bad data. Containment is the whole point: a contained failure is a stopped pipeline and
an alert; an uncontained one is corrupt data served to users.

### Catch can be specific, and failure can be deliberately non-fatal

Catch blocks are evaluated in order and can match specific errors. The crawler start treats "already
running" as harmless and only routes *real* errors to failure
([step_functions.tf:220](../terraform/step_functions.tf#L220)):

```hcl
Catch = [
  { ErrorEquals = ["Glue.CrawlerRunningException"], Next = "WaitForCrawler" },  # expected — just poll
  { ErrorEquals = ["States.ALL"],                   Next = "NotifyFailure" }    # real failure
]
```

And the curated crawler (which refreshes Athena partitions) is treated as **non-fatal** — all its
catches route *forward* to `ArchiveFiles`, because a missing partition refresh shouldn't fail the
whole run ([step_functions.tf:387](../terraform/step_functions.tf#L387)). Error handling is a
**per-step design decision**, not a blanket rule.

---

## 3. Retry and Exponential Backoff — Beating Transient Failures

Many failures are **transient**: a service is briefly busy, a resource isn't ready *yet*, a network
call times out once. For these, the right response is not to give up — it's to **wait and try again**.

### What exponential backoff is

**Backoff** means waiting before a retry. **Exponential backoff** means *increasing* the wait after
each failed attempt, typically by doubling: wait 10s, then 20s, then 40s, and so on. The waits grow
exponentially rather than staying constant.

Why grow the wait instead of retrying immediately or at a fixed interval?

- **Give the problem time to clear.** If a service is overloaded, hammering it with immediate retries
  makes things *worse*. Backing off gives it room to recover.
- **Avoid a retry storm (thundering herd).** If many clients all retry at the same fixed interval,
  they synchronize and slam the service together. Increasing, spread-out waits break that
  synchronization.
- **Balance speed and patience.** Short first waits catch quick blips fast; longer later waits avoid
  pestering a service that's clearly still down, before eventually giving up.

### Where this pipeline uses backoff

This pipeline implements retry-with-backoff in the place it's most needed — waiting for the Glue
crawler to register catalog tables before validation reads them. The validation job retries a
`TableNotFound` with **exponentially increasing waits** ([validation_job.py:79](../glue_jobs/validation_job.py#L79)):

```python
max_retries = 3
retry_delay = 10
for attempt in range(max_retries):
    try:
        df = loadTable(...)          # try to read the catalog table
        ...
        return
    except TableNotFound:
        wait_time = retry_delay * (2 ** attempt)   # 10s, 20s, 40s  ← exponential backoff
        time.sleep(wait_time)
```

The waits double — 10, 20, 40 seconds — giving the crawler time to finish before each new attempt. If
the table still isn't there after 3 tries, that's treated as a genuine failure (the crawler likely
broke), and a clear error is raised.

### The crawler polling loop is the same idea in Step Functions

The state machine's crawler-readiness loop (`WaitForCrawler` 45s → `CheckCrawlerStatus` →
`IsCrawlerReady`, looping back if not `READY`) is a hand-built retry loop for "is the crawler done
yet?" — waiting between checks rather than busy-polling. (See [Step_Functions.md](Step_Functions.md).)

> **Honest note on scope:** the Step Functions *task* states here use `Catch` (fail-fast) rather than
> declared `Retry` blocks. Retry-with-backoff lives in the validation job's Python and in the polling
> loop. A natural enhancement would be to add a `Retry` block (with `IntervalSeconds`, `MaxAttempts`,
> `BackoffRate`) to each `glue:startJobRun.sync` task so a job that fails *transiently* is retried
> automatically before the pipeline declares failure.

---

## 4. What Gets Logged When a Job Fails

When something fails, the evidence needed to debug it is captured automatically in **CloudWatch**, in
two log groups (see [CloudWatch_Monitoring.md](CloudWatch_Monitoring.md)):

- **`/aws/glue/<project>`** — every Glue job streams logs here continuously
  (`--enable-continuous-cloudwatch-log`). On failure you get: the script's own `logger.error(...)`
  messages (e.g. validation's "Missing required columns"), the Spark **driver** stack trace, the
  **executor** task errors, and a **job-insights** stream summarizing the likely root cause.
- **`/aws/states/<project>`** — the state machine logs every transition at `level = ALL` with
  `include_execution_data = true`, so you can see exactly which state failed and the input/output JSON
  around it, plus an X-Ray trace.

In addition, the captured error itself (`$.error.Error` and `$.error.Cause`) is carried in the
execution data by the `Catch` block and **embedded directly into the alert message** (next section) —
so the first notification already tells you what broke.

---

## 5. How SNS Fits the Alerting Chain

Catching and logging a failure is useless if no human is told. **SNS** (Simple Notification Service)
is the pub/sub hub that turns a caught failure into a notification.

When any `Catch` routes to `NotifyFailure`, that state **publishes to the SNS topic**, formatting a
human-readable message from the captured error ([step_functions.tf:426](../terraform/step_functions.tf#L426)):

```hcl
NotifyFailure = {
  Type     = "Task"
  Resource = "arn:aws:states:::sns:publish"
  Parameters = {
    TopicArn    = aws_sns_topic.pipeline_alerts.arn
    Subject     = "❌ Music Streaming Pipeline FAILED"
    "Message.$" = "States.Format('PIPELINE FAILED\n\nError: {}\nCause: {}\n...console links...', $.error.Error, $.error.Cause)"
  }
  Next = "PipelineFailed"
}
```

`States.Format(...)` injects the actual error and cause, plus console links, into the message. After
publishing, the execution ends at `PipelineFailed` (a `Fail` state) — so the run is correctly marked
failed.

The full alerting chain:

```
 A step fails
   → Catch saves the error to $.error, routes to NotifyFailure
     → NotifyFailure publishes to the SNS topic (pipeline_alerts)
        ├─→ Email subscriber(s)                 (human-readable failure message)
        └─→ AWS Chatbot → Slack channel          (if Slack is configured)
```

SNS is the **single hub**: both the state machine *and* the independent CloudWatch alarms (Step
Functions failures, DLQ depth, stuck queues, per-Glue-job task failures) publish to the same topic, so
every alert path converges and you manage subscribers in one place. The CloudWatch alarms are
deliberately **independent** of the state machine, so they catch failures that prevent the pipeline
from even starting — belt-and-suspenders coverage (see [CloudWatch_Monitoring.md](CloudWatch_Monitoring.md)).

There is also a **success** notification: an EventBridge rule catches the state machine's `SUCCEEDED`
event and posts a "✅ Pipeline SUCCEEDED" message, so silence never has to be interpreted as success.

---

## 6. The Three Layers Working Together — An Example

Trace a transient crawler delay and a genuine bad-data failure through all three layers:

**Transient (crawler not ready):** validation's `TableNotFound` → **Retry** with backoff (10s, 20s,
40s) → table appears on attempt 2 → success. No alert, no human involvement. *The pipeline healed
itself.*

**Genuine (missing column):** validation finds `streams` is missing `track_id` → raises `ValueError`
→ Glue job fails → Step Functions **Catch** saves the error and routes to `NotifyFailure` →
**SNS** publishes "❌ FAILED — streams missing track_id" to Slack/email → execution ends at
`PipelineFailed`; **transform/aggregate/load never run** → logs in `/aws/glue/<project>` hold the full
detail. *The failure was contained and announced; no bad data was served.*

---

## 7. Summary

| Layer | Mechanism | Role |
|---|---|---|
| **Retry** | Exponential backoff (10/20/40s) in the validation job; the SFN crawler polling loop | Auto-heal transient failures without human involvement |
| **Catch** | `Catch` on every SFN task → save `$.error` → `NotifyFailure` (specific catches & non-fatal steps too) | Contain failures; stop downstream steps from running on bad data |
| **Log** | `/aws/glue/<project>` (driver/executor/insights) + `/aws/states/<project>` (transitions, execution data, X-Ray) | Capture the evidence to diagnose root cause |
| **Alert** | `NotifyFailure` → SNS topic → email + Slack; independent CloudWatch alarms also publish to SNS | Make every failure visible to a human, with the error embedded |

The pattern is: **retry what might be transient, catch what truly failed, log everything, and
announce it through SNS.** Together these turn an inevitable failure into a self-healed blip or a
contained, clearly-explained alert — never silent corruption.
