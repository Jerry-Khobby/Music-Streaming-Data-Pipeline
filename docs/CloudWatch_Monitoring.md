# Amazon CloudWatch — Logging, Metrics, Alarms, and Debugging

## What This Document Covers

This document explains how Amazon CloudWatch is used to observe and debug the music streaming
pipeline. It covers the three distinct CloudWatch concepts — **log groups**, **metrics**, and
**alarms** — and exactly how each is wired in this project; what every service in the pipeline
writes to logs; and a step-by-step playbook for debugging a failed pipeline run using CloudWatch.
Every claim maps to concrete code in [terraform/monitoring.tf](../terraform/monitoring.tf),
[terraform/main.tf](../terraform/main.tf), [terraform/glue_jobs.tf](../terraform/glue_jobs.tf),
and [terraform/step_functions.tf](../terraform/step_functions.tf).

---

## 1. The Three CloudWatch Concepts — and How They Differ

People often blur these together, but they are three different things doing three different jobs:

| Concept | What it is | Question it answers | In this project |
|---|---|---|---|
| **Log group** | A container of timestamped text lines emitted by a service | *"What exactly happened, line by line?"* | `/aws/glue/<project>`, `/aws/states/<project>` |
| **Metric** | A numeric time series (a number sampled over time) | *"How many / how much / how long?"* | `ExecutionsFailed`, `numFailedTasks`, queue depth/age |
| **Alarm** | A rule that watches a metric and changes state when it breaches a threshold | *"Should a human be told right now?"* | 5 alarm types → SNS → Slack/email |

The mental model: **logs are the detailed narrative**, **metrics are the numbers extracted from
that activity**, and **alarms are the automation that watches the numbers and raises a hand when
something is wrong**. Debugging flows backwards through them — an alarm tells you *that* something
broke and roughly *where*, and the logs tell you *why*.

---

## 2. Log Groups — What Gets Logged From Each Service

A **log group** is a named bucket of log streams. This project provisions two of them explicitly,
each with a 30-day retention so logs don't accumulate cost forever
([main.tf:350](../terraform/main.tf#L350)):

```hcl
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws/glue/${var.project_name}"
  retention_in_days = 30
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/states/${var.project_name}"
  retention_in_days = 30
}
```

### What each service logs

**Glue jobs → `/aws/glue/<project>`.** Every Glue job is configured with continuous CloudWatch
logging via the shared `glue_common_args` ([glue_jobs.tf:58](../terraform/glue_jobs.tf#L58)):

```hcl
locals {
  glue_common_args = {
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_jobs.name
    "--enable-job-insights"              = "true"
    ...
  }
}
```

This means **all five jobs** (validation, etl_transform, kpi_aggregation, dynamodb_loader,
archive) stream their output to the same log group as the run proceeds — not only at the end.
What lands there:

- Every `logger.info(...)` / `logger.warning(...)` / `logger.error(...)` call in the job scripts.
  For example the validation job logs `"[streams] Column check passed"` and the ETL job logs
  `"Merged N date partition(s); M rows after deduplication"`.
- The Spark **driver** and **executor** logs (stack traces, OOM errors, task failures).
- `--enable-job-insights` adds a Glue-generated diagnostic stream that summarizes the most likely
  root cause of a failure in plain language.

**Step Functions → `/aws/states/<project>`.** The state machine is configured to log at the most
verbose level with full execution data ([step_functions.tf:460](../terraform/step_functions.tf#L460)):

```hcl
logging_configuration {
  log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
  include_execution_data = true
  level                  = "ALL"
}
tracing_configuration {
  enabled = true   # X-Ray tracing
}
```

- `level = "ALL"` logs **every** state transition (entered, exited, failed) for every state in the
  pipeline — crawler start, crawler polling, each Glue job invocation, every Choice branch.
- `include_execution_data = true` records the **actual input and output JSON** of each state, so
  you can see exactly what data flowed between steps.
- `tracing_configuration` turns on **AWS X-Ray**, which produces a visual trace of where time was
  spent across the run.

**Other services.** SQS, EventBridge, and the Pipe don't write narrative logs here; they are
observed through **metrics** (next section). Their "logs" are effectively the queue-depth and
message-age numbers CloudWatch collects automatically.

---

## 3. Metrics — The Numbers CloudWatch Collects

A **metric** is a numeric time series. AWS services publish metrics automatically into namespaces.
This project's alarms watch metrics from three namespaces:

| Namespace | Metric | What it measures | Used by alarm |
|---|---|---|---|
| `AWS/States` | `ExecutionsFailed` | Count of failed state-machine executions | sfn_execution_failed |
| `AWS/States` | `ExecutionsTimedOut` | Count of executions that hit their timeout | sfn_execution_timed_out |
| `AWS/SQS` | `ApproximateNumberOfMessagesVisible` | Messages sitting in the dead-letter queue | sqs_dlq_has_messages |
| `AWS/SQS` | `ApproximateAgeOfOldestMessage` | Age (seconds) of the oldest unprocessed message | sqs_messages_stuck |
| `Glue` | `glue.driver.aggregate.numFailedTasks` | Failed Spark tasks per Glue job run | glue_job_failed (one per job) |

The Glue metric is especially useful because it is **per job** — the alarm uses
`dimensions = { JobName = each.value, JobRunId = "ALL", Type = "gauge" }`
([monitoring.tf:130](../terraform/monitoring.tf#L130)), so a failure can be attributed to the
exact job that broke.

---

## 4. Alarms — Turning Metrics Into Notifications

An **alarm** watches a single metric, compares it to a threshold over an evaluation window, and
changes state to `ALARM` when breached. This project defines five categories of alarm, all in
[monitoring.tf](../terraform/monitoring.tf):

1. **`sfn_execution_failed`** — `ExecutionsFailed >= 1` over 5 minutes. The pipeline run failed.
2. **`sfn_execution_timed_out`** — `ExecutionsTimedOut >= 1`. A step hung (e.g. a Glue job stuck or
   a crawler poll loop that never resolves).
3. **`sqs_dlq_has_messages`** — DLQ `ApproximateNumberOfMessagesVisible >= 1`. A trigger event was
   poison and EventBridge gave up on it — meaning a run may never have started.
4. **`sqs_messages_stuck`** — main queue `ApproximateAgeOfOldestMessage >= 900s` for 3 periods. The
   Pipe isn't draining the queue into the state machine.
5. **`glue_job_failed`** (one per job, via `for_each` over all five jobs,
   [monitoring.tf:115](../terraform/monitoring.tf#L115)) — `numFailedTasks >= 1`. Pinpoints
   *which* Glue job had a task failure.

### Why alarms on SQS and Step Functions and Glue — defense in depth

The comment block at the top of `monitoring.tf` explains the design rationale: the alarms are
**independent of the pipeline's own success-path notifications**. They catch failures that prevent
the state machine from even starting (a broken Pipe, misconfigured IAM, a disabled EventBridge
rule) and stuck-queue conditions that produce *no* state-machine activity at all. This is
belt-and-suspenders monitoring — if any layer of the pipeline silently stalls, a different alarm
still fires.

### Where alarms go — the notification fan-out

```
 CloudWatch metric breaches threshold
   → alarm enters ALARM state
     → SNS topic (pipeline_alerts)
        ├─→ AWS Chatbot → Slack channel        (if slack_workspace_id/channel_id set)
        └─→ Email subscription (alert_email)
```

The alarms publish to the `pipeline_alerts` SNS topic. SNS fans out to two human channels:

- **Slack**, via `aws_chatbot_slack_channel_configuration`
  ([monitoring.tf:276](../terraform/monitoring.tf#L276)), created only when the Slack workspace and
  channel IDs are supplied. Chatbot has read-only CloudWatch access so it can enrich the alert with
  a metric snapshot.
- **Human-readable email.** Rather than ship CloudWatch's raw structured alarm payload, an
  EventBridge rule (`alarm_state_change`, [monitoring.tf:180](../terraform/monitoring.tf#L180))
  catches the `CloudWatch Alarm State Change` event for this project's alarms and rewrites it with
  an **input transformer** into plain language — *what fired, why, when, where to look* — including
  a direct console link to the alarm ([monitoring.tf:217](../terraform/monitoring.tf#L217)).

There is also a **success** path: an EventBridge rule (`pipeline_succeeded`,
[monitoring.tf:141](../terraform/monitoring.tf#L141)) catches the state machine's `SUCCEEDED`
event and posts a "✅ Pipeline SUCCEEDED … All KPIs computed and loaded to DynamoDB" message, so
operators get positive confirmation, not just failure noise.

---

## 5. Debugging a Failed Pipeline Run With CloudWatch

Here is the practical playbook this monitoring setup is designed to support. The flow goes
**alarm → which layer → logs → root cause**.

### Step 1 — Read the alert to identify the layer

The alarm name tells you immediately *where* the failure is, because each is prefixed
`<project>-`:

- `…-sfn-execution-failed` → a step inside the pipeline threw an error.
- `…-sfn-execution-timed-out` → a step hung (likely Glue or the crawler poll loop).
- `…-glue-<jobname>-failed` → a specific Glue job (validation / etl-transform / kpi-aggregation /
  dynamodb-loader / archive) had a task failure. **This names the exact job.**
- `…-sqs-dlq-has-messages` → the run probably never started; the trigger event was rejected.
- `…-sqs-messages-stuck` → events are arriving but not being processed; the Pipe or state machine
  is throttled/unhealthy.

### Step 2 — If it's a Step Functions failure, open the execution

Because the state machine logs at `level = ALL` with `include_execution_data = true`:

1. Open the **execution in the Step Functions console** — the visual graph highlights the failed
   state in red. The state names map directly to the pipeline stages documented at the top of
   [step_functions.tf](../terraform/step_functions.tf) (`StartRawCrawler`, `ValidateData`,
   `TransformData`, `AggregateKPIs`, `LoadDynamoDB`, `ArchiveFiles`).
2. Click the failed state to see its **input and output JSON** (captured because
   `include_execution_data = true`) — this shows the exact error cause and the data that triggered
   it.
3. Cross-reference the `/aws/states/<project>` log group for the full transition history, and the
   **X-Ray trace** (tracing enabled) to see where time was spent if it was a timeout.

### Step 3 — If it's a Glue job failure, go to the Glue log group

The per-job alarm tells you which job; now read its logs in `/aws/glue/<project>`:

1. Find the failing job's **JobRunId** (from the Step Functions execution output or the Glue
   console run history).
2. In the `/aws/glue/<project>` log group, open the log streams for that run:
   - The **driver** stream holds the top-level stack trace and all the script's `logger` output —
     e.g. validation's `"Missing required columns"` ValueError or the ETL job's
     `"streams table has no columns — the Glue catalog schema is stale"` warning.
   - The **executor** streams hold per-task Spark errors (data skew, OOM, serialization).
   - The **job-insights** stream (enabled via `--enable-job-insights`) summarizes the probable root
     cause in plain language.
3. The alarm description itself points you here: *"check /aws/glue/<project> logs"*
   ([monitoring.tf:119](../terraform/monitoring.tf#L119)).

### Step 4 — If it's an SQS alarm, inspect the queues

- **DLQ has messages:** the alarm description embeds the exact command to inspect it
  ([monitoring.tf:65](../terraform/monitoring.tf#L65)):
  `aws sqs receive-message --queue-url <dlq-url>`. The message body reveals the malformed trigger
  event EventBridge couldn't deliver.
- **Messages stuck:** check the EventBridge Pipe's own metrics/health and the state machine's
  concurrency — the run isn't being dequeued.

### Step 5 — Confirm the fix

After remediation, re-run the pipeline. Success is confirmed by the `pipeline_succeeded`
EventBridge rule posting the ✅ message, and by the alarms returning to `OK` state (the
`sfn_execution_failed` alarm has `ok_actions` wired too, so recovery is announced as well).

---

## 6. Summary

| CloudWatch piece | This project's implementation |
|---|---|
| **Log groups** | `/aws/glue/<project>` (all 5 jobs, continuous logs + job insights) and `/aws/states/<project>` (full execution data + X-Ray), 30-day retention |
| **Metrics watched** | `AWS/States` (failed, timed out), `AWS/SQS` (DLQ depth, oldest-message age), `Glue` (per-job failed tasks) |
| **Alarms** | 5 categories incl. one per Glue job; all publish to the `pipeline_alerts` SNS topic |
| **Notifications** | SNS → AWS Chatbot → Slack, and EventBridge-reshaped human-readable email; plus a success notification |
| **Debugging path** | Alarm names the layer → Step Functions graph + execution data → Glue driver/executor/insights logs → root cause |

The result is a pipeline where a failure is detected automatically at whatever layer it occurs,
an operator is told in plain language which job or stage broke, and the logs needed to find the
root cause are already being captured continuously.
