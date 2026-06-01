# Monitoring and Observability

## What This Document Covers

This document explains the **difference between logging, monitoring, and alerting**, the **metrics
that matter** for this pipeline (job duration, records processed, queue depth, failures), and **how
you would know the pipeline is healthy without opening the AWS console**. It is written for a data
engineer new to observability. It is the conceptual companion to
[CloudWatch_Monitoring.md](CloudWatch_Monitoring.md) (the resource-by-resource detail) and
[Error_Handling_and_Retry.md](Error_Handling_and_Retry.md) (the failure path).

---

## 1. Logging vs Monitoring vs Alerting — Three Different Things

These three words are used interchangeably in casual talk, but they are distinct layers, and a good
pipeline needs all three. The clearest way to see the difference is by the question each answers:

| Layer | Question it answers | Form | In this pipeline |
| --- | --- | --- | --- |
| **Logging** | *"What exactly happened, step by step?"* | Timestamped text lines | Glue job logs, Step Functions execution logs |
| **Monitoring** | *"How is the system behaving over time — numbers, trends?"* | Numeric metrics / time series | Job failures, queue depth, message age, durations |
| **Alerting** | *"Is something wrong right now that needs a human?"* | A notification triggered by a rule | CloudWatch alarms → SNS → Slack/email + direct webhook |

The relationship between them:

- **Logging** is the detailed *narrative* — the most granular, but too voluminous to watch constantly.
- **Monitoring** distills activity into *numbers you can track* — far less data, good for spotting
  trends and thresholds, but it doesn't tell you *why*.
- **Alerting** sits on top of monitoring: it *watches the numbers for you* and pings a human only when
  a threshold is crossed — so nobody has to stare at dashboards.

You debug by going **down** the stack: an **alert** tells you *that* something is wrong and roughly
where; **monitoring** shows the *shape* of the problem (when it started, how bad); **logs** reveal the
exact *why*. **Observability** is the umbrella term for having all three so the system's internal
state is understandable from the outside.

---

## 2. Logging in This Pipeline

Logs are captured automatically into two CloudWatch **log groups**, each with 30-day retention (see
[CloudWatch_Monitoring.md](CloudWatch_Monitoring.md)):

- **`/aws/glue/<project>`** — every Glue job streams its `logger` output, Spark driver/executor logs,
  and a job-insights summary. This is where the *narrative* of a transformation lives: "Merged N
  partitions; M rows after deduplication," stack traces, etc.
- **`/aws/states/<project>`** — the Step Functions state machine logs every state transition at
  `level = ALL` with full input/output data, plus an X-Ray trace.

Logs are the ground truth for *why* something happened, but you don't *watch* them — you consult them
when monitoring or an alert points you there.

---

## 3. The Metrics That Matter (Monitoring)

Monitoring turns activity into numbers. For an event-driven ETL pipeline like this one, a handful of
metrics tell you almost everything about its health. Here are the ones that matter and what each
reveals:

### Failure metrics — *did it work?*

- **`ExecutionsFailed` / `ExecutionsTimedOut`** (`AWS/States`) — did a pipeline run fail or hang? A
  non-zero value is the headline "something broke" signal. A timeout specifically suggests a Glue job
  hung or the crawler poll loop never resolved.
- **`glue.driver.aggregate.numFailedTasks`** (`Glue`, per job) — did a specific Glue job have task
  failures? Because it's **per job**, it pinpoints *which* stage (validation / transform / aggregate /
  load / archive) broke.

### Queue metrics — *is the trigger chain flowing?*

- **`ApproximateNumberOfMessagesVisible`** on the **DLQ** (`AWS/SQS`) — are there poison events that
  failed repeatedly? Any value ≥ 1 means something couldn't be processed at all — often that the
  pipeline never even started.
- **`ApproximateAgeOfOldestMessage`** on the **main queue** — are events piling up unprocessed? A
  growing age (the alarm fires at >15 min) means the consumer isn't draining the queue — a backlog or
  stuck Pipe.

### Throughput / behavior metrics — *is it doing the right amount of work?*

- **Job duration** — how long each Glue job runs. A sudden jump can signal data growth, skew, or a
  performance regression (and feeds cost — see [Cost_Optimisation.md](Cost_Optimisation.md)). Available
  from Glue's CloudWatch metrics.
- **Records processed** — the pipeline logs row counts (e.g. the transform job's "M rows after
  deduplication"). Watching this reveals whether a run processed a plausible volume — a run that
  suddenly processes *zero* or *10×* the usual rows is suspicious even if it didn't error.

The art of monitoring is choosing the *few* numbers that summarize health. For this pipeline that's:
**did runs fail/timeout, are the queues healthy, and is work volume/duration normal?**

### Which of these are wired as alarms today

This project provisions CloudWatch **alarms** on the failure and queue metrics specifically
([monitoring.tf](../terraform/monitoring.tf)): Step Functions failed, Step Functions timed out, DLQ
has messages, main queue messages stuck, and per-Glue-job task failures. Job-duration and
records-processed are available as metrics/logs for investigation and dashboards; the *alarmed* set is
deliberately focused on the signals that mean "act now."

---

## 4. Alerting — Two Independent Channels

This pipeline uses two independent alerting paths that complement each other. Understanding both is
important because they serve different scopes and fire at different granularities.

### Channel 1 — CloudWatch alarms → SNS → email + Chatbot

An **alarm** watches one metric against a threshold and changes state to `ALARM` when breached. This
pipeline's alarms all publish to one **SNS topic** (`pipeline_alerts`), which fans out to **email** and
**Slack** (via AWS Chatbot). The chain:

```text
 metric crosses threshold → CloudWatch alarm → SNS topic → ├─ Slack (AWS Chatbot)
                                                            └─ Email
```

Two design choices make this path trustworthy (see [CloudWatch_Monitoring.md](CloudWatch_Monitoring.md)):

- **Independent of the pipeline's own success path.** These alarms catch failures that stop the
  pipeline from even starting — a broken Pipe, an IAM misconfiguration, a stuck queue — failures that
  produce *no* Step Functions activity and would be invisible to an in-process alerting scheme.
- **Human-readable.** An EventBridge rule reshapes raw alarm payloads into plain-language emails
  ("what fired, why, when, where to look"), and the failure notification embeds the actual error.

### Channel 2 — Direct Slack webhook (in-flight, stage-granular)

A second path operates from inside the running pipeline using two components:

- **`monitoring/` package (`PipelineMonitor` + `SlackNotifier`)** — every Glue job wraps each of
  its stages in a `PipelineMonitor.stage()` context manager. This calls the `SlackNotifier` on start,
  success, and failure as the stage runs, posting rich Slack Block Kit messages directly to the
  webhook URL. This is the source of the per-stage visibility inside each job.
- **`lambda/pipeline_notifier.py`** — a Lambda function invoked by three dedicated Step Functions
  states (`NotifyPipelineStarted`, `NotifyPipelineSucceeded`, `NotifySlackPipelineFailed`) to post
  pipeline-level Block Kit messages at the very start and end of each run.

```text
 Pipeline starts      → NotifyPipelineStarted Lambda  →  :rocket: Pipeline — Started (Slack)
 Glue stage starts    → PipelineMonitor hook           →  :hourglass: Job — In Progress (Slack)
 Glue stage ends      → PipelineMonitor hook           →  :white_check_mark: Job — Succeeded (Slack)
 Pipeline succeeds    → NotifyPipelineSucceeded Lambda →  :large_green_circle: Pipeline — Succeeded (Slack)
 Pipeline fails       → NotifyFailure (SNS) + NotifySlackPipelineFailed Lambda → both channels fire
```

Unlike the CloudWatch path (which fires *after* a metric threshold is crossed), this path fires
*in real time* as each stage begins and ends — so you see the pipeline progress live, not just the
outcome.

---

## 5. How You Know the Pipeline Is Healthy *Without* Looking at the Console

This is the real test of observability: can you trust the system is fine **without** logging in to
check? For this pipeline, **yes** — health is *pushed to you*, not something you have to *pull*:

### Healthy looks like

1. **A `:rocket: Pipeline — Started` message** arrives in Slack at the beginning of each run, posted
   by the `NotifyPipelineStarted` Lambda, confirming the execution is in flight.
2. **Stage-by-stage progress messages** arrive as each Glue job runs — `:hourglass:` when a stage
   starts, `:white_check_mark:` when it completes — posted directly from the `PipelineMonitor`
   context manager inside each job.
3. **A `:large_green_circle: Pipeline — Succeeded` message** arrives after `ArchiveFiles` completes,
   posted by the `NotifyPipelineSucceeded` Lambda. Positive confirmation — not just absence of bad news.
4. **No failure alerts on either channel.** Because CloudWatch alarms cover every infrastructure
   failure mode (run failed, run timed out, DLQ non-empty, queue backed up, any Glue job failed),
   *the absence of an alarm is meaningful* — none of those thresholds were crossed.

### The key principle: trustworthy silence

A pipeline is observable when **silence is trustworthy** — when "no alert" reliably means "all good"
because *every* meaningful failure mode has an alarm that *would* have fired. This pipeline achieves
that by alarming on the independent, comprehensive set of signals above. So an operator's day looks
like: see the ✅ on success, and otherwise hear nothing — and *trust* that nothing means healthy. They
only open the console when an alert actively pulls them in, at which point the alert names the layer,
the metrics show the shape, and the logs give the why.

### If you *do* want a glance-view

Beyond push notifications, the same CloudWatch metrics can back a **dashboard** (failures, queue
depth/age, job durations, record counts on one screen) for an at-a-glance health view — but the design
goal is that you shouldn't *need* to look unless an alert tells you to.

---

## 6. Summary

| Layer | Purpose | This pipeline |
| --- | --- | --- |
| **Logging** | The detailed "what happened" narrative | Glue logs + Step Functions execution logs (30-day retention) |
| **Monitoring** | Numeric health signals over time | Failures/timeouts, per-job task failures, DLQ depth, queue age, durations, record counts |
| **Alerting (CloudWatch path)** | Infrastructure-level: notify when a metric threshold breaks | CloudWatch alarms → SNS → Chatbot/Slack + email; independent of the state machine |
| **Alerting (webhook path)** | In-flight: real-time stage and pipeline progress | `PipelineMonitor` stage hooks (all 5 jobs) + `NotifyPipelineStarted/Succeeded/Failed` Lambda |
| **Health without the console** | Trustworthy silence + active confirmation | Live stage messages + `:large_green_circle:` on success; no CloudWatch alarm = healthy |

Logging, monitoring, and alerting are three layers, not synonyms: logs explain *why*, metrics show
*how it's trending*, and alarms decide *when to wake a human*. This pipeline wires all three so that
its health is pushed to you — a green checkmark when a run succeeds and silence the rest of the time —
and the console is only ever needed once an alert has already told you where to look.
