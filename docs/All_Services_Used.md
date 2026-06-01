# All AWS Services Used in This Project — A Complete Map

## What This Document Covers

This document is a complete inventory of **every AWS service** in the music streaming pipeline. For
each service it answers three questions in plain terms a data engineer new to the cloud can follow:

1. **What is it?** — a one-paragraph explanation of the service itself.
2. **What problem does it solve here?** — the specific job it does in *this* pipeline.
3. **Why this service over the alternatives?** — what else could have been used, and why this was
   chosen.

Every service below is backed by a real resource in the [terraform/](../terraform/) directory.

---

## 1. The Big Picture — How the Services Fit Together

Before the per-service detail, here is the end-to-end flow so each service has a place in your
mental model:

```text
 [Producer script] → Kinesis Data Firehose → lands a JSON batch file
        │
        ▼
 ┌─────────────┐  ObjectCreated event   ┌──────────────┐   rule matches    ┌──────────────┐
 │  Amazon S3   │ ─────────────────────▶ │ EventBridge  │ ────────────────▶ │  Amazon SQS   │
 │ (raw bucket) │                        │   (rule)     │                   │ (main queue)  │
 └─────────────┘                         └──────────────┘                   └──────┬───────┘
                                                                                   │ poll
                                                                          ┌────────▼────────┐
                                                                          │ EventBridge Pipe│
                                                                          └────────┬────────┘
                                                                                   │ StartExecution
                                                                          ┌────────▼────────┐
                                                                          │ Step Functions  │  ◀── the orchestrator
                                                                          └────────┬────────┘
        ┌──────────────────────────────────────────────────────────────────────── │ ────────────────────────────┐
        ▼                          ▼                       ▼                        ▼                  ▼           ▼
 ┌────────────┐           ┌────────────────┐      ┌──────────────┐         ┌──────────────┐   ┌──────────┐  ┌──────────┐
 │ Glue Crawler│  schema  │   Glue Jobs     │ data │  Amazon S3    │  load   │   DynamoDB    │   │   SNS     │  │  Lambda  │
 │ + Data      │ ───────▶ │ (validate→...→  │────▶ │ silver/ gold/ │ ──────▶ │ (3 tables)    │   │ (alerts)  │  │notifier  │
 │ Catalog     │          │  archive)       │      │  archive      │         └──────────────┘   └────┬─────┘  └────┬─────┘
 └────────────┘           └──────┬──────────┘      └──────────────┘                                  │             │
                                  │ stage hooks — direct webhook                              ┌────────┴─────────────┘
        ▲                         └─────────────────────────────────────────────────────────▶│   Slack channel       │
        │ all steps logged / alarmed                                                          │ (direct webhook path) │
 ┌──────────────────────────────────────────┐                                                └───────────────────────┘
 │           Amazon CloudWatch               │                        ┌────────────────────────────────────────────────┐
 │  (log groups · metrics · alarms)          │── alarm fires ──▶ SNS ─┤─ Chatbot ──▶ Slack   (CloudWatch / SNS path)  │
 └──────────────────────────────────────────┘                        └─ Email                                          │
                                                                      └────────────────────────────────────────────────┘
 Underneath everything:  IAM (permissions) · Terraform (provisions it all)
```

There are two broad groups of services:

- **The data path** — S3, Glue (Crawler, Catalog, Jobs), DynamoDB. This is where data actually
  moves and transforms.
- **The control & orchestration path** — EventBridge, SQS, EventBridge Pipes, Step Functions, IAM.
  This is what *triggers* and *orchestrates* the data path.
- **The observability & alerting path** — CloudWatch, SNS, Chatbot for infrastructure-level alarms;
  Lambda + direct Slack webhook for real-time in-flight stage and pipeline notifications.

---

## 2. Amazon S3 — Object Storage (the Data Lake)

**What it is.** Amazon S3 (Simple Storage Service) is a service for storing files ("objects") in
containers called "buckets." It is effectively infinite, cheap, durable storage you access over
the network. It is the de-facto storage foundation of nearly every data lake on AWS.

**What problem it solves here.** S3 is the home for *all* the pipeline's data at every stage. The
project uses three buckets:

- **raw** (`aws_s3_bucket.raw`, [main.tf:10](../terraform/main.tf#L10)) — the Bronze landing zone
  where incoming CSVs arrive under `songs/`, `users/`, `streams/`.
- **curated** ([main.tf:64](../terraform/main.tf#L64)) — holds the Silver (`silver/`) and Gold
  (`gold/`) Parquet layers produced by the Glue jobs.
- **archive** ([main.tf:104](../terraform/main.tf#L104)) — processed raw files are moved here, then
  aged to Glacier after 90 days.

It also fires the event that starts the whole pipeline (S3 → EventBridge).

**Why S3 over alternatives.** The alternative to a data lake on S3 would be loading everything
straight into a database (e.g. RDS/Redshift). S3 was chosen because it decouples storage from
compute — you pay for storage cheaply and only spin up Glue compute when you process. It handles
any file format, scales without provisioning, and integrates natively with Glue, Athena, and
EventBridge. (See [S3_Bucket_Layers.md](S3_Bucket_Layers.md) for the layer design.)

---

## 2a. Kinesis Data Firehose — Streaming Ingestion

**What it is.** Kinesis Data Firehose is a fully managed service that **ingests streaming records and
delivers them, in batches, to a destination** like S3. It buffers incoming records and flushes them
as files when a size or time threshold is reached — with no servers to manage.

**What problem it solves here.** Firehose (`aws_kinesis_firehose_delivery_stream.streams_ingestion`,
[ingestion.tf](../terraform/ingestion.tf)) is the **automated ingestion front end** that replaces
manual CSV upload. A producer sends play events to a **Direct PUT** delivery stream; Firehose buffers
them and lands JSON batch files in `streams/`, which triggers the existing pipeline unchanged. Its
buffering is what turns a **burst** of events into one tidy file and still flushes **sparse** data
within minutes. (Full detail in [Streaming_Ingestion_Firehose.md](Streaming_Ingestion_Firehose.md).)

**Why Firehose over alternatives.** The obvious alternative was **Kinesis Data Streams (KDS)**. KDS
was *not* chosen because it solves a **distribution** problem — multiple consumers, replay, strict
ordering — that this single-consumer, no-replay pipeline does not have, and it bills per shard-hour
even when idle (costly for sparse traffic). The problem here is **buffering variable arrivals into
batch files cheaply**, which is exactly Firehose's job. The guiding rule: *choose components by the
problem they solve, not by the shape of the traffic.* (The other alternative — the producer writing
straight to S3 — was rejected because a burst would create hundreds of tiny files and pipeline runs;
Firehose's buffer prevents that.)

---

## 3. AWS Glue — Crawler, Data Catalog, and Jobs

AWS Glue is a managed **serverless ETL** (Extract-Transform-Load) service. It is really three
cooperating pieces in this project, so each is covered separately. (Full detail in
[Glue_Crawlers_and_Jobs.md](Glue_Crawlers_and_Jobs.md).)

### 3a. Glue Crawler

**What it is.** A crawler scans files in S3, *infers their schema* (column names and types), and
registers that schema as tables — without moving any data.

**What problem it solves here.** Two crawlers (`raw_crawler`, `curated_crawler`,
[main.tf:277](../terraform/main.tf#L277)) turn raw CSVs and Gold Parquet into queryable catalog
tables, so the Glue jobs can read inputs *by table name* and Athena can run SQL over the outputs.

**Why over alternatives.** You could hard-code each file's schema in every job. The crawler was
chosen so schema lives in one place and adapts automatically if the source files change — no code
edits when a column is added.

### 3b. Glue Data Catalog

**What it is.** A central **metastore** — a registry of table definitions (schema, location,
partitions) kept separately from the data itself.

**What problem it solves here.** `aws_glue_catalog_database.music_db`
([main.tf:268](../terraform/main.tf#L268)) is the contract between the crawler (which *writes*
table definitions) and the jobs and Athena (which *read* them). It decouples the physical files in
S3 from the schema describing them.

**Why over alternatives.** A self-managed Hive metastore would need a server to run on. The Glue
Data Catalog is serverless, managed, and natively understood by Glue and Athena.

### 3c. Glue Jobs

**What it is.** Glue Jobs run your Python/Spark code on managed, on-demand compute. PySpark jobs
(`glueetl`) run distributed Spark; Python Shell jobs run lightweight single-node Python.

**What problem it solves here.** Five jobs do the actual work — `validation`, `etl_transform`
(Bronze→Silver), `kpi_aggregation` (Silver→Gold), `dynamodb_loader` (Gold→DynamoDB), and `archive`
([glue_jobs.tf](../terraform/glue_jobs.tf)). (Full breakdown in
[Glue_Transformation_Code.md](Glue_Transformation_Code.md).)

**Why over alternatives.** The realistic alternatives were **EMR** (you manage the Spark cluster —
more control, far more operational overhead) and **Lambda** (15-minute limit, no native Spark, bad
fit for large joins). Glue was chosen because it is serverless Spark: no cluster to manage, scales
per job, and integrates directly with the Catalog and S3.

---

## 4. Amazon DynamoDB — the Serving Database

**What it is.** DynamoDB is a fully managed **NoSQL key-value** database that returns items in
single-digit milliseconds at any scale. You design tables around the exact lookups your application
will make.

**What problem it solves here.** It is the **serving layer** the dashboard reads from. Three tables
(`genre_kpis`, `top_songs`, `top_genres`, [main.tf:146](../terraform/main.tf#L146)) hold the final
KPIs, keyed so the app can fetch "top 3 songs for this genre today" in one lookup. (Key design in
[DynamoDB_Key_Design.md](DynamoDB_Key_Design.md).)

**Why over alternatives.** A relational database (RDS/Aurora) would serve the same data but needs
provisioning, scaling management, and is overkill for fixed key-lookups. DynamoDB was chosen
because the access patterns are simple, known in advance, and need to be fast and cheap at scale —
exactly DynamoDB's sweet spot. It is also serverless (on-demand billing), matching the rest of the
stack.

---

## 5. AWS Step Functions — the Orchestrator

**What it is.** A managed service that runs **state machines** — workflows defined as a series of
steps ("states") with branching, waiting, and error handling.

**What problem it solves here.** It is the brain of the pipeline
([step_functions.tf](../terraform/step_functions.tf)): it runs the crawler, waits for it, validates,
transforms, aggregates, loads, and archives — in strict order, with conditional branches (skip if
no files, wait if another run is active) and a single consistent failure path. (Full detail in
[Step_Functions.md](Step_Functions.md).)

**Why over alternatives.** The alternatives were the **Glue Workflow** (can only chain Glue
jobs/crawlers — no branching, no calling other services, no rich alerts) and a **cron/Lambda
script** (you'd hand-code ordering, waiting, retries, and error handling, and host it somewhere).
Step Functions was chosen because it gives ordering, conditional logic, error handling, visibility,
and serverless execution declaratively, and can orchestrate *any* AWS service, not just Glue.

---

## 6. Amazon EventBridge (Rule) — the Event Detector

**What it is.** EventBridge is a serverless **event bus**. Services emit events to it; *rules*
match events by pattern and route them to targets.

**What problem it solves here.** The rule `streams_uploaded`
([messaging.tf:119](../terraform/messaging.tf#L119)) listens for S3 `Object Created` events under
the `streams/` prefix and routes them to the SQS queue. This is what makes the pipeline
**event-driven** — uploading a file *is* the trigger; nobody runs anything by hand.

**Why over alternatives.** S3 can notify Lambda or SNS directly, but EventBridge was chosen for its
rich **content-based filtering** (match only `streams/` uploads, ignore `songs/`/`users/`) and
because it decouples the producer (S3) from the consumer (the queue) cleanly. It's the modern,
flexible front door for AWS events.

---

## 7. Amazon SQS — the Buffer (with a Dead-Letter Queue)

**What it is.** SQS (Simple Queue Service) is a managed **message queue** — a durable buffer that
holds messages until something is ready to process them.

**What problem it solves here.** Two queues ([messaging.tf:65](../terraform/messaging.tf#L65)):

- **main queue** (`pipeline_events`) buffers S3 upload events between EventBridge and Step
  Functions, so a burst of uploads can't overwhelm the system — they queue up and are processed one
  at a time.
- **dead-letter queue** (`pipeline_dlq`) catches "poison" messages that fail to process 3 times
  (`maxReceiveCount = 3`), so one bad event can't block the queue forever and can be inspected
  later.

**Why over alternatives.** Wiring EventBridge straight to Step Functions would lose this buffering
and have no safe place for failed events. SQS was chosen to add **durability and back-pressure**: a
14-day retention on the DLQ means failures are never silently lost, and the queue smooths out
traffic spikes.

---

## 8. Amazon EventBridge Pipes — the Connector

**What it is.** EventBridge Pipes is a managed **point-to-point connector** that polls a source
(like SQS), optionally transforms each message, and delivers it to a target (like Step Functions) —
no code required.

**What problem it solves here.** `sqs_to_sfn` ([messaging.tf:212](../terraform/messaging.tf#L212))
polls the SQS queue and calls `StartExecution` on the state machine, one message at a time
(`batch_size = 1`, `FIRE_AND_FORGET`). It also strips the raw SQS envelope so Step Functions gets a
clean input.

**Why over alternatives.** Before Pipes existed, you'd write a **Lambda function** to poll SQS and
start the state machine — code to write, deploy, monitor, and pay for. Pipes was chosen because it
does exactly this one job natively, with no Lambda to maintain.

---

## 9. Amazon SNS — the Notification Fan-Out

**What it is.** SNS (Simple Notification Service) is a managed **pub/sub** service: publish one
message to a "topic," and it fans out to many subscribers (email, SMS, other services).

**What problem it solves here.** The `pipeline_alerts` topic ([messaging.tf:15](../terraform/messaging.tf#L15))
is the single place failures and successes are published. Both Step Functions (`NotifyFailure`) and
CloudWatch alarms publish to it, and it fans out to **email** subscribers and to **AWS Chatbot →
Slack**.

**Why over alternatives.** You could message Slack or email directly from each source, but that
scatters notification logic everywhere. SNS was chosen as the **infrastructure-level hub**: every
CloudWatch alarm and the state machine's `NotifyFailure` step publish to one topic, and you
add/remove subscribers without touching the pipeline. Note that SNS covers the *infrastructure*
alert path (failures, timeouts, stuck queues); a second, independent path uses a **direct Slack
webhook** from the Glue jobs (`PipelineMonitor`) and the `pipeline_notifier` Lambda for richer,
in-flight stage and pipeline progress messages (see section 10a).

---

## 10. AWS Chatbot — Slack Delivery

**What it is.** A managed service that forwards AWS notifications (from SNS) into Slack or Microsoft
Teams channels, formatting them nicely.

**What problem it solves here.** `aws_chatbot_slack_channel_configuration`
([monitoring.tf:276](../terraform/monitoring.tf#L276)) subscribes a Slack channel to the alerts
topic, so the on-call team sees pipeline failures in Slack in real time (only enabled when Slack IDs
are configured).

**Why over alternatives.** You could build a Lambda + Slack webhook integration yourself. Chatbot
was chosen because it is the managed, no-code path from SNS to Slack, with built-in read-only
access to enrich alerts with metric snapshots.

---

## 10a. AWS Lambda — Pipeline-level Slack Notifier

**What it is.** AWS Lambda runs a single Python function on demand with no servers to provision or
maintain. It is invoked by name, executes, and returns — billing only for the milliseconds it runs.

**What problem it solves here.** The `pipeline_notifier` Lambda
([lambda/pipeline_notifier.py](../lambda/pipeline_notifier.py)) is the **pipeline-level half** of
the two-channel Slack notification architecture. It is invoked by three dedicated Step Functions
states and posts rich Slack Block Kit messages (coloured attachments, bold headers, named fields)
directly to the `SLACK_APP_WEBHOOK_URL` endpoint:

| Step Functions state | When it fires | Slack message |
| --- | --- | --- |
| `NotifyPipelineStarted` | After concurrency guard passes, before `StartRawCrawler` | `:rocket: Pipeline — Started` with execution ID and timestamp |
| `NotifyPipelineSucceeded` | After `ArchiveFiles` succeeds | `:large_green_circle: Pipeline — Succeeded` with execution ID |
| `NotifySlackPipelineFailed` | After `NotifyFailure` (SNS), before `PipelineFailed` | `:red_circle: Pipeline — FAILED` with failed step name and error cause |

All three states catch their own errors and route to the next pipeline state — a Slack delivery
failure can never block or fail the execution. The `requests` library is intentionally avoided;
the function uses Python's built-in `urllib.request` so no Lambda layer or packaging is needed.

This Lambda complements the job-level notifications already in `monitoring/pipeline_monitor.py`:
each Glue job's `PipelineMonitor` stage hooks call `SlackNotifier` directly for per-stage
start/success/fail messages. Together the two components give a complete picture of every run in
Slack from first state to last stage.

**Why Lambda over alternatives.**

- **Direct HTTP call from Step Functions?** Step Functions can call HTTPS endpoints via
  `arn:aws:states:::http:invoke`, but constructing and signing the Slack Block Kit payload inside
  an ASL `Parameters` block is impractical — it becomes a maintenance burden. Lambda lets the
  payload logic live in version-controlled Python.
- **SNS for this path too?** SNS's email/Chatbot formatting is generic plain text. The webhook path
  was specifically chosen for the richer Slack Block Kit format — colours, named fields, structured
  attachments — which SNS cannot produce.
- **Why not put it in one of the Glue jobs?** Pipeline-level events (start, end) happen at the
  Step Functions level, not inside any individual job. Lambda is the right invocation unit for
  Step Functions to call.

**Terraform resource.** Defined in [terraform/lambda.tf](../terraform/lambda.tf). The `archive_file`
data source packages `lambda/pipeline_notifier.py` into a zip at plan time; `source_code_hash`
ensures Lambda is re-deployed whenever the code changes. IAM uses the
`AWSLambdaBasicExecutionRole` managed policy — CloudWatch Logs only, no other permissions needed.
The Step Functions role has an additional `lambda:InvokeFunction` statement scoped to this
function's ARN.

---

## 11. Amazon CloudWatch — Logs, Metrics, and Alarms

**What it is.** CloudWatch is the AWS **observability** service: it stores logs (`log groups`),
collects numeric `metrics`, and runs `alarms` that react when a metric crosses a threshold.

**What problem it solves here.** It is how the pipeline is watched and debugged
([monitoring.tf](../terraform/monitoring.tf), [main.tf:350](../terraform/main.tf#L350)):

- **Log groups** `/aws/glue/<project>` and `/aws/states/<project>` capture every Glue job line and
  every Step Functions transition.
- **Alarms** watch Step Functions failures/timeouts, SQS queue depth/age, and per-Glue-job task
  failures — each routed to SNS.

(Full detail in [CloudWatch_Monitoring.md](CloudWatch_Monitoring.md).)

**Why over alternatives.** CloudWatch is the native, zero-setup destination for AWS service logs and
metrics — Glue and Step Functions emit to it automatically. A third-party tool (Datadog, etc.)
would add cost and integration work for what CloudWatch already does inside the account.

---

## 12. AWS IAM — Permissions and Security

**What it is.** IAM (Identity and Access Management) controls **who can do what** in AWS, via
*roles* (identities a service assumes) and *policies* (the permissions attached to them).

**What problem it solves here.** Every service that acts on another needs a role scoped to exactly
what it must do, following least-privilege:

- a **Glue role** the jobs/crawlers assume to read/write S3, the Catalog, and DynamoDB;
- a **Step Functions role** (`sfn_role`) allowed to start Glue jobs/crawlers, list executions,
  read S3, and publish to SNS;
- a **Pipes role** allowed only to consume SQS and start the state machine
  ([messaging.tf:177](../terraform/messaging.tf#L177));
- a **Firehose role** allowed only to write to the raw bucket and its own log group
  ([ingestion.tf](../terraform/ingestion.tf));
- a **Chatbot role** with read-only CloudWatch access.

(Full detail in [iam-roles-and-policies.md](iam-roles-and-policies.md).)

**Why over alternatives.** The "alternative" — broad admin permissions — is a security
anti-pattern. Scoped IAM roles were chosen so a compromise or bug in one component can't reach
beyond its job, and so each service can only touch the specific resources it needs.

---

## 13. Terraform — Infrastructure as Code (the Foundation)

**What it is.** Terraform is an **Infrastructure-as-Code** tool: you declare your cloud resources in
`.tf` files, and Terraform creates, updates, and destroys them to match. (It is HashiCorp software,
not an AWS service, but it provisions everything above.)

**What problem it solves here.** Every resource in this document is defined in
[terraform/](../terraform/) (`provider.tf` pins AWS provider `~> 5.0`). The entire pipeline —
buckets, tables, jobs, the state machine, queues, alarms, IAM — can be stood up or torn down
reproducibly with one workflow, and `default_tags` stamps every resource with project/environment
metadata.

**Why over alternatives.** The alternatives were clicking through the AWS Console (not repeatable,
error-prone, undocumented) or CloudFormation (AWS-native but more verbose). Terraform was chosen for
its concise syntax, broad provider ecosystem, and a state file that tracks exactly what exists — so
the infrastructure is version-controlled and auditable like the application code.

---

## 14. Service Inventory at a Glance

| Service | Role in pipeline | Chosen over | Key resource |
| --- | --- | --- | --- |
| **Amazon S3** | Stores all data: raw, silver/gold, archive; fires the trigger event | Loading straight to a DB | `aws_s3_bucket.{raw,curated,archive}` |
| **Kinesis Data Firehose** | Ingests producer events, batches them into S3 files | Kinesis Data Streams / direct-to-S3 | `aws_kinesis_firehose_delivery_stream.streams_ingestion` |
| **Glue Crawler** | Infers schema, registers catalog tables | Hard-coding schemas | `aws_glue_crawler.{raw,curated}_crawler` |
| **Glue Data Catalog** | Central metastore; crawler→jobs/Athena contract | Self-managed Hive metastore | `aws_glue_catalog_database.music_db` |
| **Glue Jobs** | Validate, transform, aggregate, load, archive | EMR / Lambda | `aws_glue_job.*` (5 jobs) |
| **Amazon DynamoDB** | Fast serving layer for final KPIs | RDS/Aurora | `aws_dynamodb_table.*` (3 tables) |
| **Step Functions** | Orchestrates the whole workflow with branching + error handling | Glue Workflow / cron script | `aws_sfn_state_machine.pipeline` |
| **EventBridge (rule)** | Detects new S3 uploads, routes them | Direct S3→Lambda/SNS | `aws_cloudwatch_event_rule.streams_uploaded` |
| **Amazon SQS** | Buffers events; DLQ catches poison messages | Direct EventBridge→SFN | `aws_sqs_queue.{pipeline_events,pipeline_dlq}` |
| **EventBridge Pipes** | Connects SQS → Step Functions, no code | A custom Lambda poller | `aws_pipes_pipe.sqs_to_sfn` |
| **Amazon SNS** | Infrastructure-level alert hub (failures, alarms) → email + Chatbot | Per-source notifications | `aws_sns_topic.pipeline_alerts` |
| **AWS Chatbot** | Delivers CloudWatch/SNS alerts into Slack | Custom Lambda + webhook | `aws_chatbot_slack_channel_configuration.*` |
| **AWS Lambda** | Posts pipeline-level Block Kit messages to Slack at start, success, and failure | Bespoke SNS formatting / API Gateway | `aws_lambda_function.pipeline_notifier` |
| **Amazon CloudWatch** | Logs, metrics, alarms for the whole pipeline | Third-party observability | `aws_cloudwatch_log_group.*`, `aws_cloudwatch_metric_alarm.*` |
| **AWS IAM** | Least-privilege roles for every component | Broad admin permissions | `aws_iam_role.*` (5 roles) |
| **Terraform** | Provisions and version-controls all of the above | Console clicks / CloudFormation | all `.tf` files |

The design philosophy across every choice is consistent: **prefer managed, serverless, event-driven
services**, wire them together with the smallest possible permissions, and define the entire system
as code so it is reproducible and auditable.
