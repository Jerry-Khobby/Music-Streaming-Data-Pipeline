# Music Streaming Real-Time Data Pipeline

A production-grade, event-driven ETL pipeline built entirely on AWS that ingests music streaming
events, transforms them through a three-tier data lake, computes daily KPIs per genre, loads the
results into DynamoDB for millisecond-latency serving, and provides full real-time observability
through two independent Slack notification channels.

Every resource is provisioned through Terraform. Every architectural decision is documented in the
`docs/` directory.

---

## What the Pipeline Produces

For each day of streaming activity, the pipeline computes and serves three sets of KPIs:

| Output | Description | Served from |
| --- | --- | --- |
| Genre KPIs | Listen count, unique listeners, total and average listening time per genre per day | DynamoDB `genre_kpis` |
| Top Songs | Top 3 songs by play count per genre per day | DynamoDB `top_songs` |
| Top Genres | Top 5 genres by listen count per day | DynamoDB `top_genres` |

All three datasets are also queryable via Amazon Athena for ad-hoc SQL analysis.

---

## Architecture

The pipeline follows a Medallion Architecture — Bronze (raw) to Silver (enriched) to Gold (aggregated)
— with event-driven triggering, serverless compute, and full observability at every layer.

```text
Producer --> Firehose --> S3 Bronze --> EventBridge --> SQS --> Pipe --> Step Functions
                                                                              |
                          +---------------------------------------------------+
                          |
                          v
             Glue Crawler (schema discovery)
                          |
                          v
             Glue: Validation --> Silver (enriched streams)
                          |
                          v
             Glue: KPI Aggregation --> Gold (genre KPIs, top songs, top genres)
                          |
                          v
             Glue: DynamoDB Loader --> DynamoDB (3 tables, ms-latency serving)
             Glue: Archive --> S3 Archive (Glacier after 90 days)
                          |
                          v
             Notifications: Lambda + PipelineMonitor --> Slack (live, stage-level)
                            SNS + Chatbot --> Slack + Email (infrastructure alerts)
```

The full resource map with every arrow labeled is in
[docs/Architecture_Diagram_Sketch.md](docs/Architecture_Diagram_Sketch.md).

---

## Technology Stack

| Layer | Service | Purpose |
| --- | --- | --- |
| Ingestion | Kinesis Data Firehose | Buffers streaming events, lands JSON batch files in S3 |
| Storage | Amazon S3 (3 buckets) | Bronze, Silver/Gold, and Archive data lake layers |
| Schema | AWS Glue Data Catalog | Central metastore for all table definitions and partitions |
| Schema discovery | AWS Glue Crawlers (2) | Infer schema from raw files; refresh Athena partitions |
| Transformation | AWS Glue Jobs (5) | PySpark ETL jobs: validate, enrich, aggregate, load, archive |
| Orchestration | AWS Step Functions | State machine with conditional branching, polling loops, error handling |
| Event trigger | Amazon EventBridge | Detects S3 uploads, routes to the pipeline |
| Queue | Amazon SQS | Buffers events; dead-letter queue for poison messages |
| Connector | EventBridge Pipes | Connects SQS to Step Functions with no code |
| Serving | Amazon DynamoDB | Millisecond-latency KPI lookups, on-demand billing |
| Analytics | Amazon Athena | Ad-hoc SQL queries over Gold layer Parquet |
| Notifications | AWS Lambda | Pipeline-level Slack Block Kit messages |
| Notifications | Slack Incoming Webhook | Direct stage-level notifications from Glue jobs |
| Alerts | Amazon SNS | Infrastructure-level alert hub (failures, timeouts, stuck queues) |
| Alerts | AWS Chatbot | Forwards SNS alerts to Slack channel |
| Observability | Amazon CloudWatch | Log groups, metrics, 9 alarms, X-Ray traces |
| IaC | Terraform | Provisions and version-controls all AWS resources |
| Security | AWS IAM | Least-privilege roles for every component |

---

## Repository Structure

```text
.
+-- glue_jobs/                    PySpark and Python job scripts
|   +-- validation_job.py         Step 1: validate raw catalog tables
|   +-- etl_transform_job.py      Step 2: join + dedup, Bronze to Silver
|   +-- kpi_aggregation_job.py    Step 3: compute genre KPIs, Silver to Gold
|   +-- dynamodb_loader.py        Step 4: load Gold Parquet into DynamoDB
|   +-- archive_job.py            Step 5: copy processed files to archive
|
+-- monitoring/                   Shared Python notification package
|   +-- __init__.py               Package exports
|   +-- pipeline_monitor.py       PipelineMonitor context manager (stage hooks)
|   +-- notifier.py               SlackNotifier + pipeline-level notification methods
|   +-- logger.py                 Structured logger setup
|
+-- lambda/                       Lambda function for pipeline-level Slack alerts
|   +-- pipeline_notifier.py      Handles started/succeeded/failed events
|
+-- terraform/                    Infrastructure as Code
|   +-- provider.tf               AWS provider, version constraints, default tags
|   +-- variables.tf              All configurable input values
|   +-- main.tf                   S3 buckets, DynamoDB tables, IAM role, Glue DB
|   +-- ingestion.tf              Kinesis Data Firehose, Firehose IAM role
|   +-- glue_jobs.tf              5 Glue job definitions, workflow, triggers, script uploads
|   +-- step_functions.tf         Step Functions state machine, SFN IAM role
|   +-- messaging.tf              SNS, SQS, EventBridge rule, EventBridge Pipe, Pipe IAM role
|   +-- monitoring.tf             CloudWatch alarms, EventBridge rules, Chatbot
|   +-- lambda.tf                 Lambda function, packaging, Lambda IAM role
|   +-- outputs.tf                Resource names and ARNs printed after apply
|
+-- step_functions/
|   +-- pipeline_definition.json  Human-readable copy of the state machine definition
|
+-- tests/                        Pytest unit tests for job logic
|   +-- conftest.py               Shared fixtures
|   +-- test_validation_job.py
|   +-- test_etl_transform_job.py
|   +-- test_kpi_aggregation_job.py
|   +-- test_dynamodb_loader.py
|
+-- stubs/                        Local stubs for awsglue modules (for testing)
+-- data/                         Sample CSV files for local testing (gitignored)
+-- producer/                     Producer script that sends events to Firehose
+-- docs/                         Full documentation suite (31 documents)
+-- .gitignore
+-- pytest.ini
+-- requirements-test.txt
```

---

## Prerequisites

- An AWS account with permissions to create IAM, S3, DynamoDB, Glue, Step Functions, SQS, SNS,
  EventBridge, Kinesis Firehose, Lambda, and CloudWatch resources.
- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.3.0
- [AWS CLI](https://aws.amazon.com/cli/) configured with the target account credentials
- Python 3.9+ (for running tests locally)
- A Slack app with an Incoming Webhook URL (optional — disabling it skips all Slack notifications,
  CloudWatch and email alerts still work)

---

## Configuration

All configurable values are in `terraform/variables.tf`. Sensitive values go in
`terraform/terraform.tfvars` (gitignored — never commit this file).

Create `terraform/terraform.tfvars` with the following:

```hcl
# Required
aws_region  = "us-east-1"
environment = "dev"

# Optional — leave empty to disable
alert_email        = "you@example.com"
slack_workspace_id = "T01234ABCDE"
slack_channel_id   = "C01234ABCDE"
slack_webhook_url  = "https://hooks.slack.com/services/..."
```

Key variables reference:

| Variable | Default | Description |
| --- | --- | --- |
| `aws_region` | `us-east-1` | AWS region for all resources |
| `environment` | `dev` | Appended to all bucket names (dev/staging/prod) |
| `raw_bucket_name` | `music-streaming-raw` | Bronze S3 bucket base name |
| `curated_bucket_name` | `music-streaming-curated` | Silver/Gold S3 bucket base name |
| `archive_bucket_name` | `music-streaming-archive` | Archive S3 bucket base name |
| `dynamodb_billing_mode` | `PAY_PER_REQUEST` | On-demand or PROVISIONED |
| `glue_database_name` | `music_streaming_db` | Glue Data Catalog database name |
| `alert_email` | `""` | Email address for SNS failure alerts |
| `slack_workspace_id` | `""` | Slack workspace ID for AWS Chatbot integration |
| `slack_channel_id` | `""` | Slack channel ID for AWS Chatbot |
| `slack_webhook_url` | `""` | Slack Incoming Webhook URL for direct notifications |

---

## Deployment

```bash
cd terraform

# Download the AWS provider
terraform init

# Preview what will be created
terraform plan

# Deploy (approximately 5 minutes)
terraform apply
```

After apply, Terraform prints the names and ARNs of every provisioned resource. Copy the
`firehose_stream_name` output — you will need it to configure the producer.

To deploy to a different environment without touching dev resources:

```bash
terraform apply -var="environment=prod"
```

---

## Uploading Reference Data

The songs and users CSVs are static reference data that the ETL transform job joins against. Upload
them once before running the pipeline:

```bash
RAW_BUCKET=$(terraform -chdir=terraform output -raw raw_bucket_name)

aws s3 cp data/songs/songs.csv    s3://$RAW_BUCKET/songs/
aws s3 cp data/users/users.csv    s3://$RAW_BUCKET/users/
```

---

## Running the Pipeline

The pipeline is event-driven. It starts automatically when a file lands under `streams/` in the raw
S3 bucket. You can trigger it in two ways:

### Option A — Use the producer script (realistic ingestion)

The producer sends play events to Kinesis Data Firehose, which buffers and lands a JSON batch file
in S3, which triggers the pipeline:

```bash
cd producer
python producer.py --stream-name <firehose_stream_name_from_terraform_output>
```

### Option B — Upload a CSV directly

```bash
aws s3 cp data/streams/streams1.csv s3://$RAW_BUCKET/streams/
```

Either method triggers the EventBridge rule, which routes the event through SQS, the EventBridge
Pipe, and into Step Functions within seconds.

**Manual run via Glue Workflow** (fallback, for testing individual jobs):

```bash
WORKFLOW=$(terraform -chdir=terraform output -raw glue_workflow_name)
aws glue start-workflow-run --name $WORKFLOW
```

---

## How the Pipeline Executes

Once triggered, Step Functions runs the following sequence:

1. **Normalize input** — strips the raw SQS envelope from the execution input.
2. **Concurrency guard** — if another execution is already running, this one waits and polls every
   60 seconds until the older run finishes. This prevents data races in Silver and Gold.
3. **Slack: Pipeline Started** — the `pipeline_notifier` Lambda posts a start message to Slack.
4. **Start raw crawler** — fires the Glue crawler on the raw bucket. Polls every 45 seconds until
   the crawler reports READY.
5. **Catalog and stream check** — verifies the streams table was registered and that actual files
   exist under `streams/`. If not, the execution exits cleanly with no failure alert.
6. **Validation job** — confirms all three tables (streams, songs, users) exist in the Glue
   catalog, are non-empty, and contain the required columns. Fails early with a clear error if not.
7. **ETL transform job** — joins streams to songs on `track_id`, derives `stream_date` from
   `listen_time`, deduplicates on `(user_id, track_id, listen_time)`, and writes enriched Parquet
   to the Silver layer, partitioned by date.
8. **KPI aggregation job** — reads Silver, computes listen counts, unique listeners, listening
   times, top songs per genre, and top genres per day; writes Gold Parquet partitioned by date.
9. **DynamoDB loader** — reads all three Gold datasets, deduplicates on primary key, converts
   numeric types, and upserts every row into the three DynamoDB tables using batch writes.
10. **Curated crawler** — refreshes the Glue catalog for the Gold layer so Athena can immediately
    query the new date partitions.
11. **Archive job** — copies every processed stream file from `raw/streams/` to the archive bucket
    and deletes the originals. Only the files that were present at the start of this execution are
    archived; files that arrived during the run remain for the next execution.
12. **Slack: Pipeline Succeeded** — the Lambda posts a success message to Slack.

---

## Notification System

The pipeline uses two independent notification channels. Both run simultaneously — losing one does
not affect the other.

### Channel 1 — Direct Slack webhook (in-flight, stage-granular)

Every Glue job imports the `monitoring/` package. The `PipelineMonitor` context manager wraps each
named stage and calls `SlackNotifier` on start, success, and failure as the stage runs. A typical
run produces a live Slack thread with an "In Progress" message at the start of each stage and a
"Succeeded" message at its completion.

At the pipeline level, the `pipeline_notifier` Lambda is invoked by three Step Functions states to
post Block Kit messages: one when the run starts, one on success, and one on failure. All three
invocation states catch errors and route forward — a Slack delivery failure cannot block or fail
the pipeline.

Configuration: set `slack_webhook_url` in `terraform.tfvars`.

### Channel 2 — SNS / CloudWatch / Chatbot (infrastructure-level)

CloudWatch alarms monitor five failure signals independently of the state machine itself:

- Step Functions execution failed
- Step Functions execution timed out
- SQS dead-letter queue received a message (a run may never have started)
- SQS main queue messages stuck older than 15 minutes
- Per-Glue-job task failures (one alarm per job, five total)

Each alarm publishes to the `pipeline_alerts` SNS topic, which fans out to the AWS Chatbot Slack
integration and to an email subscriber. An EventBridge rule also catches the state machine's own
`SUCCEEDED` event and sends a success message through SNS, so the email path also receives
positive confirmation.

Because this channel is independent of the pipeline itself, it catches failure modes that the
state machine cannot observe — a broken EventBridge Pipe, a misconfigured IAM role, or a stuck SQS
queue produce no Step Functions activity and would be invisible to an in-process-only alerting scheme.

Configuration: set `slack_workspace_id`, `slack_channel_id`, and `alert_email` in `terraform.tfvars`.

---

## Monitoring and Debugging

**CloudWatch log groups** (30-day retention):

| Log group | Contents |
| --- | --- |
| `/aws/glue/music-streaming` | All five Glue jobs: driver logs, executor logs, job insights |
| `/aws/states/music-streaming` | All Step Functions state transitions with full input/output data |
| `/aws/kinesisfirehose/music-streaming-streams-ingestion` | Firehose S3 delivery errors |

**Debugging a failed run:**

1. The alarm or Slack failure message names the failing layer. For a Glue job failure, the alarm
   names the exact job.
2. Open the Step Functions execution in the console. The visual graph highlights the failed state
   in red. Click it to see the error message and cause captured by the `Catch` block.
3. Cross-reference the Glue job's log group. The driver stream contains the Python stack trace;
   the job-insights stream summarizes the probable root cause in plain language.
4. For an SQS alarm, inspect the dead-letter queue:
   `aws sqs receive-message --queue-url <dlq-url>`

---

## Data Model

### `genre_kpis`

Primary key: `genre_date` (String, e.g. `"Afrobeats#2026-05-17"`)

Stores daily listen metrics per genre. A single `GetItem` on `genre_date` returns all KPIs for
that genre on that day in single-digit milliseconds.

### `top_songs`

Primary key: `genre_date` (String), sort key: `rank` (Number, 1-3)

Stores the top 3 songs for each genre per day. A single `Query` on `genre_date` returns all three
rows ordered by rank. Rank ties are broken deterministically by `track_id` using a window function,
so the result is always exactly 3 rows.

### `top_genres`

Primary key: `date` (String, e.g. `"2026-05-17"`), sort key: `rank` (Number, 1-5)

Stores the top 5 genres across all genres for each day. A single `Query` on `date` returns all
five rows ordered by rank. Dashboard queries for "what are today's top genres?" need one API call.

---

## Idempotency

The pipeline is safe to re-run against the same files:

- The ETL transform job uses dynamic partition overwrite mode. Re-processing the same date
  partition rewrites it to the same deduplicated result.
- The DynamoDB loader uses `put_item` (upsert by primary key). Writing the same KPI row twice
  overwrites with identical data — no duplicates accumulate.
- The archive job only archives the specific file keys that were present at the start of the
  execution, not whatever is in `streams/` at the time of archiving.

---

## Running Tests

```bash
pip install -r requirements-test.txt
pytest
```

Tests use local stubs in `stubs/` for `awsglue` modules so they run without a Glue or Spark
runtime. Fixtures are in `tests/conftest.py`. Coverage targets: statement and branch coverage for
all job scripts.

---

## Cost

The pipeline's total cost at daily run frequency is approximately $7-21 per month, depending on
data volume. Glue compute is the dominant line item. The full breakdown — including per-run
estimates, standing costs, optimised vs unoptimised decisions, and the two actionable fixes — is in
[docs/Cost_Optimisation.md](docs/Cost_Optimisation.md).

The complete notification stack (Lambda, SNS, Chatbot, webhook calls) adds effectively $0 to the
monthly bill.

---

## Environment Management

The `environment` variable is appended to every bucket name (`music-streaming-raw-dev`,
`music-streaming-raw-prod`). This means `dev` and `prod` can coexist in the same AWS account with
zero naming conflicts. Deploy to a new environment with:

```bash
terraform apply -var="environment=prod"
terraform destroy -var="environment=dev"
```

---

## Security

- All S3 buckets: AES-256 server-side encryption, private access only (no public ACLs).
- All DynamoDB tables: point-in-time recovery enabled (35-day restore window).
- IAM: one role per service, each scoped to exactly the resources and actions it needs. No shared
  admin roles.
- Secrets: `terraform.tfvars` and `.env` are gitignored. Terraform state files (`*.tfstate`,
  `*.tfstate.*`) are gitignored — they contain resolved secret values and must never be committed.

---

## Documentation

The `docs/` directory contains 31 documents covering every aspect of the pipeline in depth.

| Document | What it covers |
| --- | --- |
| Architecture_Diagram_Sketch.md | Full resource map and flow reference for the draw.io diagram |
| End_to_End_Data_Flow.md | A single event traced from producer tap to DynamoDB item |
| All_Services_Used.md | Every AWS service: what it is, why it was chosen |
| Step_Functions.md | State machine states, patterns, error handling |
| Glue_Crawlers_and_Jobs.md | Crawler configuration, job code walkthrough |
| Glue_Transformation_Code.md | ETL logic: joins, deduplication, aggregations |
| Monitoring_and_Observability.md | Logging, monitoring, alerting — the three layers |
| CloudWatch_Monitoring.md | Log groups, metrics, alarms, debugging playbook |
| Lambda_Pipeline_Notifier.md | Lambda design, three event types, integration details |
| Error_Handling_and_Retry.md | Catch blocks, exponential backoff, dual alert channels |
| Cost_Optimisation.md | Per-run and standing costs, optimised vs unoptimised, fix recommendations |
| Medallion_Architecture.md | Bronze / Silver / Gold data lake pattern |
| DynamoDB_Key_Design.md | Table design, access patterns, composite key rationale |
| Streaming_Ingestion_Firehose.md | Firehose vs Kinesis Data Streams, buffering design |
| Infrastructure_as_Code_Terraform.md | Terraform file structure, variable strategy |
| Event_Driven_Architecture.md | Why event-driven over scheduled |
| Idempotency_in_Data_Pipelines.md | How idempotency is enforced at every stage |
| Archival_Strategy.md | Copy-then-delete, Glacier lifecycle, audit retention |
| Data_Validation.md | Validation logic, failure modes, column checks |
| KPI_Design_and_Computation.md | What each KPI means and how it is computed |
| Partitioning_Strategy_S3.md | Date partitioning, partition pruning, Athena performance |
| Schema_Management_and_Glue_Catalog.md | Catalog-driven schema, crawler update policies |
| SQS_and_Dead_Letter_Queue.md | Queue design, DLQ, visibility timeout |
| Amazon_EventBridge.md | Rules, content filtering, Pipes |
| Amazon_Athena.md | Querying Gold layer, partition management |
| Real_Time_vs_Batch_Justification.md | Why micro-batch over pure streaming for this use case |
| Data_Lineage_and_Auditability.md | Traceability from raw file to DynamoDB item |
| Encryption_in_This_Pipeline.md | Encryption at rest and in transit |
| Idempotency_in_Data_Pipelines.md | Deduplication and safe re-processing |
| S3_Bucket_Layers.md | Three-bucket medallion storage design |
| DynamoDB_Sample_Queries.md | Example queries for each table |
