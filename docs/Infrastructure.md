# Music Streaming Data Pipeline — Infrastructure Documentation

> **Project:** Music Streaming Real-Time Data Pipeline
> **IaC Tool:** Terraform >= 1.3.0
> **Cloud Provider:** AWS
> **Author:** Data Engineering Team
> **Last Updated:** 2026-05-19

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Infrastructure Summary](#2-infrastructure-summary)
3. [Project Structure](#3-project-structure)
4. [Terraform Files Explained](#4-terraform-files-explained)
5. [Resource Documentation](#5-resource-documentation)
   - [S3 Buckets](#51-amazon-s3-buckets)
   - [DynamoDB Tables](#52-amazon-dynamodb-tables)
   - [IAM Role](#53-iam-role--policies)
   - [Glue Data Catalog](#54-aws-glue-data-catalog)
   - [Glue Crawlers](#55-aws-glue-crawlers)
   - [CloudWatch Log Groups](#56-amazon-cloudwatch-log-groups)
6. [Architecture Best Practices](#6-architecture-best-practices)
7. [Edge Case Handling](#7-edge-case-handling)
8. [How to Deploy](#8-how-to-deploy)
9. [Outputs Reference](#9-outputs-reference)
10. [Teardown](#10-teardown)

---

## 1. Project Overview

A music streaming service generates millions of user events every day — every play, skip, and repeat is captured as a streaming event. This infrastructure provisions the foundational AWS resources that power an automated data pipeline responsible for:

- **Ingesting** raw streaming events (CSV files) from Amazon S3
- **Validating and transforming** that data using AWS Glue
- **Computing daily KPIs** at the genre level — listen counts, unique listeners, top songs, top genres
- **Storing results** in Amazon DynamoDB for millisecond-latency lookups by downstream applications
- **Archiving** processed files to prevent duplicate processing

All infrastructure is provisioned and managed through **Terraform**, ensuring the environment is reproducible, version-controlled, and deployable in minutes rather than hours of console clicking.

---

## 2. Infrastructure Summary

| Resource | Count | Purpose |
|---|---|---|
| S3 Buckets | 3 | Raw, Curated, Archive storage layers |
| DynamoDB Tables | 3 | Genre KPIs, Top Songs, Top Genres |
| IAM Role | 1 | Glue job execution permissions |
| IAM Policy Attachments | 4 | S3, DynamoDB, Glue, CloudWatch access |
| Glue Catalog Database | 1 | Central schema registry |
| Glue Crawlers | 2 | Raw schema detection + Curated partition updates |
| CloudWatch Log Groups | 2 | Glue job logs + Step Functions logs |

**Total resources provisioned by `terraform apply`: 21**

---

## 3. Project Structure

```
music-streaming-pipeline/
├── terraform/
│   ├── provider.tf        # AWS provider config and version constraints
│   ├── variables.tf       # All configurable input values
│   ├── main.tf            # All resource definitions
│   └── outputs.tf         # Values printed after apply
│
├── glue_jobs/
│   ├── validation_job.py
│   ├── etl_transform_job.py
│   ├── kpi_aggregation_job.py
│   ├── dynamodb_loader_job.py
│   └── archive_job.py
│
├── step_functions/
│   └── state_machine_definition.json
│
├── data/
│   ├── songs/songs.csv
│   ├── streams/streams1.csv
│   ├── streams/streams2.csv
│   ├── streams/streams3.csv
│   └── users/users.csv
│
└── INFRASTRUCTURE.md      # This file
```

---

## 4. Terraform Files Explained

### `provider.tf`

Defines the cloud provider, AWS region, and Terraform version requirements.

```hcl
terraform {
  required_version = ">= 1.3.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}
```

**Why this matters for the project:**
The `default_tags` block automatically applies `Project`, `Environment`, and `ManagedBy` tags to every single resource created. This means every S3 bucket, DynamoDB table, and IAM role is identifiable in AWS Cost Explorer, making it easy to track exactly how much this pipeline costs per environment.

---

### `variables.tf`

Centralises all configurable values. No hardcoded strings exist in `main.tf`.

**Key variables and their role:**

| Variable | Default | Why it exists |
|---|---|---|
| `aws_region` | `us-east-1` | Single place to move the entire pipeline to another region |
| `environment` | `dev` | Appended to bucket names so `dev` and `prod` never conflict |
| `raw_bucket_name` | `music-streaming-raw` | Controls the Bronze layer bucket name |
| `curated_bucket_name` | `music-streaming-curated` | Controls the Silver/Gold layer bucket name |
| `archive_bucket_name` | `music-streaming-archive` | Controls where processed files are stored |
| `dynamodb_billing_mode` | `PAY_PER_REQUEST` | On-demand pricing — no capacity planning needed |
| `glue_database_name` | `music_streaming_db` | The Glue Catalog database all jobs reference |
| `glue_role_name` | `glue-pipeline-role` | The single IAM role all Glue jobs assume |

**Best practice applied:** The `environment` variable is appended to all bucket names (e.g. `music-streaming-raw-dev`). This is critical because S3 bucket names are globally unique across all AWS accounts. It also means you can run `dev` and `prod` in the same AWS account without naming conflicts.

---

### `main.tf`

The core file. Contains all resource definitions organised into logical sections:

1. S3 Buckets and folder structure
2. DynamoDB Tables
3. IAM Role and policy attachments
4. Glue Data Catalog Database
5. Glue Crawlers
6. CloudWatch Log Groups

Each section is clearly commented explaining what it does and why it exists in the pipeline.

---

### `outputs.tf`

Prints key resource identifiers after `terraform apply` completes. These values are referenced directly in Glue job scripts and Step Functions definitions.

**Why outputs matter:** Without outputs, a developer would need to navigate the AWS console to find the exact bucket name, table name, or role ARN before writing any code. Outputs surface all of that instantly in the terminal.

---

## 5. Resource Documentation

### 5.1 Amazon S3 Buckets

Three buckets implement the **Medallion Architecture** — a data engineering pattern where data moves through progressively cleaner layers.

---

#### `aws_s3_bucket.raw` — Bronze Layer

```
Bucket name: music-streaming-raw-{environment}
```

**What it stores:**
Raw, unmodified CSV files exactly as they arrive from the source:
```
music-streaming-raw-dev/
├── songs/
│   └── songs.csv           # track_id, artists, album, genre
├── streams/
│   ├── streams1.csv        # user_id, track_id, listen_time
│   ├── streams2.csv
│   └── streams3.csv
└── users/
    └── users.csv           # user_id, user_name, user_age, user_country
```

**Features enabled:**

| Feature | Configuration | Reason |
|---|---|---|
| Versioning | Enabled | Protects against accidental overwrites. If a bad file lands, the previous version is recoverable |
| Server-side encryption | AES256 | All data encrypted at rest — security baseline |
| Folder placeholders | Empty S3 objects | Creates visible folder structure in the console before any files are uploaded |
| `force_destroy` | true | Allows `terraform destroy` to clean up even if the bucket has files — useful in dev |

**Role in the pipeline:**
This is the **source of truth**. Files here are never modified by the pipeline. If any downstream job fails or produces incorrect results, this bucket allows a full replay from the original raw data.

---

#### `aws_s3_bucket.curated` — Silver/Gold Layer

```
Bucket name: music-streaming-curated-{environment}
```

**What it stores:**
```
music-streaming-curated-dev/
├── silver/     # Cleaned and joined data written by the PySpark ETL job
└── gold/       # Aggregated KPI results written by the KPI aggregation job
```

**Features enabled:**

| Feature | Configuration | Reason |
|---|---|---|
| Versioning | Enabled | KPI results are overwritten daily — versioning allows rollback to yesterday's output |
| Server-side encryption | AES256 | KPI data can contain business-sensitive metrics |

**Role in the pipeline:**
The Silver layer holds the cleaned, joined dataset (streams + songs + users). The Gold layer holds the final computed KPIs. The DynamoDB Loader job reads from Gold. Amazon Athena also queries Gold directly for ad-hoc SQL analysis.

---

#### `aws_s3_bucket.archive` — Archive Layer

```
Bucket name: music-streaming-archive-{environment}
```

**Features enabled:**

| Feature | Configuration | Reason |
|---|---|---|
| Lifecycle rule | Transition to GLACIER after 90 days | Archive files are rarely accessed after processing. Moving them to Glacier reduces storage cost by ~70% automatically |
| Server-side encryption | AES256 | Consistent security posture across all buckets |

**Role in the pipeline:**
After every successful pipeline run, the Archive Glue job moves the processed stream files from the raw bucket into this archive bucket. This is a **critical idempotency mechanism** — it ensures the same file is never processed twice. If streams1.csv has been archived, the crawler will not pick it up in a future run.

---

### 5.2 Amazon DynamoDB Tables

Three tables, each designed around a specific **query access pattern**. DynamoDB is a NoSQL database — table design starts with how the data will be queried, not how it is stored.

All tables use `PAY_PER_REQUEST` billing and have **Point-in-Time Recovery (PITR)** enabled.

---

#### `aws_dynamodb_table.genre_kpis` — Genre KPIs Table

```
Table name:      genre_kpis
Partition key:   genre_date    (String)   e.g. "Afrobeats#2026-05-17"
Billing:         PAY_PER_REQUEST
PITR:            Enabled
```

**What it stores per item:**
```json
{
  "genre_date":        "Afrobeats#2026-05-17",
  "genre":             "Afrobeats",
  "date":              "2026-05-17",
  "listen_count":      48200,
  "unique_listeners":  12400,
  "total_listen_time": 9876543,
  "avg_listen_time":   204.7
}
```

**Why this key design:**
A downstream app asking "what are the KPIs for Afrobeats today?" makes a single GetItem call with `genre_date = "Afrobeats#2026-05-17"`. One network call, single-digit milliseconds. No scans, no filters.

---

#### `aws_dynamodb_table.top_songs` — Top Songs Table

```
Table name:      top_songs
Partition key:   genre_date    (String)   e.g. "Afrobeats#2026-05-17"
Sort key:        rank          (Number)   1, 2, or 3
Billing:         PAY_PER_REQUEST
PITR:            Enabled
```

**What it stores per item:**
```json
{
  "genre_date":    "Afrobeats#2026-05-17",
  "rank":          1,
  "track_id":      "T445",
  "song_name":     "Calm Down",
  "artist":        "Rema",
  "listen_count":  8900
}
```

**Why this key design:**
The sort key `rank` means all 3 top songs for a genre on a given day are stored in the same partition. A single Query call retrieves all 3 rows ordered by rank. The partition and sort key together also enforce uniqueness — there can only ever be one rank-1 song per genre per day.

---

#### `aws_dynamodb_table.top_genres` — Top Genres Table

```
Table name:      top_genres
Partition key:   date    (String)   e.g. "2026-05-17"
Sort key:        rank    (Number)   1 through 5
Billing:         PAY_PER_REQUEST
PITR:            Enabled
```

**What it stores per item:**
```json
{
  "date":          "2026-05-17",
  "rank":          1,
  "genre":         "Afrobeats",
  "listen_count":  48200
}
```

**Why this key design:**
A dashboard asking "what are today's top 5 genres?" makes one Query call on `date = "2026-05-17"` and gets back 5 items sorted by rank. Clean and fast.

---

**Why Point-in-Time Recovery is enabled on all tables:**
PITR allows restoring any table to any second within the last 35 days. If a bug in the DynamoDB Loader job writes incorrect KPI values, the table can be restored to the state before the bad write without any data loss. This is non-negotiable for production data stores.

---

### 5.3 IAM Role & Policies

```
Role name: glue-pipeline-role
Assumed by: glue.amazonaws.com
```

**Attached policies:**

| Policy | Type | Why it is needed |
|---|---|---|
| `AWSGlueServiceRole` | AWS Managed | Allows Glue to write logs to CloudWatch, access Glue APIs, and manage job runs |
| `AmazonS3FullAccess` | AWS Managed | Glue jobs must read from the raw bucket, write to curated, and move files to archive |
| `AmazonDynamoDBFullAccess` | AWS Managed | The DynamoDB Loader job must write items into all 3 tables |
| `CloudWatchLogsFullAccess` | AWS Managed | All Glue jobs stream logs to CloudWatch for monitoring and debugging |

**Trust policy — why it matters:**
The trust policy restricts which AWS service can assume this role. By specifying `glue.amazonaws.com` only, this role cannot be assumed by EC2, Lambda, or any other service — only Glue jobs. This follows the principle of least privilege at the service level.

**One role for all jobs — deliberate design decision:**
A single role is used across all 5 Glue jobs rather than one role per job. For a project of this scope, this is the pragmatic choice. In a large enterprise with multiple teams, you would create per-job roles with tighter permissions. Here it keeps the Terraform code clean and the mental model simple.

---

### 5.4 AWS Glue Data Catalog

```
Database name: music_streaming_db
```

**What it contains after the crawler runs:**

| Catalog Table | Source | Columns |
|---|---|---|
| `songs` | `s3://raw/songs/songs.csv` | id, track_id, artists, album, genre |
| `streams` | `s3://raw/streams/*.csv` | user_id, track_id, listen_time |
| `users` | `s3://raw/users/users.csv` | user_id, user_name, user_age, user_country, created_at |
| `gold` | `s3://curated/gold/` | KPI output (after pipeline runs) |

**Role in the pipeline:**
The Data Catalog is the shared schema registry. Every Glue PySpark job references it by database and table name rather than hardcoding S3 paths and schemas. This means:

- If a column is added to `songs.csv`, the Crawler updates the Catalog automatically and all jobs adapt
- Athena can query any table by name without knowing the underlying S3 path
- Schema lineage is tracked centrally — you can always see what a table looked like at any point in time

---

### 5.5 AWS Glue Crawlers

Two crawlers are provisioned, each serving a distinct purpose in the pipeline lifecycle.

---

#### `aws_glue_crawler.raw_crawler` — Bronze Crawler

```
Name:    music-streaming-raw-crawler
Targets: s3://raw/songs/
         s3://raw/streams/
         s3://raw/users/
```

**When it runs:**
This is the **first step inside the Step Functions state machine**. Before any validation or transformation happens, this crawler scans the raw bucket, detects the schema of any new files, and registers or updates tables in the Glue Data Catalog.

**Why it must run first:**
The validation job checks that all required columns exist. To check columns, it needs the schema. The schema comes from the Catalog. The Catalog is only accurate after the Crawler runs. The sequence is non-negotiable.

**Schema change policy:**
```
update_behavior = "UPDATE_IN_DATABASE"   # if schema changes, update the catalog table
delete_behavior = "LOG"                  # if a file disappears, log it — don't delete the table
```

The `delete_behavior = "LOG"` setting is important. If a stream file is missing or has been archived, the crawler will not delete the corresponding Catalog table — it will only log a warning. This prevents downstream jobs from failing because a table disappeared.

---

#### `aws_glue_crawler.curated_crawler` — Silver/Gold Crawler

```
Name:    music-streaming-curated-crawler
Target:  s3://curated/gold/
```

**When it runs:**
This is **Step 5 in the Step Functions state machine**, immediately after the KPI Aggregation job writes new Parquet files to the Gold layer.

**Why it must run after the KPI job:**
Glue writes data to S3 partitioned by date, creating new folder paths like:
```
gold/year=2026/month=05/day=17/
```

Athena needs to know these partitions exist to query them. Without running the Crawler (or manually calling `MSCK REPAIR TABLE`), Athena would not see any data written after the initial table creation. Running this Crawler automatically after every pipeline execution keeps Athena's partition list perfectly up to date.

---

### 5.6 Amazon CloudWatch Log Groups

```
/aws/glue/music-streaming         # All Glue job logs
/aws/states/music-streaming       # All Step Functions execution logs
Retention: 30 days
```

**Why 30-day retention:**
CloudWatch storage costs money. Logs older than 30 days have diminishing debugging value for a batch pipeline that runs daily. If longer retention is required for compliance, increase the `retention_in_days` variable. Logs older than 30 days that need archiving can be exported to S3 at a fraction of the cost.

**What gets logged:**
- Every Glue job: start time, end time, records processed, errors, Python print statements
- Every Step Functions execution: which step ran, how long it took, which step failed and why
- Every Crawler run: how many tables were created or updated, schema changes detected

---

## 6. Architecture Best Practices

### Medallion Architecture (Bronze → Silver → Gold)

The three S3 buckets implement the industry-standard medallion pattern:

```
Raw CSV files          S3 Bronze (raw)
       ↓
Cleaned + Joined       S3 Silver (curated/silver/)
       ↓
Aggregated KPIs        S3 Gold (curated/gold/)
       ↓
Fast lookups           DynamoDB
```

**Why this matters:**
Each layer is independently queryable and replayable. If the KPI aggregation logic has a bug, you do not need to re-ingest data from scratch — you replay from Silver. If the ETL transform has a bug, you replay from Bronze. Data is never modified in place; it only moves forward through the layers.

---

### Infrastructure as Code (IaC)

Every resource is defined in Terraform rather than created manually in the AWS console. This provides:

- **Reproducibility** — `terraform apply` creates an identical environment every time
- **Version control** — infrastructure changes go through the same Git review process as application code
- **Auditability** — every infrastructure change is tracked with who made it, when, and why
- **Disaster recovery** — if the AWS account is compromised or resources are accidentally deleted, the entire infrastructure is recreated in under 5 minutes

---

### Separation of Environments

The `environment` variable appended to all bucket names means running:

```bash
terraform apply -var="environment=prod"
```

creates a completely separate set of resources (`music-streaming-raw-prod`, etc.) without touching dev resources. This is critical for safe deployment — new pipeline code is tested in dev before being promoted to prod.

---

### Cost Optimisation

| Decision | Cost Impact |
|---|---|
| DynamoDB `PAY_PER_REQUEST` | Zero cost when the pipeline is idle. Pay only for actual reads and writes during pipeline execution |
| S3 Glacier lifecycle on archive bucket | Reduces archive storage cost by ~70% after 90 days automatically |
| CloudWatch 30-day log retention | Prevents log storage from accumulating indefinitely |
| Glue G.1X workers (to be set on jobs) | Half the cost of G.2X for data volumes under 100GB/day |

---

### Security Baseline

| Control | Implementation |
|---|---|
| Encryption at rest | AES256 on all 3 S3 buckets |
| Least-privilege service trust | IAM role only assumable by `glue.amazonaws.com` |
| PITR on DynamoDB | All 3 tables recoverable to any second within 35 days |
| No public S3 access | Buckets are private by default — no `public-read` ACL |

---

## 7. Edge Case Handling

### Edge Case 1 — Duplicate File Arrives

**Scenario:** `streams1.csv` lands in S3 twice due to a upstream system bug.

**How the architecture handles it:**
The Archive job moves `streams1.csv` to the archive bucket after the first successful run. On the second arrival, the file lands in the raw bucket again with a new timestamp. The Crawler detects it as a new file. The pipeline processes it. However, the DynamoDB Loader performs **upserts** (PutItem), not inserts — so writing the same KPI for the same `genre_date` key simply overwrites the existing value with an identical value. No duplicate data is stored.

---

### Edge Case 2 — Malformed or Missing Columns

**Scenario:** A stream file arrives where the `listen_time` column is missing or named incorrectly.

**How the architecture handles it:**
The Validation job (Step 1 in Step Functions) checks all required columns before any Spark processing begins. If a required column is absent, the job raises an exception immediately. Step Functions catches this via its `Catch` block, routes to the Error Handler, and fires an SNS alert. The malformed file stays in the raw bucket — it is never archived — so it can be corrected and reprocessed. No expensive PySpark job is ever started for invalid data.

---

### Edge Case 3 — Pipeline Fails Midway

**Scenario:** The KPI Aggregation job succeeds and writes to S3 Gold, but then the DynamoDB Loader job fails.

**How the architecture handles it:**
Step Functions retries the failed step automatically (configurable retry count). Because the KPI Aggregation job has already written its output to S3 Gold, the retry of the DynamoDB Loader simply reads from that existing output — it does not re-run the entire pipeline from scratch. Each step is **idempotent** — it can safely be re-run multiple times and produce the same result.

---

### Edge Case 4 — No New Files Arrive

**Scenario:** No stream files arrive in S3 for an entire day.

**How the architecture handles it:**
The pipeline is **event-driven** via S3 Event Notifications → EventBridge → SQS. If no files land, no events are published, SQS stays empty, and Step Functions is never triggered. There is zero compute cost for idle days. This is fundamentally different from a scheduled pipeline that would wake up, find nothing, and waste Glue DPU-hours.

---

### Edge Case 5 — Schema Changes in Source Data

**Scenario:** The upstream team adds a new column `device_type` to the streams CSV files.

**How the architecture handles it:**
The Glue Crawler is configured with `update_behavior = "UPDATE_IN_DATABASE"`. When it scans the new file and detects the extra column, it automatically updates the Catalog table schema to include `device_type`. Existing Glue jobs that do not reference this column are unaffected. New jobs can immediately start using it. No manual schema migration is required.

---

### Edge Case 6 — Athena Partition Not Visible

**Scenario:** The KPI job writes new daily partitions to S3 Gold but Athena returns zero results.

**How the architecture handles it:**
The curated Crawler (Step 5 in Step Functions) runs automatically after every KPI job completion. It scans the Gold layer and registers all new partitions in the Catalog. By the time Step Functions completes, Athena is guaranteed to have visibility of the latest data. This eliminates the need to manually run `MSCK REPAIR TABLE` after each pipeline execution.

---

## 8. How to Deploy

### Prerequisites

- [Terraform](https://developer.hashicorp.com/terraform/install) >= 1.3.0 installed
- AWS CLI configured with credentials that have permissions to create IAM, S3, DynamoDB, and Glue resources
- An AWS account with the target region available

### Commands

```bash
# 1. Navigate to the terraform directory
cd music-streaming-pipeline/terraform

# 2. Initialise — downloads the AWS provider plugin
terraform init

# 3. Preview what will be created — always review this before applying
terraform plan

# 4. Deploy all resources
terraform apply
# Type 'yes' when prompted

# 5. After apply, outputs are printed — save these values
# They will be referenced in your Glue job scripts
```

### Expected output after apply

```
Apply complete! Resources: 21 added, 0 changed, 0 destroyed.

Outputs:

raw_bucket_name             = "music-streaming-raw-dev"
curated_bucket_name         = "music-streaming-curated-dev"
archive_bucket_name         = "music-streaming-archive-dev"
dynamodb_genre_kpis_table   = "genre_kpis"
dynamodb_top_songs_table    = "top_songs"
dynamodb_top_genres_table   = "top_genres"
glue_role_arn               = "arn:aws:iam::123456789012:role/glue-pipeline-role"
glue_role_name              = "glue-pipeline-role"
glue_database_name          = "music_streaming_db"
glue_raw_crawler_name       = "music-streaming-raw-crawler"
glue_curated_crawler_name   = "music-streaming-curated-crawler"
glue_log_group              = "/aws/glue/music-streaming"
step_functions_log_group    = "/aws/states/music-streaming"
```

### After deploying — upload your data

```bash
# Upload songs reference data
aws s3 cp data/songs/songs.csv s3://music-streaming-raw-dev/songs/

# Upload stream files
aws s3 cp data/streams/streams1.csv s3://music-streaming-raw-dev/streams/
aws s3 cp data/streams/streams2.csv s3://music-streaming-raw-dev/streams/
aws s3 cp data/streams/streams3.csv s3://music-streaming-raw-dev/streams/

# Upload users reference data
aws s3 cp data/users/users.csv s3://music-streaming-raw-dev/users/
```

### Run the Crawler manually (first time only)

```bash
aws glue start-crawler --name music-streaming-raw-crawler
```

After it completes, verify tables were created:

```bash
aws glue get-tables --database-name music_streaming_db
```

You should see `songs`, `streams`, and `users` tables listed.

---

## 9. Outputs Reference

These are the values your Glue job Python scripts will reference:

| Output | Used in |
|---|---|
| `raw_bucket_name` | All Glue jobs — source path for reading raw files |
| `curated_bucket_name` | ETL job (write Silver), KPI job (write Gold), Athena queries |
| `archive_bucket_name` | Archive job — destination for processed files |
| `dynamodb_genre_kpis_table` | DynamoDB Loader job |
| `dynamodb_top_songs_table` | DynamoDB Loader job |
| `dynamodb_top_genres_table` | DynamoDB Loader job |
| `glue_role_arn` | Every Glue job definition and Step Functions role |
| `glue_database_name` | Every Glue job that reads from the Catalog |
| `glue_raw_crawler_name` | Step Functions state machine — Step 1 |
| `glue_curated_crawler_name` | Step Functions state machine — Step 5 |

---

## 10. Teardown

To destroy all provisioned resources:

```bash
cd music-streaming-pipeline/terraform
terraform destroy
# Type 'yes' when prompted
```

> **Warning:** `force_destroy = true` is set on S3 buckets in dev. This means `terraform destroy` will delete the buckets **and all their contents** without warning. Do not set `force_destroy = true` in production.

To destroy only a specific resource:

```bash
# Example: destroy only the archive bucket
terraform destroy -target=aws_s3_bucket.archive
```

---

*This document covers the infrastructure provisioning layer only. For Glue job code documentation, see `glue_jobs/README.md`. For Step Functions state machine documentation, see `step_functions/README.md`.*