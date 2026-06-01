# Pipeline Cost Analysis and Optimisation

## What This Document Covers

This document is a **complete, honest cost review** of the music streaming pipeline as it currently
stands. For every service it answers: what does it cost, is that choice already optimised, and —
where it is not — what exactly should change while keeping every feature running: Glue jobs, Step
Functions orchestration, both Slack notification channels (direct webhook + SNS/Chatbot), CloudWatch
alarms, Lambda notifier, DynamoDB serving, and Athena access.

The document ends with a **verdict table**: green for already optimised, amber for a quick
actionable fix, and the estimated monthly saving for each.

---

## 1. The Cost Model — Pay for Use, Not for Idle

The cheapest pipeline is one that costs **nothing when nothing is happening**. This is the
serverless / pay-per-use principle, and it shapes nearly every choice in this project. Two bills to
distinguish:

- **Per-run costs** — what you pay each time the pipeline executes (mostly Glue compute).
- **Standing costs** — what you pay 24/7 regardless of activity (mostly storage and a handful of
  always-on resources).

The goal is to keep standing costs **flat and small**, and per-run costs **proportional to actual
work done**.

---

## 2. Per-Run Cost Breakdown

The dominant per-run cost is **AWS Glue**. Everything else — Step Functions transitions, Lambda
invocations, SNS publishes, EventBridge events, SQS messages — is so close to zero per run it
rounds off.

### Glue compute pricing

Glue 4.0 bills **per DPU-hour**, with a **1-minute minimum per job run**. One G.1X worker = 1 DPU.

```text
G.1X worker cost = $0.44 / DPU-hour
2 workers        = 2 DPU  →  $0.88 / hour  →  $0.0147 minimum per job (1 min)
```

### Five jobs — current cost per run

| Job | Worker type | DPUs | Typical duration | Cost per run |
| --- | --- | --- | --- | --- |
| validation | glueetl G.1X × 2 | 2 | 2–3 min | $0.029–$0.044 |
| etl_transform | glueetl G.1X × 2 | 2 | 5–15 min | $0.073–$0.22 |
| kpi_aggregation | glueetl G.1X × 2 | 2 | 3–10 min | $0.044–$0.147 |
| dynamodb_loader | glueetl G.1X × 2 | 2 | 5–10 min | $0.073–$0.147 |
| archive | glueetl G.1X × 2 | 2 | 1–3 min | $0.015–$0.044 |
| **Total per run** | | | | **~$0.23 – $0.60** |

At **30 runs/month** (one per day): **$7 – $18/month** in Glue alone.

### Everything else per run (effectively $0)

| Service | Per-run cost | Why negligible |
| --- | --- | --- |
| Step Functions (STANDARD) | $0.000025 | ~20 transitions × $0.025/1000 |
| Lambda (pipeline_notifier) | < $0.000001 | 3 invocations × ~100 ms; 1M free invocations/month |
| SNS (`NotifyFailure` / success) | < $0.000001 | First 1M publishes/month free |
| SQS (1 message per run) | < $0.000001 | First 1M requests/month free |
| EventBridge (1 event per upload) | < $0.000001 | First 1M events/month free |
| CloudWatch log ingestion | ~$0.001–0.005 | ~1 GB logs/month across all jobs; first 5 GB free |

---

## 3. Standing Costs (Idle — 24/7)

These are the charges that accrue whether or not the pipeline runs.

### Amazon S3

| Item | Rate | Estimated monthly cost |
| --- | --- | --- |
| Raw bucket — Standard storage | $0.023/GB | Depends on data volume; stream CSVs are small |
| Curated bucket — Standard storage (silver/gold Parquet + scripts) | $0.023/GB | ~$0.05–$0.15 |
| Archive bucket — Standard for first 90 days, then Glacier | $0.004/GB (Glacier) | Very low after transition |
| **Non-current object versions** (raw + curated — versioning enabled) | $0.023/GB | **Accumulates indefinitely without a lifecycle rule** |

The non-current version item is the **main unoptimised S3 cost**. Versioning is enabled on the raw
and curated buckets (correctly — for recovery), but without a lifecycle rule to expire old versions,
every old script upload, every re-written Parquet partition, and every replaced stream file leaves
a non-current copy billing forever.

### Amazon DynamoDB

| Item | Rate | Notes |
| --- | --- | --- |
| On-demand throughput (reads/writes) | Per request; **$0 when idle** | Only charges when the pipeline loads data or the dashboard queries |
| **PITR (point-in-time recovery)** | $0.20/GB/month per table | Enabled on all 3 tables; billed on table size continuously |
| Table storage | $0.25/GB/month | Minimal for KPI-sized tables |

PITR on 3 tables of typical KPI size (~0.1 GB combined) = ~$0.06/month. Small, but it's there.

### CloudWatch

| Item | Rate | Notes |
| --- | --- | --- |
| Log storage (3 log groups, 30-day retention) | First 5 GB free, then $0.03/GB/month | Capped by retention — cost stays flat |
| Log ingestion | First 5 GB/month free, then $0.50/GB | Bounded by retention; typically free tier |
| Alarms (9 total) | First 10 alarms free (free tier), then $0.10/alarm/month | Within free tier during first 12 months |
| EventBridge rules (3 rules) | First 1M events/month free | Effectively $0 |

### Kinesis Data Firehose

| Item | Rate | Notes |
| --- | --- | --- |
| Data ingested | $0.029/GB | Billed on records sent to the stream |
| No idle cost | $0 | No charge when no records are sent |

Firehose has **no standing cost** — it is purely pay-per-use. At typical event volumes (CSV-sized
play events), this is a few cents per month.

### Lambda

| Item | Rate | Notes |
| --- | --- | --- |
| Invocations | First 1M/month free | 3 per run × 30 runs = 90/month — **permanently free tier** |
| Duration | First 400K GB-seconds free | ~100 ms × 128 MB × 90 = 1.5 GB-seconds — **permanently free tier** |

The `pipeline_notifier` Lambda costs **exactly $0** at any realistic usage for this project. It
will never exceed the free tier.

### AWS Chatbot

No separate charge. You pay for the SNS messages it receives, which are already free tier.

### Step Functions

No standing cost. Charged per state transition only during executions. ~$0.0005 per run.

---

## 4. Service-by-Service Optimisation Verdict

### Amazon S3 — AMBER (one fix needed)

**What is optimised:**

- Glacier lifecycle on the archive bucket after 90 days ✅
- Standard storage on raw/curated (actively read — correct class) ✅
- Encryption enabled (AES256, no extra cost) ✅
- `force_destroy = true` only in dev (safe) ✅

**What is NOT optimised:**
Non-current version expiry is missing on both raw and curated buckets. Every Glue job re-upload
(triggered by `etag` change), every re-written Silver/Gold partition, and every re-processed stream
file leaves a non-current copy billing at Standard rates indefinitely.

**Fix — add to `main.tf` for both buckets:**

```hcl
resource "aws_s3_bucket_lifecycle_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id

  rule {
    id     = "expire-noncurrent-versions"
    status = "Enabled"

    noncurrent_version_expiration {
      noncurrent_days = 7
    }
  }
}
```

7 days is enough to recover from a bad deploy without accumulating unbounded history. This fix
changes **nothing about how the pipeline operates** — versioning stays enabled, recovery is still
possible within 7 days, and all notifications and features continue running.

---

### AWS Glue — AMBER (one fix needed)

**What is optimised:**

- G.1X workers (not G.2X) — correct for dataset size ✅
- 2 workers per job — minimum required for Spark ✅
- Serverless: $0 between runs ✅
- Timeouts are proportionate (10 min for fast jobs, 30 min for heavy jobs) ✅
- Glue 4.0 — latest version, best performance per DPU ✅
- `etag`-based script uploads: Terraform only re-uploads when code changes ✅

**What is NOT optimised:**
The **archive job** is configured as `glueetl` (Spark). Its code uses only `boto3` — it does no
Spark operations, no DataFrame joins, no Parquet reads. Starting a Spark cluster for a pure-Python
S3 copy-and-delete is like hiring a lorry to deliver an envelope. It adds ~30-second startup time
and bills at 2 DPU minimum instead of 1/16 DPU.

| Config | Min billing | Cost per run |
| --- | --- | --- |
| Current: glueetl G.1X × 2 | 1 minute × 2 DPU | $0.0147 |
| Optimised: pythonshell | per second × 0.0625 DPU | ~$0.0002 |
| **Saving per run** | | **~$0.014** |

At 30 runs/month: **~$0.42/month** — not large, but also the easiest change in the whole project.
More importantly, the archive job starts ~30 seconds faster, which shortens the total pipeline
run time.

**Fix — in `glue_jobs.tf`, change the `archive` job:**

```hcl
resource "aws_glue_job" "archive" {
  name     = "${var.project_name}-archive"
  role_arn = aws_iam_role.glue_role.arn

  command {
    name            = "pythonshell"                          # was: glueetl
    script_location = "s3://${aws_s3_bucket.curated.id}/scripts/archive_job.py"
    python_version  = "3"
  }

  # pythonshell uses max_capacity instead of worker_type/number_of_workers
  max_capacity = 0.0625                                      # remove: glue_version, worker_type, number_of_workers
  timeout      = 10

  default_arguments = merge(local.glue_common_args, {
    "--raw_bucket"     = aws_s3_bucket.raw.id
    "--archive_bucket" = aws_s3_bucket.archive.id
    "--aws_region"     = var.aws_region
  })
  ...
}
```

`pythonshell` does not support `glue_version`, `worker_type`, or `number_of_workers` — those three
fields must be removed and replaced with `max_capacity = 0.0625`. The job script itself needs no
changes; it is already pure Python with no Spark imports.

> **Why not convert the validation job too?** Validation uses `GlueContext` and
> `create_dynamic_frame.from_catalog()`, which require a Spark context. It must stay as `glueetl`.
> The ETL, KPI, and DynamoDB jobs similarly require Spark. Only archive is a clean fit for
> `pythonshell`.

---

### Amazon DynamoDB — GREEN (optimised)

- `PAY_PER_REQUEST` billing: $0 throughput cost when idle ✅
- Three tables — one per query access pattern ✅
- PITR enabled: small, deliberate cost for data durability ✅
- No GSIs or LSIs beyond what the access patterns require ✅

PITR is a conscious choice: 35-day restore window for a production serving layer is worth the
~$0.06/month. The `dynamodb_billing_mode` variable makes it trivial to switch to `PROVISIONED`
if traffic ever becomes predictably high.

---

### AWS Step Functions — GREEN (optimised)

- STANDARD type (correct for long-running ETL) ✅
- ~$0.0005 per run, $0 between runs ✅
- `NotifyPipelineStarted` / `NotifyPipelineSucceeded` / `NotifySlackPipelineFailed` Lambda states
  each add one state transition — approximately $0.000001 per run ✅

The three new Lambda notification states add a rounding error to the Step Functions bill. They are
cost-neutral.

---

### AWS Lambda — GREEN (optimised)

- `urllib.request` (stdlib) — no Lambda Layer, no packaging overhead, no dependency cost ✅
- 128 MB memory (default) — sufficient for an HTTP POST ✅
- 10-second timeout — matched to the webhook HTTP timeout constant ✅
- `source_code_hash` ensures Lambda is only redeployed when code changes ✅
- **Permanently within the free tier**: 90 invocations/month vs 1M free ✅

---

### Kinesis Data Firehose — GREEN (optimised)

- `UNCOMPRESSED` storage is correct here: the Glue crawler must read raw JSONL; compressed files
  require Glue to decompress before schema inference ✅
- No idle cost ✅
- Buffer thresholds (`firehose_buffer_size_mb = 5`, `firehose_buffer_interval_seconds = 60`) are
  already at the minimum interval (60s is Firehose's hard floor) ✅
- Firehose log group has 30-day retention ✅

---

### Notification stack (SNS · Chatbot · PipelineMonitor · Lambda) — GREEN (optimised)

This is the area most likely to concern people when adding features — but the full two-channel
notification system adds almost no cost:

| Component | Monthly cost | Why |
| --- | --- | --- |
| SNS `pipeline_alerts` topic | $0 | First 1M publishes free; maybe 30–60 publishes/month |
| AWS Chatbot | $0 | No separate charge |
| Direct Slack webhook (PipelineMonitor) | $0 | HTTP POST from inside a Glue job; no extra AWS resource |
| Lambda `pipeline_notifier` | $0 | 90 invocations/month — permanently free tier |
| EventBridge `pipeline_succeeded` rule | < $0.001 | Minimal events/month |

Keeping all notifications running — stage-level Slack from `PipelineMonitor`, pipeline-level Slack
from the Lambda, SNS failure alerts, CloudWatch alarm → Chatbot alerts — adds effectively **$0** to
the monthly bill. There is nothing to remove here.

---

### CloudWatch — GREEN (optimised)

- 30-day retention on all log groups (Glue, Step Functions, Firehose) ✅
- 9 alarms — within the free-tier threshold for the first 12 months; $0.90/month after ✅
- Per-Glue-job alarms (one per job): deliberate choice for precise failure attribution ✅
- EventBridge alarm-state-change transformer: no extra cost beyond the rule ✅

After the AWS free tier expires the 9 alarms cost $0.90/month — a deliberate trade-off for the
observability value. Removing any of them would save $0.10/month at the cost of a monitoring blind
spot; the current set is the right balance.

---

### Amazon SQS — GREEN (optimised)

- Main queue and DLQ: effectively $0 (well within 1M free requests/month) ✅
- DLQ 14-day retention (safety net for poison events) ✅
- Main queue 1-day retention (events not processed within a day are stale anyway) ✅
- `maxReceiveCount = 3` before DLQ routing ✅

---

### IAM — GREEN (no cost)

IAM has no usage-based charge. Roles and policies are free.

---

### Terraform — GREEN (no cost)

Terraform is a local tool. The Terraform AWS provider makes API calls that are either free-tier
(most resource describes) or sub-cent (a few paid API operations).

---

## 5. The Two Unoptimised Items — Summary and Priority

Both are genuine gaps. Neither requires removing any feature.

### Fix 1 — S3 noncurrent version expiry (PRIORITY: HIGH)

| | Without fix | With fix |
| --- | --- | --- |
| **Risk** | Old object versions accumulate silently; S3 bill grows month-over-month | None — versions still kept for 7 days for recovery |
| **Saving** | Depends on churn rate; could be $1–$5/month within a few months | Flat storage cost from day one |
| **Effort** | Add 2 `aws_s3_bucket_lifecycle_configuration` blocks to `main.tf` | ~10 lines of Terraform |
| **Feature impact** | None | None |

This fix is high priority because its cost grows over time — the longer you leave it, the larger
the cleanup bill.

### Fix 2 — Archive job: glueetl → pythonshell (PRIORITY: MEDIUM)

| | Without fix | With fix |
| --- | --- | --- |
| **Cost per run** | $0.0147 minimum | ~$0.0002 |
| **Monthly saving (30 runs)** | — | ~$0.42 |
| **Startup time** | +30 s (Spark init) | ~3 s |
| **Effort** | Change `command.name`, replace `glue_version/worker_type/number_of_workers` with `max_capacity = 0.0625` | 4-line change in `glue_jobs.tf` |
| **Feature impact** | None | None — script is already pure Python |

---

## 6. Overall Verdict

```text
OPTIMISED ✅                              NOT YET OPTIMISED 🟡
─────────────────────────────────────    ─────────────────────────────────────────────
DynamoDB: on-demand billing              S3: missing noncurrent version expiry
Glue: G.1X workers (right-sized)          → add lifecycle rule, $1–5/month saving
Glue: serverless, $0 idle               Archive job: glueetl instead of pythonshell
S3: Glacier lifecycle on archive           → change to max_capacity = 0.0625, $0.42/month
S3: Standard on hot buckets             
CloudWatch: 30-day log retention        
All notifications: effectively $0       
Lambda notifier: permanently free tier  
Firehose: no idle cost                  
SQS/SNS/EventBridge: free tier          
Step Functions: $0.0005/run             
```

### What NOT to touch

The following are sometimes flagged as "cost savings" but should **not** be changed:

| Suggestion | Why to reject it |
| --- | --- |
| Remove PITR from DynamoDB | Loses 35-day point-in-time recovery on the serving layer; $0.06/month is worth the insurance |
| Reduce CloudWatch alarms | Each alarm covers a distinct failure mode; removing any creates a monitoring blind spot for $0.10/month saving |
| Remove SNS/Chatbot | Removes the infrastructure-level alert path that catches failures the state machine can't see itself |
| Remove `PipelineMonitor` hooks | Removes per-stage Slack visibility from all 5 Glue jobs |
| Remove Lambda notifier | Removes pipeline start/success/fail Slack messages |
| Disable S3 versioning | Removes the ability to recover from a bad file upload or accidental overwrite |
| Switch DynamoDB to PROVISIONED | More expensive at current traffic levels; re-evaluate only if traffic becomes steady and predictable |
| Reduce Glue worker count below 2 | 2 is the Spark minimum; 1 worker cannot start a Glue ETL job |

---

## 7. Estimated Monthly Bill at 30 Runs/Month

| Category | Low estimate | High estimate | Notes |
| --- | --- | --- | --- |
| Glue compute (5 jobs × 30 runs) | $7.00 | $18.00 | Dominant cost; data-volume dependent |
| S3 storage (raw + curated + archive) | $0.05 | $0.30 | Small at typical KPI dataset sizes |
| S3 noncurrent versions (no fix applied) | $0.10 | $2.00+ | Grows over time without Fix 1 |
| DynamoDB PITR (3 tables) | $0.06 | $0.10 | Table size dependent |
| CloudWatch alarms (post free-tier) | $0.00 | $0.90 | Free first 12 months |
| CloudWatch log storage | $0.00 | $0.15 | Within free tier for most months |
| Firehose ingestion | $0.01 | $0.10 | Event volume dependent |
| Everything else (Lambda, SNS, SQS, SFN, EventBridge) | $0.00 | $0.01 | Free tier / negligible |
| **Total** | **~$7.22** | **~$21.56** | |

After applying both fixes:

- Fix 1 (version expiry): saves $0.10–$2.00/month growing over time
- Fix 2 (archive job): saves ~$0.42/month
- **New range**: ~$6.80 – $19.14/month

The pipeline is **inexpensive for what it does** — a full multi-stage Spark ETL pipeline with
end-to-end observability, two-channel Slack notifications, CloudWatch alarms, DynamoDB serving, and
Athena access, for under $20/month at daily run frequency.
