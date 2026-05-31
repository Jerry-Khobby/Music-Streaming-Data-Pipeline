# Cost Optimisation in AWS Data Pipelines

## What This Document Covers

This document explains the cost decisions in this pipeline: **on-demand vs provisioned DynamoDB**,
**Glue G.1X vs G.2X workers**, **S3 Intelligent-Tiering vs Glacier**, **CloudWatch log retention**,
and — importantly — **which services cost money even when the pipeline is idle**. It is written for a
data engineer new to AWS billing. Settings map to [terraform/](../terraform/).

---

## 1. The Core Cost Principle — Pay for Use, Not for Idle

The cheapest pipeline is one that costs **nothing when nothing is happening** and scales its cost with
*actual work done*. This is the **serverless / pay-per-use** philosophy, and it shapes nearly every
choice here. The opposite — *provisioned* capacity — means paying a fixed rate around the clock
whether or not you use it. The trade-off is always: pay-per-use is cheaper for spiky/low/unpredictable
workloads; provisioned is cheaper only for steady, high, predictable workloads. This pipeline is
event-driven and bursty, so pay-per-use wins almost everywhere.

---

## 2. DynamoDB — On-Demand vs Provisioned

DynamoDB has two billing modes, and the project chooses on-demand via a variable
([variables.tf:41](../terraform/variables.tf#L41)):

```hcl
variable "dynamodb_billing_mode" {
  description = "DynamoDB billing mode — PAY_PER_REQUEST (on-demand) or PROVISIONED"
  default     = "PAY_PER_REQUEST"
}
```

| Mode | How you pay | Best for |
|---|---|---|
| **PAY_PER_REQUEST** (on-demand) | Per read/write request actually made; **$0 when idle** | Spiky, unpredictable, or low traffic |
| **PROVISIONED** | A fixed number of read/write capacity units per second, billed 24/7 whether used or not | Steady, predictable, high traffic |

**Why on-demand here:** this pipeline writes to DynamoDB in **bursts** — only when a daily batch is
loaded — and the read traffic is a dashboard that may be quiet for long stretches. Provisioned
capacity would mean paying every second for throughput that sits unused between loads. On-demand bills
only for the writes the loader actually performs and the reads the dashboard actually makes, and drops
to **zero when idle**. It also can't be throttled by under-provisioning during a load spike.

Because `dynamodb_billing_mode` is a **variable**, a future high-traffic production deployment could
switch to `PROVISIONED` (where it becomes cheaper) without changing code — see
[Infrastructure_as_Code_Terraform.md](Infrastructure_as_Code_Terraform.md).

> **A cost caveat to know:** all three tables enable **point-in-time recovery** (PITR,
> `point_in_time_recovery { enabled = true }` in [main.tf](../terraform/main.tf)). PITR is a
> continuous-backup feature that bills based on table size — a small but real cost that exists for
> data protection. It's a deliberate durability-over-cost choice.

---

## 3. Glue — G.1X vs G.2X Workers (and Why It's Serverless)

Glue jobs run on **workers**, and the worker type sets each worker's compute/memory. All five jobs in
this project use **G.1X with 2 workers** ([glue_jobs.tf:82](../terraform/glue_jobs.tf#L82)):

```hcl
glue_version      = "4.0"
worker_type       = "G.1X"
number_of_workers = 2
```

| Worker type | Resources per worker | Cost per worker | When to use |
|---|---|---|---|
| **G.1X** | 1 DPU — 4 vCPU, 16 GB RAM | Lower | Default; fine for small/medium data |
| **G.2X** | 2 DPU — 8 vCPU, 32 GB RAM | ~2× G.1X | Memory-heavy jobs, large joins/shuffles |

**Why G.1X here:** Glue bills **per DPU-hour for the duration of each job run** (with a minimum
billing time). This pipeline's datasets are modest, so 2× G.1X workers are sufficient — choosing G.2X
would roughly double the per-second compute cost for headroom the jobs don't need. The rule of thumb:
**start at G.1X and only move to G.2X if jobs run out of memory or spill to disk** (visible in the
Glue metrics/logs). Over-provisioning the worker type is paying for RAM that sits unused.

Two more Glue cost levers in the project:

- **Glue is serverless** — you pay only while a job runs, nothing between runs. An idle pipeline costs
  no Glue money at all.
- **The archive job is a candidate to be cheaper.** It does pure boto3 S3 work (no Spark), so it is
  *intended* to be a **Python Shell** job (which costs a fraction of a Spark job and starts in
  seconds). As noted in [Glue_Crawlers_and_Jobs.md](Glue_Crawlers_and_Jobs.md), it's currently still
  configured as `glueetl` — converting it to `pythonshell` would cut its per-run cost substantially.

---

## 4. S3 Storage Classes — Intelligent-Tiering vs Glacier

S3 offers multiple **storage classes** at different price/access trade-offs. The relevant ones:

| Storage class | Cost | Retrieval | Best for |
|---|---|---|---|
| **S3 Standard** | Highest storage cost, instant access | Instant, free | Hot, frequently-read data |
| **S3 Intelligent-Tiering** | Auto-moves objects between tiers based on access; small monitoring fee | Instant | Data with **unknown/changing** access patterns |
| **S3 Glacier** | Very low storage cost | Slow (minutes-hours) + retrieval fee | Cold archives rarely or never read |

This project uses a **Glacier lifecycle rule on the archive bucket**
([main.tf:124](../terraform/main.tf#L124)):

```hcl
transition {
  days          = 90
  storage_class = "GLACIER"
}
```

**Why Glacier (not Intelligent-Tiering) for the archive:** the access pattern of archived files is
*known* — they are processed once and then almost never read again (kept only for audit/replay). When
the pattern is known and cold, **Glacier is the cheapest correct choice**: pay rock-bottom storage and
accept slow retrieval, because retrieval is rare. **Intelligent-Tiering** is the right tool when you
*don't know* the access pattern and want S3 to optimize automatically — it avoids the risk of Glacier's
retrieval cost/latency on data you turn out to need often. Here the pattern isn't unknown, so paying
Intelligent-Tiering's monitoring fee would be wasteful; Glacier after a 90-day "might still need it
quickly" window is the deliberate, cheaper fit. (See [Archival_Strategy.md](Archival_Strategy.md).)

Note the **hot** buckets (raw, curated) stay in Standard — they're actively read by the pipeline and
Athena, so instant access matters more than storage savings.

> **Versioning cost note:** the raw and curated buckets have **versioning enabled**. Old object
> versions consume storage (and bill) until cleaned up — a durability feature with an ongoing cost.

---

## 5. CloudWatch Log Retention

Logs accumulate forever by default, and stored logs cost money indefinitely. This project caps that by
setting **30-day retention** on both log groups ([main.tf:350](../terraform/main.tf#L350)):

```hcl
resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws/glue/${var.project_name}"
  retention_in_days = 30
}
```

**Why this matters:** without a retention setting, every Glue and Step Functions log line would be
stored and billed *forever*, growing without bound. 30 days is a deliberate balance — long enough to
debug a recent failure (the alarms fire in real time, so investigation happens within days), short
enough that storage cost stays flat instead of climbing month after month. It's a small line item, but
it's the kind of "set it and forget it" leak that bloats bills if ignored.

---

## 6. Which Services Cost Money Even When Idle

This is the question that catches people out. Most of the stack is genuinely **$0 when idle**, but a
few components bill continuously regardless of activity:

| Component | Idle cost? | Why |
|---|---|---|
| **S3 storage** | **Yes** | You pay for stored bytes 24/7, whether or not anything reads them (Glacier/lifecycle minimizes the cold part) |
| **S3 object versions** | **Yes** | Old versions (versioning enabled on raw/curated) keep consuming storage until cleaned |
| **DynamoDB PITR / backups** | **Yes** | Continuous backup bills on table size even with zero traffic (on-demand *throughput* itself is $0 idle) |
| **CloudWatch stored logs** | **Yes (decaying)** | Retained logs bill until they age out at 30 days |
| **CloudWatch alarms** | **Yes (tiny)** | A small per-alarm monthly charge regardless of state |
| **Glue jobs** | **No** | Billed only while a job runs |
| **Step Functions** | **No** | Billed per state transition — nothing between executions |
| **SQS / EventBridge / Pipes / SNS** | **No (effectively)** | Pay per message/event/request; idle = ~$0 |
| **DynamoDB on-demand throughput** | **No** | Pay per request; no requests = no throughput charge |

The takeaway: **the compute and messaging layers cost nothing at rest** — the only standing costs are
**storage-related** (S3 bytes, old versions, DynamoDB backups, retained logs) plus a trivial
per-alarm fee. That's exactly the profile you want for an event-driven pipeline that may sit idle
between data arrivals: you're paying to *keep data safe*, not to *keep servers waiting*.

---

## 7. Summary

| Decision | Choice here | Why it's cost-optimal |
|---|---|---|
| **DynamoDB billing** | `PAY_PER_REQUEST` (on-demand) | Bursty writes + quiet reads → $0 idle throughput; switchable to provisioned via variable if traffic becomes steady |
| **Glue workers** | G.1X × 2 | Datasets are modest; G.2X would double compute cost for unneeded RAM; serverless = $0 between runs |
| **Archive storage** | Glacier after 90 days | Access pattern is known-cold → cheapest correct class; Intelligent-Tiering is for *unknown* patterns |
| **Hot buckets** | S3 Standard | Actively read by pipeline/Athena; instant access matters |
| **Log retention** | 30 days | Caps log storage cost instead of growing forever |
| **Idle cost** | Mostly storage only | Compute/messaging are pay-per-use; standing cost is keeping data safe, not idle servers |

The pipeline is cost-optimised by leaning serverless and pay-per-use everywhere it can, right-sizing
the few provisioned-ish choices (G.1X, Glacier, 30-day logs), and keeping the only unavoidable
standing costs limited to durable storage of data that's worth keeping.
