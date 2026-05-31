# Encryption in This Pipeline

## What This Document Covers

This document explains the encryption in this pipeline: **encryption at rest** (AES256 / SSE-S3 on
every S3 bucket), **encryption in transit** (HTTPS), the difference between **KMS and SSE-S3**, and
**where sensitive data lives**. It is written for a data engineer new to cloud security. The
implemented settings map to [terraform/main.tf](../terraform/main.tf).

---

## 1. The Two Kinds of Encryption

Data exists in two states, and each needs its own protection:

- **Data at rest** — data sitting in storage (files in S3, items in DynamoDB). Encryption at rest
  protects it if someone gained access to the underlying disks or storage system: the bytes are
  scrambled and useless without the key.
- **Data in transit** — data moving over the network between services (S3 → Glue, Glue → DynamoDB,
  your laptop → AWS). Encryption in transit protects it from being intercepted or tampered with while
  it travels.

A secure pipeline encrypts **both**. This one does.

---

## 2. Encryption at Rest — AES256 (SSE-S3) on Every Bucket

Every S3 bucket in this pipeline — raw (Bronze), curated (Silver/Gold), and archive — is configured
with **server-side encryption using AES256**. For example the raw bucket
([main.tf:33](../terraform/main.tf#L33)):

```hcl
resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}
```

Identical blocks exist for the curated bucket ([main.tf:81](../terraform/main.tf#L81)) and the archive
bucket ([main.tf:114](../terraform/main.tf#L114)). What this means in practice:

- **`sse_algorithm = "AES256"`** selects **SSE-S3** — Server-Side Encryption with S3-managed keys,
  using the strong, industry-standard AES-256 cipher.
- **"Server-side"** means S3 itself encrypts each object as it's written to disk and decrypts it when
  an authorized caller reads it — automatically and transparently. No application code has to handle
  encryption.
- **"By default"** means *every* object written to the bucket is encrypted, with no way to accidentally
  store an unencrypted object. It's enforced at the bucket level.

So all three layers of the data lake — the raw source of truth, the cleansed/aggregated data, and the
long-term archive — are encrypted at rest with zero effort from the jobs that read and write them.

### DynamoDB at rest

DynamoDB **encrypts all tables at rest by default** using AWS-owned keys — there is no "unencrypted
DynamoDB." So the three KPI tables are encrypted at rest automatically, even though no explicit
encryption block is needed in the Terraform.

---

## 3. Encryption in Transit — HTTPS / TLS

Encryption in transit is handled by the AWS platform itself. **Every AWS service API call in this
pipeline travels over HTTPS (TLS)** by default:

- S3 read/write (the Glue jobs reading CSV, writing Parquet) — HTTPS endpoints.
- DynamoDB writes (the loader's `put_item` via boto3) — HTTPS.
- EventBridge → SQS → Pipes → Step Functions — all internal AWS service-to-service traffic, encrypted
  in transit.
- The AWS SDK (boto3) and Glue's connectors use TLS endpoints automatically.

Because all inter-service communication uses AWS's TLS-secured endpoints, data is never exposed in
plaintext on the wire as it moves between the pipeline's components. (For environments with strict
requirements, S3 bucket policies can additionally *enforce* `aws:SecureTransport` to reject any
non-HTTPS request — a hardening step worth noting, though the default is already HTTPS.)

---

## 4. KMS vs SSE-S3 — The Two Key-Management Options

The choice in `sse_algorithm` is really a choice about **who manages the encryption keys**. There are
two main options for S3 server-side encryption:

| | **SSE-S3** (`AES256`) — *used here* | **SSE-KMS** (`aws:kms`) |
|---|---|---|
| Key management | Keys fully managed by S3; invisible to you | Keys are AWS KMS Customer Master Keys you control |
| Setup | Zero — just enable it | Create/manage a KMS key, grant usage in IAM |
| Access control | S3 permissions only | **Extra layer**: callers also need KMS key permissions |
| Audit | S3 access logs | KMS logs **every encrypt/decrypt** in CloudTrail (fine-grained) |
| Key rotation | Automatic, AWS-handled | Configurable; you can rotate/disable keys |
| Cost | No extra charge | Per-key monthly fee + per-request API charges |
| Best for | Strong default encryption with no overhead | Strict compliance, key control, separation of duties |

**What this project uses and why:** the pipeline uses **SSE-S3 (AES256)**. It provides strong,
standards-based encryption at rest for every bucket with **no key management overhead and no extra
cost** — the right default for a pipeline whose data is operational analytics rather than highly
regulated PII.

**When you'd upgrade to SSE-KMS:** if the data were sensitive enough to require *control over the
keys themselves* — e.g. regulatory mandates to rotate keys, to revoke access by disabling a key, to
log every single decrypt in CloudTrail, or to enforce separation of duties (someone can have S3 access
but still be denied decrypt rights via KMS). SSE-KMS adds a second, independent permission gate (the
KMS key policy) on top of S3 permissions. That control comes at the cost of key administration and
per-use KMS charges. This project doesn't need that layer, so SSE-S3 is the deliberate, appropriate
choice — and switching is a localized change to the `sse_algorithm` setting plus a KMS key resource if
requirements change later.

---

## 5. Where Sensitive Data Lives in This Pipeline

Knowing *what* you're protecting is as important as *how*. Here's where potentially sensitive data
sits and how it's protected:

| Data | Where it lives | Sensitivity | Protection |
|---|---|---|---|
| **User identifiers** (`user_id`) | raw `streams/`, `silver/enriched_streams` | Pseudonymous ID — links activity to a user | AES256 at rest; HTTPS in transit; IAM-scoped access |
| **User profile** (`user_name`, `user_country`) | raw `users/` | The most personal data — names | AES256 at rest; note it's **only in Bronze** (see below) |
| **Listening behavior** (`track_id`, `listen_time`) | raw `streams/`, Silver | Behavioral data | AES256 at rest; HTTPS in transit |
| **Song catalogue** (`track_name`, `genre`, `duration`) | raw `songs/`, Silver | Not sensitive (reference data) | Encrypted anyway by default |
| **Aggregated KPIs** | Gold, DynamoDB | **De-identified** — counts per genre/day, no individual users | Encrypted at rest by default |

A few observations specific to this pipeline's design that *limit* sensitive-data exposure:

- **The `users` table is validated but not propagated to the serving layer.** The KPI computations
  aggregate streams joined to *songs* (for genre/duration); personal profile fields like `user_name`
  and `user_country` are not carried into Gold or DynamoDB. The most personal data effectively stays
  in the Bronze layer.
- **`user_id` is used only for counting, then dropped.** In the KPIs, `user_id` appears only inside
  `countDistinct(user_id)` to compute *unique listeners* — the identifier itself is never stored in
  Gold or served. So **the data served to dashboards is aggregated and de-identified**: "Afrobeats had
  4,200 unique listeners," not "user X listened."
- **Least-privilege IAM** controls *who* can read each layer (see
  [iam-roles-and-policies.md](iam-roles-and-policies.md)), so encryption at rest is backed by access
  control — encryption protects the bytes, IAM controls who can ask S3/DynamoDB to decrypt them.

The result is defense in depth: the rawest, most identifying data lives in encrypted Bronze with tight
IAM, the data narrows and de-identifies as it moves up the layers, and everything is encrypted at rest
and in transit throughout.

---

## 6. Summary

| Aspect | This pipeline |
|---|---|
| **At rest — S3** | SSE-S3 / **AES256** enforced by default on all three buckets (raw, curated, archive) |
| **At rest — DynamoDB** | Encrypted by default (AWS-managed) on all three tables |
| **In transit** | All S3/DynamoDB/messaging API calls use HTTPS/TLS automatically |
| **Key management** | SSE-S3 (S3-managed keys) — strong, zero-overhead, no extra cost |
| **KMS not used** | Would add key control + per-decrypt audit + a second permission gate; not needed for this data, switchable later |
| **Sensitive data** | Most personal fields stay in encrypted Bronze; `user_id` only counted, never served; Gold/DynamoDB are aggregated and de-identified |

Encryption in this pipeline is comprehensive but low-overhead: every byte at rest is AES256-encrypted
by default, every byte in transit rides HTTPS, and the data model itself minimizes sensitive exposure
by de-identifying as it flows from Bronze to the served KPIs — with SSE-S3 chosen as the right-sized
key-management approach for this workload.
