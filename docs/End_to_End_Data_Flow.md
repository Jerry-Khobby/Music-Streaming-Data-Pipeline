# The Full End-to-End Data Flow

## What This Document Covers

This document traces a **single streaming event** — one person tapping "play" on their phone — all
the way through the pipeline until it shows up inside a DynamoDB KPI table. It names **every AWS
service the event touches** and **every transformation it undergoes**. It is written for a data
engineer new to the cloud, and it ties together every other doc in this folder into one continuous
story. Code references map to [glue_jobs/](../glue_jobs/) and [terraform/](../terraform/).

> **One honest framing note:** this pipeline ingests stream events in **batch files**, not as a live
> per-tap stream. Play events are sent to **Kinesis Data Firehose**, which buffers them and lands them
> in S3 as JSON batch files (see [Streaming_Ingestion_Firehose.md](Streaming_Ingestion_Firehose.md)).
> So "a single event" travels to the cloud *inside a Firehose-delivered file* alongside many others.
> We follow one event's row through that file's journey — which is exactly how its data becomes a KPI.
> (The static `songs`/`users` reference data is still plain CSV; only the stream events flow through
> Firehose.)

---

## 1. The Whole Journey at a Glance

```
 📱 User taps play
     │  (app sends the event to Kinesis Data Firehose)
     ▼
 0. Firehose buffers events, lands a JSON batch file in S3        ← Kinesis Data Firehose (ingestion)
     │
     ▼
 1. File in S3 raw bucket  streams/2024/06/25/…json              ← Amazon S3 (Bronze)
     │  S3 emits "Object Created"
     ▼
 2. EventBridge rule matches streams/ upload                      ← Amazon EventBridge
     │
     ▼
 3. Event buffered in SQS main queue                              ← Amazon SQS (+ DLQ safety net)
     │
     ▼
 4. EventBridge Pipe polls SQS, starts the state machine          ← EventBridge Pipes
     │
     ▼
 5. Step Functions orchestrates the rest ───────────────┐         ← AWS Step Functions
     │                                                   │
     ├─ 5a. Run Glue crawler → register schema           │         ← Glue Crawler + Data Catalog
     ├─ 5b. Validate (schema, non-empty, columns)        │         ← Glue Job: validation
     ├─ 5c. Transform: join + dedup → Silver Parquet      │        ← Glue Job: etl_transform
     ├─ 5d. Aggregate: compute KPIs → Gold Parquet        │        ← Glue Job: kpi_aggregation
     ├─ 5e. Load Gold → DynamoDB items                    │        ← Glue Job: dynamodb_loader
     ├─ 5f. Refresh Athena partitions (non-fatal)         │        ← Glue Crawler (curated)
     └─ 5g. Archive processed raw file                    │        ← Glue Job: archive
     ▼                                                   ▼
 6. KPI is now queryable in DynamoDB and Athena      ✅ success alert (SNS → Slack/email)
```

Everything is encrypted at rest (AES256) and in transit (HTTPS) throughout (see
[Encryption_in_This_Pipeline.md](Encryption_in_This_Pipeline.md)). Now let's follow one event through
each step.

---

## 2. The Event Is Born

A user opens the music app and taps **play** on a song. Say the app records:

```
user_id = U_8841,  track_id = T_553,  listen_time = 2026-05-17T14:22:09
```

The app's backend sends events like this to **Kinesis Data Firehose**, which **buffers** them and
writes them out, in batches, as **JSON files** in S3 (see
[Streaming_Ingestion_Firehose.md](Streaming_Ingestion_Firehose.md)). Our one event becomes a single
**JSON record** inside a Firehose-delivered file, alongside many other plays. At this moment, the
event holds only three facts: *who*, *what*, and *when*. It does **not** yet know the song's name,
genre, or duration — that enrichment happens later.

---

## 3. Step 1 — Landing in S3 (Bronze)

Firehose writes the batch file into the **raw S3 bucket** under the `streams/` prefix (Firehose adds
its own date path):

```
s3://music-streaming-raw-dev/streams/2024/06/25/14/music-streaming-streams-ingestion-…json
```

- **Service:** Amazon S3 (the Bronze / landing layer — see [S3_Bucket_Layers.md](S3_Bucket_Layers.md)
  and [Medallion_Architecture.md](Medallion_Architecture.md)).
- **Transformation:** none yet — this is the immutable, encrypted record of "what arrived." Our event
  is now one untouched JSON record in a raw batch file.
- Because the bucket has EventBridge notifications enabled, the upload makes S3 **emit an `Object
  Created` event** describing the new file.

---

## 4. Step 2 — EventBridge Detects It

The `Object Created` event lands on the **default EventBridge bus**, where the `streams_uploaded` rule
is waiting (see [Amazon_EventBridge.md](Amazon_EventBridge.md)).

- **Service:** Amazon EventBridge.
- **Transformation:** none to the data — this is **routing/filtering**. The rule checks: is it from
  S3? an object creation? in the raw bucket? **with a key starting `streams/`?** Our file matches all
  four, so the event is forwarded. (A `songs/` upload would be ignored here.)
- **Target:** the SQS main queue.

---

## 5. Step 3 — Buffered in SQS

EventBridge drops the event into the **main SQS queue** (`pipeline_events`) (see
[SQS_and_Dead_Letter_Queue.md](SQS_and_Dead_Letter_Queue.md)).

- **Service:** Amazon SQS.
- **Transformation:** none — the event waits durably as a message. If many files arrived at once, they
  queue here in order (back-pressure). If this event somehow failed to process 3 times, it would be
  moved to the **dead-letter queue** for inspection — but normally it's picked up immediately.

---

## 6. Step 4 — The Pipe Starts the Pipeline

The **EventBridge Pipe** (`sqs_to_sfn`) is continuously polling the queue (see
[Event_Driven_Architecture.md](Event_Driven_Architecture.md)).

- **Service:** EventBridge Pipes.
- **Transformation:** it **reshapes** the raw SQS message envelope into a clean input and calls
  `StartExecution` on the state machine (`batch_size = 1` → this one file starts one pipeline run;
  `FIRE_AND_FORGET` → the Pipe returns to polling immediately).

Our event is now inside a running **Step Functions execution**.

---

## 7. Step 5 — Step Functions Orchestrates Everything

The state machine is the conductor (see [Step_Functions.md](Step_Functions.md)). It first does some
housekeeping our event passes through invisibly: it **normalizes the input** to a clean `{}`, and
**checks no other run is active** (if one were, it would wait and retry). Then the real work begins.

### Step 5a — Crawler registers the schema

- **Services:** Glue Crawler + Glue Data Catalog (see
  [Glue_Crawlers_and_Jobs.md](Glue_Crawlers_and_Jobs.md)).
- **What happens:** the state machine starts the raw crawler and **polls until it reports `READY`**.
  The crawler scans `streams/`, `songs/`, `users/`, infers each file's columns and types, and
  registers them as tables in the `music_streaming_db` catalog. Our event's file now has a known
  **schema** (`user_id`, `track_id`, `listen_time`). The state machine then checks S3 directly that
  `streams/` actually has files (it does — ours is there) before spending compute.
- **Transformation to our event:** none to the values — but its *structure is now described* in the
  catalog so the jobs can read it.

### Step 5b — Validation (the gate)

- **Service:** Glue Job `validation` (PySpark) (see [Data_Validation.md](Data_Validation.md)).
- **What happens:** the job confirms `streams`, `songs`, `users` exist, are non-empty, and have all
  required columns. Our event's row has `user_id`, `track_id`, `listen_time` — so `streams` passes.
- **Transformation:** none — this is a pass/fail **gate**. If the data were malformed, the pipeline
  would stop here and alert, and our event would never be processed further (failing cheap, before any
  expensive work).

### Step 5c — Transform: Bronze → Silver (the event gets enriched)

- **Service:** Glue Job `etl_transform` (PySpark) (see
  [Glue_Transformation_Code.md](Glue_Transformation_Code.md)).
- **What happens to our event — this is its biggest transformation:**
  1. **Join.** The job joins the `streams` rows to the `songs` catalogue on `track_id`. Our event's
     `track_id = T_553` is matched to its song, so the row gains `track_name`, `track_genre` (say
     "Afrobeats"), and `duration_ms` (say 215000). The event is now *enriched* — it finally knows
     *what* was played, not just its ID.
  2. **Derive `stream_date`.** From `listen_time = 2026-05-17T14:22:09`, the job derives
     `stream_date = 2026-05-17`.
  3. **Deduplicate.** The row is deduplicated on `(user_id, track_id, listen_time)` so reprocessing
     can't double-count it (see [Idempotency_in_Data_Pipelines.md](Idempotency_in_Data_Pipelines.md)).
  4. **Write as Parquet, partitioned by date:**
     `s3://…-curated-dev/silver/enriched_streams/stream_date=2026-05-17/…parquet`
- **Result:** our event is now a clean, typed, enriched row in the **Silver** layer:
  `U_8841, T_553, 2026-05-17T14:22:09, "<song name>", Afrobeats, 215000, 2026-05-17`.

### Step 5d — Aggregate: Silver → Gold (the event becomes a statistic)

- **Service:** Glue Job `kpi_aggregation` (PySpark) (see
  [KPI_Design_and_Computation.md](KPI_Design_and_Computation.md)).
- **What happens to our event — it stops being an individual row and becomes part of aggregates:**
  - In **`genre_kpis`**, our event is *one of the plays counted* for `(2026-05-17, Afrobeats)`. It adds
    **+1 to `listen_count`**, contributes `U_8841` to **`unique_listeners`**, and adds its
    `duration_ms` to **`total_listen_time_ms`** (and thus the average per user).
  - In **`top_songs`**, our event adds **+1 to the `play_count`** of song `T_553` within Afrobeats; if
    that song is among the genre's top 3 that day, it appears with a `rank`.
  - In **`top_genres`**, our event's contribution to Afrobeats' total `listen_count` helps determine
    whether Afrobeats lands in the day's top 5 genres.
  - The composite key `genre_date = "Afrobeats#2026-05-17"` is built for the DynamoDB tables.
  - Results are written as **Gold** Parquet, partitioned by date.
- **Result:** our single tap is now baked into the day's Afrobeats metrics — it no longer exists as an
  individual row in Gold, only as part of the counts and sums.

### Step 5e — Load: Gold → DynamoDB (the KPI becomes servable)

- **Service:** Glue Job `dynamodb_loader` (PySpark) (see
  [Glue_Transformation_Code.md](Glue_Transformation_Code.md) and
  [DynamoDB_Data_Modeling.md](DynamoDB_Data_Modeling.md)).
- **What happens:** each Gold row is reshaped into a DynamoDB item (floats → `Decimal`, counts →
  `int`), deduplicated on the table's primary key, and written with `put_item`. The item for
  `genre_date = "Afrobeats#2026-05-17"` is **upserted** into the `genre_kpis` table — and because
  `put_item` overwrites by key, re-running is safe.
- **Result:** the KPI our event contributed to is now a live item in **DynamoDB**, ready for the
  dashboard to read in single-digit milliseconds.

### Step 5f — Refresh Athena partitions (non-fatal)

- **Service:** Glue Crawler (curated) (see [Amazon_Athena.md](Amazon_Athena.md)).
- **What happens:** the curated crawler registers the new `gold/` date partition in the catalog so
  **Athena** can immediately query it with SQL. If this step fails, the pipeline still succeeds — Athena
  just misses the newest day until next run.

### Step 5g — Archive the raw file

- **Service:** Glue Job `archive` (see [Archival_Strategy.md](Archival_Strategy.md)).
- **What happens:** now that all value has been extracted, the original `streams1.csv` is **copied to
  the archive bucket and deleted from `streams/`** (copy-then-delete for safety), so the next run only
  sees new files. Our event's source file is preserved in the archive (→ Glacier after 90 days) for
  lineage and audit (see [Data_Lineage_and_Auditability.md](Data_Lineage_and_Auditability.md)) — it is
  retained, not destroyed.

---

## 8. Step 6 — The Result Is Live (and the Run Is Announced)

The execution reaches its `Succeed` state. An EventBridge rule catches the `SUCCEEDED` event and posts
a **"✅ Pipeline SUCCEEDED — all KPIs computed and loaded"** message to Slack/email (see
[Monitoring_and_Observability.md](Monitoring_and_Observability.md)). Throughout, every step logged to
CloudWatch; had anything failed, a `Catch` would have routed to an SNS failure alert and stopped the
run (see [Error_Handling_and_Retry.md](Error_Handling_and_Retry.md)).

Our user's tap is now reflected in:

- **DynamoDB** — as part of `genre_kpis`, `top_songs`, and possibly `top_genres` for 2026-05-17, served
  to the dashboard.
- **Athena** — queryable via SQL over the Gold layer for ad-hoc analysis.

---

## 9. The Complete Service + Transformation Table

| Step | Service | What it does to our event | Data shape |
|---|---|---|---|
| Born | App → **Firehose** | Records `user_id, track_id, listen_time`; Firehose batches into a JSON file | A JSON record |
| 1 | **Amazon S3** (Bronze) | Stores the raw JSON file immutably, encrypted | Raw JSON record |
| 2 | **EventBridge** | Detects & filters the `streams/` upload, routes it | (event, not data) |
| 3 | **SQS** | Buffers the event durably (DLQ as safety net) | (message) |
| 4 | **EventBridge Pipes** | Reshapes envelope, starts the state machine | (clean input) |
| 5 | **Step Functions** | Orchestrates all steps below, with error handling | — |
| 5a | **Glue Crawler + Catalog** | Registers schema so jobs can read the file | schema known |
| 5b | **Glue: validation** | Gate — confirms schema/non-empty/columns | unchanged (pass/fail) |
| 5c | **Glue: etl_transform** | **Joins to song → enriched; derives date; dedupes** → Silver Parquet | enriched, typed row |
| 5d | **Glue: kpi_aggregation** | **Aggregates into genre/day KPIs & rankings** → Gold Parquet | a statistic (counted/summed) |
| 5e | **Glue: dynamodb_loader** | Upserts Gold rows as DynamoDB items (typed, key-deduped) | DynamoDB item |
| 5f | **Glue Crawler (curated)** | Registers Gold partition for Athena | (metadata) |
| 5g | **Glue: archive** | Moves the raw file to the archive bucket | raw file relocated |
| 6 | **SNS / EventBridge** | Announces success (or failure) to Slack/email | (notification) |

---

## 10. Summary

A single tap on a phone is sent to **Kinesis Data Firehose**, which batches it into a JSON file that
lands in **S3**, is detected by **EventBridge**, buffered in
**SQS**, and handed by **Pipes** to **Step Functions**, which drives **Glue** through schema discovery,
validation, enrichment (join + dedup → Silver), aggregation (→ Gold KPIs), and loading into
**DynamoDB**, then refreshes **Athena** partitions and archives the source file — finishing with a
success alert via **SNS**.

Along the way the event is transformed from *three raw facts* → *an enriched, typed record* → *a
contribution to daily genre statistics* → *a servable KPI item*. That is the whole pipeline in one
event's journey: **twelve services, one directional flow from raw tap to live metric, each step
preserved, validated, idempotent, encrypted, and observable.**
