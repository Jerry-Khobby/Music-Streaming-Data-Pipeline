# Architecture Diagram Sketch — Music Streaming ETL Pipeline

## How to Read This Document and Use It for draw.io

This document is a structured reference sketch. It captures every AWS resource, every data flow arrow, every notification path, and every IAM boundary in the pipeline. It is intended to be used as a specification document when constructing a formal architecture diagram in draw.io (diagrams.net).

To use this document with draw.io:

1. Open draw.io (app.diagrams.net or the desktop app).
2. Use the "Extras > Edit Diagram" feature to paste XML, or build the diagram manually by following the zone layout described in Section 2.
3. Create one swimlane or container shape per zone. Label each container with the zone name (e.g., "Zone 1 — Ingestion").
4. Place each resource box inside its corresponding zone container. Use the resource names exactly as listed in the Resources Table (Section 5) so the diagram stays in sync with the code.
5. Draw directional arrows between boxes using the arrow list in Section 3. Each arrow entry in that list provides the exact source, label, and destination.
6. Use a distinct color or border style per zone to make zone boundaries immediately visible. A recommended color scheme is provided under the zone layout in Section 2.
7. For notification paths (Section 4), use a dashed line or contrasting color to distinguish monitoring/alerting flows from primary data flows.
8. For IAM roles (Zone 10), you may either place each IAM role box adjacent to the service it governs, or group all IAM roles in a separate swimlane at the bottom.
9. The Failure Path (Section 6) can be rendered as a separate sub-diagram or as a highlighted overlay on the main diagram using red-colored arrows.

---

## Section 1 — Zone Definitions

The pipeline is organized into eleven logical zones. Each zone groups resources that share a single concern.

- Zone 1: Ingestion — The entry point where stream events enter the system via Kinesis Firehose.
- Zone 2: Event-Driven Trigger Chain — The chain from S3 notification through EventBridge, SQS, and EventBridge Pipe that launches the Step Functions execution.
- Zone 3: Step Functions State Machine — The orchestration layer that runs all pipeline stages in sequence.
- Zone 4: Glue Data Layer — All five Glue jobs and both Glue crawlers, plus the Glue Data Catalog database and the monitoring library.
- Zone 5: Storage Layers — The three S3 buckets (raw, curated, archive) and their prefixes.
- Zone 6: Serving Layer — The three DynamoDB tables and Amazon Athena.
- Zone 7: Direct Slack Webhook Notification Path — The Lambda function and PipelineMonitor hooks that POST directly to a Slack Incoming Webhook URL.
- Zone 8: SNS and CloudWatch Infrastructure Notification Path — SNS, AWS Chatbot, and EventBridge rules for email and Chatbot-mediated Slack alerts.
- Zone 9: Observability — The three CloudWatch log groups and the nine CloudWatch alarms.
- Zone 10: IAM Roles — One least-privilege IAM role per service principal.
- Zone 11: Infrastructure as Code — Terraform files and their responsibilities.

---

## Section 2 — Spatial Layout for draw.io

The diagram should be laid out on a wide canvas. The following describes the recommended spatial arrangement when viewed left to right and top to bottom. This arrangement minimizes arrow crossing and groups related zones visually.

### Recommended Zone Positions

```
+-------------------------+--------------------------------------------------+----------------------------+
|  ZONE 1 — INGESTION     |  ZONE 2 — EVENT-DRIVEN TRIGGER CHAIN              |                            |
|                         |                                                  |                            |
|  Producer               |  S3 raw bucket (EventBridge enabled)             |  ZONE 8 — SNS / CHATBOT    |
|  Kinesis Firehose        |  EventBridge default bus                         |                            |
|  CW /firehose log group  |  EventBridge rule: streams_uploaded              |  SNS pipeline_alerts       |
|                         |  SQS pipeline_events queue                       |  EventBridge pipeline_succeeded rule  |
|                         |  SQS pipeline_dlq                                |  EventBridge alarm_state_change rule  |
|                         |  EventBridge Pipe: sqs_to_sfn                    |  AWS Chatbot               |
+-------------------------+--------------------------------------------------+----------------------------+
|  ZONE 3 — STEP FUNCTIONS STATE MACHINE (wide center lane)                                               |
|                                                                                                         |
|  NormalizeInput --> CheckAlreadyRunning --> FindOldestRunning --> IsAnotherRunning                      |
|    --> WaitForPreviousRun (loops back)                                                                  |
|    --> NotifyPipelineStarted --> StartRawCrawler --> WaitForCrawler --> CheckCrawlerStatus              |
|    --> IsCrawlerReady --> CheckCatalogTables --> CheckStreamsExist --> AreThereStreams                   |
|    --> ValidateData --> TransformData --> AggregateKPIs --> LoadDynamoDB                                |
|    --> StartCuratedCrawler --> ArchiveFiles --> NotifyPipelineSucceeded --> PipelineSucceeded           |
|  Failure path: any task --> NotifyFailure --> NotifySlackPipelineFailed --> PipelineFailed              |
|  CW /aws/states log group                                                                               |
+---------------------------------------------------------------------------------------------------------+
|  ZONE 4 — GLUE DATA LAYER                         |  ZONE 5 — STORAGE LAYERS                           |
|                                                   |                                                    |
|  Glue Data Catalog (music_streaming_db)           |  S3 raw bucket: songs/, users/, streams/           |
|  Raw crawler                                      |  S3 curated bucket: silver/, gold/, scripts/, tmp/ |
|  Curated crawler                                  |  S3 archive bucket: streams/                       |
|  Job 1: validation                                |                                                    |
|  Job 2: etl_transform                             +----------------------------------------------------+
|  Job 3: kpi_aggregation                           |  ZONE 6 — SERVING LAYER                            |
|  Job 4: dynamodb_loader                           |                                                    |
|  Job 5: archive                                   |  DynamoDB: genre_kpis table                        |
|  monitoring/ package (monitoring.zip)             |  DynamoDB: top_songs table                         |
|  CW /aws/glue log group                           |  DynamoDB: top_genres table                        |
|                                                   |  Amazon Athena                                     |
+---------------------------------------------------+----------------------------------------------------+
|  ZONE 7 — DIRECT SLACK WEBHOOK PATH              |  ZONE 9 — OBSERVABILITY                            |
|                                                   |                                                    |
|  Lambda: pipeline_notifier                        |  CW alarm: sfn_execution_failed                    |
|  PipelineMonitor (in each Glue job)               |  CW alarm: sfn_execution_timed_out                 |
|  SlackNotifier                                    |  CW alarm: sqs_dlq_has_messages                    |
|  Slack Incoming Webhook URL                       |  CW alarm: sqs_messages_stuck                      |
|  Slack channel                                    |  CW alarm: glue_job_failed x5                      |
+---------------------------------------------------+----------------------------------------------------+
|  ZONE 10 — IAM ROLES                             |  ZONE 11 — TERRAFORM IaC FILES                     |
|                                                   |                                                    |
|  glue_role                                        |  provider.tf                                       |
|  sfn_role                                         |  variables.tf                                      |
|  pipes_role                                       |  main.tf                                           |
|  firehose_role                                    |  ingestion.tf                                      |
|  chatbot_role                                     |  glue_jobs.tf                                      |
|  pipeline_notifier_role                           |  step_functions.tf                                 |
|                                                   |  messaging.tf                                      |
|                                                   |  monitoring.tf                                     |
|                                                   |  lambda.tf                                         |
|                                                   |  outputs.tf                                        |
+---------------------------------------------------+----------------------------------------------------+
```

### Recommended Color Scheme per Zone

- Zone 1 (Ingestion): light blue
- Zone 2 (Trigger Chain): light orange
- Zone 3 (Step Functions): light purple
- Zone 4 (Glue): light green
- Zone 5 (Storage): light yellow
- Zone 6 (Serving): light teal
- Zone 7 (Direct Slack): light pink
- Zone 8 (SNS/Chatbot): light red-orange
- Zone 9 (Observability): light grey
- Zone 10 (IAM): white with dark border
- Zone 11 (IaC): light brown/tan

---

## Section 3 — Complete Ordered Data Flow Arrow List

Every arrow in the pipeline is listed below in the order events travel through the system, from ingestion to serving and notification. Each entry states: source box, arrow label, destination box.

### Primary Data Path

1. Producer (Python script) --PutRecord API call--> Kinesis Data Firehose delivery stream
2. Kinesis Data Firehose delivery stream --JSON batch file, prefix streams/YYYY/MM/DD/HH/--> S3 raw bucket
3. Kinesis Data Firehose delivery stream --delivery error logs--> CloudWatch log group /aws/kinesisfirehose/music-streaming-streams-ingestion
4. S3 raw bucket --ObjectCreated event (EventBridge notifications enabled)--> EventBridge default bus
5. EventBridge default bus --matches rule: source=aws.s3, detail-type=Object Created, bucket=raw, prefix=streams/--> EventBridge rule "streams_uploaded"
6. EventBridge rule "streams_uploaded" --routes matched event--> SQS queue "pipeline_events"
7. SQS queue "pipeline_events" --message received 3 times without success (maxReceiveCount=3)--> SQS DLQ "pipeline_dlq"
8. EventBridge Pipe "sqs_to_sfn" --polls, batch_size=1--> SQS queue "pipeline_events"
9. EventBridge Pipe "sqs_to_sfn" --StartExecution (FIRE_AND_FORGET)--> Step Functions state machine

### Step Functions Internal State Transitions

10. Step Functions entry --NormalizeInput (Pass, sets input to {})--> CheckAlreadyRunning state
11. CheckAlreadyRunning state --aws-sdk:sfn:listExecutions call--> Step Functions (self, lists RUNNING executions)
12. CheckAlreadyRunning state --result--> FindOldestRunning state (Pass, extracts oldest running ARN)
13. FindOldestRunning state --extracted ARN--> IsAnotherRunning state (Choice)
14. IsAnotherRunning (oldest ARN == my ARN) --condition true--> NotifyPipelineStarted state
15. IsAnotherRunning (oldest ARN != my ARN) --condition false--> WaitForPreviousRun state
16. WaitForPreviousRun state --wait 60s, loops back--> CheckAlreadyRunning state
17. NotifyPipelineStarted state --lambda:invoke (event_type=started)--> Lambda pipeline_notifier function
18. NotifyPipelineStarted state --on success--> StartRawCrawler state
19. StartRawCrawler state --aws-sdk:glue:startCrawler--> Glue raw crawler "music-streaming-raw-crawler"
20. StartRawCrawler state --on success--> WaitForCrawler state
21. WaitForCrawler state --wait 45s--> CheckCrawlerStatus state
22. CheckCrawlerStatus state --aws-sdk:glue:getCrawler--> Glue raw crawler (reads State field)
23. CheckCrawlerStatus state --result--> IsCrawlerReady (Choice)
24. IsCrawlerReady (State == READY) --condition true--> CheckCatalogTables state
25. IsCrawlerReady (State != READY) --condition false--> WaitForCrawler state (loops back)
26. CheckCatalogTables state --aws-sdk:glue:getTable (streams table)--> Glue Data Catalog
27. CheckCatalogTables state (EntityNotFoundException) --table not found--> NoStreamsToProcess (Succeed)
28. CheckCatalogTables state --on success--> CheckStreamsExist state
29. CheckStreamsExist state --aws-sdk:s3:listObjectsV2 (prefix=streams/)--> S3 raw bucket
30. CheckStreamsExist state --result--> AreThereStreams (Choice)
31. AreThereStreams (KeyCount > 0) --condition true--> ValidateData state
32. AreThereStreams (KeyCount == 0) --condition false--> NoStreamsToProcess (Succeed terminal)
33. ValidateData state --glue:startJobRun.sync--> Glue Job 1: validation
34. ValidateData state --on success--> TransformData state
35. TransformData state --glue:startJobRun.sync--> Glue Job 2: etl_transform
36. TransformData state --on success--> AggregateKPIs state
37. AggregateKPIs state --glue:startJobRun.sync--> Glue Job 3: kpi_aggregation
38. AggregateKPIs state --on success--> LoadDynamoDB state
39. LoadDynamoDB state --glue:startJobRun.sync--> Glue Job 4: dynamodb_loader
40. LoadDynamoDB state --on success--> StartCuratedCrawler state
41. StartCuratedCrawler state --aws-sdk:glue:startCrawler (non-fatal)--> Glue curated crawler "music-streaming-curated-crawler"
42. StartCuratedCrawler state --on success or error (non-fatal, always continues)--> ArchiveFiles state
43. ArchiveFiles state --glue:startJobRun.sync--> Glue Job 5: archive
44. ArchiveFiles state --on success--> NotifyPipelineSucceeded state
45. NotifyPipelineSucceeded state --lambda:invoke (event_type=succeeded)--> Lambda pipeline_notifier function
46. NotifyPipelineSucceeded state --on success--> PipelineSucceeded (Succeed terminal)
47. Step Functions (SUCCEEDED execution) --execution status change event--> EventBridge default bus
48. EventBridge default bus --matches rule: execution status SUCCEEDED--> EventBridge rule "pipeline_succeeded"
49. EventBridge rule "pipeline_succeeded" --input transformer formatted message--> SNS topic "pipeline_alerts"

### Glue Job Internal Data Flows

50. Glue Job 1 (validation) --reads streams, songs, users tables--> Glue Data Catalog (music_streaming_db)
51. Glue Data Catalog --resolves S3 locations--> S3 raw bucket (songs/, users/, streams/ prefixes)
52. Glue Job 1 (validation) --continuous log stream--> CloudWatch log group /aws/glue/music-streaming
53. Glue Job 2 (etl_transform) --reads streams and songs tables--> Glue Data Catalog
54. Glue Job 2 (etl_transform) --writes enriched_streams (Parquet, partitioned by stream_date)--> S3 curated bucket: silver/enriched_streams/
55. Glue Job 2 (etl_transform) --continuous log stream--> CloudWatch log group /aws/glue/music-streaming
56. Glue Job 3 (kpi_aggregation) --reads enriched_streams (Parquet)--> S3 curated bucket: silver/enriched_streams/
57. Glue Job 3 (kpi_aggregation) --writes genre_kpis (Parquet, partitioned by stream_date)--> S3 curated bucket: gold/genre_kpis/
58. Glue Job 3 (kpi_aggregation) --writes top_songs (Parquet, partitioned by stream_date)--> S3 curated bucket: gold/top_songs/
59. Glue Job 3 (kpi_aggregation) --writes top_genres (Parquet, partitioned by date)--> S3 curated bucket: gold/top_genres/
60. Glue Job 3 (kpi_aggregation) --continuous log stream--> CloudWatch log group /aws/glue/music-streaming
61. Glue Job 4 (dynamodb_loader) --reads gold/ Parquet datasets (all three)--> S3 curated bucket: gold/
62. Glue Job 4 (dynamodb_loader) --batch_writer writes genre_kpis rows (PK: genre_date)--> DynamoDB genre_kpis table
63. Glue Job 4 (dynamodb_loader) --batch_writer writes top_songs rows (PK: genre_date, SK: rank)--> DynamoDB top_songs table
64. Glue Job 4 (dynamodb_loader) --batch_writer writes top_genres rows (PK: date, SK: rank)--> DynamoDB top_genres table
65. Glue Job 4 (dynamodb_loader) --continuous log stream--> CloudWatch log group /aws/glue/music-streaming
66. Glue Job 5 (archive) --reads key list from Step Functions input ($.streamsCheck.Contents[*].Key)--> S3 raw bucket: streams/ (boto3 GetObject per key)
67. Glue Job 5 (archive) --boto3 CopyObject per key--> S3 archive bucket: streams/
68. Glue Job 5 (archive) --boto3 DeleteObject per key (copy-first safety)--> S3 raw bucket: streams/
69. Glue Job 5 (archive) --continuous log stream--> CloudWatch log group /aws/glue/music-streaming
70. Glue curated crawler "music-streaming-curated-crawler" --crawls gold/ prefix--> S3 curated bucket: gold/
71. Glue curated crawler "music-streaming-curated-crawler" --updates partition metadata--> Glue Data Catalog (music_streaming_db)

### Serving Layer Flows

72. Amazon Athena --SQL query via catalog--> Glue Data Catalog (music_streaming_db)
73. Glue Data Catalog --resolves partition locations--> S3 curated bucket: gold/ (genre_kpis/, top_songs/, top_genres/)
74. Amazon Athena --reads Parquet data directly--> S3 curated bucket: gold/

### Step Functions Logging

75. Step Functions state machine --every state transition (level=ALL, include_execution_data=true)--> CloudWatch log group /aws/states/music-streaming
76. Step Functions state machine --X-Ray trace segments--> AWS X-Ray service

### Failure Path Arrows (Catch on every fatal task state)

77. Any fatal task state (on error) --Catch --> NotifyFailure state
78. NotifyFailure state --sns:publish (error.Error + error.Cause)--> SNS topic "pipeline_alerts"
79. NotifyFailure state --on success--> NotifySlackPipelineFailed state
80. NotifySlackPipelineFailed state --lambda:invoke (event_type=failed, passes $.error object)--> Lambda pipeline_notifier function
81. NotifySlackPipelineFailed state --on success or error (always continues)--> PipelineFailed (Fail terminal)

---

## Section 4 — Notification Paths

The pipeline has two independent notification paths. They operate in parallel: one is a direct HTTP webhook path driven by Lambda and PipelineMonitor, the other is an infrastructure-mediated path driven by SNS and AWS Chatbot.

### Path A — Direct Slack Webhook Notification Path

This path sends messages directly to Slack via an Incoming Webhook URL. It is low-latency and does not depend on SNS or email infrastructure. It covers pipeline lifecycle events (started, succeeded, failed) and per-stage Glue job events.

#### A1 — Pipeline Lifecycle Notifications (via Lambda pipeline_notifier)

1. Step Functions NotifyPipelineStarted state --lambda:invoke (event_type=started)--> Lambda pipeline_notifier function
2. Lambda pipeline_notifier --HTTP POST Block Kit "Pipeline Started" message--> Slack Incoming Webhook URL --> Slack channel
3. Step Functions NotifyPipelineSucceeded state --lambda:invoke (event_type=succeeded)--> Lambda pipeline_notifier function
4. Lambda pipeline_notifier --HTTP POST Block Kit "Pipeline Succeeded" message--> Slack Incoming Webhook URL --> Slack channel
5. Step Functions NotifySlackPipelineFailed state --lambda:invoke (event_type=failed, passes $.error object)--> Lambda pipeline_notifier function
6. Lambda pipeline_notifier --HTTP POST Block Kit "Pipeline FAILED" message with error details--> Slack Incoming Webhook URL --> Slack channel

Note: All three Lambda invoke states catch their own errors and route forward so that a Slack delivery failure never blocks the pipeline state machine.

#### A2 — Per-Stage Glue Job Notifications (via PipelineMonitor and SlackNotifier)

These fire from inside each Glue job via the monitoring/ package (monitoring.zip, attached to all five jobs via --extra-py-files).

7. PipelineMonitor (inside Glue Job 1: validation) --on stage start: SlackNotifier.sendJobStarted--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
8. PipelineMonitor (inside Glue Job 1: validation) --on stage success: SlackNotifier.sendJobSucceeded--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
9. PipelineMonitor (inside Glue Job 1: validation) --on stage failure: SlackNotifier.sendJobFailed--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
10. PipelineMonitor (inside Glue Job 2: etl_transform) --on stage start--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
11. PipelineMonitor (inside Glue Job 2: etl_transform) --on stage success--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
12. PipelineMonitor (inside Glue Job 2: etl_transform) --on stage failure--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
13. PipelineMonitor (inside Glue Job 3: kpi_aggregation) --on stage start--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
14. PipelineMonitor (inside Glue Job 3: kpi_aggregation) --on stage success--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
15. PipelineMonitor (inside Glue Job 3: kpi_aggregation) --on stage failure--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
16. PipelineMonitor (inside Glue Job 4: dynamodb_loader) --on stage start--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
17. PipelineMonitor (inside Glue Job 4: dynamodb_loader) --on stage success--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
18. PipelineMonitor (inside Glue Job 4: dynamodb_loader) --on stage failure--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
19. PipelineMonitor (inside Glue Job 5: archive) --on stage start--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
20. PipelineMonitor (inside Glue Job 5: archive) --on stage success--> HTTP POST to Slack Incoming Webhook URL --> Slack channel
21. PipelineMonitor (inside Glue Job 5: archive) --on stage failure--> HTTP POST to Slack Incoming Webhook URL --> Slack channel

The webhook URL is resolved at runtime in each Glue job by the resolveWebhookUrl function, which reads the --slack_webhook_url job argument or the SLACK_APP_WEBHOOK_URL environment variable. In the Lambda function, the URL comes from a Lambda environment variable named SLACK_APP_WEBHOOK_URL.

### Path B — Infrastructure SNS / AWS Chatbot Notification Path

This path uses SNS as a fan-out hub. Three distinct publishers send messages to the SNS topic "pipeline_alerts". Two downstream subscribers consume from SNS: an email address and AWS Chatbot.

#### B1 — Publishers to SNS "pipeline_alerts"

1. Step Functions NotifyFailure state --sns:publish (error.Error + error.Cause from the caught exception)--> SNS topic "pipeline_alerts"
2. CloudWatch alarm sfn_execution_failed --alarm state change (ALARM) publishes metric alert--> SNS topic "pipeline_alerts"
3. CloudWatch alarm sfn_execution_timed_out --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
4. CloudWatch alarm sqs_dlq_has_messages --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
5. CloudWatch alarm sqs_messages_stuck --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
6. CloudWatch alarm glue_job_failed (validation) --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
7. CloudWatch alarm glue_job_failed (etl_transform) --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
8. CloudWatch alarm glue_job_failed (kpi_aggregation) --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
9. CloudWatch alarm glue_job_failed (dynamodb_loader) --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
10. CloudWatch alarm glue_job_failed (archive) --alarm state change (ALARM)--> SNS topic "pipeline_alerts"
11. EventBridge rule "pipeline_succeeded" --input transformer: "Pipeline SUCCEEDED at TIME. Execution: ARN"--> SNS topic "pipeline_alerts"
12. EventBridge rule "alarm_state_change" --input transformer: plain-language email (what fired, why, when, console link)--> SNS topic "pipeline_alerts"

Note: The EventBridge rule "alarm_state_change" listens for CloudWatch alarm state transitions to ALARM for this project's alarms and republishes them with a human-readable formatted message to SNS.

#### B2 — Subscribers from SNS "pipeline_alerts"

13. SNS topic "pipeline_alerts" --message delivery--> Email subscriber (alert_email variable, configured in Terraform)
14. SNS topic "pipeline_alerts" --message delivery--> AWS Chatbot (slack_channel_configuration)
15. AWS Chatbot --forwards alert with optional CloudWatch enrichment (read-only)--> Slack channel

Note: AWS Chatbot is only provisioned when the Terraform variables slack_workspace_id and slack_channel_id are set. The Chatbot IAM role grants CloudWatch ReadOnly access so Chatbot can optionally enrich alert messages with metric context.

---

## Section 5 — Complete Resources Table

| Zone | Resource Name | AWS Service | Role in Pipeline | Key Configuration |
|------|---------------|-------------|-----------------|-------------------|
| Zone 1 | Producer script | Python script (local or EC2) | Generates synthetic stream event records and pushes them into Firehose via PutRecord API | Python, calls PutRecord on Firehose delivery stream |
| Zone 1 | Kinesis Data Firehose delivery stream | Amazon Kinesis Data Firehose | Receives stream events from the producer, buffers them, and delivers batch JSON files to S3 raw bucket | Direct PUT mode, buffer 5 MB or 60 seconds, lands JSON batch files under streams/YYYY/MM/DD/HH/ date path prefix |
| Zone 1 | CloudWatch log group /aws/kinesisfirehose/music-streaming-streams-ingestion | Amazon CloudWatch Logs | Captures Firehose delivery errors and S3 delivery logs | 30-day retention |
| Zone 2 | S3 raw bucket (EventBridge source) | Amazon S3 | Emits ObjectCreated events to EventBridge whenever Firehose lands a new file | Versioning enabled, AES256 encryption, EventBridge notifications enabled |
| Zone 2 | EventBridge default bus | Amazon EventBridge | Receives ObjectCreated events from S3 and routes them to matching rules | Default AWS event bus |
| Zone 2 | EventBridge rule "streams_uploaded" | Amazon EventBridge | Filters S3 events to only those from the raw bucket under the streams/ prefix and routes them to SQS | Filter: source=aws.s3, detail-type=Object Created, bucket=raw bucket name, key prefix=streams/ |
| Zone 2 | SQS queue "pipeline_events" | Amazon SQS | Decouples the EventBridge event from the Step Functions execution; provides retry buffering | Visibility timeout=300s, message retention=1 day |
| Zone 2 | SQS DLQ "pipeline_dlq" | Amazon SQS | Receives messages that failed processing three times so they can be inspected and replayed | maxReceiveCount=3, message retention=14 days |
| Zone 2 | EventBridge Pipe "sqs_to_sfn" | Amazon EventBridge Pipes | Polls SQS and starts a new Step Functions execution per message | batch_size=1, enrichment_execution_role=pipes_role, target_parameters FIRE_AND_FORGET, calls StartExecution on Step Functions |
| Zone 3 | Step Functions state machine | AWS Step Functions (Standard workflow) | Orchestrates all pipeline stages in order, handles concurrency locking, routes to failure states on error | STANDARD type, logging level=ALL, include_execution_data=true, X-Ray tracing enabled, logs to /aws/states/music-streaming |
| Zone 3 | State: NormalizeInput | Step Functions Pass state | Resets input to an empty object {} to prevent stale input from affecting downstream states | Pass state, sets ResultPath to root input |
| Zone 3 | State: CheckAlreadyRunning | Step Functions Task state | Lists all RUNNING executions of this state machine to detect concurrency conflicts | aws-sdk:sfn:listExecutions |
| Zone 3 | State: FindOldestRunning | Step Functions Pass state | Extracts the ARN of the oldest running execution from the listExecutions result | Pass state with JSONPath extraction |
| Zone 3 | State: IsAnotherRunning | Step Functions Choice state | Routes to the main pipeline if this is the oldest execution; otherwise waits | Compares oldest ARN to current execution ARN |
| Zone 3 | State: WaitForPreviousRun | Step Functions Wait state | Pauses execution for 60 seconds before rechecking for concurrency | Wait 60 seconds, loops back to CheckAlreadyRunning |
| Zone 3 | State: NotifyPipelineStarted | Step Functions Task state | Invokes the pipeline_notifier Lambda to post a "Pipeline Started" message to Slack | lambda:invoke, event_type=started |
| Zone 3 | State: StartRawCrawler | Step Functions Task state | Triggers the Glue raw crawler to discover and catalog any new files in the raw S3 bucket | aws-sdk:glue:startCrawler |
| Zone 3 | State: WaitForCrawler | Step Functions Wait state | Pauses for 45 seconds to allow the crawler to run before checking its status | Wait 45 seconds |
| Zone 3 | State: CheckCrawlerStatus | Step Functions Task state | Reads the current State field of the raw crawler to determine if it has finished | aws-sdk:glue:getCrawler |
| Zone 3 | State: IsCrawlerReady | Step Functions Choice state | Advances when crawler State==READY; loops through WaitForCrawler otherwise | Choice on getCrawler State field |
| Zone 3 | State: CheckCatalogTables | Step Functions Task state | Verifies the streams table exists in the Glue Data Catalog; routes to NoStreamsToProcess on EntityNotFoundException | aws-sdk:glue:getTable |
| Zone 3 | State: CheckStreamsExist | Step Functions Task state | Lists objects in the raw streams/ prefix to confirm files are present before launching Glue jobs | aws-sdk:s3:listObjectsV2 |
| Zone 3 | State: AreThereStreams | Step Functions Choice state | Advances to ValidateData if files are present (KeyCount > 0); routes to NoStreamsToProcess otherwise | Choice on KeyCount field |
| Zone 3 | State: NoStreamsToProcess | Step Functions Succeed state | Terminal success state when there is nothing to process | Succeed terminal |
| Zone 3 | State: ValidateData | Step Functions Task state | Runs the Glue validation job synchronously | glue:startJobRun.sync |
| Zone 3 | State: TransformData | Step Functions Task state | Runs the Glue etl_transform job synchronously | glue:startJobRun.sync |
| Zone 3 | State: AggregateKPIs | Step Functions Task state | Runs the Glue kpi_aggregation job synchronously | glue:startJobRun.sync |
| Zone 3 | State: LoadDynamoDB | Step Functions Task state | Runs the Glue dynamodb_loader job synchronously | glue:startJobRun.sync |
| Zone 3 | State: StartCuratedCrawler | Step Functions Task state | Triggers the curated crawler to refresh Athena partition metadata; non-fatal (always proceeds to ArchiveFiles) | aws-sdk:glue:startCrawler, Catch routes to ArchiveFiles on any error |
| Zone 3 | State: ArchiveFiles | Step Functions Task state | Runs the Glue archive job synchronously to copy and delete processed raw files | glue:startJobRun.sync |
| Zone 3 | State: NotifyPipelineSucceeded | Step Functions Task state | Invokes the pipeline_notifier Lambda to post a "Pipeline Succeeded" message to Slack | lambda:invoke, event_type=succeeded |
| Zone 3 | State: PipelineSucceeded | Step Functions Succeed state | Terminal success state | Succeed terminal |
| Zone 3 | State: NotifyFailure | Step Functions Task state | Publishes the error details to SNS pipeline_alerts; reached via Catch from any fatal task state | sns:publish, publishes error.Error and error.Cause |
| Zone 3 | State: NotifySlackPipelineFailed | Step Functions Task state | Invokes the pipeline_notifier Lambda to post a "Pipeline FAILED" message with error details to Slack | lambda:invoke, event_type=failed, passes $.error object |
| Zone 3 | State: PipelineFailed | Step Functions Fail state | Terminal failure state | Fail terminal |
| Zone 3 | CloudWatch log group /aws/states/music-streaming | Amazon CloudWatch Logs | Receives all Step Functions state transition logs including full input/output data | 30-day retention, level=ALL, include_execution_data=true, X-Ray |
| Zone 4 | Glue Data Catalog database "music_streaming_db" | AWS Glue Data Catalog | Central metadata store for all tables; maps table names to S3 locations and schemas | Database name: music_streaming_db |
| Zone 4 | Raw crawler "music-streaming-raw-crawler" | AWS Glue Crawler | Discovers and catalogs the songs, streams, and users datasets in the raw S3 bucket | Targets: s3://raw/songs/, s3://raw/streams/, s3://raw/users/; update_behavior=UPDATE_IN_DATABASE; delete_behavior=LOG |
| Zone 4 | Curated crawler "music-streaming-curated-crawler" | AWS Glue Crawler | Refreshes Athena partition metadata after each successful pipeline run | Target: s3://curated/gold/; update_behavior=UPDATE_IN_DATABASE |
| Zone 4 | Glue Job 1: validation | AWS Glue (Spark, Glue 4.0) | Validates that required tables exist, are non-empty, and have required columns | G.1X, 2 workers, timeout=10min, logs to /aws/glue/music-streaming, --extra-py-files monitoring.zip, retries TableNotFound with exponential backoff (10s, 20s, 40s) |
| Zone 4 | Glue Job 2: etl_transform | AWS Glue (Spark, Glue 4.0) | Joins streams and songs on track_id, derives stream_date, deduplicates, writes enriched Parquet to silver/ | G.1X, 2 workers, timeout=30min, dynamic partition overwrite mode, output: s3://curated/silver/enriched_streams/ |
| Zone 4 | Glue Job 3: kpi_aggregation | AWS Glue (Spark, Glue 4.0) | Computes genre KPIs, top 3 songs per genre per day, top 5 genres per day; writes three gold/ Parquet datasets | G.1X, 2 workers, timeout=30min, uses row_number() window function, composite key genre_date = track_genre + "#" + stream_date |
| Zone 4 | Glue Job 4: dynamodb_loader | AWS Glue (Spark, Glue 4.0) | Reads all three gold/ Parquet datasets and loads them into the three DynamoDB tables | G.1X, 2 workers, timeout=30min, foreachPartition + batch_writer, deduplicates on primary key, converts floats to Decimal |
| Zone 4 | Glue Job 5: archive | AWS Glue (pythonshell) | Copies processed raw stream files to the archive bucket then deletes originals from raw | pythonshell worker type, timeout=10min, pure boto3, reads key list from Step Functions input |
| Zone 4 | monitoring/ package (monitoring.zip) | Python package on S3 | Provides PipelineMonitor context manager and SlackNotifier HTTP client to all Glue jobs | Uploaded to s3://curated/scripts/monitoring.zip, attached via --extra-py-files to all five jobs |
| Zone 4 | CloudWatch log group /aws/glue/music-streaming | Amazon CloudWatch Logs | Receives continuous driver and executor logs from all five Glue jobs; job insights stream | 30-day retention, --enable-continuous-cloudwatch-log, --enable-job-insights |
| Zone 5 | S3 raw bucket (Bronze) | Amazon S3 | Landing zone for all ingested data; songs/ and users/ hold static reference CSVs; streams/ holds Firehose JSON batches that trigger the pipeline | Versioning enabled, AES256 encryption, EventBridge notifications enabled; lifecycle: noncurrent version expiry 7 days (recommended) |
| Zone 5 | S3 curated bucket (Silver/Gold) | Amazon S3 | Holds Glue job outputs at Silver (enriched_streams/) and Gold (genre_kpis/, top_songs/, top_genres/) quality levels; also holds Glue scripts and tmp/ | Versioning enabled, AES256 encryption; lifecycle: noncurrent version expiry 7 days (recommended) |
| Zone 5 | S3 archive bucket | Amazon S3 | Long-term cold storage for processed raw stream files after the archive Glue job has moved them out of the raw bucket | AES256 encryption; lifecycle: transition to GLACIER after 90 days |
| Zone 6 | DynamoDB genre_kpis table | Amazon DynamoDB | Stores daily per-genre KPI aggregations written by the dynamodb_loader Glue job | PK: genre_date (String, format: "Afrobeats#2026-05-17"); attributes: stream_date, track_genre, listen_count, unique_listeners, total_listen_time_ms, avg_listen_time_ms_per_user; PAY_PER_REQUEST, PITR enabled |
| Zone 6 | DynamoDB top_songs table | Amazon DynamoDB | Stores the top 3 songs per genre per day written by the dynamodb_loader Glue job | PK: genre_date (String), SK: rank (Number, 1-3); attributes: stream_date, track_genre, track_id, track_name, play_count; PAY_PER_REQUEST, PITR enabled |
| Zone 6 | DynamoDB top_genres table | Amazon DynamoDB | Stores the top 5 genres per day written by the dynamodb_loader Glue job | PK: date (String), SK: rank (Number, 1-5); attributes: track_genre, listen_count; PAY_PER_REQUEST, PITR enabled |
| Zone 6 | Amazon Athena | Amazon Athena | Ad-hoc SQL query engine for the gold/ Parquet datasets; reads data directly from S3 via the Glue Data Catalog | Queries gold/ Parquet; partitions refreshed by curated crawler after each pipeline run |
| Zone 7 | Lambda function pipeline_notifier | AWS Lambda | Posts Block Kit formatted messages to Slack via direct Incoming Webhook URL for pipeline lifecycle events | Python 3.12, 128 MB, 10s timeout, SLACK_APP_WEBHOOK_URL environment variable, IAM role: pipeline_notifier_role (AWSLambdaBasicExecutionRole only) |
| Zone 7 | PipelineMonitor | Python class (monitoring/ package) | Context manager that wraps each Glue job stage and calls SlackNotifier on start, success, and failure | Imported in all five Glue jobs via monitoring.zip --extra-py-files |
| Zone 7 | SlackNotifier | Python class (monitoring/ package) | HTTP client that sends Block Kit formatted messages to the Slack Incoming Webhook URL | Called by PipelineMonitor; resolves URL via resolveWebhookUrl from job arg or env var |
| Zone 7 | Slack Incoming Webhook URL | Slack (external) | Target endpoint for all direct webhook notifications from Lambda and Glue jobs | External HTTP endpoint; URL stored in Lambda env var and Glue job argument --slack_webhook_url |
| Zone 8 | SNS topic "pipeline_alerts" | Amazon SNS | Fan-out hub for all infrastructure-level alerts; receives from Step Functions NotifyFailure, CloudWatch alarms, and EventBridge rules | Publishers: NotifyFailure state, 9 CloudWatch alarms, EventBridge pipeline_succeeded rule, EventBridge alarm_state_change rule |
| Zone 8 | EventBridge rule "pipeline_succeeded" | Amazon EventBridge | Catches Step Functions SUCCEEDED execution status events and formats a success notification for SNS | Trigger: execution status change to SUCCEEDED; input transformer formats human-readable message with timestamp and ARN |
| Zone 8 | EventBridge rule "alarm_state_change" | Amazon EventBridge | Catches CloudWatch alarm transitions to ALARM for this project's alarms and republishes with a human-readable formatted message | Trigger: CloudWatch alarm state change to ALARM; input transformer includes what fired, why, when, console link |
| Zone 8 | AWS Chatbot (slack_channel_configuration) | AWS Chatbot | Subscribes to SNS pipeline_alerts and forwards messages to a Slack channel; conditionally provisioned | Only created when Terraform variables slack_workspace_id and slack_channel_id are set; IAM role: chatbot_role (CloudWatch ReadOnly) |
| Zone 9 | CloudWatch alarm sfn_execution_failed | Amazon CloudWatch | Triggers when Step Functions reports one or more failed executions | Metric: ExecutionsFailed >= 1, 5-minute evaluation window, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm sfn_execution_timed_out | Amazon CloudWatch | Triggers when a Step Functions execution times out | Metric: ExecutionsTimedOut >= 1, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm sqs_dlq_has_messages | Amazon CloudWatch | Triggers when any message lands in the SQS DLQ, indicating a failed trigger chain event | Metric: DLQ ApproximateNumberOfMessagesVisible >= 1, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm sqs_messages_stuck | Amazon CloudWatch | Triggers when the main SQS queue has old messages that have not been consumed, indicating a stalled consumer | Metric: pipeline_events ApproximateAgeOfOldestMessage >= 900s, 3 evaluation periods, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm glue_job_failed (validation) | Amazon CloudWatch | Triggers when the validation Glue job reports a task failure | Metric: numFailedTasks >= 1, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm glue_job_failed (etl_transform) | Amazon CloudWatch | Triggers when the etl_transform Glue job reports a task failure | Metric: numFailedTasks >= 1, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm glue_job_failed (kpi_aggregation) | Amazon CloudWatch | Triggers when the kpi_aggregation Glue job reports a task failure | Metric: numFailedTasks >= 1, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm glue_job_failed (dynamodb_loader) | Amazon CloudWatch | Triggers when the dynamodb_loader Glue job reports a task failure | Metric: numFailedTasks >= 1, publishes to SNS pipeline_alerts |
| Zone 9 | CloudWatch alarm glue_job_failed (archive) | Amazon CloudWatch | Triggers when the archive Glue job reports a task failure | Metric: numFailedTasks >= 1, publishes to SNS pipeline_alerts |
| Zone 10 | IAM role: glue_role | AWS IAM | Service role assumed by all five Glue jobs and both Glue crawlers | Trusted principal: glue.amazonaws.com; grants S3 R/W on all three buckets, DynamoDB writes on all three tables, CloudWatch Logs, Glue Data Catalog access |
| Zone 10 | IAM role: sfn_role | AWS IAM | Service role assumed by the Step Functions state machine | Trusted principal: states.amazonaws.com; grants Glue job run and crawler control, S3 ListBucket on streams/, SNS publish to pipeline_alerts, Lambda invoke on pipeline_notifier, states:ListExecutions (self), CloudWatch Logs, X-Ray |
| Zone 10 | IAM role: pipes_role | AWS IAM | Service role assumed by the EventBridge Pipe | Trusted principal: pipes.amazonaws.com; grants SQS consume on pipeline_events queue, states:StartExecution on the pipeline state machine |
| Zone 10 | IAM role: firehose_role | AWS IAM | Service role assumed by the Kinesis Data Firehose delivery stream | Trusted principal: firehose.amazonaws.com; grants S3 PutObject on the raw bucket, CloudWatch Logs write to the firehose log group |
| Zone 10 | IAM role: chatbot_role | AWS IAM | Service role assumed by AWS Chatbot | Trusted principal: chatbot.amazonaws.com; grants CloudWatch ReadOnly for alert enrichment |
| Zone 10 | IAM role: pipeline_notifier_role | AWS IAM | Service role assumed by the pipeline_notifier Lambda function | Trusted principal: lambda.amazonaws.com; grants AWSLambdaBasicExecutionRole (CloudWatch Logs write only) |
| Zone 11 | provider.tf | Terraform file | Declares the AWS provider, region, and default tags applied to all resources | default_tags: Project, Environment, ManagedBy; AWS provider ~> 5.0 |
| Zone 11 | variables.tf | Terraform file | Declares all configurable input variables | region, environment, bucket names, Slack IDs, webhook URL, alert email, and other per-environment knobs |
| Zone 11 | main.tf | Terraform file | Provisions S3 buckets, DynamoDB tables, Glue Data Catalog database, and glue_role IAM role | Core infrastructure; all other .tf files depend on outputs from this file |
| Zone 11 | ingestion.tf | Terraform file | Provisions the Kinesis Data Firehose delivery stream and its IAM role | Firehose delivery stream, firehose_role |
| Zone 11 | glue_jobs.tf | Terraform file | Provisions all five Glue job definitions, uploads monitoring.zip and Glue scripts to S3 | Also creates a Glue workflow and five triggers for manual/scheduled triggering outside of Step Functions |
| Zone 11 | step_functions.tf | Terraform file | Provisions the Step Functions state machine using jsonencode for the ASL definition, and the sfn_role | State machine definition, sfn_role |
| Zone 11 | messaging.tf | Terraform file | Provisions SQS queues, SNS topic, EventBridge rule streams_uploaded, EventBridge Pipe, and pipes_role | pipeline_events, pipeline_dlq, pipeline_alerts, sqs_to_sfn Pipe |
| Zone 11 | monitoring.tf | Terraform file | Provisions all nine CloudWatch alarms, EventBridge success and alarm_state_change rules, AWS Chatbot, and chatbot_role | All observability infrastructure except log groups (those are created by each service) |
| Zone 11 | lambda.tf | Terraform file | Packages and provisions the pipeline_notifier Lambda function and pipeline_notifier_role | Uses archive_file data source for packaging |
| Zone 11 | outputs.tf | Terraform file | Prints all resource names and ARNs after terraform apply | All resource identifiers |

---

## Section 6 — Failure Path: Step-by-Step Walkthrough

This section documents exactly what happens when any Glue job fails during a pipeline execution. The same Catch mechanism applies to ValidateData, TransformData, AggregateKPIs, LoadDynamoDB, and ArchiveFiles. The non-fatal StartCuratedCrawler state does not use this path.

### Scenario: Glue Job Fails During Execution

#### Step 1 — Glue reports a job run failure

The Glue job encounters an unrecoverable error (for example, a schema mismatch in the validation job, a Spark executor crash in etl_transform, or a DynamoDB throttling exception in dynamodb_loader that exhausts retries). Glue transitions the job run to FAILED status and returns an error to the Step Functions task state.

#### Step 2 — PipelineMonitor fires a Slack failure notification (direct webhook path)

Before the exception propagates out of the Glue job Python process, the PipelineMonitor context manager catches it. It calls SlackNotifier.sendJobFailed, which performs an HTTP POST to the Slack Incoming Webhook URL. A Block Kit formatted message with a red indicator arrives in the Slack channel within seconds. This happens independently of Step Functions and SNS.

Boxes that activate:
- PipelineMonitor (inside the failing Glue job)
- SlackNotifier
- Slack Incoming Webhook URL
- Slack channel

#### Step 3 — CloudWatch Glue alarm fires (infrastructure path, parallel)

The failing Glue job emits a numFailedTasks metric to CloudWatch. The corresponding CloudWatch alarm (one of the five glue_job_failed alarms) transitions from OK to ALARM. It publishes an alarm notification to SNS topic "pipeline_alerts".

Boxes that activate:
- CloudWatch log group /aws/glue/music-streaming (receives the failure log)
- CloudWatch alarm glue_job_failed (for the specific job)
- SNS topic "pipeline_alerts"
- Email subscriber (receives alarm email)
- AWS Chatbot (receives alarm, forwards to Slack channel)

#### Step 4 — Step Functions Catch triggers NotifyFailure

The glue:startJobRun.sync task state receives the failure. The Catch block on that state matches any error type (States.ALL) and transitions execution to the NotifyFailure state, passing the error context including error.Error (the error type string) and error.Cause (the full error message and stack trace) into the state input as $.error.

Boxes that activate:
- Step Functions state machine (transitions to NotifyFailure)
- CloudWatch log group /aws/states/music-streaming (records the state transition and full error payload, level=ALL)

#### Step 5 — NotifyFailure publishes to SNS

The NotifyFailure state executes sns:publish. It sends a message to SNS topic "pipeline_alerts" containing error.Error and error.Cause. This is a second SNS notification (separate from the CloudWatch alarm that fired in Step 3; this one carries the actual error details from inside the state machine).

Boxes that activate:
- SNS topic "pipeline_alerts"
- Email subscriber (receives failure detail email)
- AWS Chatbot (receives failure detail, forwards to Slack channel)

#### Step 6 — NotifySlackPipelineFailed invokes the Lambda function

After NotifyFailure completes, execution moves to NotifySlackPipelineFailed. This state invokes the pipeline_notifier Lambda function with event_type=failed and the full $.error object. The Lambda function constructs a Block Kit formatted "Pipeline FAILED" message that includes the error type and cause, then performs an HTTP POST to the Slack Incoming Webhook URL.

Boxes that activate:
- Step Functions state machine (transitions to NotifySlackPipelineFailed)
- Lambda pipeline_notifier function
- Slack Incoming Webhook URL
- Slack channel (receives "Pipeline FAILED" Block Kit message with error details)

Note: The NotifySlackPipelineFailed state has its own Catch that routes forward to PipelineFailed even if the Lambda invocation fails, ensuring that a Slack delivery failure does not cause the state machine to get stuck.

#### Step 7 — PipelineFailed terminal state

Execution transitions to PipelineFailed, which is a Fail terminal state. The Step Functions execution is now marked as FAILED.

Boxes that activate:
- Step Functions state machine (enters PipelineFailed Fail state, execution status becomes FAILED)
- CloudWatch log group /aws/states/music-streaming (records the terminal state transition)

#### Step 8 — sfn_execution_failed CloudWatch alarm fires

The Step Functions execution failure causes the ExecutionsFailed metric in CloudWatch to increment. The sfn_execution_failed alarm (threshold >= 1, 5-minute window) transitions to ALARM and publishes to SNS topic "pipeline_alerts" again. This is a third SNS notification, this time from the Step Functions execution-level metric.

Boxes that activate:
- CloudWatch alarm sfn_execution_failed
- SNS topic "pipeline_alerts"
- Email subscriber
- AWS Chatbot --> Slack channel

### Summary of All Boxes That Light Up During a Glue Job Failure

Listed in approximate chronological order of activation:

1. Failing Glue job (job run transitions to FAILED)
2. PipelineMonitor context manager (inside the job, catches exception)
3. SlackNotifier (called by PipelineMonitor)
4. Slack Incoming Webhook URL (receives HTTP POST from SlackNotifier)
5. Slack channel (receives per-stage failure message from PipelineMonitor -- direct webhook path)
6. CloudWatch log group /aws/glue/music-streaming (receives job failure logs)
7. CloudWatch alarm glue_job_failed for the specific job (transitions to ALARM)
8. SNS topic "pipeline_alerts" (receives CloudWatch alarm notification)
9. Email subscriber (receives CloudWatch alarm email)
10. AWS Chatbot (receives CloudWatch alarm from SNS, forwards to Slack)
11. Slack channel (receives CloudWatch alarm notification via Chatbot -- infrastructure path)
12. Step Functions state machine (Catch fires, transitions to NotifyFailure)
13. CloudWatch log group /aws/states/music-streaming (records Catch transition and error payload)
14. NotifyFailure state (sns:publish with error.Error and error.Cause)
15. SNS topic "pipeline_alerts" (receives failure detail from NotifyFailure state)
16. Email subscriber (receives failure detail email from SNS)
17. AWS Chatbot (receives failure detail from SNS, forwards to Slack)
18. Slack channel (receives failure detail via Chatbot -- infrastructure path)
19. NotifySlackPipelineFailed state (lambda:invoke with event_type=failed and $.error)
20. Lambda pipeline_notifier function (builds "Pipeline FAILED" Block Kit message)
21. Slack Incoming Webhook URL (receives HTTP POST from Lambda)
22. Slack channel (receives "Pipeline FAILED" Block Kit message with error details -- direct webhook path)
23. PipelineFailed Fail terminal state (execution marked FAILED)
24. CloudWatch log group /aws/states/music-streaming (records terminal Fail state)
25. CloudWatch alarm sfn_execution_failed (transitions to ALARM on ExecutionsFailed metric)
26. SNS topic "pipeline_alerts" (receives execution-failed alarm notification)
27. Email subscriber (receives execution-failed alarm email)
28. AWS Chatbot (receives execution-failed alarm from SNS, forwards to Slack)
29. Slack channel (receives execution-failed alarm via Chatbot -- infrastructure path)

### Arrows That Fire During a Glue Job Failure (referencing Section 3 arrow numbering)

- Arrow 52, 55, 60, 65, or 69 (Glue job --> CloudWatch Logs, whichever job failed)
- A2 notification arrows 7-21 (PipelineMonitor --> SlackNotifier --> Slack, for the failing stage)
- B1 notification arrows 6-10 (CloudWatch glue_job_failed alarm --> SNS)
- Section 3 arrows 77-81 (Catch --> NotifyFailure --> SNS --> NotifySlackPipelineFailed --> Lambda --> Slack --> PipelineFailed)
- B1 notification arrow 2 (sfn_execution_failed alarm --> SNS)
- B2 notification arrows 13-15 (SNS --> Email, SNS --> Chatbot --> Slack, for each of the above SNS publishes)
- Section 3 arrows 75-76 (Step Functions --> CloudWatch Logs, X-Ray, for every state transition including the failure path)
