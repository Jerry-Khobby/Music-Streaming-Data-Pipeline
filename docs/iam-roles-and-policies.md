# IAM Roles and Policies

## What IAM Roles and Policies Are

AWS Identity and Access Management (IAM) controls who or what can act inside your AWS account and what those actors are allowed to do. Two constructs do this work:

**A role** is an identity that an AWS service can temporarily assume. It is not a user. It has no password and no long-term credentials. When a service like Glue or Step Functions needs to call another AWS API, it assumes a role and receives short-lived credentials automatically. Once the task is done, the credentials expire.

**A policy** is a JSON document attached to a role that lists exactly which API actions are allowed or denied on which resources. Without a policy, a role can do nothing. With a policy, it can do only what the policy explicitly permits.

Every role in this project follows the same two-part structure:

1. A **trust policy** that declares which service is allowed to assume the role.
2. One or more **permission policies** that declare what that service can do once it has assumed the role.

---

## The Least Privilege Principle

The least privilege principle means granting only the permissions required for a task to succeed and nothing more. A Glue job that reads from S3 and writes to DynamoDB does not need permission to create EC2 instances or delete IAM users. Scoping permissions tightly limits the blast radius if credentials are ever misused or if a job is accidentally misconfigured.

This project applies least privilege in the following ways:

- Each of the three roles in the project is scoped to a specific service and a specific set of tasks.
- Permission policies are grouped by logical concern using named `sid` (statement ID) blocks, making it clear why each permission exists.
- Where a resource ARN is known at deploy time, it is used directly rather than a wildcard. Where AWS requires a wildcard (Glue job ARNs for the `.sync` integration, CloudWatch Logs delivery APIs), that is noted below.

---

## Role 1 — glue-pipeline-role

**Terraform resource:** `aws_iam_role.glue_role`
**Role name:** `glue-pipeline-role`
**Defined in:** `main.tf`

### Purpose

This role is assumed by every AWS Glue job in the pipeline. When Glue starts a job run, it assumes this role to obtain the credentials needed to read from S3, write to S3, write to DynamoDB, and emit logs to CloudWatch.

### Trust Policy

```json
{
  "Effect": "Allow",
  "Action": "sts:AssumeRole",
  "Principal": {
    "Service": "glue.amazonaws.com"
  }
}
```

The trust policy names `glue.amazonaws.com` as the only principal allowed to assume this role. No human user, no other service, and no other account can assume it.

### Attached Policies

Four AWS managed policies are attached using `aws_iam_role_policy_attachment`:

**AWSGlueServiceRole**
Grants the baseline permissions Glue requires to function: reading job definitions from the Glue Data Catalog, writing job metrics, and accessing the Glue control plane. Without this policy, Glue cannot start or report the status of a job run.

**AmazonS3FullAccess**
Grants read and write access across all S3 buckets in the account. The Glue jobs in this pipeline read raw CSV files from the bronze bucket, write enriched Parquet files to the silver layer, write aggregated KPI Parquet files to the gold layer, and the archive job copies files between the raw and archive buckets. All of these operations require broad S3 access. In a production environment with strict data boundaries, this would be replaced with a custom policy scoped to the three specific bucket ARNs.

**AmazonDynamoDBFullAccess**
Grants read and write access to all DynamoDB tables. The `dynamodb_loader` job performs `BatchWriteItem` operations across three tables: `genre_kpis`, `top_songs`, and `top_genres`. Full DynamoDB access is used here for simplicity. In production this would be scoped to those three table ARNs with only `dynamodb:BatchWriteItem` and `dynamodb:PutItem` allowed.

**CloudWatchLogsFullAccess**
Grants the ability to create and write to CloudWatch log streams. All five Glue jobs are configured with `--enable-continuous-cloudwatch-log`, which streams driver and executor logs in real time to the `/aws/glue/music-streaming` log group. Without this policy, logs would not appear in CloudWatch and debugging job failures would require downloading logs from S3 after the job completes.

### Why One Shared Role for All Glue Jobs

All five Glue jobs share a single role rather than one role per job. This is a deliberate trade-off. Because every job in this pipeline touches the same three S3 buckets and the same DynamoDB tables, their permission requirements are identical. Creating five separate roles with identical permissions would add infrastructure complexity without adding security value. The shared role is justified because the jobs form a single pipeline with unified data ownership.

---

## Role 2 — music-streaming-sfn-role

**Terraform resource:** `aws_iam_role.sfn_role`
**Role name:** `music-streaming-sfn-role`
**Defined in:** `step_functions.tf`

### Purpose

This role is assumed by the Step Functions state machine. Step Functions itself does not process data. Its job is to orchestrate other services: starting Glue crawlers, starting Glue jobs, polling their status, publishing SNS notifications, and writing execution logs to CloudWatch. Every API call the state machine makes on behalf of the pipeline goes through this role.

### Trust Policy

```json
{
  "Effect": "Allow",
  "Action": "sts:AssumeRole",
  "Principal": {
    "Service": "states.amazonaws.com"
  }
}
```

Only the Step Functions service can assume this role.

### Permission Policy Statements

The inline policy attached via `aws_iam_role_policy.sfn_permissions` contains four named statements:

**GlueJobControl**
```
glue:StartJobRun
glue:GetJobRun
glue:GetJobRuns
glue:BatchStopJobRun
```
The `.sync` resource integration (`arn:aws:states:::glue:startJobRun.sync`) causes Step Functions to poll the Glue API internally until the job reaches a terminal state. This requires `StartJobRun` to launch the job and `GetJobRun` to check its status. `BatchStopJobRun` allows Step Functions to cancel a running job if the state machine execution is aborted. The resource is `*` because the Glue optimised integration requires it — the internal polling mechanism constructs job run ARNs dynamically and cannot be pre-scoped.

**GlueCrawlerControl**
```
glue:StartCrawler
glue:GetCrawler
```
The `StartRawCrawler` state uses the AWS SDK integration (`arn:aws:states:::aws-sdk:glue:startCrawler`) to fire the raw crawler at the start of every pipeline execution. The `CheckCrawlerStatus` state uses `glue:GetCrawler` to poll the crawler's `State` field every 30 seconds until it returns `READY`. These two actions are the minimum required to automate the crawler within the state machine.

**SnsPublish**
```
sns:Publish  →  scoped to aws_sns_topic.pipeline_alerts.arn only
```
The `NotifyFailure` state publishes a failure message to the SNS alerts topic when any pipeline step fails. The resource is scoped to the exact ARN of the alerts topic. The Step Functions role cannot publish to any other SNS topic in the account.

**CloudWatchLogs**
```
logs:CreateLogDelivery
logs:GetLogDelivery
logs:UpdateLogDelivery
logs:DeleteLogDelivery
logs:ListLogDeliveries
logs:PutResourcePolicy
logs:DescribeResourcePolicies
logs:DescribeLogGroups
```
The state machine is configured with `logging_configuration` set to `ERROR` level, sending execution event data to the `/aws/states/music-streaming` CloudWatch log group. AWS requires all eight of these log delivery management actions for the logging integration to function. They cannot be scoped to a specific log group ARN because the delivery management APIs operate at the account level.

**XRayTracing**
```
xray:PutTraceSegments
xray:PutTelemetryRecords
xray:GetSamplingRules
xray:GetSamplingTargets
```
The state machine has `tracing_configuration` enabled, which sends execution segments to AWS X-Ray. This enables distributed tracing across the Step Functions execution and any downstream Glue job logs that also emit X-Ray segments. These four actions are the minimum required by the X-Ray SDK.

---

## Role 3 — music-streaming-pipes-role

**Terraform resource:** `aws_iam_role.pipes_role`
**Role name:** `music-streaming-pipes-role`
**Defined in:** `messaging.tf`

### Purpose

This role is assumed by the EventBridge Pipe that connects the SQS queue to the Step Functions state machine. The Pipe's job is narrow: poll the SQS queue for S3 ObjectCreated events, consume each message, and call `StartExecution` on the state machine. It requires no access to S3, DynamoDB, Glue, or any other service.

### Trust Policy

```json
{
  "Effect": "Allow",
  "Action": "sts:AssumeRole",
  "Principal": {
    "Service": "pipes.amazonaws.com"
  }
}
```

Only EventBridge Pipes can assume this role.

### Permission Policy Statements

The inline policy attached via `aws_iam_role_policy.pipes_permissions` contains two named statements:

**SqsConsume**
```
sqs:ReceiveMessage    →  scoped to pipeline_events queue ARN only
sqs:DeleteMessage     →  scoped to pipeline_events queue ARN only
sqs:GetQueueAttributes →  scoped to pipeline_events queue ARN only
```
EventBridge Pipes uses long-polling to read messages from the SQS queue. `ReceiveMessage` fetches the message, `DeleteMessage` removes it after successful processing so it is not delivered twice, and `GetQueueAttributes` lets the Pipe read the queue configuration. All three are scoped to the single pipeline events queue ARN. The Pipe cannot touch the DLQ or any other queue in the account.

**StartExecution**
```
states:StartExecution  →  scoped to pipeline state machine ARN only
```
Once a message is received from SQS, the Pipe calls `StartExecution` on the state machine to begin a pipeline run. The resource is the exact ARN of the `music-streaming-pipeline` state machine. The Pipe cannot start any other state machine in the account.

---

## Resource-Based Policies

In addition to the three IAM roles, two resource-based policies control which services can send messages to shared resources:

**SQS queue policy — pipeline_events**
Allows `events.amazonaws.com` (the EventBridge service) to call `sqs:SendMessage` on the pipeline events queue. The condition `aws:SourceArn` restricts this to messages originating specifically from the `music-streaming-streams-uploaded` EventBridge rule. Without this condition, any EventBridge rule in the account could send messages to this queue.

**SNS topic policy — pipeline_alerts**
Allows `states.amazonaws.com` (the Step Functions service) to call `sns:Publish` on the alerts topic. This permission exists in addition to the `SnsPublish` statement in the Step Functions role policy because SNS evaluates both the identity-based policy on the caller and the resource-based policy on the topic. Both must allow the action for it to succeed.

---

## Summary Table

| Role | Assumed by | Key permissions | Resource scope |
|---|---|---|---|
| `glue-pipeline-role` | `glue.amazonaws.com` | S3 read/write, DynamoDB write, CloudWatch Logs | All buckets and tables (managed policies) |
| `music-streaming-sfn-role` | `states.amazonaws.com` | Glue job control, crawler control, SNS publish, logs, X-Ray | SNS scoped to alerts topic ARN; Glue requires wildcard |
| `music-streaming-pipes-role` | `pipes.amazonaws.com` | SQS consume, Step Functions start | Scoped to pipeline queue ARN and state machine ARN |

---

## How the Roles Connect During a Pipeline Run

When a new file lands in S3:

1. EventBridge detects the upload and sends a message to SQS. The **SQS queue policy** permits this.
2. EventBridge Pipes polls SQS using the **pipes-role** (`SqsConsume`), deletes the message, then calls `StartExecution` using the same role (`StartExecution`).
3. Step Functions assumes the **sfn-role** and begins executing states. It calls `glue:StartCrawler` and `glue:GetCrawler` (`GlueCrawlerControl`), then starts each Glue job using `glue:StartJobRun` and polls with `glue:GetJobRun` (`GlueJobControl`).
4. Each Glue job runs under the **glue-pipeline-role**, reading from S3 (`AmazonS3FullAccess`), writing to S3, writing to DynamoDB (`AmazonDynamoDBFullAccess`), and streaming logs to CloudWatch (`CloudWatchLogsFullAccess`).
5. If any step fails, Step Functions calls `sns:Publish` using the **sfn-role** (`SnsPublish`). The **SNS topic policy** permits this call from the Step Functions service principal.
