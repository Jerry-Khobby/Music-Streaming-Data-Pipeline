# IAM Roles and Policies

## 1. Core Concepts

### What IAM Is and Why It Exists

AWS Identity and Access Management (IAM) is the authorization layer that sits in front of every AWS API call. Every action taken in AWS — starting a Glue job, writing a file to S3, publishing to SNS, starting a Step Functions execution — passes through IAM before it is permitted or denied. IAM answers two questions: who is making this request, and are they allowed to do it?

In a data pipeline, services talk to other services constantly. A Glue job reads from S3. Step Functions starts Glue jobs. An EventBridge Pipe consumes from SQS. None of these services can act on their own authority. Each one must assume a role that carries the permissions it needs. IAM is the mechanism that makes this controlled cross-service communication possible without any hardcoded credentials.

### Roles vs Users vs Groups

An IAM **user** represents a human or a long-lived machine identity with permanent credentials. An IAM **group** is a collection of users sharing the same policies. An IAM **role** is a temporary identity that a service, a user, or another account assumes for a specific purpose. When a service assumes a role, AWS Security Token Service (STS) issues short-lived credentials that expire automatically. No static access keys exist, which eliminates the most common credential leak vector.

This project uses only roles — never users or groups — because all actors are AWS services, not humans.

### Identity-Based Policies vs Resource-Based Policies

There are two distinct policy types in play in this project:

**Identity-based policies** are attached to a role and travel with that role. They say: this role is allowed to perform these actions. The Glue role carrying `AmazonS3FullAccess` is an example.

**Resource-based policies** are attached to a resource and control who can interact with that resource from outside the role system. The SQS queue policy and the SNS topic policy in this project are examples. For an API call to succeed across a service boundary, both the identity-based policy on the caller and the resource-based policy on the target must permit the action.

### Trust Policies

Every role has a trust policy — a special JSON document that defines which principal is allowed to call `sts:AssumeRole` on the role. Without a trust policy, a role cannot be assumed by anything and is useless. Every role in this project has a trust policy that names exactly one AWS service principal. This means the role can only ever be assumed by that one service and nothing else in the account.

### Inline Policies vs Managed Policies

**Managed policies** are standalone IAM policies created and versioned independently of any role. AWS publishes a library of managed policies. Attaching `AmazonS3FullAccess` to a role means the role inherits that policy's permissions. Managed policies are reusable but their permission boundaries are fixed by AWS.

**Inline policies** are written directly into a role and exist only as long as that role exists. They are used in this project for the Step Functions and EventBridge Pipes roles because those policies need to reference specific resource ARNs that are only known after Terraform provisions the infrastructure — for example, the exact ARN of the SNS topic or the SQS queue.

### The STS AssumeRole Flow

When a Glue job starts, the sequence is:

1. The Glue service contacts AWS STS and presents the role ARN configured on the job.
2. STS checks the trust policy on that role. Because `glue.amazonaws.com` is the configured principal and Glue is the caller, the check passes.
3. STS issues a set of temporary credentials: an access key ID, a secret access key, and a session token. These expire after a maximum of 12 hours.
4. Glue uses those temporary credentials to make API calls on the job's behalf.
5. Every API call is evaluated against the permission policies attached to the role.

This flow repeats for Step Functions, EventBridge Pipes, and every other service in the pipeline.

---

## 2. The Least Privilege Principle

### What It Means

Least privilege means granting an identity exactly the permissions it needs to perform its task and nothing more. An identity with excess permissions creates unnecessary risk: if the identity is compromised, a misconfigured job runs amok, or a bug introduces unintended behavior, the blast radius is proportional to the permissions it holds.

### How This Project Applies It

**Separate roles for separate services.** Rather than one omnipotent role shared across everything, this project has three distinct roles — one for Glue, one for Step Functions, one for EventBridge Pipes. Each role can only be assumed by its specific service. A compromised Glue job cannot assume the Step Functions role or call `states:StartExecution`. A misconfigured EventBridge Pipe cannot call `glue:StartJobRun` directly.

**Statement IDs communicate intent.** Every inline policy statement carries a `sid` (statement ID) such as `GlueJobControl`, `GlueCrawlerControl`, `SqsConsume`, and `StartExecution`. These names document why each permission group exists and make auditing the policy straightforward.

**Resource scoping where ARNs are known.** The Step Functions role's `SnsPublish` statement is scoped to the single SNS topic ARN. The EventBridge Pipes role's `SqsConsume` and `StartExecution` statements are each scoped to a single ARN. These roles cannot affect any other resource of those types in the account.

**Wildcards only where AWS requires them.** The Glue job control statement uses `resources = ["*"]` because the Step Functions `.sync` integration constructs job run ARNs dynamically at runtime and cannot be pre-scoped. The CloudWatch Logs delivery statements also require wildcards because the delivery management APIs are account-scoped. These exceptions are deliberate and documented rather than being a result of convenience.

**Managed policies are an acknowledged trade-off.** The Glue role uses `AmazonS3FullAccess` and `AmazonDynamoDBFullAccess` — AWS-managed policies that are broader than strictly necessary. This is an accepted trade-off for a development pipeline where all three buckets and all three DynamoDB tables belong to the same project. In a production environment with shared infrastructure, these would be replaced with custom inline policies scoped to the six specific resource ARNs.

---

## 3. IAM Role 1 — glue-pipeline-role

**Terraform resource:** `aws_iam_role.glue_role`
**Role name:** controlled by `var.glue_role_name`, defaults to `glue-pipeline-role`
**Defined in:** `main.tf`
**Used by:** all five Glue jobs and both Glue crawlers

### What This Role Does

This is the execution identity for everything that runs inside the Glue service — the validation job, the ETL transform job, the KPI aggregation job, the DynamoDB loader, the archive job, the raw crawler, and the curated crawler. When any of these run, they operate under this role's identity.

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

The trust policy is defined as a `data "aws_iam_policy_document"` block in Terraform, which generates the JSON. Only the Glue service can assume this role. A human developer, a Lambda function, an EC2 instance, or any other service in the account cannot assume it.

### Why Both Crawlers Use the Same Role as the Jobs

Both `aws_glue_crawler.raw_crawler` and `aws_glue_crawler.curated_crawler` reference `aws_iam_role.glue_role.arn` in their `role` attribute. Glue crawlers are Glue-service resources just like Glue jobs. They need the same trust principal (`glue.amazonaws.com`) and the same S3 read permissions to traverse bucket prefixes and infer schemas. Because the crawlers and jobs all touch the same S3 buckets and the same Glue Data Catalog, using one shared role is correct and avoids maintaining duplicate IAM configurations.

### Attached Managed Policies

**AWSGlueServiceRole**
ARN: `arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole`

This is the baseline policy published by AWS for Glue service roles. It grants permissions for Glue to manage its own metadata operations: reading job and crawler definitions from the Glue Data Catalog, writing job metrics and status, tagging resources, and working with Glue connections. Without this policy, the Glue service cannot perform its internal bookkeeping and job runs fail at startup before any user code executes.

**AmazonS3FullAccess**
ARN: `arn:aws:iam::aws:policy/AmazonS3FullAccess`

Every Glue job in this pipeline interacts with S3 in some way:

- `validation_job` reads table definitions that the crawler registered from S3 paths.
- `etl_transform_job` reads raw CSV data from the bronze bucket (`streams/`, `songs/`) and writes enriched Parquet files to the silver layer of the curated bucket.
- `kpi_aggregation_job` reads from silver and writes KPI Parquet files to the gold layer.
- `dynamodb_loader` reads from the gold layer.
- `archive_job` copies files from the raw bucket's `streams/` prefix to the archive bucket and then deletes them from the raw bucket.
- Both crawlers traverse S3 prefixes to infer schemas.

The operations span three different buckets across read, write, copy, and delete verbs. `AmazonS3FullAccess` covers all of them. In production, this would be replaced with a custom policy granting `s3:GetObject` and `s3:ListBucket` on the raw and curated buckets, and `s3:PutObject`, `s3:DeleteObject`, and `s3:GetObject` on the archive bucket.

**AmazonDynamoDBFullAccess**
ARN: `arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess`

The `dynamodb_loader` job writes to three DynamoDB tables using `boto3`'s `batch_writer`, which internally calls `BatchWriteItem`. The full access managed policy is used here because `BatchWriteItem` also requires `PutItem` and `DeleteItem` at the API level depending on the batch content. In production this would be a custom policy granting only `dynamodb:BatchWriteItem` and `dynamodb:PutItem` scoped to the three table ARNs: `genre_kpis`, `top_songs`, and `top_genres`.

**CloudWatchLogsFullAccess**
ARN: `arn:aws:iam::aws:policy/CloudWatchLogsFullAccess`

All five Glue jobs are configured with two job arguments that activate continuous logging:

```
--enable-continuous-cloudwatch-log = true
--continuous-log-logGroup          = /aws/glue/music-streaming
```

Continuous logging streams driver and executor output to CloudWatch in real time, rather than only making logs available after a job completes. This is critical for debugging long-running PySpark jobs. The `CloudWatchLogsFullAccess` policy grants the Glue executor the ability to create log streams under the log group and push log events to them.

### How the Role Is Referenced Across Resources

```
aws_iam_role.glue_role.arn
  → aws_glue_job.validation.role_arn
  → aws_glue_job.etl_transform.role_arn
  → aws_glue_job.kpi_aggregation.role_arn
  → aws_glue_job.dynamodb_loader.role_arn
  → aws_glue_job.archive.role_arn
  → aws_glue_crawler.raw_crawler.role
  → aws_glue_crawler.curated_crawler.role
```

Eight Glue resources reference the same role ARN. Terraform manages this as a dependency — if the role is not yet created, none of these resources can be provisioned.

---

## 4. IAM Role 2 — music-streaming-sfn-role

**Terraform resource:** `aws_iam_role.sfn_role`
**Role name:** `music-streaming-sfn-role`
**Defined in:** `step_functions.tf`
**Used by:** the Step Functions state machine `music-streaming-pipeline`

### Role Purpose

This role is the execution identity for the Step Functions state machine. Step Functions is an orchestrator — it does not process data itself. Instead it calls other AWS services on the pipeline's behalf. Every API call the state machine makes — starting a Glue crawler, starting a Glue job, publishing to SNS, writing execution logs — happens under this role's identity.

### Step Functions Trust Policy

```json
{
  "Effect": "Allow",
  "Action": "sts:AssumeRole",
  "Principal": {
    "Service": "states.amazonaws.com"
  }
}
```

Only the Step Functions service can assume this role. The Glue service, EventBridge, and all other services in the account cannot.

### Step Functions Inline Permission Policy

The policy is defined as a `data "aws_iam_policy_document"` block and attached as an inline policy via `aws_iam_role_policy.sfn_permissions`. It contains five named statements:

#### Statement: GlueJobControl

```
Actions:  glue:StartJobRun
          glue:GetJobRun
          glue:GetJobRuns
          glue:BatchStopJobRun
Resource: *
```

When the state machine reaches `ValidateData`, `TransformData`, `AggregateKPIs`, `LoadDynamoDB`, or `ArchiveFiles`, it calls `glue:StartJobRun` to launch the job. The `.sync` resource integration (`arn:aws:states:::glue:startJobRun.sync`) means Step Functions then polls `glue:GetJobRun` internally every few seconds until the job reaches a terminal state (`SUCCEEDED`, `FAILED`, `STOPPED`, or `TIMEOUT`). `glue:BatchStopJobRun` is required to cancel running jobs if the state machine execution itself is aborted or times out.

The resource must be `*` because the `.sync` integration constructs job run ARNs at runtime by appending a dynamically generated run ID. There is no way to pre-declare those ARNs in a policy.

#### Statement: GlueCrawlerControl

```
Actions:  glue:StartCrawler
          glue:GetCrawler
Resource: *
```

The `StartRawCrawler` state uses the AWS SDK integration (`arn:aws:states:::aws-sdk:glue:startCrawler`) to fire the raw crawler at the beginning of every pipeline execution. The `CheckCrawlerStatus` state polls `glue:GetCrawler` every 30 seconds, reading the `Crawler.State` field to determine when the crawler reaches `READY`. These two actions are the exact minimum required to automate the crawler inside the state machine without a separate trigger or manual intervention.

#### Statement: SnsPublish

```
Actions:  sns:Publish
Resource: aws_sns_topic.pipeline_alerts.arn  (exact ARN)
```

The `NotifyFailure` state publishes a failure message to the alerts SNS topic whenever any pipeline step raises an error. This is the only statement in the entire project where the resource is scoped to a specific ARN at the statement level. The Step Functions role cannot publish to any other SNS topic in the account.

#### Statement: CloudWatchLogs

```
Actions:  logs:CreateLogDelivery
          logs:GetLogDelivery
          logs:UpdateLogDelivery
          logs:DeleteLogDelivery
          logs:ListLogDeliveries
          logs:PutResourcePolicy
          logs:DescribeResourcePolicies
          logs:DescribeLogGroups
Resource: *
```

The state machine is configured with `logging_configuration` at `ERROR` level, directing execution event data to the `/aws/states/music-streaming` CloudWatch log group with `include_execution_data = true`. AWS requires all eight of these log delivery management actions for the Step Functions logging integration to configure itself. These APIs operate at the account level and cannot be scoped to a specific log group ARN — AWS enforces this requirement and the resource must be `*`.

#### Statement: XRayTracing

```
Actions:  xray:PutTraceSegments
          xray:PutTelemetryRecords
          xray:GetSamplingRules
          xray:GetSamplingTargets
Resource: *
```

The state machine has `tracing_configuration` enabled with X-Ray. This sends execution segments to AWS X-Ray, enabling distributed tracing across the full pipeline execution timeline. You can visualize exactly how long each state took, where latency occurred, and which state failed — all in the X-Ray service map. These four actions are the minimum required by the X-Ray SDK embedded in the Step Functions runtime.

---

## 5. IAM Role 3 — music-streaming-pipes-role

**Terraform resource:** `aws_iam_role.pipes_role`
**Role name:** `music-streaming-pipes-role`
**Defined in:** `messaging.tf`
**Used by:** the EventBridge Pipe `music-streaming-sqs-to-sfn`

### Pipes Role Purpose

This is the narrowest role in the project. EventBridge Pipes is the bridge between the SQS queue and the Step Functions state machine. Its only job is to poll the queue for messages and call `StartExecution` on the state machine when one arrives. It needs no access to S3, DynamoDB, Glue, SNS, or any other service.

### Pipes Trust Policy

```json
{
  "Effect": "Allow",
  "Action": "sts:AssumeRole",
  "Principal": {
    "Service": "pipes.amazonaws.com"
  }
}
```

Only the EventBridge Pipes service can assume this role.

### Pipes Inline Permission Policy

The policy contains exactly two statements:

#### Statement: SqsConsume

```
Actions:  sqs:ReceiveMessage
          sqs:DeleteMessage
          sqs:GetQueueAttributes
Resource: aws_sqs_queue.pipeline_events.arn  (exact ARN)
```

EventBridge Pipes uses long-polling to continuously check the queue for messages. `sqs:ReceiveMessage` fetches up to one message at a time (configured with `batch_size = 1`). `sqs:DeleteMessage` removes the message after it has been successfully forwarded to Step Functions, preventing it from being redelivered. `sqs:GetQueueAttributes` allows the Pipe to read the queue's visibility timeout and other configuration attributes needed to manage polling behavior.

All three actions are scoped to the exact ARN of the `music-streaming-pipeline-events` queue. The Pipes role cannot read from the DLQ or any other SQS queue in the account.

#### Statement: StartExecution

```
Actions:  states:StartExecution
Resource: aws_sfn_state_machine.pipeline.arn  (exact ARN)
```

After consuming a message from SQS, the Pipe calls `states:StartExecution` to begin a pipeline run. The resource is scoped to the exact ARN of the `music-streaming-pipeline` state machine. The Pipes role cannot start any other state machine in the account. It also cannot stop, describe, or list executions — it can only start new ones.

---

## 6. Resource-Based Policies

Resource-based policies are not attached to roles. They are attached to resources and define which external identities can interact with those resources. This project has two resource-based policies.

### SQS Queue Policy — pipeline_events

**Terraform resource:** `aws_sqs_queue_policy.pipeline_events`
**Attached to:** `aws_sqs_queue.pipeline_events`

```json
{
  "Sid": "AllowEventBridgeSend",
  "Effect": "Allow",
  "Principal": { "Service": "events.amazonaws.com" },
  "Action": "sqs:SendMessage",
  "Resource": "<queue-arn>",
  "Condition": {
    "ArnEquals": {
      "aws:SourceArn": "<eventbridge-rule-arn>"
    }
  }
}
```

This policy allows the EventBridge service to call `sqs:SendMessage` on the pipeline events queue. Without it, EventBridge cannot deliver events to SQS regardless of any identity-based permissions, because SQS enforces its queue policy independently.

The condition `aws:SourceArn` scoped to the specific EventBridge rule ARN is a critical security detail. Without it, any EventBridge rule in the account — including rules created by other teams or services — could send messages to this queue and potentially trigger unintended pipeline executions. The condition ensures that only the `music-streaming-streams-uploaded` rule can write to this queue.

### SNS Topic Policy — pipeline_alerts

**Terraform resource:** `aws_sns_topic_policy.pipeline_alerts`
**Attached to:** `aws_sns_topic.pipeline_alerts`

```json
{
  "Sid": "AllowStepFunctionsPublish",
  "Effect": "Allow",
  "Principal": { "Service": "states.amazonaws.com" },
  "Action": "sns:Publish",
  "Resource": "<topic-arn>"
}
```

This policy allows the Step Functions service to publish to the alerts SNS topic. SNS evaluates both the caller's identity-based policy and the topic's resource-based policy. The Step Functions role already carries `sns:Publish` permission via its inline policy. However, SNS also requires the topic itself to permit the caller. Both must say yes for the publish to succeed. The resource-based policy on the topic is the second half of that handshake.

---

## 7. The Complete IAM Map

The diagram below shows every IAM boundary crossed during a single pipeline run, from S3 upload to DynamoDB write.

```
S3 ObjectCreated event
       |
       | [EventBridge rule evaluates event pattern — no IAM role needed here]
       |
       v
SQS pipeline_events queue
       |
       | [SQS queue policy: allows events.amazonaws.com via ArnEquals condition]
       |
       v
EventBridge Pipe (music-streaming-pipes-role)
  - sqs:ReceiveMessage    \
  - sqs:DeleteMessage      > SqsConsume statement, scoped to queue ARN
  - sqs:GetQueueAttributes/
  - states:StartExecution  > StartExecution statement, scoped to state machine ARN
       |
       v
Step Functions state machine (music-streaming-sfn-role)
  - glue:StartCrawler   \
  - glue:GetCrawler      > GlueCrawlerControl statement
       |
  [crawler runs under glue-pipeline-role]
       |
  - glue:StartJobRun    \
  - glue:GetJobRun       > GlueJobControl statement (x5 for each Glue job)
  - glue:BatchStopJobRun/
       |
       | [on any failure]
       |
  - sns:Publish           > SnsPublish statement, scoped to topic ARN
                          > SNS topic policy: allows states.amazonaws.com
       |
       v
Each Glue job (glue-pipeline-role)
  - S3 read/write/delete    > AmazonS3FullAccess (managed)
  - DynamoDB BatchWriteItem > AmazonDynamoDBFullAccess (managed)
  - CloudWatch Logs write   > CloudWatchLogsFullAccess (managed)
  - Glue catalog access     > AWSGlueServiceRole (managed)
```

---

## 8. Best Practices Applied in This Project

**No hardcoded credentials anywhere.** All five Glue jobs, both crawlers, the state machine, and the EventBridge Pipe use IAM roles. There are no access keys, no secret keys, and no environment variables containing credentials anywhere in the codebase.

**Service principal trust policies.** Every trust policy names a single AWS service principal. No human user ARN and no cross-account principal appears in any trust policy. This ensures the roles can only be assumed by AWS services acting within their normal operational scope.

**One role per service boundary.** Three services (Glue, Step Functions, EventBridge Pipes) have three separate roles. No role is shared across service boundaries.

**Inline policies for dynamic resource ARNs.** The Step Functions and EventBridge Pipes inline policies reference specific resource ARNs that only exist after Terraform creates them. Using `aws_iam_policy_document` data sources with Terraform interpolation ensures the deployed policy contains the actual ARN rather than a placeholder.

**Condition keys on resource-based policies.** The SQS queue policy uses `aws:SourceArn` to restrict message delivery to one specific EventBridge rule. This is a defense-in-depth measure against confused deputy attacks, where a legitimate service is tricked into acting on behalf of an unauthorized caller.

**Terraform manages all IAM.** Every role, policy, attachment, and resource-based policy is declared in Terraform. No IAM resource in this project was created manually in the AWS console. This means the full permission model is version-controlled, reviewable in pull requests, and reproducible across environments.

**Separation between data plane and control plane permissions.** The Glue role holds data plane permissions: reading and writing data. The Step Functions role holds control plane permissions: starting and stopping other services. The EventBridge Pipes role holds only queue consumer permissions. No role mixes data plane and control plane authority.
