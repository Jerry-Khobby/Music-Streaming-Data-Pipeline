# AWS Lambda — Pipeline Notifier

## What This Document Covers

This document explains the `pipeline_notifier` Lambda function — what it does, why it exists as a
Lambda, how Step Functions calls it, and how it fits alongside the `monitoring/` Python package
used inside the Glue jobs. It is the reference companion to the code in
[lambda/pipeline_notifier.py](../lambda/pipeline_notifier.py) and the Terraform in
[terraform/lambda.tf](../terraform/lambda.tf).

---

## 1. The Problem It Solves

The pipeline has two levels of activity worth announcing:

| Level | What it covers | Who owns it |
| --- | --- | --- |
| **Stage-level** | Each named stage inside a Glue job starting and completing or failing | The `PipelineMonitor` context manager inside each job ([monitoring/pipeline_monitor.py](../monitoring/pipeline_monitor.py)) |
| **Pipeline-level** | The entire pipeline starting, succeeding, or failing | **This Lambda** — invoked by Step Functions |

Before this Lambda existed, pipeline-level Slack visibility came only from the AWS Chatbot path:
a CloudWatch alarm or an EventBridge rule would catch a Step Functions `SUCCEEDED` or `FAILED`
event and route a plain-text message through SNS → Chatbot → Slack. That path is still active
(belt-and-suspenders), but it has two limitations for this use case:

1. **No start notification.** There is no CloudWatch event for "execution just began" that Chatbot
   can forward. An operator had no Slack signal that a run was in flight — only silence until it
   succeeded or failed.
2. **Plain text only.** SNS/Chatbot messages are unformatted strings. The direct webhook path
   supports **Slack Block Kit** — coloured sidebar, bold section headers, key-value fields, context
   lines — which makes it immediately scannable in a busy channel.

The Lambda solves both: it posts a `:rocket:` start message the moment Step Functions takes
ownership of the pipeline, and `:large_green_circle:` / `:red_circle:` messages at the outcome, all
in the richer Block Kit format.

---

## 2. The Three Event Types

The Lambda handles exactly three event types, distinguished by `event.event_type`:

### `"started"` — pipeline just began

Posted by `NotifyPipelineStarted`, the first Task state after the concurrency guard resolves.

```text
:rocket:  Music Streaming Pipeline — Started
          Execution:  ...last 12 chars of ARN
          Time:       2026-05-17 14:22:09 UTC
          ─────────────────────────────────────
          Crawler → Validate → Transform → KPIs → DynamoDB → Archive
```

### `"succeeded"` — all steps completed

Posted by `NotifyPipelineSucceeded`, inserted between `ArchiveFiles` and the terminal
`PipelineSucceeded` state.

```text
:large_green_circle:  Music Streaming Pipeline — Succeeded
                      All KPIs computed and loaded to DynamoDB.
                      Execution:  ...last 12 chars of ARN
                      Completed:  2026-05-17 14:35:44 UTC
```

### `"failed"` — a step threw an error

Posted by `NotifySlackPipelineFailed`, chained after the existing `NotifyFailure` (SNS) state and
before the terminal `PipelineFailed` state. The Lambda receives the `$.error` object Step Functions
captured from the failing step's `Catch` block, and extracts `Error` (the error class name) and
`Cause` (the full message).

```text
:red_circle:  Music Streaming Pipeline — FAILED
              Failed Step:  AggregateKPIs
              Execution:    ...last 12 chars of ARN
              Time:         2026-05-17 14:31:07 UTC
              ─────────────────────────────────────
              Error:
              AnalysisException: Column 'duration_ms' not found in schema
```

---

## 3. How Step Functions Calls It

Three Task states in the state machine invoke the Lambda using the `arn:aws:states:::lambda:invoke`
resource. All three are non-blocking by design: each has a `Catch` that catches `States.ALL` and
routes to the next state, so a Slack delivery failure can never block or fail the execution.

### `NotifyPipelineStarted`

```hcl
NotifyPipelineStarted = {
  Type     = "Task"
  Resource = "arn:aws:states:::lambda:invoke"
  Parameters = {
    FunctionName = aws_lambda_function.pipeline_notifier.arn
    Payload = {
      "event_type"     = "started"
      "execution_id.$" = "$$.Execution.Id"
    }
  }
  ResultPath = null
  Next       = "StartRawCrawler"
  Catch = [{
    ErrorEquals = ["States.ALL"]
    ResultPath  = null
    Next        = "StartRawCrawler"   # pipeline continues whether or not Slack receives the message
  }]
}
```

### `NotifyPipelineSucceeded`

```hcl
NotifyPipelineSucceeded = {
  Type     = "Task"
  Resource = "arn:aws:states:::lambda:invoke"
  Parameters = {
    FunctionName = aws_lambda_function.pipeline_notifier.arn
    Payload = {
      "event_type"     = "succeeded"
      "execution_id.$" = "$$.Execution.Id"
    }
  }
  ResultPath = null
  Next       = "PipelineSucceeded"
  Catch = [{
    ErrorEquals = ["States.ALL"]
    ResultPath  = null
    Next        = "PipelineSucceeded"
  }]
}
```

### `NotifySlackPipelineFailed`

This state runs *after* the existing `NotifyFailure` (SNS publish) state, so the SNS alert (email +
Chatbot) fires first, then the richer Slack message follows. Both paths report the same failure.

```hcl
NotifySlackPipelineFailed = {
  Type     = "Task"
  Resource = "arn:aws:states:::lambda:invoke"
  Parameters = {
    FunctionName = aws_lambda_function.pipeline_notifier.arn
    Payload = {
      "event_type"     = "failed"
      "execution_id.$" = "$$.Execution.Id"
      "error.$"        = "$.error"       # the object Catch wrote: {Error: "...", Cause: "..."}
    }
  }
  ResultPath = null
  Next       = "PipelineFailed"
  Catch = [{
    ErrorEquals = ["States.ALL"]
    ResultPath  = null
    Next        = "PipelineFailed"
  }]
}
```

The Step Functions IAM role has one extra permission statement to allow this:

```hcl
statement {
  sid     = "LambdaInvoke"
  effect  = "Allow"
  actions = ["lambda:InvokeFunction"]
  resources = [aws_lambda_function.pipeline_notifier.arn]
}
```

---

## 4. The Lambda Handler in Detail

The full source is in [lambda/pipeline_notifier.py](../lambda/pipeline_notifier.py). Its structure
follows the same shape as `SlackNotifier` in the `monitoring/` package, but self-contained so the
Lambda has zero dependencies beyond the Python standard library.

### Key design decisions

**`urllib.request` instead of `requests`.** The Lambda runtime includes no third-party packages by
default. Using `urllib.request` (Python standard library) means no Lambda Layer, no packaging step,
and no `pip install` in CI. The function is a single file that Terraform zips and deploys directly.

**`resolveWebhookUrl()` reads from the environment.** The webhook URL is passed as the
`SLACK_APP_WEBHOOK_URL` environment variable, set from the `var.slack_webhook_url` Terraform
variable. If the variable is empty (webhook not configured), the function logs a warning and returns
`200` — the execution is never disrupted.

**The `error` field is an object, not a string.** Step Functions `Catch` blocks capture the error
as `{"Error": "...", "Cause": "..."}`. The payload passes the whole `$.error` object; the handler
extracts the fields with `.get()` defaults so it is resilient to missing or null values.

```python
def handler(event, context):
    webhookUrl  = resolveWebhookUrl()       # reads SLACK_APP_WEBHOOK_URL env var
    eventType   = event.get("event_type")
    executionId = event.get("execution_id", "unknown")

    if eventType == "started":
        payload = buildStartedPayload(executionId)

    elif eventType == "succeeded":
        payload = buildSucceededPayload(executionId)

    elif eventType == "failed":
        errorObj   = event.get("error", {})
        failedStep = errorObj.get("Error", "Unknown") if isinstance(errorObj, dict) else str(errorObj)
        error      = errorObj.get("Cause", "Unknown error") if isinstance(errorObj, dict) else str(errorObj)
        payload    = buildFailedPayload(executionId, failedStep, error)

    else:
        logger.error(f"Unknown event_type: {eventType}")
        return {"statusCode": 400}

    try:
        postToSlack(webhookUrl, payload)
        return {"statusCode": 200}
    except urllib.error.URLError as error:
        logger.error(f"Slack alert could not be delivered: {error}")
        raise
```

---

## 5. Terraform Resources

All resources are in [terraform/lambda.tf](../terraform/lambda.tf).

### Packaging

```hcl
data "archive_file" "pipeline_notifier" {
  type        = "zip"
  source_file = "${path.module}/../lambda/pipeline_notifier.py"
  output_path = "${path.module}/../lambda/pipeline_notifier.zip"
}
```

The `archive_file` data source creates the zip at `terraform plan` / `terraform apply` time.
`source_code_hash = data.archive_file.pipeline_notifier.output_base64sha256` ensures Lambda is
re-deployed whenever the Python file changes — no manual version bumps.

### IAM role

```hcl
resource "aws_iam_role" "pipeline_notifier" {
  name = "${var.project_name}-pipeline-notifier-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "pipeline_notifier_logs" {
  role       = aws_iam_role.pipeline_notifier.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}
```

`AWSLambdaBasicExecutionRole` grants only CloudWatch Logs write access — the minimum needed for a
Lambda to emit its own log lines. No S3, DynamoDB, or Glue permissions are needed.

### Lambda function

```hcl
resource "aws_lambda_function" "pipeline_notifier" {
  function_name    = "${var.project_name}-pipeline-notifier"
  role             = aws_iam_role.pipeline_notifier.arn
  runtime          = "python3.12"
  handler          = "pipeline_notifier.handler"
  filename         = data.archive_file.pipeline_notifier.output_path
  source_code_hash = data.archive_file.pipeline_notifier.output_base64sha256
  timeout          = 10

  environment {
    variables = {
      SLACK_APP_WEBHOOK_URL = var.slack_webhook_url
    }
  }
}
```

`timeout = 10` matches the `REQUEST_TIMEOUT_SECS` constant in the function — the Lambda will not
hang waiting for a Slack response longer than the HTTP timeout itself.

---

## 6. How It Relates to `monitoring/pipeline_monitor.py`

The `monitoring/` package and the Lambda solve the same broad problem — notifying Slack — but at
different levels of granularity:

| Component | Scope | Triggered by | Message granularity |
| --- | --- | --- | --- |
| `PipelineMonitor` + `SlackNotifier` | Stage-level (inside a single Glue job) | The Python `with monitor.stage(...)` context manager in job code | One `:hourglass:` + one `:white_check_mark:` (or `:red_circle:`) per named stage |
| `pipeline_notifier` Lambda | Pipeline-level (the entire Step Functions run) | A Step Functions Task state invoking Lambda | One `:rocket:` at start, one `:large_green_circle:` or `:red_circle:` at the end |

A complete healthy pipeline run produces a Slack thread that looks like:

```
:rocket:              Pipeline — Started               [Lambda]
:hourglass:           Validation Job — In Progress     [PipelineMonitor]
:white_check_mark:    Validation Job — Succeeded       [PipelineMonitor]
:hourglass:           ETL Transform — In Progress      [PipelineMonitor]
:white_check_mark:    ETL Transform — Succeeded        [PipelineMonitor]
  ... (KPI Aggregation, DynamoDB Loader, Archive — same pattern)
:large_green_circle:  Pipeline — Succeeded             [Lambda]
```

The two components do not call each other — they are independent. The Lambda does not know about
stages; `PipelineMonitor` does not know about the pipeline's start or end. They collaborate only
through the shared Slack webhook URL.

---

## 7. The Non-Blocking Contract

A critical invariant of this design: **Slack notifications must never cause a pipeline failure.**

This is enforced at two levels:

1. **Step Functions Catch.** Every Lambda invocation state (`NotifyPipelineStarted`,
   `NotifyPipelineSucceeded`, `NotifySlackPipelineFailed`) catches `States.ALL` and routes to the
   next pipeline state with `ResultPath = null` — the error is silently discarded.

2. **Lambda itself.** `postToSlack()` only raises on `urllib.error.URLError`. If the webhook URL is
   empty, the function returns `{"statusCode": 200}` without posting — no exception, no Lambda
   error, no state machine error.

The combined effect: if the Slack webhook is misconfigured, if the Slack API is down, or if the
Lambda itself fails to start, the pipeline runs unchanged and the only evidence is a warning in
CloudWatch Logs — never a failed execution.

---

## 8. Summary

| Aspect | Detail |
| --- | --- |
| **File** | [lambda/pipeline_notifier.py](../lambda/pipeline_notifier.py) |
| **Terraform** | [terraform/lambda.tf](../terraform/lambda.tf) |
| **Runtime** | Python 3.12, no external dependencies |
| **Invoked by** | Three Step Functions Task states (`NotifyPipelineStarted`, `NotifyPipelineSucceeded`, `NotifySlackPipelineFailed`) |
| **Webhook source** | `SLACK_APP_WEBHOOK_URL` environment variable (from `var.slack_webhook_url`) |
| **Message format** | Slack Block Kit — coloured attachment, section blocks, key-value fields |
| **Non-blocking** | Yes — all three invoking states catch `States.ALL` and route forward on error |
| **IAM** | `AWSLambdaBasicExecutionRole` (CloudWatch Logs only); SFN role has `lambda:InvokeFunction` |
| **Complements** | `monitoring/pipeline_monitor.py` (stage-level) + SNS/Chatbot (infrastructure-level) |
