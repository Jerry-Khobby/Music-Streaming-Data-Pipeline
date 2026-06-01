#  LAMBDA — Pipeline-level Slack notifier
#
#  Called by Step Functions at three points in the state machine:
#    • NotifyPipelineStarted     — after the concurrency guard passes
#    • NotifyPipelineSucceeded   — after ArchiveFiles completes
#    • NotifySlackPipelineFailed — after the SNS failure alert fires
#
#  The function reads SLACK_APP_WEBHOOK_URL from its environment and posts
#  a rich Block Kit message for each pipeline event.  If the webhook URL is
#  empty, the function logs a warning and returns 200 — the pipeline never
#  fails because a notification could not be delivered.


# ── PACKAGING ────────────────────────────────────────────────────────────────

data "archive_file" "pipeline_notifier" {
  type        = "zip"
  source_file = "${path.module}/../lambda/pipeline_notifier.py"
  output_path = "${path.module}/../lambda/pipeline_notifier.zip"
}


# ── IAM ROLE ─────────────────────────────────────────────────────────────────

resource "aws_iam_role" "pipeline_notifier" {
  name        = "${var.project_name}-pipeline-notifier-role"
  description = "Role assumed by the pipeline-notifier Lambda"

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


# ── LAMBDA FUNCTION ──────────────────────────────────────────────────────────

resource "aws_lambda_function" "pipeline_notifier" {
  function_name = "${var.project_name}-pipeline-notifier"
  role          = aws_iam_role.pipeline_notifier.arn
  runtime       = "python3.12"
  handler       = "pipeline_notifier.handler"

  filename         = data.archive_file.pipeline_notifier.output_path
  source_code_hash = data.archive_file.pipeline_notifier.output_base64sha256

  timeout = 10

  environment {
    variables = {
      SLACK_APP_WEBHOOK_URL = var.slack_webhook_url
    }
  }

  tags = {
    Pipeline = "music-streaming"
  }
}
