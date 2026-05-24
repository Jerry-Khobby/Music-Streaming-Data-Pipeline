
#  MESSAGING — SNS · SQS · EventBridge · EventBridge Pipes
#
#  Event flow:
#    S3 ObjectCreated (streams/)
#      → EventBridge rule
#        → SQS queue          (buffers events; DLQ catches poison messages)
#          → EventBridge Pipe
#            → Step Functions StartExecution


# ── SNS TOPIC — failure alerts ────────────────────────────────────────────────

resource "aws_sns_topic" "pipeline_alerts" {
  name         = "${var.project_name}-pipeline-alerts"
  display_name = "Music Streaming Pipeline Alerts"

  tags = {
    Purpose = "Step Functions failure notifications"
  }
}

# Add an email subscription here after apply:
#   aws sns subscribe --topic-arn <arn> --protocol email --notification-endpoint you@example.com
resource "aws_sns_topic_policy" "pipeline_alerts" {
  arn = aws_sns_topic.pipeline_alerts.arn

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowStepFunctionsPublish"
      Effect = "Allow"
      Principal = {
        Service = "states.amazonaws.com"
      }
      Action   = "sns:Publish"
      Resource = aws_sns_topic.pipeline_alerts.arn
    }]
  })
}


# ── SQS — dead-letter queue ───────────────────────────────────────────────────

resource "aws_sqs_queue" "pipeline_dlq" {
  name                       = "${var.project_name}-pipeline-dlq"
  message_retention_seconds  = 1209600 # 14 days
  visibility_timeout_seconds = 30

  tags = {
    Purpose = "Dead-letter queue for unprocessable S3 events"
  }
}


# ── SQS — main queue ─────────────────────────────────────────────────────────

resource "aws_sqs_queue" "pipeline_events" {
  name                       = "${var.project_name}-pipeline-events"
  visibility_timeout_seconds = 300 # must be >= Step Functions startup time
  message_retention_seconds  = 86400 # 1 day

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.pipeline_dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Purpose = "Buffers S3 ObjectCreated events before Step Functions execution"
  }
}

# Allow EventBridge to send messages to the queue
resource "aws_sqs_queue_policy" "pipeline_events" {
  queue_url = aws_sqs_queue.pipeline_events.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Sid    = "AllowEventBridgeSend"
      Effect = "Allow"
      Principal = {
        Service = "events.amazonaws.com"
      }
      Action   = "sqs:SendMessage"
      Resource = aws_sqs_queue.pipeline_events.arn
      Condition = {
        ArnEquals = {
          "aws:SourceArn" = aws_cloudwatch_event_rule.streams_uploaded.arn
        }
      }
    }]
  })
}


# ── EVENTBRIDGE RULE — S3 ObjectCreated on streams/ ──────────────────────────

resource "aws_cloudwatch_event_rule" "streams_uploaded" {
  name        = "${var.project_name}-streams-uploaded"
  description = "Fires when a new file is uploaded to the raw bucket under streams/"

  event_pattern = jsonencode({
    source      = ["aws.s3"]
    "detail-type" = ["Object Created"]
    detail = {
      bucket = {
        name = [aws_s3_bucket.raw.id]
      }
      object = {
        key = [{ prefix = "streams/" }]
      }
    }
  })

  tags = {
    Purpose = "Detects new stream files arriving in S3"
  }
}

resource "aws_cloudwatch_event_target" "sqs_target" {
  rule      = aws_cloudwatch_event_rule.streams_uploaded.name
  target_id = "SendToSQS"
  arn       = aws_sqs_queue.pipeline_events.arn
}


# ── IAM ROLE FOR EVENTBRIDGE PIPES ───────────────────────────────────────────

data "aws_iam_policy_document" "pipes_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["pipes.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "pipes_role" {
  name               = "${var.project_name}-pipes-role"
  assume_role_policy = data.aws_iam_policy_document.pipes_trust.json
  description        = "IAM role assumed by EventBridge Pipes to poll SQS and start Step Functions"
}

data "aws_iam_policy_document" "pipes_permissions" {
  statement {
    sid    = "SqsConsume"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
    ]
    resources = [aws_sqs_queue.pipeline_events.arn]
  }

  statement {
    sid     = "StartExecution"
    effect  = "Allow"
    actions = ["states:StartExecution"]
    resources = [aws_sfn_state_machine.pipeline.arn]
  }
}

resource "aws_iam_role_policy" "pipes_permissions" {
  name   = "${var.project_name}-pipes-permissions"
  role   = aws_iam_role.pipes_role.id
  policy = data.aws_iam_policy_document.pipes_permissions.json
}


# ── EVENTBRIDGE PIPE — SQS → Step Functions ───────────────────────────────────

resource "aws_pipes_pipe" "sqs_to_sfn" {
  name        = "${var.project_name}-sqs-to-sfn"
  description = "Polls SQS for S3 events and starts the Step Functions pipeline execution"
  role_arn    = aws_iam_role.pipes_role.arn

  source = aws_sqs_queue.pipeline_events.arn
  source_parameters {
    sqs_queue_parameters {
      batch_size                         = 1    # one file → one pipeline execution
      maximum_batching_window_in_seconds = 0
    }
  }

  target = aws_sfn_state_machine.pipeline.arn
  target_parameters {
    step_function_state_machine_parameters {
      invocation_type = "FIRE_AND_FORGET"
    }
  }

  tags = {
    Purpose = "Connects S3 upload events to Step Functions pipeline trigger"
  }

  depends_on = [
    aws_iam_role_policy.pipes_permissions,
    aws_sfn_state_machine.pipeline,
  ]
}
