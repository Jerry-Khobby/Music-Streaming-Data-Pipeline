
#  MONITORING — CloudWatch Alarms + AWS Chatbot Slack integration
#
#  Alert flow:
#    CloudWatch metric breaches threshold
#      → SNS topic (pipeline_alerts)
#        → AWS Chatbot
#          → Slack channel
#
#  Why this is more reliable than the state machine's NotifyFailure step:
#    - Catches failures that prevent the state machine from starting at all
#      (Pipe broken, IAM misconfigured, EventBridge rule disabled)
#    - Catches stuck queues (DLQ depth) which produce no state machine activity
#    - Independent of the state machine's own success — belt-and-suspenders


# ── CLOUDWATCH ALARMS ────────────────────────────────────────────────────────

# 1. State Functions execution failed in the last 5 minutes
resource "aws_cloudwatch_metric_alarm" "sfn_execution_failed" {
  alarm_name          = "${var.project_name}-sfn-execution-failed"
  alarm_description   = "Step Functions execution failed — investigate via execution history"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  metric_name = "ExecutionsFailed"
  namespace   = "AWS/States"
  period      = 300
  statistic   = "Sum"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
  ok_actions    = [aws_sns_topic.pipeline_alerts.arn]
}

# 2. Step Functions execution timed out
resource "aws_cloudwatch_metric_alarm" "sfn_execution_timed_out" {
  alarm_name          = "${var.project_name}-sfn-execution-timed-out"
  alarm_description   = "Step Functions execution exceeded its timeout — possible Glue hang or infinite poll loop"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  metric_name = "ExecutionsTimedOut"
  namespace   = "AWS/States"
  period      = 300
  statistic   = "Sum"

  dimensions = {
    StateMachineArn = aws_sfn_state_machine.pipeline.arn
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# 3. Dead-letter queue has messages — poison events that EventBridge gave up on
resource "aws_cloudwatch_metric_alarm" "sqs_dlq_has_messages" {
  alarm_name          = "${var.project_name}-sqs-dlq-has-messages"
  alarm_description   = "Messages landed in the dead-letter queue. Inspect with: aws sqs receive-message --queue-url ${aws_sqs_queue.pipeline_dlq.url}"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  metric_name = "ApproximateNumberOfMessagesVisible"
  namespace   = "AWS/SQS"
  period      = 60
  statistic   = "Maximum"

  dimensions = {
    QueueName = aws_sqs_queue.pipeline_dlq.name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# 4. Main queue backed up — messages older than 15 minutes mean the Pipe isn't draining
resource "aws_cloudwatch_metric_alarm" "sqs_messages_stuck" {
  alarm_name          = "${var.project_name}-sqs-messages-stuck"
  alarm_description   = "Messages in the main queue are >15 min old — Pipe may be unhealthy or state machine is throttled"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 3
  threshold           = 900
  treat_missing_data  = "notBreaching"

  metric_name = "ApproximateAgeOfOldestMessage"
  namespace   = "AWS/SQS"
  period      = 300
  statistic   = "Maximum"

  dimensions = {
    QueueName = aws_sqs_queue.pipeline_events.name
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}

# 5. Per-Glue-job failure alarms — useful for pinpointing WHICH job broke
locals {
  glue_jobs_to_monitor = {
    validation      = aws_glue_job.validation.name
    etl_transform   = aws_glue_job.etl_transform.name
    kpi_aggregation = aws_glue_job.kpi_aggregation.name
    dynamodb_loader = aws_glue_job.dynamodb_loader.name
    archive         = aws_glue_job.archive.name
  }
}

resource "aws_cloudwatch_metric_alarm" "glue_job_failed" {
  for_each = local.glue_jobs_to_monitor

  alarm_name          = "${var.project_name}-glue-${each.key}-failed"
  alarm_description   = "Glue job '${each.value}' reported task failures — check /aws/glue/${var.project_name} logs"
  comparison_operator = "GreaterThanOrEqualToThreshold"
  evaluation_periods  = 1
  threshold           = 1
  treat_missing_data  = "notBreaching"

  metric_name = "glue.driver.aggregate.numFailedTasks"
  namespace   = "Glue"
  period      = 300
  statistic   = "Sum"

  dimensions = {
    JobName  = each.value
    JobRunId = "ALL"
    Type     = "gauge"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
}



resource "aws_cloudwatch_event_rule" "pipeline_succeeded" {
  name        = "${var.project_name}-pipeline-succeeded"
  description = "Fires when a pipeline execution completes successfully"

  event_pattern = jsonencode({
    source        = ["aws.states"]
    "detail-type" = ["Step Functions Execution Status Change"]
    detail = {
      stateMachineArn = [aws_sfn_state_machine.pipeline.arn]
      status          = ["SUCCEEDED"]
    }
  })
}

resource "aws_cloudwatch_event_target" "pipeline_succeeded_sns" {
  rule      = aws_cloudwatch_event_rule.pipeline_succeeded.name
  target_id = "SuccessAlert"
  arn       = aws_sns_topic.pipeline_alerts.arn

  input_transformer {
    input_paths = {
      execution = "$.detail.executionArn"
      time      = "$.time"
    }
    input_template = "\"✅ Pipeline SUCCEEDED at <time>. All KPIs computed and loaded to DynamoDB. Execution: <execution>\""
  }
}


# ── HUMAN-READABLE EMAIL ALERTS ──────────────────────────────────────────────
# CloudWatch's default email is a structured blob that mixes ARNs, metric math
# and the actual signal. We catch the same state-change event via EventBridge
# and rewrite it with an input transformer so subscribers see plain language:
# what fired, why, when, and where to look.
#
# The alarms above intentionally have NO alarm_actions — both the email and
# Slack paths converge through this single transformer, so the message format
# stays consistent everywhere and nobody gets duplicate alerts.

resource "aws_cloudwatch_event_rule" "alarm_state_change" {
  name        = "${var.project_name}-alarm-state-change"
  description = "Catches ALARM transitions for this pipeline's CloudWatch alarms and reshapes them before SNS forwards them"

  event_pattern = jsonencode({
    source        = ["aws.cloudwatch"]
    "detail-type" = ["CloudWatch Alarm State Change"]
    detail = {
      state = {
        value = ["ALARM"]
      }
      # Limits the rule to this project's alarms only.
      alarmName = [{ prefix = "${var.project_name}-" }]
    }
  })

  tags = {
    Purpose = "Source of human-readable email alerts"
  }
}

resource "aws_cloudwatch_event_target" "alarm_to_sns" {
  rule      = aws_cloudwatch_event_rule.alarm_state_change.name
  target_id = "FormattedSnsAlert"
  arn       = aws_sns_topic.pipeline_alerts.arn

  # Each quoted line below becomes one line in the email body.
  # <placeholders> are substituted from input_paths.
  input_transformer {
    input_paths = {
      alarm       = "$.detail.alarmName"
      description = "$.detail.configuration.description"
      reason      = "$.detail.state.reason"
      time        = "$.time"
      region      = "$.region"
    }

    input_template = <<EOT
"Pipeline alert: <alarm>"
""
"What this alarm watches:"
"  <description>"
""
"Why it fired now:"
"  <reason>"
""
"When: <time>"
"Region: <region>"
""
"Open the alarm in the console:"
"  https://<region>.console.aws.amazon.com/cloudwatch/home?region=<region>#alarmsV2:alarm/<alarm>"
EOT
  }
}


# ── AWS CHATBOT — Slack integration ──────────────────────────────────────────
# Created only when slack_workspace_id and slack_channel_id are set.
# One-time setup before first apply with Slack enabled:
#   1. AWS Console → AWS Chatbot → Configure new client → Slack
#   2. Authorise via Slack OAuth (admins only)
#   3. Copy the Workspace ID into terraform.tfvars
#   4. In Slack, right-click the alert channel → Copy link → grab the ID
#   5. Invite @aws into that channel: /invite @aws

locals {
  slack_enabled = var.slack_workspace_id != "" && var.slack_channel_id != ""
}

# IAM role assumed by AWS Chatbot when it formats and forwards alerts to Slack
resource "aws_iam_role" "chatbot" {
  count = local.slack_enabled ? 1 : 0

  name        = "${var.project_name}-chatbot-role"
  description = "Role AWS Chatbot uses to read CloudWatch state when posting alerts to Slack"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Principal = {
        Service = "chatbot.amazonaws.com"
      }
      Action = "sts:AssumeRole"
    }]
  })
}

# Read-only access — Chatbot enriches alerts with metric snapshots/log excerpts
resource "aws_iam_role_policy_attachment" "chatbot_readonly" {
  count = local.slack_enabled ? 1 : 0

  role       = aws_iam_role.chatbot[0].name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchReadOnlyAccess"
}

resource "aws_chatbot_slack_channel_configuration" "pipeline_alerts" {
  count = local.slack_enabled ? 1 : 0

  configuration_name = "${var.project_name}-slack-alerts"
  slack_team_id      = var.slack_workspace_id
  slack_channel_id   = var.slack_channel_id

  iam_role_arn = aws_iam_role.chatbot[0].arn

  sns_topic_arns = [aws_sns_topic.pipeline_alerts.arn]

  logging_level         = "INFO"
  guardrail_policy_arns = ["arn:aws:iam::aws:policy/ReadOnlyAccess"]

  tags = {
    Purpose = "Slack channel that receives all pipeline alarms"
  }

  depends_on = [aws_iam_role_policy_attachment.chatbot_readonly]
}


# ── OUTPUTS ──────────────────────────────────────────────────────────────────

output "monitoring_alarms" {
  description = "List of all CloudWatch alarms protecting this pipeline"
  value = concat(
    [
      aws_cloudwatch_metric_alarm.sfn_execution_failed.alarm_name,
      aws_cloudwatch_metric_alarm.sfn_execution_timed_out.alarm_name,
      aws_cloudwatch_metric_alarm.sqs_dlq_has_messages.alarm_name,
      aws_cloudwatch_metric_alarm.sqs_messages_stuck.alarm_name,
    ],
    [for a in aws_cloudwatch_metric_alarm.glue_job_failed : a.alarm_name],
  )
}

output "slack_alerts_status" {
  description = "Whether Slack alerts are wired up"
  value       = local.slack_enabled ? "Enabled — posting to channel ${var.slack_channel_id}" : "Disabled — set slack_workspace_id and slack_channel_id to enable"
}

output "email_alerts_status" {
  description = "Whether email alerts are wired up — recipient must click the confirmation link in their inbox before alerts start flowing"
  value       = var.alert_email == "" ? "Disabled — set alert_email to enable" : "Subscribed ${var.alert_email} (confirm via the link sent to that inbox)"
}
