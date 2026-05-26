
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
  threshold           = 900 # seconds
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
    validation       = aws_glue_job.validation.name
    etl_transform    = aws_glue_job.etl_transform.name
    kpi_aggregation  = aws_glue_job.kpi_aggregation.name
    dynamodb_loader  = aws_glue_job.dynamodb_loader.name
    archive          = aws_glue_job.archive.name
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
    JobName = each.value
    JobRunId = "ALL"
    Type     = "gauge"
  }

  alarm_actions = [aws_sns_topic.pipeline_alerts.arn]
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

  logging_level     = "INFO"
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
