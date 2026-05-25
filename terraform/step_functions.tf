
#  STEP FUNCTIONS — State machine + IAM role
#
#  Execution order:
#    0. StartRawCrawler    → fires the Glue raw crawler (auto — no manual step needed)
#       WaitForCrawler     → waits 30 s between polls
#       CheckCrawlerStatus → reads crawler state via AWS SDK
#       IsCrawlerReady     → loops back if still RUNNING, proceeds when READY
#    1. ValidateData       → validation_job        (fails fast if schema is wrong)
#    2. TransformData      → etl_transform_job     (bronze → silver enriched streams)
#    3. AggregateKPIs      → kpi_aggregation_job   (silver → gold KPIs)
#    4. LoadDynamoDB       → dynamodb_loader        (gold → DynamoDB tables)
#    5. ArchiveFiles       → archive_job            (move raw streams to archive)
#
#  Any step failure → NotifyFailure → SNS alert → PipelineFailed (Fail state)


# ── IAM ROLE FOR STEP FUNCTIONS ──────────────────────────────────────────────

data "aws_iam_policy_document" "sfn_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["states.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "sfn_role" {
  name               = "${var.project_name}-sfn-role"
  assume_role_policy = data.aws_iam_policy_document.sfn_trust.json
  description        = "IAM role assumed by the Step Functions state machine"
}

data "aws_iam_policy_document" "sfn_permissions" {
  statement {
    sid    = "GlueJobControl"
    effect = "Allow"
    actions = [
      "glue:StartJobRun",
      "glue:GetJobRun",
      "glue:GetJobRuns",
      "glue:BatchStopJobRun",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "GlueCrawlerControl"
    effect = "Allow"
    actions = [
      "glue:StartCrawler",
      "glue:GetCrawler",
    ]
    resources = ["*"]
  }

  statement {
    sid       = "SnsPublish"
    effect    = "Allow"
    actions   = ["sns:Publish"]
    resources = [aws_sns_topic.pipeline_alerts.arn]
  }

  statement {
    sid    = "CloudWatchLogs"
    effect = "Allow"
    actions = [
      "logs:CreateLogDelivery",
      "logs:GetLogDelivery",
      "logs:UpdateLogDelivery",
      "logs:DeleteLogDelivery",
      "logs:ListLogDeliveries",
      "logs:PutResourcePolicy",
      "logs:DescribeResourcePolicies",
      "logs:DescribeLogGroups",
    ]
    resources = ["*"]
  }

  statement {
    sid    = "XRayTracing"
    effect = "Allow"
    actions = [
      "xray:PutTraceSegments",
      "xray:PutTelemetryRecords",
      "xray:GetSamplingRules",
      "xray:GetSamplingTargets",
    ]
    resources = ["*"]
  }
}

resource "aws_iam_role_policy" "sfn_permissions" {
  name   = "${var.project_name}-sfn-permissions"
  role   = aws_iam_role.sfn_role.id
  policy = data.aws_iam_policy_document.sfn_permissions.json
}


# ── STATE MACHINE DEFINITION ─────────────────────────────────────────────────

locals {
  sfn_definition = jsonencode({
    Comment = "Music Streaming ETL Pipeline — orchestrates 5 Glue jobs in sequence"
    StartAt = "NormalizeInput"

    States = {

      # EventBridge Pipes delivers the SQS record as an array [{}].
      # $[0] unwraps it into a plain object so all ResultPath writes succeed.
      NormalizeInput = {
        Type      = "Pass"
        InputPath = "$[0]"
        Next      = "StartRawCrawler"
      }

      StartRawCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.raw_crawler.name
        }
        ResultPath = null
        Next       = "WaitForCrawler"
        Catch = [
          {
            # Already running is fine — skip to polling
            ErrorEquals = ["Glue.CrawlerRunningException"]
            ResultPath  = null
            Next        = "WaitForCrawler"
          },
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = "$.error"
            Next        = "NotifyFailure"
          }
        ]
      }

      WaitForCrawler = {
        Type    = "Wait"
        Seconds = 30
        Next    = "CheckCrawlerStatus"
      }

      CheckCrawlerStatus = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:getCrawler"
        Parameters = {
          Name = aws_glue_crawler.raw_crawler.name
        }
        ResultPath = "$.crawlerStatus"
        Next       = "IsCrawlerReady"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      IsCrawlerReady = {
        Type = "Choice"
        Choices = [
          {
            Variable     = "$.crawlerStatus.Crawler.State"
            StringEquals = "READY"
            Next         = "ValidateData"
          }
        ]
        Default = "WaitForCrawler"
      }

      ValidateData = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.validation.name
        }
        ResultPath = "$.validationResult"
        Next       = "TransformData"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      TransformData = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.etl_transform.name
        }
        ResultPath = "$.transformResult"
        Next       = "AggregateKPIs"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      AggregateKPIs = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.kpi_aggregation.name
        }
        ResultPath = "$.kpiResult"
        Next       = "LoadDynamoDB"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      LoadDynamoDB = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.dynamodb_loader.name
        }
        ResultPath = "$.dynamoResult"
        Next       = "ArchiveFiles"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      ArchiveFiles = {
        Type     = "Task"
        Resource = "arn:aws:states:::glue:startJobRun.sync"
        Parameters = {
          JobName = aws_glue_job.archive.name
        }
        ResultPath = "$.archiveResult"
        Next       = "PipelineSucceeded"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      NotifyFailure = {
        Type     = "Task"
        Resource = "arn:aws:states:::sns:publish"
        Parameters = {
          TopicArn    = aws_sns_topic.pipeline_alerts.arn
          Subject     = "Music Streaming Pipeline FAILED"
          "Message.$" = "States.Format('Pipeline failed. Error details: {}', States.JsonToString($.error))"
        }
        ResultPath = "$.snsResult"
        Next       = "PipelineFailed"
      }

      PipelineFailed = {
        Type  = "Fail"
        Error = "PipelineError"
        Cause = "One or more pipeline steps failed — check CloudWatch logs and SNS alert for details"
      }

      PipelineSucceeded = {
        Type = "Succeed"
      }
    }
  })
}


# ── STATE MACHINE RESOURCE ───────────────────────────────────────────────────

resource "aws_sfn_state_machine" "pipeline" {
  name       = "${var.project_name}-pipeline"
  role_arn   = aws_iam_role.sfn_role.arn
  definition = local.sfn_definition
  type       = "STANDARD"

  logging_configuration {
    log_destination        = "${aws_cloudwatch_log_group.step_functions.arn}:*"
    include_execution_data = true
    level                  = "ALL"
  }

  tracing_configuration {
    enabled = true
  }

  tags = {
    Pipeline = "music-streaming"
  }

  depends_on = [
    aws_glue_job.validation,
    aws_glue_job.etl_transform,
    aws_glue_job.kpi_aggregation,
    aws_glue_job.dynamodb_loader,
    aws_glue_job.archive,
  ]
}
