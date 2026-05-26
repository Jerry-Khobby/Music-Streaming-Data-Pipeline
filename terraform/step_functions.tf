
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
    sid    = "ListExecutionsForSingletonGuard"
    effect = "Allow"
    actions = [
      "states:ListExecutions",
      "states:DescribeExecution",
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
        Type       = "Pass"
        Parameters = {}
        Next       = "CheckRunningExecutions"
      }
      # Singleton guard — when 3 stream files upload in quick succession the Pipe
      # spawns 3 executions. The first does the work; the others see a running
      # peer and exit immediately, preventing Glue concurrent-run failures.
      CheckRunningExecutions = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sfn:listExecutions"
        Parameters = {
          StateMachineArn = "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.project_name}-pipeline"
          StatusFilter    = "RUNNING"
          MaxResults      = 10
        }
        ResultPath = "$.peers"
        Next       = "IsAnotherExecutionRunning"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      # If more than one execution is in RUNNING state (this one + at least one peer),
      # exit gracefully so the active execution can complete.
      IsAnotherExecutionRunning = {
        Type = "Choice"
        Choices = [{
          Variable  = "$.peers.Executions[1].ExecutionArn"
          IsPresent = true
          Next      = "PipelineSkipped"
        }]
        Default = "StartRawCrawler"
      }

      PipelineSkipped = {
        Type = "Succeed"
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
        Next       = "StartCuratedCrawler"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      # Refresh Athena partitions for gold/ so analysts can query the new KPIs
      # immediately. Failure here is non-fatal — Athena queries will just miss
      # the latest partition until the next run.
      StartCuratedCrawler = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:glue:startCrawler"
        Parameters = {
          Name = aws_glue_crawler.curated_crawler.name
        }
        ResultPath = null
        Next       = "ArchiveFiles"
        Catch = [
          {
            ErrorEquals = ["Glue.CrawlerRunningException"]
            ResultPath  = null
            Next        = "ArchiveFiles"
          },
          {
            ErrorEquals = ["States.ALL"]
            ResultPath  = null
            Next        = "ArchiveFiles"
          }
        ]
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
          Subject     = "❌ Music Streaming Pipeline FAILED"
          "Message.$" = "States.Format('PIPELINE FAILED\n\nError: {}\nCause: {}\n\nStep Functions console (click the red execution):\nhttps://us-east-1.console.aws.amazon.com/states/home?region=us-east-1#/statemachines\n\nGlue job run history (if a Glue job failed):\nhttps://us-east-1.console.aws.amazon.com/glue/home?region=us-east-1#/etl/jobs', $.error.Error, $.error.Cause)"
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
