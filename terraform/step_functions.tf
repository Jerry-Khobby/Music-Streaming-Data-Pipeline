
#  STEP FUNCTIONS — State machine + IAM role
#
#  Execution order:
#    0. NormalizeInput      → discards raw SQS envelope, replaces with clean {}
#       CheckAlreadyRunning → lists RUNNING executions for this state machine
#       IsAnotherRunning    → if another execution is already running, wait and re-check
#       WaitForPreviousRun  → waits 60 s before re-checking (tune to pipeline duration)
#    1. StartRawCrawler    → fires the Glue raw crawler (auto — no manual step needed)
#       WaitForCrawler     → waits 30 s between polls
#       CheckCrawlerStatus → reads crawler state via AWS SDK
#       IsCrawlerReady     → loops back if still RUNNING, proceeds when READY
#    2. ValidateData       → validation_job        (fails fast if schema is wrong)
#    3. TransformData      → etl_transform_job     (bronze → silver enriched streams)
#    4. AggregateKPIs      → kpi_aggregation_job   (silver → gold KPIs)
#    5. LoadDynamoDB       → dynamodb_loader        (gold → DynamoDB tables)
#    6. ArchiveFiles       → archive_job            (move raw streams to archive)
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

  # Required so the state machine can list its own running executions
  # and implement the wait-for-previous-run concurrency guard.
  statement {
    sid     = "SelfIntrospect"
    effect  = "Allow"
    actions = ["states:ListExecutions"]
    resources = [
      "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.project_name}-pipeline"
    ]
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

    # The Pipe passes the SQS batch as a raw array regardless of provider version.
    # This Pass state uses Parameters = {} to discard the entire array input
    # and replace it with a clean empty object before any ResultPath writes happen.
    # Parameters = {} is different from InputPath — it does not extract from the
    # input, it completely replaces it, which is what we need here.

    States = {

      # ── CONCURRENCY GUARD ──────────────────────────────────────────────────
      # Multiple S3 uploads can trigger multiple executions in quick succession.
      # Each Glue job has max_concurrent_runs = 1, so a second execution trying
      # to start the same job will get ConcurrentRunsExceededException.
      #
      # Instead of aborting, we poll until the earlier execution finishes,
      # then proceed normally — all uploaded files are picked up in one run.
      #
      # How it works:
      #   ListExecutions returns up to 2 RUNNING executions (MaxResults = 2).
      #   If Executions[1] is present, at least 2 executions are running — we
      #   are the newer one, so we wait 60 s and check again.
      #   Once the previous run finishes, Executions[1] disappears and we
      #   fall through to StartRawCrawler.

      NormalizeInput = {
        Type       = "Pass"
        Parameters = {}
        Next       = "CheckAlreadyRunning"
      }

      CheckAlreadyRunning = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sfn:listExecutions"
        Parameters = {
          StateMachineArn = "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.project_name}-pipeline"
          StatusFilter    = "RUNNING"
          MaxResults      = 2
        }
        ResultPath = "$.runningCheck"
        Next       = "IsAnotherRunning"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      IsAnotherRunning = {
        Type = "Choice"
        Choices = [{
          # Executions[1] existing means 2+ executions are running — we are the
          # newer one, so yield and wait for the previous run to finish.
          Variable  = "$.runningCheck.Executions[1]"
          IsPresent = true
          Next      = "WaitForPreviousRun"
        }]
        # Only one execution running (us) — safe to proceed.
        Default = "StartRawCrawler"
      }

      WaitForPreviousRun = {
        # Poll every 60 s. Tune this to roughly half your typical pipeline
        # duration — shorter means faster start after the previous run ends,
        # but more Step Functions state-transition cost while waiting.
        Type    = "Wait"
        Seconds = 60
        Next    = "CheckAlreadyRunning"
      }

      # ── CRAWLER ───────────────────────────────────────────────────────────

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

      # ── GLUE JOBS ─────────────────────────────────────────────────────────

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

      # ── FAILURE HANDLING ──────────────────────────────────────────────────

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
