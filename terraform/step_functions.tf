#  STEP FUNCTIONS — State machine + IAM role
#
#  Execution order:
#    0. NormalizeInput       → discards raw SQS envelope, replaces with clean {}
#       CheckAlreadyRunning  → lists RUNNING executions for this state machine
#       IsAnotherRunning     → if another execution is already running, wait and re-check
#       WaitForPreviousRun   → waits 60 s before re-checking (tune to pipeline duration)
#    1. StartRawCrawler      → fires the Glue raw crawler
#       WaitForCrawler       → waits 45 s between polls
#       CheckCrawlerStatus   → reads crawler state via AWS SDK
#       IsCrawlerReady       → loops back if still RUNNING, proceeds when READY
#       CheckStreamsExist     → inspects S3 for files under streams/ before running jobs
#       AreThereStreams       → exits cleanly (Succeed) if no files found
#    2. ValidateData         → validation_job        (fails fast if schema is wrong)
#    3. TransformData        → etl_transform_job     (bronze → silver enriched streams)
#    4. AggregateKPIs        → kpi_aggregation_job   (silver → gold KPIs)
#    5. LoadDynamoDB         → dynamodb_loader        (gold → DynamoDB tables)
#    6. StartCuratedCrawler  → refreshes Athena gold/ partitions (non-fatal)
#    7. ArchiveFiles         → archive_job            (move raw streams to archive)
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

  # Required so the state machine can check S3 for stream files before
  # running any Glue jobs — allows a clean early-exit when streams/ is empty.
  statement {
    sid     = "S3ListStreams"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = [
      "arn:aws:s3:::${var.raw_bucket_name}-${var.environment}"
    ]
    condition {
      test     = "StringLike"
      variable = "s3:prefix"
      values   = ["streams/*"]
    }
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

      # ── INPUT NORMALIZATION ────────────────────────────────────────────────
      # The Pipe passes the SQS batch as a raw array regardless of provider
      # version.  Parameters = {} completely replaces the input with a clean
      # empty object before any ResultPath writes happen.

      NormalizeInput = {
        Type       = "Pass"
        Parameters = {}
        Next       = "CheckAlreadyRunning"
      }

      # ── CONCURRENCY GUARD ──────────────────────────────────────────────────
      # Multiple S3 uploads can trigger multiple executions in quick succession.
      # Each Glue job allows concurrent runs, but running two full pipelines at
      # the same time wastes DPU hours and can cause data races in silver/gold.
      #
      # Instead of aborting, we poll until every earlier execution finishes,
      # then proceed normally — all uploaded files are picked up in one run.
      #
      # How it works (oldest-wins serialization):
      #   ListExecutions returns RUNNING executions newest-first, so the OLDEST
      #   still-running execution is always the LAST element. FindOldestRunning
      #   extracts that element's ARN and IsAnotherRunning compares it to our
      #   own execution ARN ($$.Execution.Id):
      #     • oldest == me  → no earlier run is in flight → proceed.
      #     • oldest != me  → an older run owns the pipeline → wait 60 s, recheck.
      #   Exactly one execution (the oldest) proceeds at a time; as each finishes
      #   the next-oldest becomes the last element and takes its turn.
      #
      #   Why not "wait if Executions[1] exists"?  That deadlocks: when several
      #   executions all sit in the wait loop, each sees the others as RUNNING
      #   and none can tell it is the one that should go.  Comparing against the
      #   oldest ARN breaks the symmetry so the queue actually drains.

      CheckAlreadyRunning = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:sfn:listExecutions"
        Parameters = {
          StateMachineArn = "arn:aws:states:${var.aws_region}:${data.aws_caller_identity.current.account_id}:stateMachine:${var.project_name}-pipeline"
          StatusFilter    = "RUNNING"
          # Fetch a generous window so the genuinely oldest run is always in the
          # result set even during an upload burst (well under the 256 KB state
          # limit — ~100 entries is tens of KB).
          MaxResults = 100
        }
        ResultPath = "$.runningCheck"
        Next       = "FindOldestRunning"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      # The oldest still-running execution is the last element of the
      # newest-first list.  Pull its ARN out so the Choice below can compare it
      # against our own execution ARN.
      FindOldestRunning = {
        Type = "Pass"
        Parameters = {
          "oldest.$" = "States.ArrayGetItem($.runningCheck.Executions, States.MathAdd(States.ArrayLength($.runningCheck.Executions), -1))"
        }
        ResultPath = "$.concurrency"
        Next       = "IsAnotherRunning"
      }

      IsAnotherRunning = {
        Type = "Choice"
        Choices = [{
          # The oldest running execution is us — no earlier run is in flight,
          # so it is our turn to own the pipeline.
          Variable         = "$.concurrency.oldest.ExecutionArn"
          StringEqualsPath = "$$.Execution.Id"
          Next             = "StartRawCrawler"
        }]
        # An older execution is still running — yield and re-check in 60 s.
        Default = "WaitForPreviousRun"
      }

      WaitForPreviousRun = {
        # Poll every 60 s.  Tune this to roughly half your typical pipeline
        # duration — shorter means a faster start after the previous run ends,
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
        Seconds = 45
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
            Next         = "CheckStreamsExist"
          }
        ]
        Default = "WaitForCrawler"
      }

      # ── STREAMS EXISTENCE CHECK ────────────────────────────────────────────
      # After the crawler finishes, verify there are actual CSV files under
      # streams/ before spending DPU time on Glue jobs.
      #
      # Why this matters:
      #   The Glue crawler registers the streams table schema from whatever
      #   files existed at crawl time.  If streams/ was empty when it ran
      #   (e.g. the previous archive job moved all files out before this
      #   crawler re-ran) the table is registered with ZERO columns.
      #   The validation job exits cleanly (NoNewStreams), but etl_transform
      #   loads the same stale catalog entry and crashes with:
      #     "streams is missing required columns: {listen_time, user_id, track_id}"
      #   because the DataFrame literally has no columns.
      #
      # Checking S3 directly here is the authoritative source of truth —
      # if no objects exist under streams/ there is nothing to process and
      # we exit cleanly (Succeed) without starting any Glue job.

      CheckStreamsExist = {
        Type     = "Task"
        Resource = "arn:aws:states:::aws-sdk:s3:listObjectsV2"
        Parameters = {
          Bucket  = "${var.raw_bucket_name}-${var.environment}"
          Prefix  = "streams/"
          MaxKeys = 1
        }
        ResultPath = "$.streamsCheck"
        Next       = "AreThereStreams"
        Catch = [{
          ErrorEquals = ["States.ALL"]
          ResultPath  = "$.error"
          Next        = "NotifyFailure"
        }]
      }

      AreThereStreams = {
        Type = "Choice"
        Choices = [
          {
            # KeyCount > 0 means at least one object exists under streams/ — proceed.
            Variable           = "$.streamsCheck.KeyCount"
            NumericGreaterThan = 0
            Next               = "ValidateData"
          }
        ]
        # No files found — nothing to process.  Exit cleanly so the CloudWatch
        # alarm does not fire and the SNS alert does not send a false failure.
        Default = "NoStreamsToProcess"
      }

      NoStreamsToProcess = {
        Type = "Succeed"
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
      # immediately.  Failure here is non-fatal — Athena queries will just miss
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
