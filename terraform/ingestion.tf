#  STREAMING INGESTION — Kinesis Data Firehose (Direct PUT → S3)
#
#  Replaces manual CSV upload with an automated, realistic ingestion front end:
#
#    producer script  →  Firehose delivery stream (Direct PUT)  →  S3 raw bucket  →  existing pipeline
#
#  Why Firehose and NOT Kinesis Data Streams:
#    The challenge here is BUFFERING variable arrivals (bursty + sparse) into batch
#    files cheaply — that is exactly what Firehose does. Kinesis Data Streams solves a
#    DISTRIBUTION problem (multiple consumers, replay, strict ordering) that this
#    single-consumer, no-replay pipeline does not have. Adding it would be cost and
#    complexity for no benefit. See docs/Streaming_Ingestion_Firehose.md.
#
#  Firehose flushes a file to S3 when EITHER threshold trips first:
#    • buffer fills to firehose_buffer_size_mb, OR
#    • firehose_buffer_interval_seconds elapse since the first buffered record.
#  This is what makes a burst consolidate into one file, while sparse data still
#  lands within minutes (the interval timer forces the flush).


# ── CLOUDWATCH LOG GROUP — Firehose delivery errors ──────────────────────────

resource "aws_cloudwatch_log_group" "firehose" {
  name              = "/aws/kinesisfirehose/${var.project_name}-streams-ingestion"
  retention_in_days = 30

  tags = {
    Usage = "Kinesis Data Firehose delivery logs"
  }
}

resource "aws_cloudwatch_log_stream" "firehose_s3_delivery" {
  name           = "S3Delivery"
  log_group_name = aws_cloudwatch_log_group.firehose.name
}


# ── IAM ROLE FOR FIREHOSE ────────────────────────────────────────────────────

data "aws_iam_policy_document" "firehose_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["firehose.amazonaws.com"]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [data.aws_caller_identity.current.account_id]
    }
  }
}

resource "aws_iam_role" "firehose_role" {
  name               = "${var.project_name}-firehose-role"
  assume_role_policy = data.aws_iam_policy_document.firehose_trust.json
  description        = "IAM role assumed by Firehose to write stream files into the raw bucket"
}

# Least-privilege: write only to the raw bucket, and log only to this stream's group.
data "aws_iam_policy_document" "firehose_permissions" {
  statement {
    sid    = "WriteToRawBucket"
    effect = "Allow"
    actions = [
      "s3:AbortMultipartUpload",
      "s3:GetBucketLocation",
      "s3:GetObject",
      "s3:ListBucket",
      "s3:ListBucketMultipartUploads",
      "s3:PutObject",
    ]
    resources = [
      aws_s3_bucket.raw.arn,
      "${aws_s3_bucket.raw.arn}/*",
    ]
  }

  statement {
    sid       = "WriteDeliveryLogs"
    effect    = "Allow"
    actions   = ["logs:PutLogEvents"]
    resources = ["${aws_cloudwatch_log_group.firehose.arn}:*"]
  }
}

resource "aws_iam_role_policy" "firehose_permissions" {
  name   = "${var.project_name}-firehose-permissions"
  role   = aws_iam_role.firehose_role.id
  policy = data.aws_iam_policy_document.firehose_permissions.json
}


# ── FIREHOSE DELIVERY STREAM — Direct PUT → S3 raw/streams/ ───────────────────
# No kinesis_source_configuration block ⇒ the stream is "Direct PUT": producers
# call PutRecord / PutRecordBatch directly. This is the simplest source and the
# correct one for a single producer feeding a single S3 destination.

resource "aws_kinesis_firehose_delivery_stream" "streams_ingestion" {
  name        = "${var.project_name}-streams-ingestion"
  destination = "extended_s3"

  extended_s3_configuration {
    role_arn   = aws_iam_role.firehose_role.arn
    bucket_arn = aws_s3_bucket.raw.arn

    # Land files under streams/ so the existing EventBridge rule (prefix "streams/")
    # and the raw crawler pick them up with no changes. Firehose appends its own
    # YYYY/MM/DD/HH/ date path after this prefix.
    prefix              = "streams/"
    error_output_prefix = "streams-errors/!{firehose:error-output-type}/"

    buffering_size     = var.firehose_buffer_size_mb
    buffering_interval = var.firehose_buffer_interval_seconds

    compression_format = "UNCOMPRESSED" # keep raw JSONL readable by the crawler

    cloudwatch_logging_options {
      enabled         = true
      log_group_name  = aws_cloudwatch_log_group.firehose.name
      log_stream_name = aws_cloudwatch_log_stream.firehose_s3_delivery.name
    }
  }

  tags = {
    Pipeline = "music-streaming"
    Stage    = "ingestion"
  }

  depends_on = [aws_iam_role_policy.firehose_permissions]
}
