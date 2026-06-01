variable "aws_region" {
  description = "AWS region to deploy all resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name used as a prefix for all resource names"
  type        = string
  default     = "music-streaming"
}

# ── S3 ──────────────────────────────────────────────────────────────────────

variable "raw_bucket_name" {
  description = "S3 bucket for raw incoming streaming data (Bronze layer)"
  type        = string
  default     = "music-streaming-raw"
}

variable "curated_bucket_name" {
  description = "S3 bucket for cleansed and aggregated data (Silver/Gold layer)"
  type        = string
  default     = "music-streaming-curated"
}

variable "archive_bucket_name" {
  description = "S3 bucket for processed/archived raw files"
  type        = string
  default     = "music-streaming-archive"
}

# ── Ingestion / Firehose ─────────────────────────────────────────────────────
# These two thresholds control how often Firehose lands a batch file in S3 (it
# flushes when EITHER trips first):
#   • interval = 60 s  → the AWS minimum, so small/sparse data lands as fast as
#     Firehose allows (60 s is a hard floor — Firehose cannot deliver instantly).
#   • size     = 5 MB  → a genuine high-volume burst still consolidates into one
#     file by size before the 60 s timer, avoiding a flood of tiny files.
# This pairing gives the most demo-responsive delivery Firehose supports while
# keeping burst consolidation. Raise the interval (toward 900) for fewer, larger
# files and fewer pipeline runs if latency stops mattering.

variable "firehose_buffer_size_mb" {
  description = "Firehose buffer size in MB before flushing to S3 (1–128)"
  type        = number
  default     = 5

  validation {
    condition     = var.firehose_buffer_size_mb >= 1 && var.firehose_buffer_size_mb <= 128
    error_message = "firehose_buffer_size_mb must be between 1 and 128."
  }
}

variable "firehose_buffer_interval_seconds" {
  description = "Max seconds Firehose buffers records before flushing to S3 (60 = AWS minimum, up to 900)"
  type        = number
  default     = 60

  validation {
    condition     = var.firehose_buffer_interval_seconds >= 60 && var.firehose_buffer_interval_seconds <= 900
    error_message = "firehose_buffer_interval_seconds must be between 60 and 900."
  }
}

# ── DynamoDB ─────────────────────────────────────────────────────────────────

variable "dynamodb_billing_mode" {
  description = "DynamoDB billing mode — PAY_PER_REQUEST (on-demand) or PROVISIONED"
  type        = string
  default     = "PAY_PER_REQUEST"
}

# ── Glue ─────────────────────────────────────────────────────────────────────

variable "glue_database_name" {
  description = "Glue Data Catalog database name"
  type        = string
  default     = "music_streaming_db"
}

variable "glue_role_name" {
  description = "IAM role name attached to all Glue jobs"
  type        = string
  default     = "glue-pipeline-role"
}

# ── Monitoring / Slack alerts ───────────────────────────────────────────────
# Both values come from AWS Chatbot AFTER you authorise the Slack workspace
# once in the console (AWS Console → AWS Chatbot → Configure new client → Slack).
# Leave both empty to deploy alarms only — the Chatbot resource is skipped.

variable "slack_workspace_id" {
  description = "Slack workspace ID from AWS Chatbot console (e.g. T01234ABCDE). Empty disables Slack."
  type        = string
  default     = ""
}

variable "slack_channel_id" {
  description = "Slack channel ID (right-click channel in Slack → Copy link, ID is the last segment, e.g. C01234ABCDE)"
  type        = string
  default     = ""
}

# Address subscribed to the pipeline_alerts SNS topic. AWS sends a confirmation
# link on first apply — the subscription stays "PendingConfirmation" until the
# recipient clicks it, so no alerts are delivered until then.
variable "alert_email" {
  description = "Email address that receives pipeline failure alerts. Empty disables email subscription."
  type        = string
  default     = "jeremiah.coblah@amalitechtraining.org"

  validation {
    condition     = var.alert_email == "" || can(regex("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$", var.alert_email))
    error_message = "alert_email must be a valid email address or empty."
  }
}


variable "aws_account_id" {
  description = "AWS account ID — used to build ARNs inside the state machine definition"
  type        = string
  default     = ""
}