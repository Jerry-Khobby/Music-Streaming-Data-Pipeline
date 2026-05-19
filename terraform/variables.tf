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
