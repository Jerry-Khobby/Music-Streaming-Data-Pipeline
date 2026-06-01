
#  OUTPUTS — useful values to copy after terraform apply

# ── S3 ───────────────────────────────────────────────────────

output "raw_bucket_name" {
  description = "Name of the S3 raw (Bronze) bucket — upload your CSV files here"
  value       = aws_s3_bucket.raw.id
}

output "raw_bucket_arn" {
  description = "ARN of the raw bucket"
  value       = aws_s3_bucket.raw.arn
}

output "curated_bucket_name" {
  description = "Name of the S3 curated (Silver/Gold) bucket — Glue writes output here"
  value       = aws_s3_bucket.curated.id
}

output "archive_bucket_name" {
  description = "Name of the S3 archive bucket — processed files land here"
  value       = aws_s3_bucket.archive.id
}

# ── DYNAMODB ─────────────────────────────────────────────────

output "dynamodb_genre_kpis_table" {
  description = "DynamoDB table name for genre-level daily KPIs"
  value       = aws_dynamodb_table.genre_kpis.name
}

output "dynamodb_top_songs_table" {
  description = "DynamoDB table name for top 3 songs per genre per day"
  value       = aws_dynamodb_table.top_songs.name
}

output "dynamodb_top_genres_table" {
  description = "DynamoDB table name for top 5 genres per day"
  value       = aws_dynamodb_table.top_genres.name
}

# ── IAM ──────────────────────────────────────────────────────

output "glue_role_arn" {
  description = "IAM role ARN to attach to every Glue job you create"
  value       = aws_iam_role.glue_role.arn
}

output "glue_role_name" {
  description = "IAM role name"
  value       = aws_iam_role.glue_role.name
}

# ── GLUE ─────────────────────────────────────────────────────

output "glue_database_name" {
  description = "Glue Data Catalog database name — reference this in all Glue jobs"
  value       = aws_glue_catalog_database.music_db.name
}

output "glue_raw_crawler_name" {
  description = "Name of the Glue Crawler that scans the raw bucket"
  value       = aws_glue_crawler.raw_crawler.name
}

output "glue_curated_crawler_name" {
  description = "Name of the Glue Crawler that scans the curated bucket for Athena"
  value       = aws_glue_crawler.curated_crawler.name
}

# ── CLOUDWATCH ───────────────────────────────────────────────

output "glue_log_group" {
  description = "CloudWatch log group for all Glue jobs"
  value       = aws_cloudwatch_log_group.glue_jobs.name
}

output "step_functions_log_group" {
  description = "CloudWatch log group for Step Functions executions"
  value       = aws_cloudwatch_log_group.step_functions.name
}

# ── GLUE JOBS ─────────────────────────────────────────────────

output "glue_job_validation" {
  description = "Glue job name — step 1: validates raw catalog tables"
  value       = aws_glue_job.validation.name
}

output "glue_job_etl_transform" {
  description = "Glue job name — step 2: joins streams+songs, writes KPIs to gold/"
  value       = aws_glue_job.etl_transform.name
}

output "glue_job_dynamodb_loader" {
  description = "Glue job name — step 3: loads gold parquet into DynamoDB"
  value       = aws_glue_job.dynamodb_loader.name
}

output "glue_job_archive" {
  description = "Glue job name — step 4: archives processed raw files"
  value       = aws_glue_job.archive.name
}

output "glue_job_kpi_aggregation" {
  description = "Glue job name — standalone: KPI aggregation from silver/enriched_streams"
  value       = aws_glue_job.kpi_aggregation.name
}

output "glue_workflow_name" {
  description = "Glue Workflow name — trigger via console or: aws glue start-workflow-run --name <value>"
  value       = aws_glue_workflow.pipeline.name
}

# ── STEP FUNCTIONS ────────────────────────────────────────────

output "state_machine_arn" {
  description = "ARN of the Step Functions state machine"
  value       = aws_sfn_state_machine.pipeline.arn
}

output "state_machine_name" {
  description = "Name of the Step Functions state machine"
  value       = aws_sfn_state_machine.pipeline.name
}

# ── INGESTION ─────────────────────────────────────────────────

output "firehose_stream_name" {
  description = "Kinesis Data Firehose delivery stream name — pass to the producer's --stream-name"
  value       = aws_kinesis_firehose_delivery_stream.streams_ingestion.name
}

output "firehose_stream_arn" {
  description = "ARN of the Firehose delivery stream"
  value       = aws_kinesis_firehose_delivery_stream.streams_ingestion.arn
}

# ── MESSAGING ─────────────────────────────────────────────────

output "sns_alerts_topic_arn" {
  description = "SNS topic ARN — email subscription is managed via the alert_email variable; see email_alerts_status for current state"
  value       = aws_sns_topic.pipeline_alerts.arn
}

output "sqs_pipeline_events_url" {
  description = "SQS queue URL that receives S3 ObjectCreated events"
  value       = aws_sqs_queue.pipeline_events.url
}

output "sqs_pipeline_dlq_url" {
  description = "SQS dead-letter queue URL for unprocessable events"
  value       = aws_sqs_queue.pipeline_dlq.url
}

output "eventbridge_rule_name" {
  description = "EventBridge rule that watches for new stream files in S3"
  value       = aws_cloudwatch_event_rule.streams_uploaded.name
}

output "eventbridge_pipe_name" {
  description = "EventBridge Pipe that connects SQS to Step Functions"
  value       = aws_pipes_pipe.sqs_to_sfn.name
}
