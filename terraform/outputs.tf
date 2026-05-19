
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
