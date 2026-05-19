
#  MUSIC STREAMING DATA PIPELINE — INFRASTRUCTURE
#  Provisions: S3 buckets, DynamoDB tables, IAM role, Glue DB



# ── S3 BUCKETS ───────────────────────────────────────────────────────────────

# 1. Raw / Bronze bucket — landing zone for incoming CSV files
resource "aws_s3_bucket" "raw" {
  bucket        = "${var.raw_bucket_name}-${var.environment}"
  force_destroy = true # allows terraform destroy to empty the bucket

  tags = {
    Layer = "bronze"
    Usage = "Incoming raw streaming event files"
  }
}

resource "aws_s3_bucket_versioning" "raw" {
  bucket = aws_s3_bucket.raw.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "raw" {
  bucket = aws_s3_bucket.raw.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Folder placeholders — S3 is flat but these keep the structure visible
resource "aws_s3_object" "songs_folder" {
  bucket  = aws_s3_bucket.raw.id
  key     = "songs/"
  content = ""
}

resource "aws_s3_object" "streams_folder" {
  bucket  = aws_s3_bucket.raw.id
  key     = "streams/"
  content = ""
}

resource "aws_s3_object" "users_folder" {
  bucket  = aws_s3_bucket.raw.id
  key     = "users/"
  content = ""
}


# 2. Curated / Silver-Gold bucket — Glue writes transformed output here
resource "aws_s3_bucket" "curated" {
  bucket        = "${var.curated_bucket_name}-${var.environment}"
  force_destroy = true

  tags = {
    Layer = "silver-gold"
    Usage = "Cleansed and aggregated KPI data"
  }
}

resource "aws_s3_bucket_versioning" "curated" {
  bucket = aws_s3_bucket.curated.id
  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "curated" {
  bucket = aws_s3_bucket.curated.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_object" "silver_folder" {
  bucket  = aws_s3_bucket.curated.id
  key     = "silver/"
  content = ""
}

resource "aws_s3_object" "gold_folder" {
  bucket  = aws_s3_bucket.curated.id
  key     = "gold/"
  content = ""
}


# 3. Archive bucket — processed raw files get moved here after pipeline runs
resource "aws_s3_bucket" "archive" {
  bucket        = "${var.archive_bucket_name}-${var.environment}"
  force_destroy = true

  tags = {
    Layer = "archive"
    Usage = "Processed files moved here to prevent reprocessing"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Lifecycle rule — automatically move archive files to cheaper storage after 90 days
resource "aws_s3_bucket_lifecycle_configuration" "archive" {
  bucket = aws_s3_bucket.archive.id

  rule {
    id     = "move-to-glacier"
    status = "Enabled"

    transition {
      days          = 90
      storage_class = "GLACIER"
    }
  }
}


# ── DYNAMODB TABLES ──────────────────────────────────────────────────────────

# Table 1 — Genre KPIs
# Stores: listen_count, unique_listeners, total_listen_time, avg_listen_time
# Query pattern: "give me all KPIs for genre=Afrobeats on date=2026-05-17"
resource "aws_dynamodb_table" "genre_kpis" {
  name         = "genre_kpis"
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "genre_date" # format: "Afrobeats#2026-05-17"

  attribute {
    name = "genre_date"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Purpose = "Daily genre-level KPI metrics"
  }
}


# Table 2 — Top Songs per Genre per Day
# Stores: top 3 songs per genre
# Query pattern: "give me top songs for genre=Afrobeats on date=2026-05-17"
resource "aws_dynamodb_table" "top_songs" {
  name         = "top_songs"
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "genre_date" # format: "Afrobeats#2026-05-17"
  range_key    = "rank"       # 1, 2, or 3

  attribute {
    name = "genre_date"
    type = "S"
  }

  attribute {
    name = "rank"
    type = "N"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Purpose = "Top 3 songs per genre per day"
  }
}


# Table 3 — Top Genres per Day
# Stores: top 5 genres globally ranked by listen count
# Query pattern: "give me top 5 genres for date=2026-05-17"
resource "aws_dynamodb_table" "top_genres" {
  name         = "top_genres"
  billing_mode = var.dynamodb_billing_mode
  hash_key     = "date" # format: "2026-05-17"
  range_key    = "rank" # 1 through 5

  attribute {
    name = "date"
    type = "S"
  }

  attribute {
    name = "rank"
    type = "N"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Purpose = "Top 5 genres globally per day"
  }
}


# ── IAM ROLE FOR GLUE ────────────────────────────────────────────────────────

# Trust policy — allows Glue service to assume this role
data "aws_iam_policy_document" "glue_trust" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "glue_role" {
  name               = var.glue_role_name
  assume_role_policy = data.aws_iam_policy_document.glue_trust.json
  description        = "IAM role used by all Glue jobs in the music streaming pipeline"
}

# Attach AWS managed policies
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy_attachment" "s3_full" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
}

resource "aws_iam_role_policy_attachment" "dynamodb_full" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess"
}

resource "aws_iam_role_policy_attachment" "cloudwatch_full" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchLogsFullAccess"
}


# ── GLUE DATA CATALOG DATABASE ───────────────────────────────────────────────

resource "aws_glue_catalog_database" "music_db" {
  name        = var.glue_database_name
  description = "Glue Data Catalog database for the music streaming pipeline"
}


# ── GLUE CRAWLER ─────────────────────────────────────────────────────────────

# Crawls the raw S3 bucket and registers songs, streams, users as Catalog tables
resource "aws_glue_crawler" "raw_crawler" {
  name          = "${var.project_name}-raw-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.music_db.name
  description   = "Crawls S3 raw bucket and registers schema in the Glue Data Catalog"

  s3_target {
    path = "s3://${aws_s3_bucket.raw.id}/songs/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.raw.id}/streams/"
  }

  s3_target {
    path = "s3://${aws_s3_bucket.raw.id}/users/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }
    }
  })

  tags = {
    Usage = "Registers raw file schemas into Glue Data Catalog"
  }
}


# ── GLUE CRAWLER (CURATED) ───────────────────────────────────────────────────

# Crawls Silver/Gold layer after KPI job runs — keeps Athena partitions up to date
resource "aws_glue_crawler" "curated_crawler" {
  name          = "${var.project_name}-curated-crawler"
  role          = aws_iam_role.glue_role.arn
  database_name = aws_glue_catalog_database.music_db.name
  description   = "Crawls curated S3 bucket to register new partitions for Athena"

  s3_target {
    path = "s3://${aws_s3_bucket.curated.id}/gold/"
  }

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG"
  }

  tags = {
    Usage = "Keeps Athena partition list up to date after KPI job runs"
  }
}


# ── CLOUDWATCH LOG GROUPS ────────────────────────────────────────────────────

resource "aws_cloudwatch_log_group" "glue_jobs" {
  name              = "/aws/glue/${var.project_name}"
  retention_in_days = 30

  tags = {
    Usage = "Glue job execution logs"
  }
}

resource "aws_cloudwatch_log_group" "step_functions" {
  name              = "/aws/states/${var.project_name}"
  retention_in_days = 30

  tags = {
    Usage = "Step Functions execution logs"
  }
}
