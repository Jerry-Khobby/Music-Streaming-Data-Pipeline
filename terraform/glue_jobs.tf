#  GLUE JOBS — Scripts + Job definitions + Workflow
#
#  Execution order in the workflow:
#    1. validation_job       → validates raw catalog tables exist and are non-empty
#    2. etl_transform_job    → joins streams+songs, computes KPIs, writes to gold/
#    3. dynamodb_loader      → loads gold parquet into DynamoDB tables
#    4. archive_job          → moves processed raw files to the archive bucket
#
#  kpi_aggregation_job is deployed as a standalone job (not in the workflow)
#  because it reads from silver/enriched_streams, which a separate enrichment
#  step must write before it can run.


# ── SCRIPT UPLOADS ───────────────────────────────────────────────────────────
# Store each Python script in the curated bucket under scripts/.
# etag ensures Terraform re-uploads only when file content changes.

resource "aws_s3_object" "script_validation" {
  bucket = aws_s3_bucket.curated.id
  key    = "scripts/validation_job.py"
  source = "${path.module}/../glue_jobs/validation_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/validation_job.py")
}

resource "aws_s3_object" "script_etl_transform" {
  bucket = aws_s3_bucket.curated.id
  key    = "scripts/etl_transform_job.py"
  source = "${path.module}/../glue_jobs/etl_transform_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/etl_transform_job.py")
}

resource "aws_s3_object" "script_dynamodb_loader" {
  bucket = aws_s3_bucket.curated.id
  key    = "scripts/dynamodb_loader.py"
  source = "${path.module}/../glue_jobs/dynamodb_loader.py"
  etag   = filemd5("${path.module}/../glue_jobs/dynamodb_loader.py")
}

resource "aws_s3_object" "script_archive" {
  bucket = aws_s3_bucket.curated.id
  key    = "scripts/archive_job.py"
  source = "${path.module}/../glue_jobs/archive_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/archive_job.py")
}

resource "aws_s3_object" "script_kpi_aggregation" {
  bucket = aws_s3_bucket.curated.id
  key    = "scripts/kpi_aggregation_job.py"
  source = "${path.module}/../glue_jobs/kpi_aggregation_job.py"
  etag   = filemd5("${path.module}/../glue_jobs/kpi_aggregation_job.py")
}


# ── SHARED JOB DEFAULTS ──────────────────────────────────────────────────────
# All jobs share the same logging, Glue version, and worker configuration.
# Override per-job below only where needed.

locals {
  glue_common_args = {
    "--job-language"                     = "python"
    "--enable-job-insights"              = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--continuous-log-logGroup"          = aws_cloudwatch_log_group.glue_jobs.name
    "--TempDir"                          = "s3://${aws_s3_bucket.curated.id}/tmp/"
  }
}


# ── GLUE JOB 1 — validation_job ──────────────────────────────────────────────

resource "aws_glue_job" "validation" {
  name        = "${var.project_name}-validation"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Validates that raw catalog tables exist, are non-empty, and have required columns"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.curated.id}/scripts/validation_job.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10 # minutes — validation is fast

  # REMOVED: execution_property { max_concurrent_runs = 1 }
  # Now allows unlimited concurrent runs - Glue will queue automatically

  default_arguments = merge(local.glue_common_args, {
    "--glue_database" = var.glue_database_name
  })

  tags = {
    Pipeline = "music-streaming"
    Step     = "1-validation"
  }

  depends_on = [aws_s3_object.script_validation]
}


# ── GLUE JOB 2 — etl_transform_job ───────────────────────────────────────────

resource "aws_glue_job" "etl_transform" {
  name        = "${var.project_name}-etl-transform"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Joins streams+songs, computes genre KPIs, top songs, top genres — writes to gold/"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.curated.id}/scripts/etl_transform_job.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  # REMOVED: execution_property { max_concurrent_runs = 1 }
  # Now allows unlimited concurrent runs - Glue will queue automatically

  default_arguments = merge(local.glue_common_args, {
    "--glue_database"  = var.glue_database_name
    "--curated_bucket" = aws_s3_bucket.curated.id
  })

  tags = {
    Pipeline = "music-streaming"
    Step     = "2-etl-transform"
  }

  depends_on = [aws_s3_object.script_etl_transform]
}


# ── GLUE JOB 3 — dynamodb_loader ─────────────────────────────────────────────

resource "aws_glue_job" "dynamodb_loader" {
  name        = "${var.project_name}-dynamodb-loader"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Reads gold/ parquet files and loads them into the three DynamoDB tables"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.curated.id}/scripts/dynamodb_loader.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  # REMOVED: execution_property { max_concurrent_runs = 1 }
  # Now allows unlimited concurrent runs - Glue will queue automatically

  default_arguments = merge(local.glue_common_args, {
    "--curated_bucket" = aws_s3_bucket.curated.id
    "--aws_region"     = var.aws_region
  })

  tags = {
    Pipeline = "music-streaming"
    Step     = "3-dynamodb-loader"
  }

  depends_on = [aws_s3_object.script_dynamodb_loader]
}


# ── GLUE JOB 4 — archive_job (Python Shell) ──────────────────────────────────
# Pure boto3 S3 operations — no Spark needed. Python Shell starts in seconds
# vs. minutes for a Spark cluster, and costs ~4x less per run.

resource "aws_glue_job" "archive" {
  name        = "${var.project_name}-archive"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Copies processed raw stream files to the archive bucket and deletes originals"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.curated.id}/scripts/archive_job.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 10

  # REMOVED: execution_property { max_concurrent_runs = 1 }
  # Now allows unlimited concurrent runs - Glue will queue automatically

  default_arguments = merge(local.glue_common_args, {
    "--raw_bucket"     = aws_s3_bucket.raw.id
    "--archive_bucket" = aws_s3_bucket.archive.id
    "--aws_region"     = var.aws_region
  })

  tags = {
    Pipeline = "music-streaming"
    Step     = "4-archive"
  }

  depends_on = [aws_s3_object.script_archive]
}

# ── GLUE JOB 5 — kpi_aggregation_job (standalone) ────────────────────────────

resource "aws_glue_job" "kpi_aggregation" {
  name        = "${var.project_name}-kpi-aggregation"
  role_arn    = aws_iam_role.glue_role.arn
  description = "Reads silver/enriched_streams parquet and computes KPIs — run standalone after enrichment"

  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.curated.id}/scripts/kpi_aggregation_job.py"
    python_version  = "3"
  }

  glue_version      = "4.0"
  worker_type       = "G.1X"
  number_of_workers = 2
  timeout           = 30

  # REMOVED: execution_property { max_concurrent_runs = 1 }
  # Now allows unlimited concurrent runs - Glue will queue automatically

  default_arguments = merge(local.glue_common_args, {
    "--curated_bucket" = aws_s3_bucket.curated.id
  })

  tags = {
    Pipeline = "music-streaming"
    Step     = "standalone-kpi-aggregation"
  }

  depends_on = [aws_s3_object.script_kpi_aggregation]
}


# ── GLUE WORKFLOW ─────────────────────────────────────────────────────────────
# Chains jobs 1–4 in sequence. Each conditional trigger fires only when the
# previous job SUCCEEDED, so a failure stops the whole pipeline.

resource "aws_glue_workflow" "pipeline" {
  name        = "${var.project_name}-pipeline"
  description = "Orchestrates the music streaming ETL: validate → bronze→silver → silver→gold → load → archive"

  tags = {
    Pipeline = "music-streaming"
  }
}


# Trigger 1 — starts the workflow on demand (call StartWorkflowRun in the console
# or via AWS CLI to kick off a run)
resource "aws_glue_trigger" "start" {
  name          = "${var.project_name}-start"
  type          = "ON_DEMAND"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "On-demand start — kicks off the validation job"

  actions {
    job_name = aws_glue_job.validation.name
  }
}


# Trigger 2 — runs etl_transform after validation succeeds
resource "aws_glue_trigger" "after_validation" {
  name          = "${var.project_name}-after-validation"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "Fires etl_transform_job when validation_job succeeds"

  predicate {
    conditions {
      job_name = aws_glue_job.validation.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.etl_transform.name
  }
}


# Trigger 3 — runs kpi_aggregation after etl_transform writes silver layer
resource "aws_glue_trigger" "after_etl_transform" {
  name          = "${var.project_name}-after-etl-transform"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "Fires kpi_aggregation_job when etl_transform_job (bronze→silver) succeeds"

  predicate {
    conditions {
      job_name = aws_glue_job.etl_transform.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.kpi_aggregation.name
  }
}


# Trigger 4 — runs dynamodb_loader after kpi_aggregation writes gold layer
resource "aws_glue_trigger" "after_kpi_aggregation" {
  name          = "${var.project_name}-after-kpi-aggregation"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "Fires dynamodb_loader when kpi_aggregation_job (silver→gold) succeeds"

  predicate {
    conditions {
      job_name = aws_glue_job.kpi_aggregation.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.dynamodb_loader.name
  }
}


# Trigger 5 — runs archive after dynamodb_loader succeeds
resource "aws_glue_trigger" "after_dynamodb_loader" {
  name          = "${var.project_name}-after-dynamodb-loader"
  type          = "CONDITIONAL"
  workflow_name = aws_glue_workflow.pipeline.name
  description   = "Fires archive_job when dynamodb_loader succeeds"

  predicate {
    conditions {
      job_name = aws_glue_job.dynamodb_loader.name
      state    = "SUCCEEDED"
    }
  }

  actions {
    job_name = aws_glue_job.archive.name
  }
}