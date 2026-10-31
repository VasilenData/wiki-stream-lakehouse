data "aws_caller_identity" "me" {}

locals {
  bucket = "${var.name}-lake-${data.aws_caller_identity.me.account_id}"
}

# ---------- storage: one private, encrypted, versioned bucket ----------
resource "aws_s3_bucket" "lake" {
  bucket = local.bucket
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Cost control: raw events expire, aggregates stay; old versions and failed uploads are cleaned up.
resource "aws_s3_bucket_lifecycle_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id

  rule {
    id     = "expire-bronze"
    status = "Enabled"
    filter {
      prefix = "bronze_events/"
    }
    expiration {
      days = var.bronze_expire_days
    }
  }

  rule {
    id     = "clean-up-noncurrent-and-incomplete"
    status = "Enabled"
    filter {}
    noncurrent_version_expiration {
      noncurrent_days = 7
    }
    abort_incomplete_multipart_upload {
      days_after_initiation = 3
    }
  }
}

# ---------- catalog: Glue tables so Athena (SQL) can read the Parquet files ----------
resource "aws_glue_catalog_database" "lake" {
  name = "${var.name}_lake"
}

resource "aws_glue_catalog_table" "silver" {
  name          = "silver_wiki_minute"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  parameters    = { classification = "parquet" }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/silver_wiki_minute/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "window_start"
      type = "timestamp"
    }
    columns {
      name = "window_end"
      type = "timestamp"
    }
    columns {
      name = "wiki"
      type = "string"
    }
    columns {
      name = "edits"
      type = "bigint"
    }
    columns {
      name = "bot_edits"
      type = "bigint"
    }
    columns {
      name = "bytes_changed"
      type = "bigint"
    }
    columns {
      name = "editors"
      type = "bigint"
    }
  }
}

# Partition projection: Athena computes the daily partitions itself, so no crawler (cost) is needed.
resource "aws_glue_catalog_table" "bronze" {
  name          = "bronze_events"
  database_name = aws_glue_catalog_database.lake.name
  table_type    = "EXTERNAL_TABLE"
  parameters = {
    classification              = "parquet"
    "projection.enabled"        = "true"
    "projection.dt.type"        = "date"
    "projection.dt.range"       = "2026-01-01,NOW"
    "projection.dt.format"      = "yyyy-MM-dd"
    "storage.location.template" = "s3://${aws_s3_bucket.lake.bucket}/bronze_events/dt=$${dt}/"
  }

  partition_keys {
    name = "dt"
    type = "string"
  }

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.bucket}/bronze_events/"
    input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
    output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
    ser_de_info {
      serialization_library = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
    }
    columns {
      name = "id"
      type = "bigint"
    }
    columns {
      name = "wiki"
      type = "string"
    }
    columns {
      name = "type"
      type = "string"
    }
    columns {
      name = "bot"
      type = "boolean"
    }
    columns {
      name = "user"
      type = "string"
    }
    columns {
      name = "title"
      type = "string"
    }
    columns {
      name = "bytes_delta"
      type = "bigint"
    }
    columns {
      name = "event_ts"
      type = "bigint"
    }
    columns {
      name = "event_time"
      type = "timestamp"
    }
  }
}

# ---------- query engine: Athena with a hard per-query scan limit ----------
resource "aws_s3_bucket" "athena_results" {
  bucket        = "${local.bucket}-athena"
  force_destroy = true
}

resource "aws_s3_bucket_public_access_block" "athena_results" {
  bucket                  = aws_s3_bucket.athena_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_lifecycle_configuration" "athena_results" {
  bucket = aws_s3_bucket.athena_results.id
  rule {
    id     = "expire-results"
    status = "Enabled"
    filter {}
    expiration {
      days = 7
    }
  }
}

resource "aws_athena_workgroup" "lake" {
  name          = "${var.name}-wg"
  force_destroy = true
  configuration {
    enforce_workgroup_configuration = true
    bytes_scanned_cutoff_per_query  = var.athena_scan_limit_gb * 1024 * 1024 * 1024
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_results.bucket}/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# ---------- security: the streaming job gets write access to its own prefixes only ----------
data "aws_iam_policy_document" "writer" {
  statement {
    sid       = "ListBucket"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.lake.arn]
  }
  statement {
    sid       = "ReadWriteLake"
    actions   = ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload"]
    resources = ["${aws_s3_bucket.lake.arn}/*"]
  }
}

resource "aws_iam_policy" "writer" {
  name        = "${var.name}-stream-writer"
  description = "Least-privilege access for the Spark streaming job."
  policy      = data.aws_iam_policy_document.writer.json
}

# ---------- cost: alert before the bill is a surprise ----------
resource "aws_budgets_budget" "monthly" {
  name         = "${var.name}-monthly"
  budget_type  = "COST"
  limit_amount = tostring(var.monthly_budget_usd)
  limit_unit   = "USD"
  time_unit    = "MONTHLY"

  notification {
    comparison_operator        = "GREATER_THAN"
    threshold                  = 80
    threshold_type             = "PERCENTAGE"
    notification_type          = "FORECASTED"
    subscriber_email_addresses = [var.alert_email]
  }
}
