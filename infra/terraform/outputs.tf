output "lake_bucket" {
  value       = aws_s3_bucket.lake.bucket
  description = "Point the Spark job at s3a://<this>/ (LAKE_PATH)."
}

output "athena_workgroup" {
  value = aws_athena_workgroup.lake.name
}

output "glue_database" {
  value = aws_glue_catalog_database.lake.name
}

output "writer_policy_arn" {
  value       = aws_iam_policy.writer.arn
  description = "Attach to the role or user that runs the streaming job."
}
