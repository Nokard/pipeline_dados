output "bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.medallion.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.medallion.arn
}

output "bronze_path" {
  description = "S3 path for Bronze layer"
  value       = "s3a://${aws_s3_bucket.medallion.id}/bronze"
}

output "silver_path" {
  description = "S3 path for Silver layer"
  value       = "s3a://${aws_s3_bucket.medallion.id}/silver"
}

output "gold_path" {
  description = "S3 path for Gold layer"
  value       = "s3a://${aws_s3_bucket.medallion.id}/gold"
}

output "localstack_endpoint" {
  description = "LocalStack endpoint"
  value       = var.localstack_endpoint
}
