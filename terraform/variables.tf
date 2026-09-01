variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "localstack_endpoint" {
  description = "LocalStack endpoint URL"
  type        = string
  default     = "http://localhost:4566"
}

variable "aws_access_key" {
  description = "AWS access key (LocalStack)"
  type        = string
  default     = "test"
  sensitive   = true
}

variable "aws_secret_key" {
  description = "AWS secret key (LocalStack)"
  type        = string
  default     = "test"
  sensitive   = true
}

variable "bucket_name" {
  description = "S3 bucket name for Medallion Architecture"
  type        = string
  default     = "dados-teste"
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "development"
}
