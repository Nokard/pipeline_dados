terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  endpoints {
    s3 = var.localstack_endpoint
  }

  access_key                  = var.aws_access_key
  secret_key                  = var.aws_secret_key
  skip_credentials_validation = true
  skip_requesting_account_id  = true
}

# Bucket S3
resource "aws_s3_bucket" "medallion" {
  bucket = var.bucket_name

  tags = {
    Name        = var.bucket_name
    Environment = var.environment
    Architecture = "Medallion"
  }
}

# Prefixo: Bronze (raw data)
resource "aws_s3_object" "bronze_prefix" {
  bucket = aws_s3_bucket.medallion.id
  key    = "bronze/"
  content = ""

  tags = {
    Layer = "Bronze"
  }
}

# Prefixo: Silver (cleaned data)
resource "aws_s3_object" "silver_prefix" {
  bucket = aws_s3_bucket.medallion.id
  key    = "silver/"
  content = ""

  tags = {
    Layer = "Silver"
  }
}

# Prefixo: Gold (analysis-ready data)
resource "aws_s3_object" "gold_prefix" {
  bucket = aws_s3_bucket.medallion.id
  key    = "gold/"
  content = ""

  tags = {
    Layer = "Gold"
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "medallion" {
  bucket = aws_s3_bucket.medallion.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}
