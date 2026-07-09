###############################################################################
# VeloShelf — S3 module
# Two buckets: feature Parquet storage + MLflow artifact store
###############################################################################

variable "project"     { type = string }
variable "environment" { type = string }
variable "suffix"      { type = string }

# ---------------------------------------------------------------------------
# Feature Parquet bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "features" {
  bucket        = "${var.project}-features-${var.suffix}"
  force_destroy = true   # allow destroy even with objects (safe for dev)

  tags = { Name = "${var.project}-features", Purpose = "feature-parquet" }
}

resource "aws_s3_bucket_versioning" "features" {
  bucket = aws_s3_bucket.features.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_lifecycle_configuration" "features" {
  bucket = aws_s3_bucket.features.id

  rule {
    id     = "expire-raw-features-30d"
    status = "Enabled"

    filter { prefix = "date=" }

    expiration { days = 30 }   # keep 30 days of feature history
  }
}

resource "aws_s3_bucket_public_access_block" "features" {
  bucket                  = aws_s3_bucket.features.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# MLflow artifact bucket
# ---------------------------------------------------------------------------

resource "aws_s3_bucket" "mlflow" {
  bucket        = "${var.project}-mlflow-${var.suffix}"
  force_destroy = true

  tags = { Name = "${var.project}-mlflow", Purpose = "mlflow-artifacts" }
}

resource "aws_s3_bucket_public_access_block" "mlflow" {
  bucket                  = aws_s3_bucket.mlflow.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "features_bucket_name" { value = aws_s3_bucket.features.bucket }
output "features_bucket_arn"  { value = aws_s3_bucket.features.arn }
output "mlflow_bucket_name"   { value = aws_s3_bucket.mlflow.bucket }
output "mlflow_bucket_arn"    { value = aws_s3_bucket.mlflow.arn }
