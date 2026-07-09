###############################################################################
# VeloShelf — Terraform variables
###############################################################################

variable "aws_region" {
  description = "AWS region to deploy into"
  type        = string
  default     = "ap-south-1"   # Mumbai — closest to Bengaluru
}

variable "project" {
  description = "Project name prefix for all resources"
  type        = string
  default     = "veloshelf"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "prod"
}

# ---------------------------------------------------------------------------
# Networking
# ---------------------------------------------------------------------------

variable "vpc_cidr" {
  description = "CIDR block for the VPC"
  type        = string
  default     = "10.0.0.0/16"
}

# ---------------------------------------------------------------------------
# S3
# ---------------------------------------------------------------------------

variable "s3_suffix" {
  description = "Unique suffix for S3 bucket names (use your AWS account ID or random string)"
  type        = string
  # No default — must be set in terraform.tfvars to ensure globally unique bucket names
}

# ---------------------------------------------------------------------------
# EC2
# ---------------------------------------------------------------------------

variable "ec2_instance_type" {
  description = "EC2 instance type for the VeloShelf server"
  type        = string
  default     = "m7i-flex.large"   # 2 vCPU, 8GB RAM — runs Kafka + Flink + MLflow + Dagster
}

variable "ec2_key_name" {
  description = "Name of the EC2 key pair for SSH access (must already exist in AWS)"
  type        = string
}

# ---------------------------------------------------------------------------
# RDS
# ---------------------------------------------------------------------------

variable "rds_instance_class" {
  description = "RDS instance class for Postgres"
  type        = string
  default     = "db.t3.micro"   # free-tier eligible
}

variable "db_name" {
  description = "Postgres database name"
  type        = string
  default     = "veloshelf"
}

variable "db_username" {
  description = "Postgres master username"
  type        = string
  default     = "veloshelf"
}

variable "db_password" {
  description = "Postgres master password"
  type        = string
  sensitive   = true
  # No default — must be set in terraform.tfvars or via TF_VAR_db_password env var
}
