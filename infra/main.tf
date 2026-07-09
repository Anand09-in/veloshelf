###############################################################################
# VeloShelf — Terraform root module
#
# Architecture:
#   EC2 t3.small  — Kafka, Flink, Generator, MLflow, Dagster
#   RDS Postgres  — windowed_features + alerts (t3.micro, free-tier eligible)
#   S3            — feature Parquet + MLflow artifacts
#
# Grafana, Prometheus, and Streamlit are local-dev only — not deployed to AWS.
# EKS manifests are committed (k8s/) as design intent but not applied here.
#
# Usage:
#   # One-time: create the state bucket manually first
#   aws s3 mb s3://veloshelf-tfstate-<your-suffix> --region ap-south-1
#
#   terraform init
#   terraform plan -var-file=terraform.tfvars
#   terraform apply -var-file=terraform.tfvars
###############################################################################

terraform {
  required_version = ">= 1.6"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }

  backend "s3" {
    # Fill in terraform.tfvars or pass via -backend-config
    bucket         = "veloshelf-tfstate-798644229089"
    key            = "veloshelf/terraform.tfstate"
    region         = "ap-south-1"
    use_lockfile = true
    encrypt      = true
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "veloshelf"
      Environment = var.environment
      ManagedBy   = "terraform"
    }
  }
}

###############################################################################
# Networking
###############################################################################

module "networking" {
  source = "./modules/networking"

  project     = var.project
  environment = var.environment
  vpc_cidr    = var.vpc_cidr
  az          = "${var.aws_region}a"
}

###############################################################################
# S3 — feature storage + MLflow artifacts
###############################################################################

module "s3" {
  source = "./modules/s3"

  project     = var.project
  environment = var.environment
  suffix      = var.s3_suffix
}

###############################################################################
# RDS Postgres — serving store (windowed_features + alerts)
###############################################################################

module "rds" {
  source = "./modules/rds"

  project              = var.project
  environment          = var.environment
  vpc_id               = module.networking.vpc_id
  vpc_cidr             = var.vpc_cidr
  db_subnet_group_name = module.networking.db_subnet_group
  db_name              = var.db_name
  db_username          = var.db_username
  db_password          = var.db_password
  instance_class       = var.rds_instance_class
}

###############################################################################
# EC2 — Kafka + Flink + MLflow + Dagster
###############################################################################

module "ec2" {
  source = "./modules/ec2"

  project           = var.project
  environment       = var.environment
  vpc_id            = module.networking.vpc_id
  subnet_id         = module.networking.public_subnet_id
  instance_type     = var.ec2_instance_type
  key_name          = var.ec2_key_name
  s3_bucket_arn     = module.s3.features_bucket_arn
  mlflow_bucket_arn = module.s3.mlflow_bucket_arn
  features_bucket   = module.s3.features_bucket_name
  mlflow_bucket     = module.s3.mlflow_bucket_name
  db_host           = module.rds.db_endpoint
  db_name           = var.db_name
  db_username       = var.db_username
  db_password       = var.db_password
}

###############################################################################
# Outputs
###############################################################################

output "ec2_public_ip" {
  description = "Public IP of the VeloShelf EC2 instance"
  value       = module.ec2.public_ip
}

output "ec2_public_dns" {
  description = "Public DNS of the VeloShelf EC2 instance"
  value       = module.ec2.public_dns
}

output "features_bucket" {
  description = "S3 bucket for feature Parquet files"
  value       = module.s3.features_bucket_name
}

output "mlflow_bucket" {
  description = "S3 bucket for MLflow artifacts"
  value       = module.s3.mlflow_bucket_name
}

output "rds_endpoint" {
  description = "RDS Postgres endpoint"
  value       = module.rds.db_endpoint
}

output "ssh_command" {
  description = "SSH command to connect to the EC2 instance"
  value       = "ssh -i ${var.ec2_key_name}.pem ec2-user@${module.ec2.public_ip}"
}
