###############################################################################
# VeloShelf — RDS module
# Postgres 16 on db.t3.micro (free-tier eligible)
# Replaces the local Postgres container from docker-compose
###############################################################################

variable "project"              { type = string }
variable "environment"          { type = string }
variable "vpc_id"               { type = string }
variable "vpc_cidr"             { type = string }
variable "db_subnet_group_name" { type = string }
variable "db_name"              { type = string }
variable "db_username"          { type = string }

variable "db_password" {
  type      = string
  sensitive = true
}

variable "instance_class" {
  type    = string
  default = "db.t3.micro"
}

# Security group — only the EC2 instance can connect
resource "aws_security_group" "rds" {
  name        = "${var.project}-${var.environment}-rds-sg"
  description = "Allow Postgres from EC2 only"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 5432
    to_port     = 5432
    protocol    = "tcp"
    cidr_blocks = [var.vpc_cidr]
    description = "Postgres from within the VPC"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-${var.environment}-rds-sg" }
}

resource "aws_db_instance" "postgres" {
  identifier        = "${var.project}-${var.environment}-postgres"
  engine            = "postgres"
  engine_version    = "16"
  instance_class    = var.instance_class
  allocated_storage = 20       # GB — free tier allows up to 20GB
  storage_type      = "gp2"

  db_name  = var.db_name
  username = var.db_username
  password = var.db_password

  db_subnet_group_name   = var.db_subnet_group_name
  vpc_security_group_ids = [aws_security_group.rds.id]

  # Free-tier settings
  multi_az               = false
  publicly_accessible    = false
  skip_final_snapshot    = true   # fine for dev; set to false for prod
  deletion_protection    = false

  backup_retention_period = 0   # free tier accounts don't support automated backups

  tags = { Name = "${var.project}-${var.environment}-postgres" }
}

output "db_endpoint" {
  value       = aws_db_instance.postgres.endpoint
  description = "RDS Postgres endpoint (host:port)"
}

output "db_name"     { value = var.db_name }
output "db_username" { value = var.db_username }
