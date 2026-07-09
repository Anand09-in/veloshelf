###############################################################################
# VeloShelf — EC2 module
# Single t3.small running Kafka + Flink + MLflow + Dagster via docker-compose
# IAM instance profile grants S3 access without stored credentials
###############################################################################

variable "project"         { type = string }
variable "environment"     { type = string }
variable "subnet_id"       { type = string }
variable "vpc_id"          { type = string }
variable "instance_type" {
  type    = string
  default = "t3.small"
}
variable "key_name"        { type = string }
variable "s3_bucket_arn"    { type = string }
variable "mlflow_bucket_arn" { type = string }
variable "features_bucket"  { type = string }
variable "mlflow_bucket"    { type = string }
variable "db_host"         { type = string }
variable "db_name"         { type = string }
variable "db_username" { type = string }
variable "db_password" {
  type      = string
  sensitive = true
}

# ---------------------------------------------------------------------------
# IAM role — EC2 → S3 (no stored credentials)
# ---------------------------------------------------------------------------

resource "aws_iam_role" "ec2" {
  name = "${var.project}-${var.environment}-ec2-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "s3_access" {
  name = "${var.project}-${var.environment}-s3-policy"
  role = aws_iam_role.ec2.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ]
        Resource = [
          var.s3_bucket_arn,
          "${var.s3_bucket_arn}/*",
          var.mlflow_bucket_arn,
          "${var.mlflow_bucket_arn}/*",
        ]
      }
    ]
  })
}

resource "aws_iam_instance_profile" "ec2" {
  name = "${var.project}-${var.environment}-ec2-profile"
  role = aws_iam_role.ec2.name
}

# ---------------------------------------------------------------------------
# Security group
# ---------------------------------------------------------------------------

resource "aws_security_group" "ec2" {
  name        = "${var.project}-${var.environment}-ec2-sg"
  description = "VeloShelf EC2 - Kafka, Flink, MLflow, Dagster"
  vpc_id      = var.vpc_id

  ingress {
    from_port   = 22
    to_port     = 22
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "SSH"
  }

  ingress {
    from_port   = 9092
    to_port     = 9092
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Kafka broker"
  }

  ingress {
    from_port   = 5000
    to_port     = 5000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "MLflow"
  }

  ingress {
    from_port   = 3000
    to_port     = 3001
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Dagster (3000) and Grafana (3001)"
  }

  ingress {
    from_port   = 8080
    to_port     = 8081
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Kafka UI (8080) and Flink UI (8081)"
  }

  ingress {
    from_port   = 8000
    to_port     = 8000
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Metrics exporter"
  }

  ingress {
    from_port   = 8501
    to_port     = 8501
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Streamlit"
  }

  ingress {
    from_port   = 9090
    to_port     = 9090
    protocol    = "tcp"
    cidr_blocks = ["0.0.0.0/0"]
    description = "Prometheus"
  }

  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = { Name = "${var.project}-${var.environment}-ec2-sg" }
}

# ---------------------------------------------------------------------------
# AMI — Amazon Linux 2023 (latest)
# ---------------------------------------------------------------------------

data "aws_ami" "al2023" {
  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-x86_64"]
  }

  filter {
    name   = "architecture"
    values = ["x86_64"]
  }
}

# ---------------------------------------------------------------------------
# User data — bootstrap script run on first launch
# Installs Docker + docker-compose, clones repo, starts the stack
# ---------------------------------------------------------------------------

locals {
  user_data = <<-EOF
    #!/bin/bash
    set -euxo pipefail

    # System update
    dnf update -y
    dnf install -y docker git python3-pip

    # Docker
    systemctl enable docker
    systemctl start docker
    usermod -aG docker ec2-user

    # Docker Compose v2
    COMPOSE_VERSION="2.24.6"
    mkdir -p /usr/local/lib/docker/cli-plugins
    curl -SL "https://github.com/docker/compose/releases/download/v$${COMPOSE_VERSION}/docker-compose-linux-x86_64" \
         -o /usr/local/lib/docker/cli-plugins/docker-compose
    chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

    # App directory
    mkdir -p /opt/veloshelf
    chown ec2-user:ec2-user /opt/veloshelf

    # Write .env with AWS-resolved values
    cat > /opt/veloshelf/.env << 'ENVEOF'
    KAFKA_BOOTSTRAP_SERVERS=localhost:9092
    TOPIC_RAW_ORDERS=raw-orders
    TOPIC_RAW_INVENTORY=raw-inventory
    TOPIC_FEATURES=features-windowed
    TOPIC_STOCKOUT_ALERTS=stockout-alerts
    TOPIC_SURGE_ALERTS=surge-alerts
    TOPIC_DEAD_LETTER=dead-letter
    SERVING_STORE=postgres
    POSTGRES_DSN=postgresql://${var.db_username}:${var.db_password}@${var.db_host}/${var.db_name}
    MLFLOW_TRACKING_URI=http://localhost:5000
    FEATURES_PATH=s3://${var.features_bucket}
    MLFLOW_DEFAULT_ARTIFACT_ROOT=s3://${var.mlflow_bucket}
    AWS_DEFAULT_REGION=ap-south-1
    ENVEOF

    echo "Bootstrap complete. Clone your repo to /opt/veloshelf and run: docker compose up -d"
  EOF
}

# ---------------------------------------------------------------------------
# EC2 instance
# ---------------------------------------------------------------------------

resource "aws_instance" "main" {
  ami                    = data.aws_ami.al2023.id
  instance_type          = var.instance_type
  subnet_id              = var.subnet_id
  key_name               = var.key_name
  vpc_security_group_ids = [aws_security_group.ec2.id]
  iam_instance_profile   = aws_iam_instance_profile.ec2.name
  user_data              = local.user_data

  root_block_device {
    volume_size           = 30    # GB — Docker images + Kafka data
    volume_type           = "gp3"
    delete_on_termination = true
  }

  tags = { Name = "${var.project}-${var.environment}-server" }

  lifecycle {
    ignore_changes = [ami]   # don't replace instance just because AMI updated
  }
}

# ---------------------------------------------------------------------------
# Outputs
# ---------------------------------------------------------------------------

output "public_ip"        { value = aws_instance.main.public_ip }
output "public_dns"       { value = aws_instance.main.public_dns }
output "security_group_id" { value = aws_security_group.ec2.id }
output "instance_id"      { value = aws_instance.main.id }
