###############################################################################
# VeloShelf — EKS module (DESIGN INTENT — NOT APPLIED)
#
# This file documents the intended Kubernetes deployment target for VeloShelf.
# It is committed to show architectural intent but is NOT wired into main.tf.
#
# To apply: uncomment the module block in main.tf and run terraform apply.
# Cost warning: EKS control plane costs ~$0.10/hour ($72/month). Only run
# for demos — destroy immediately after.
#
# In production this would replace the single-EC2 deployment with:
#   - EKS cluster running Kafka (Strimzi operator)
#   - Flink on Kubernetes (Flink Kubernetes Operator)
#   - MLflow as a Kubernetes deployment
#   - Dagster as a Kubernetes deployment
#   - HPA on Flink taskmanagers for burst scaling
###############################################################################

variable "project" {
  type = string
}

variable "environment" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_ids" {
  type = list(string)
}

variable "cluster_version" {
  type    = string
  default = "1.29"
}

# EKS Cluster
resource "aws_eks_cluster" "main" {
  name    = "${var.project}-${var.environment}"
  version = var.cluster_version

  role_arn = aws_iam_role.eks_cluster.arn

  vpc_config {
    subnet_ids = var.subnet_ids
  }

  depends_on = [aws_iam_role_policy_attachment.eks_cluster_policy]

  tags = { Name = "${var.project}-${var.environment}-eks" }
}

# IAM role for EKS control plane
resource "aws_iam_role" "eks_cluster" {
  name = "${var.project}-${var.environment}-eks-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "eks.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_cluster_policy" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSClusterPolicy"
  role       = aws_iam_role.eks_cluster.name
}

# Managed node group — t3.medium (2 nodes for Flink JM + TM)
resource "aws_eks_node_group" "main" {
  cluster_name    = aws_eks_cluster.main.name
  node_group_name = "${var.project}-${var.environment}-nodes"
  node_role_arn   = aws_iam_role.eks_nodes.arn
  subnet_ids      = var.subnet_ids

  instance_types = ["t3.medium"]

  scaling_config {
    desired_size = 2
    min_size     = 1
    max_size     = 4   # HPA can scale up to 4 Flink taskmanagers
  }

  depends_on = [
    aws_iam_role_policy_attachment.eks_worker_node,
    aws_iam_role_policy_attachment.eks_cni,
    aws_iam_role_policy_attachment.eks_ecr,
  ]
}

resource "aws_iam_role" "eks_nodes" {
  name = "${var.project}-${var.environment}-eks-node-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "eks_worker_node" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKSWorkerNodePolicy"
  role       = aws_iam_role.eks_nodes.name
}

resource "aws_iam_role_policy_attachment" "eks_cni" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEKS_CNI_Policy"
  role       = aws_iam_role.eks_nodes.name
}

resource "aws_iam_role_policy_attachment" "eks_ecr" {
  policy_arn = "arn:aws:iam::aws:policy/AmazonEC2ContainerRegistryReadOnly"
  role       = aws_iam_role.eks_nodes.name
}

output "cluster_name"     { value = aws_eks_cluster.main.name }
output "cluster_endpoint" { value = aws_eks_cluster.main.endpoint }
