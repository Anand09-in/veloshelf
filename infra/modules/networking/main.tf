###############################################################################
# VeloShelf — Networking module
# VPC + public subnet (EC2) + private subnet (RDS)
###############################################################################

variable "project"     { type = string }
variable "environment" { type = string }
variable "vpc_cidr"    { type = string }
variable "az"          { type = string }

resource "aws_vpc" "main" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = { Name = "${var.project}-${var.environment}-vpc" }
}

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id
  tags   = { Name = "${var.project}-${var.environment}-igw" }
}

# Public subnet — EC2 lives here
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = cidrsubnet(var.vpc_cidr, 8, 1)
  availability_zone       = var.az
  map_public_ip_on_launch = true

  tags = { Name = "${var.project}-${var.environment}-public" }
}

# Private subnet — RDS lives here (second AZ for RDS subnet group)
resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 2)
  availability_zone = var.az

  tags = { Name = "${var.project}-${var.environment}-private" }
}

# Second private subnet in a different AZ (required for RDS subnet group)
data "aws_availability_zones" "available" { state = "available" }

resource "aws_subnet" "private_b" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = cidrsubnet(var.vpc_cidr, 8, 3)
  availability_zone = data.aws_availability_zones.available.names[1]

  tags = { Name = "${var.project}-${var.environment}-private-b" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = { Name = "${var.project}-${var.environment}-public-rt" }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# RDS subnet group
resource "aws_db_subnet_group" "main" {
  name       = "${var.project}-${var.environment}-db-subnet"
  subnet_ids = [aws_subnet.private.id, aws_subnet.private_b.id]
  tags       = { Name = "${var.project}-${var.environment}-db-subnet-group" }
}

output "vpc_id"              { value = aws_vpc.main.id }
output "public_subnet_id"   { value = aws_subnet.public.id }
output "private_subnet_id"  { value = aws_subnet.private.id }
output "db_subnet_group"    { value = aws_db_subnet_group.main.name }
