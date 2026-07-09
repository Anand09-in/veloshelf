# Phase 6 — IaC and CI/CD

Terraform modules provisioning the AWS stack, GitHub Actions CI/CD with OIDC authentication (zero stored credentials), and Makefile targets for cost control. The full VeloShelf stack runs on a single EC2 instance using Docker Compose, with RDS for durable Postgres storage and S3 for Parquet features and MLflow artefacts.

---

## Terraform structure

```
infra/
├── main.tf                    # Root module — wires all sub-modules
├── variables.tf               # Input variables with defaults
├── terraform.tfvars           # Real values (gitignored)
├── terraform.tfvars.example   # Template for new contributors
└── modules/
    ├── networking/main.tf     # VPC, subnets, IGW, route tables
    ├── ec2/main.tf            # Instance, IAM role, security group, user_data
    ├── rds/main.tf            # RDS Postgres, DB subnet group, SG
    └── s3/main.tf             # Features bucket + MLflow artefact bucket
```

---

## Modules

### `modules/networking`

Creates a VPC (`10.0.0.0/16`) with two public subnets in different AZs (required for the RDS DB subnet group). An Internet Gateway and a route table with `0.0.0.0/0 → IGW` make both subnets publicly routable.

Outputs: `vpc_id`, `subnet_ids` (list of two), `vpc_cidr`.

### `modules/s3`

Two S3 buckets, both with versioning enabled and all-public-access blocked:
- `veloshelf-features-{suffix}` — stores Parquet feature exports from `ml/export_features.py`
- `veloshelf-mlflow-{suffix}` — MLflow artefact root (model binaries, Evidently reports)

`{suffix}` is the AWS account ID, making bucket names globally unique without a random suffix that changes on destroy/recreate.

Outputs: `features_bucket_name`, `features_bucket_arn`, `mlflow_bucket_name`, `mlflow_bucket_arn`.

### `modules/rds`

RDS Postgres 16 on `db.t3.micro` (free-tier eligible). Key settings:
- `multi_az = false` — single-AZ for cost
- `publicly_accessible = false` — only reachable from within the VPC
- `backup_retention_period = 0` — disables automated backups (saves storage cost; this is a demo)
- `skip_final_snapshot = true` — allows `terraform destroy` without a manual snapshot step

Security group ingress: CIDR `var.vpc_cidr` (10.0.0.0/16) on port 5432 — any EC2 in the VPC can connect, no per-instance SG coupling needed. This avoids the circular dependency that would result from referencing the EC2 SG (EC2 module would need RDS endpoint → RDS module would need EC2 SG → deadlock).

DB subnet group uses both public subnets. RDS is placed in a public subnet but `publicly_accessible = false` means the endpoint is only routable from within the VPC.

### `modules/ec2`

EC2 `m7i-flex.large` (8 vCPU, 16 GB RAM — needed to run 12 Docker containers without OOM). Key resources:

**IAM role**: `veloshelf-ec2-role` with a policy granting `s3:GetObject`, `s3:PutObject`, `s3:ListBucket` on both S3 buckets. The role is attached as an instance profile, so Docker containers on the EC2 can use the Instance Metadata Service for credentials — no access keys stored anywhere.

**Security group**: inbound rules for:

| Port(s) | Purpose |
|---|---|
| 22 | SSH (from `0.0.0.0/0` for demo; restrict to your IP in production) |
| 9092 | Kafka external listener |
| 5432 | Postgres (from VPC CIDR; RDS is not exposed externally) |
| 5000 | MLflow UI |
| 3000 | Dagster UI |
| 3001 | Grafana UI |
| 8080–8081 | Kafka UI / Flink UI |
| 8000 | Metrics exporter |
| 8501 | Streamlit |
| 9090 | Prometheus |

All outbound traffic is allowed.

**`user_data` bootstrap script**: runs at first boot via cloud-init:
1. Installs Docker and Docker Compose
2. Adds `ec2-user` to the `docker` group
3. Writes `/home/ec2-user/app/.env` with RDS endpoint, S3 bucket names, and Postgres DSN (interpolated from Terraform variables)
4. Does **not** start the stack — that's done via `make ec2-ssh` + `docker compose up -d` after cloning the repo

### State backend — `main.tf`

```hcl
backend "s3" {
  bucket     = "veloshelf-tfstate-798644229089"
  key        = "veloshelf/terraform.tfstate"
  region     = "ap-south-1"
  encrypt    = true
  use_lockfile = true
}
```

`use_lockfile = true` uses native S3 conditional writes for locking (S3 object versioning must be enabled on the state bucket). The older `dynamodb_table` locking approach is deprecated in Terraform 1.7+.

---

## GitHub Actions

### `.github/workflows/ci.yml` — Lint + Test

Triggers on pull requests to `main`. Steps:
1. Checkout + set up Python 3.11
2. `pip install -e ".[dev]"`
3. `ruff check .`
4. `pytest -q --ignore=tests/test_streaming.py --ignore=tests/test_smoke.py`

`test_streaming.py` and `test_smoke.py` are excluded — they require a running Flink or Docker environment that's not available in the GitHub-hosted runner.

Docker Compose build check: `docker compose build --no-cache` validates that all Dockerfiles build cleanly without launching services.

### `.github/workflows/deploy.yml` — Deploy on merge

Triggers on push to `main`. Two jobs:

**`terraform` job:**
1. `aws-actions/configure-aws-credentials` — OIDC exchange, no stored keys
2. `hashicorp/setup-terraform`
3. `terraform init` with backend config from secrets
4. `terraform plan` — shows what will change
5. `terraform apply -auto-approve` — provisions/updates infra

**`deploy` job** (depends on `terraform`):
1. Fetch EC2 public IP from `terraform output -raw ec2_public_ip`
2. SSH in using `EC2_SSH_PRIVATE_KEY` secret (the `.pem` contents)
3. `git pull origin main` on the EC2
4. `docker compose pull && docker compose up -d --build`

---

## OIDC — zero stored credentials

GitHub Actions authenticates to AWS via OIDC:
1. GitHub generates a short-lived JWT for the workflow run
2. AWS STS exchanges it for temporary credentials via the configured OIDC identity provider
3. The assumed role (`veloshelf-github-deploy`) has EC2/RDS/S3/IAM permissions scoped to the VeloShelf resources

Trust policy restricts assumption to `repo:Anand09-in/veloshelf:*` — no other repo can assume this role even if it has the role ARN. Credentials expire after the workflow run; nothing is stored.

---

## GitHub secrets required

| Secret | Value |
|---|---|
| `AWS_DEPLOY_ROLE_ARN` | ARN of `veloshelf-github-deploy` IAM role |
| `TF_STATE_BUCKET` | `veloshelf-tfstate-798644229089` |
| `S3_SUFFIX` | `798644229089` (AWS account ID) |
| `EC2_KEY_NAME` | `veloshelf-key` |
| `EC2_SSH_PRIVATE_KEY` | Contents of `veloshelf-key.pem` |
| `DB_PASSWORD` | RDS Postgres master password |

---

## Makefile — cost control targets

```makefile
make infra-up     # terraform apply — provision or update all AWS resources
make infra-down   # terraform destroy — tear down everything (5s countdown)
make ec2-stop     # stop the EC2 instance (~$0.10/hr saved while idle)
make ec2-start    # start the EC2 instance
make ec2-ssh      # SSH into EC2: ssh -i veloshelf-key.pem ec2-user@<ip>
```

`EC2_INSTANCE` is resolved at make-time via `terraform output -raw ec2_public_ip`. `INSTANCE_ID` is resolved via `aws ec2 describe-instances --filters "Name=ip-address,Values=<ip>"`.

Stopping the EC2 while not demoing reduces running cost from ~$85/mo to ~$12/mo (only RDS storage is billed when EC2 is stopped).

---

## Interview talking points

**"Why OIDC over stored access keys?"**
Long-lived credentials stored as GitHub secrets are a security risk — they don't expire, and a secret leak means permanent AWS access. OIDC issues short-lived STS tokens scoped to a single workflow run. Nothing to rotate, nothing to accidentally commit.

**"Why S3 backend with `use_lockfile`?"**
Local `terraform.tfstate` breaks in any multi-person or CI environment — two concurrent applies corrupt the file. S3 + native locking gives atomic state updates, versioning for rollback, and shared state accessible to any runner or developer. `use_lockfile` is the modern approach replacing the older DynamoDB locking table.

**"Why not EKS?"**
$72/month minimum for a 3-node EKS cluster, for a portfolio demo. The Terraform module for EKS and the k8s manifests are committed (`infra/modules/eks/`, `k8s/`) — the design intent is there. Running on a single EC2 with Docker Compose costs nothing and demonstrates the same architectural thinking without the spend.

**"Why RDS instead of Postgres in Docker on EC2?"**
Docker container storage is ephemeral — an EC2 restart or `docker compose down` with a volume prune loses the entire serving store and alert history. RDS gives durable storage, automated parameter management, and security group isolation. At db.t3.micro it's free-tier eligible for the first 12 months.
