# Phase 6 — IaC, CI/CD, and Polish

> Goal: Terraform-provisioned AWS infrastructure, GitHub Actions CI/CD with
> OIDC (zero stored credentials), and a polished README ready for the portfolio.

**Definition of done:**
- `terraform plan` runs without errors from a clean clone.
- GitHub Actions CI passes on every PR (lint + test).
- GitHub Actions Deploy runs on merge to main (Terraform apply + SSH deploy).
- README clearly communicates the project's value, architecture, and stack.

---

## Task list

### New files
- [x] `infra/main.tf`                    — root Terraform module
- [x] `infra/variables.tf`               — all input variables
- [x] `infra/terraform.tfvars.example`   — template (gitignored tfvars)
- [x] `infra/modules/networking/main.tf` — VPC, subnets, IGW, route tables
- [x] `infra/modules/s3/main.tf`         — features + MLflow S3 buckets
- [x] `infra/modules/rds/main.tf`        — RDS Postgres t3.micro
- [x] `infra/modules/ec2/main.tf`        — EC2 t3.small, IAM role, SG, user_data
- [x] `infra/modules/eks/main.tf`        — EKS design intent (NOT applied)
- [x] `.github/workflows/ci.yml`         — lint + test + Docker build check
- [x] `.github/workflows/deploy.yml`     — OIDC → Terraform apply → SSH deploy
- [x] `README.md`                        — portfolio README with architecture + quickstart

### Updated files
- [x] `.gitignore`                       — added Terraform + SSH key patterns

### Verification steps
- [ ] `terraform init` runs without errors
- [ ] `terraform validate` passes
- [ ] `terraform plan` produces expected resource list
- [ ] GitHub Actions CI passes on a PR
- [ ] GitHub secrets configured (see below)
- [ ] GitHub Actions Deploy runs on merge to main
- [ ] EC2 + RDS + S3 provisioned on AWS
- [ ] SSH deploy succeeds (`make up` on EC2)
- [ ] README renders correctly on GitHub

---

## Architecture decision — why not one big EC2?

Running Grafana + Prometheus + Streamlit + Postgres + Kafka + Flink + MLflow +
Dagster on a single t3.micro/small is resource contention, not production
engineering. The split is:

| What | Where | Why |
|---|---|---|
| Kafka + Flink + MLflow + Dagster | EC2 t3.small | core streaming, needs to be always-on |
| Postgres (windowed_features + alerts) | RDS t3.micro | managed, free-tier, no OOM risk |
| S3 (features + MLflow artifacts) | S3 | serverless, persistent, cheap |
| Grafana + Prometheus + Streamlit | local dev only | not needed 24/7 for a demo |
| EKS | design intent (k8s/ + infra/modules/eks/) | production evolution, not demo |

This is the "right-sized for the problem" story — a strong interview answer.

---

## GitHub secrets to configure

Go to GitHub → repo → Settings → Secrets and variables → Actions → New secret.

| Secret | Value |
|---|---|
| `AWS_DEPLOY_ROLE_ARN` | ARN of the IAM role GitHub Actions assumes via OIDC |
| `TF_STATE_BUCKET` | Name of your Terraform state S3 bucket |
| `TF_LOCK_TABLE` | Name of your DynamoDB lock table |
| `S3_SUFFIX` | Unique suffix for VeloShelf S3 bucket names |
| `EC2_KEY_NAME` | Name of the EC2 key pair in AWS (without .pem) |
| `EC2_SSH_PRIVATE_KEY` | Contents of the .pem file (for SSH deploy job) |
| `DB_PASSWORD` | RDS Postgres password |

---

## OIDC setup (one-time, ~5 minutes)

OIDC lets GitHub Actions authenticate to AWS without storing any long-lived
credentials. This is the production-standard approach.

```bash
# 1. Create the OIDC identity provider in AWS (one-time per account)
aws iam create-open-id-connect-provider \
  --url https://token.actions.githubusercontent.com \
  --client-id-list sts.amazonaws.com \
  --thumbprint-list 6938fd4d98bab03faadb97b34396831e3780aea1

# 2. Create IAM role for GitHub Actions
# Replace YOUR_GITHUB_ORG and YOUR_REPO_NAME
cat > trust-policy.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": {
      "Federated": "arn:aws:iam::ACCOUNT_ID:oidc-provider/token.actions.githubusercontent.com"
    },
    "Action": "sts:AssumeRoleWithWebIdentity",
    "Condition": {
      "StringEquals": {
        "token.actions.githubusercontent.com:aud": "sts.amazonaws.com"
      },
      "StringLike": {
        "token.actions.githubusercontent.com:sub": "repo:Anand09-in/veloshelf:*"
      }
    }
  }]
}
EOF

aws iam create-role \
  --role-name veloshelf-github-deploy \
  --assume-role-policy-document file://trust-policy.json

# 3. Attach policies the role needs
aws iam attach-role-policy \
  --role-name veloshelf-github-deploy \
  --policy-arn arn:aws:iam::aws:policy/AmazonEC2FullAccess

aws iam attach-role-policy \
  --role-name veloshelf-github-deploy \
  --policy-arn arn:aws:iam::aws:policy/AmazonRDSFullAccess

aws iam attach-role-policy \
  --role-name veloshelf-github-deploy \
  --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess

aws iam attach-role-policy \
  --role-name veloshelf-github-deploy \
  --policy-arn arn:aws:iam::aws:policy/IAMFullAccess

# 4. Copy the role ARN → set as AWS_DEPLOY_ROLE_ARN secret in GitHub
aws iam get-role --role-name veloshelf-github-deploy \
  --query 'Role.Arn' --output text
```

---

## Terraform state backend setup (one-time)

```bash
# 1. Create the state bucket (replace <suffix> with your S3_SUFFIX)
aws s3 mb s3://veloshelf-tfstate-<suffix> --region ap-south-1

# Enable versioning (recover from accidental state corruption)
aws s3api put-bucket-versioning \
  --bucket veloshelf-tfstate-<suffix> \
  --versioning-configuration Status=Enabled

# 2. Create DynamoDB lock table
aws dynamodb create-table \
  --table-name veloshelf-tfstate-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region ap-south-1

# 3. Run terraform init
cd infra
terraform init \
  -backend-config="bucket=veloshelf-tfstate-<suffix>" \
  -backend-config="key=veloshelf/terraform.tfstate" \
  -backend-config="region=ap-south-1" \
  -backend-config="dynamodb_table=veloshelf-tfstate-lock" \
  -backend-config="encrypt=true"
```

---

## Interview talking points

**Why OIDC instead of stored AWS keys?**
Long-lived credentials stored as GitHub secrets are a security risk —
they don't expire and a secret leak means permanent AWS access. OIDC
issues short-lived tokens per-workflow via AWS STS. Zero credentials
to rotate or accidentally commit.

**Why S3 remote state with DynamoDB locking?**
Local state breaks in a team or CI environment — two concurrent applies
corrupt the state file. S3 + DynamoDB gives atomic locking, versioning
(recover from bad applies), and shared state accessible to any runner.

**Why not EKS in production now?**
Cost and complexity for a demo don't justify it. The architecture is
*designed* for EKS (manifests committed, Terraform module written) —
showing that "I'd scale this to Kubernetes" is defensible, and "I
didn't waste $72/month running EKS for a portfolio piece" shows
engineering judgment.

**Why RDS instead of Postgres in Docker?**
Container storage is ephemeral — a restart loses the serving store.
RDS gives durability, automated backups, and security group isolation.
At t3.micro it's free-tier eligible and costs nothing during the demo.

---

## Cost summary (AWS)

| Resource | Spec | Monthly est. |
|---|---|---|
| EC2 t3.small | Stop when not demoing | ~$0 free tier / ~$15 if always-on |
| RDS t3.micro Postgres | 20GB gp2 | ~$0 free tier (first 12 months) |
| S3 features + MLflow | < 1GB | < $0.03 |
| DynamoDB (TF lock) | Pay per request | < $0.01 |
| **Total** | | **~$0 free tier** |

Stop the EC2 when not demoing: `aws ec2 stop-instances --instance-ids <id>`