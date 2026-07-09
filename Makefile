.PHONY: setup lint test up down seed logs initdb topics flink-submit flink-logs export-features train \
        infra-init infra-plan infra-up infra-down ec2-stop ec2-start ec2-ssh

AWS_PROFILE   ?= veloshelf
AWS_REGION    ?= ap-south-1
EC2_INSTANCE  ?= $(shell cd infra && terraform output -raw ec2_public_ip 2>/dev/null)
INSTANCE_ID   ?= $(shell conda run -n veloshelf aws ec2 describe-instances \
                   --filters "Name=ip-address,Values=$(EC2_INSTANCE)" \
                   --query "Reservations[0].Instances[0].InstanceId" \
                   --output text --profile $(AWS_PROFILE) --region $(AWS_REGION) 2>/dev/null)

setup:  ## Install package + dev deps
	pip install -e ".[dev]"

lint:  ## Run ruff
	ruff check .

test:  ## Run pytest
	pytest -q

seed:  ## Validate seed dimension CSVs
	python -m generator.seed_loader

up:  ## Start the local stack
	docker-compose up -d

down:  ## Stop the local stack
	docker-compose down

logs:  ## Tail stack logs
	docker-compose logs -f

initdb:  ## Create Postgres tables (run once after `make up`)
	docker-compose exec -T postgres psql -U veloshelf -d veloshelf \
		-f /dev/stdin < infra/init_db.sql

topics:  ## Create all Kafka topics (run once after `make up`)
	docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic raw-orders      --partitions 3 --replication-factor 1
	docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic raw-inventory   --partitions 3 --replication-factor 1
	docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic dead-letter     --partitions 1 --replication-factor 1
	docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic stockout-alerts --partitions 1 --replication-factor 1
	docker-compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
		--create --if-not-exists --topic surge-alerts    --partitions 1 --replication-factor 1
	@echo "All topics created."

flink-submit:  ## Submit the PyFlink job to the Flink cluster (detached)
	docker-compose exec -T flink-jobmanager \
		bash -c "PYFLINK_PYTHON=python3 flink run -py /opt/veloshelf/streaming/job.py --detached"

flink-logs:  ## List running Flink jobs via the REST API
	docker-compose exec -T flink-jobmanager curl -s http://localhost:8081/jobs | python3 -m json.tool

export-features:  ## Dump windowed_features from Postgres → data/features/features.parquet
	python -m ml.export_features

train:  ## Export features then train the anomaly detector
	python -m ml.export_features
	python -m ml.train_detector

forecast:
	python -m ml.train_forecast

# ---------------------------------------------------------------------------
# AWS infra
# ---------------------------------------------------------------------------

infra-init:  ## terraform init (run once)
	cd infra && conda run -n veloshelf terraform init

infra-plan:  ## terraform plan
	cd infra && conda run -n veloshelf terraform plan -var-file=terraform.tfvars

infra-up:  ## terraform apply — provision / update AWS infra
	cd infra && conda run -n veloshelf terraform apply -var-file=terraform.tfvars -auto-approve

infra-down:  ## terraform destroy — tear down all AWS resources
	@echo "WARNING: This will destroy ALL AWS resources. Press Ctrl+C to cancel."
	@sleep 5
	cd infra && conda run -n veloshelf terraform destroy -var-file=terraform.tfvars -auto-approve

ec2-stop:  ## Stop the EC2 instance to save cost (~\$$0.10/hr saved)
	conda run -n veloshelf aws ec2 stop-instances \
		--instance-ids $(INSTANCE_ID) \
		--profile $(AWS_PROFILE) --region $(AWS_REGION)
	@echo "EC2 stopped. RDS still runs (~\$$0.016/hr)."

ec2-start:  ## Start the EC2 instance
	conda run -n veloshelf aws ec2 start-instances \
		--instance-ids $(INSTANCE_ID) \
		--profile $(AWS_PROFILE) --region $(AWS_REGION)
	@echo "EC2 starting... wait ~60s then check: http://$(EC2_INSTANCE):3000"

ec2-ssh:  ## SSH into the EC2 instance
	ssh -i veloshelf-key.pem -o StrictHostKeyChecking=no ec2-user@$(EC2_INSTANCE)