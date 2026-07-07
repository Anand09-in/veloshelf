.PHONY: setup lint test up down seed logs initdb topics flink-submit flink-logs export-features train

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