.PHONY: setup lint test up down seed logs

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