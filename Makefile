.DEFAULT_GOAL := help

PYTHON ?= python3
COMPOSE ?= docker compose
AIRFLOW_CONSTRAINTS ?= https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt

.PHONY: help install lint format test elt ci up down logs backfill rerun clear refresh-sample

help: ## Show targets
	@awk 'BEGIN {FS = ":.*## "} /^[a-zA-Z_-]+:.*## / {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## Install Airflow, DuckDB, pytest, and ruff
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt -c "$(AIRFLOW_CONSTRAINTS)"
	$(PYTHON) -m pip install -r requirements-dev.txt

lint: ## Run ruff
	ruff check .
	ruff format --check .

format: ## Format Python with ruff
	ruff format .
	ruff check --fix .

test: ## Run the offline pytest suite
	pytest

elt: ## Run the pipeline on the bundled sample
	$(PYTHON) -m pipeline run --offline

ci: lint test ## Lint and test

up: ## Start local Airflow at http://localhost:8080
	$(COMPOSE) up -d --build

down: ## Stop local Airflow and keep the metadata volume
	$(COMPOSE) down --remove-orphans

logs: ## Follow Airflow logs
	$(COMPOSE) logs -f --tail=100

backfill: ## Backfill logical dates. Usage: make backfill START=2024-01-01 END=2024-01-31
	$(COMPOSE) exec airflow-scheduler airflow dags backfill usgs_earthquakes_elt --start-date "$(START)" --end-date "$(END)" --reset-dagruns -y

rerun: ## Run one logical date in the scheduler container. Usage: make rerun DATE=2024-01-15
	$(COMPOSE) exec airflow-scheduler airflow dags test usgs_earthquakes_elt "$(DATE)"

clear: ## Clear one logical date so the scheduler runs it again. Usage: make clear DATE=2024-01-15
	$(COMPOSE) exec airflow-scheduler airflow tasks clear usgs_earthquakes_elt --start-date "$(DATE)" --end-date "$(DATE)" --yes

refresh-sample: ## Re-download the January 2024 M4.5+ USGS sample
	curl --fail --retry 3 --user-agent "airflow-data-pipeline/1.0 (educational ELT)" \
		-o data/samples/usgs_earthquakes.csv \
		"https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime=2024-01-01&endtime=2024-02-01&minmagnitude=4.5&orderby=time-asc"
