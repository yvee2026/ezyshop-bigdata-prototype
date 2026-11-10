# EzyShop Big Data prototype: every step runs inside Docker. Only `docker` and `make` are needed.
# Works with the GNU make 3.81 that ships with macOS.

SHELL   := /bin/bash
DC      := docker compose
SPARK   := $(DC) exec -T spark
GEN     := $(DC) exec -T generator
PY      := python -m

# Tunables (override on the command line, e.g. `make generate EPS=500 DURATION=120`)
EPS      ?= $(shell grep -E '^EVENTS_PER_SEC=' .env 2>/dev/null | cut -d= -f2)
DURATION ?= 0
CUSTOMER ?=

.DEFAULT_GOAL := help
.PHONY: help env up down clean init seed backfill generate burst stream nightly dashboard \
        erase test test-unit test-integration bench demo logs ps diagram shell

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n",$$1,$$2}'

env: ## Create .env from .env.example if it does not exist
	@test -f .env || (cp .env.example .env && echo "created .env from .env.example")

up: env ## Build images and start all services; waits until every service is healthy
	$(DC) up -d --build --wait
	@$(DC) ps

down: ## Stop all services (keeps data volumes)
	$(DC) --profile init down

clean: ## Stop everything and DELETE all data volumes (Kafka, lake, Mongo, Postgres)
	$(DC) --profile init down -v --remove-orphans

init: env ## Create Kafka topics, the lake bucket, and seed Postgres (idempotent)
	$(DC) run --rm kafka-init
	$(DC) run --rm storage-init
	$(SPARK) $(PY) src.generator.catalogue --seed-db

seed: init ## Alias for init

backfill: ## Write 18 months of historical orders + reviews to bronze Delta
	$(SPARK) $(PY) src.generator.backfill

generate: ## Stream live events to Kafka (EPS=events/sec, DURATION=seconds; 0 = until Ctrl-C)
	$(GEN) $(PY) src.generator.produce --eps $(EPS) --duration $(DURATION)

burst: ## Black Friday burst: 10x the normal event rate (DURATION defaults to 180 s)
	$(GEN) $(PY) src.generator.produce --eps $(EPS) --burst --duration $(if $(filter 0,$(DURATION)),180,$(DURATION))

stream: ## Run the speed layer (Kafka -> bronze Delta + Mongo live metrics). Ctrl-C to stop
	$(SPARK) $(PY) src.streaming.speed_layer

nightly: ## Run the batch layer: bronze->silver->sentiment->forecast->at-risk->optimize
	$(SPARK) $(PY) src.batch.run_nightly

dashboard: ## Print the dashboard URL (the service is already running after `make up`)
	@echo "Dashboard: http://localhost:$$(grep -E '^HOST_PORT_DASHBOARD=' .env | cut -d= -f2)"

erase: ## GDPR erasure of one customer: make erase CUSTOMER=C00042
	@test -n "$(CUSTOMER)" || (echo "usage: make erase CUSTOMER=C00042" && exit 1)
	$(SPARK) $(PY) src.gdpr.erase_customer --customer-id $(CUSTOMER)

test: test-unit ## Run unit tests (fast, no running stream needed)

test-unit:
	$(SPARK) $(PY) pytest -q tests/unit

test-integration: ## End-to-end smoke test (~5 min; needs `make up init`)
	$(SPARK) $(PY) pytest -q -s tests/integration

bench: ## Throughput + latency benchmarks; writes tests/bench/results/*.csv|png
	$(SPARK) $(PY) tests.bench.throughput
	$(SPARK) $(PY) tests.bench.latency

demo: up init backfill ## Full demo from scratch: up, init, backfill, short stream, nightly, dashboard
	$(SPARK) bash scripts/demo.sh
	@$(MAKE) --no-print-directory dashboard

logs: ## Follow logs of all services
	$(DC) logs -f --tail=100

ps: ## Show service status
	$(DC) ps

shell: ## Open a shell in the Spark container
	$(DC) exec spark bash

diagram: ## Re-render docs/architecture.png from docs/architecture.mmd (needs Node/npx on the host)
	npx -y @mermaid-js/mermaid-cli@11.4.2 -i docs/architecture.mmd -o docs/architecture.png -w 1800 -b white
