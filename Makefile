.PHONY: help up down clean seed ingest build test demo query

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## start the docker stack
	docker compose up -d

down: ## stop the docker stack
	docker compose down

clean: ## stop and remove volumes + local data
	docker compose down -v
	rm -rf data/raw/* spark-warehouse

seed: ## download MovieLens + generate playback events (host venv)
	. .venv/bin/activate && python -m ingestion.download_movielens
	. .venv/bin/activate && python -m ingestion.generate_playback_events --days 7 --per-day 2000

ingest: ## load raw data into bronze Iceberg tables (in spark container)
	docker exec spark-iceberg spark-submit /home/iceberg/ingestion/ingest_bronze.py

build: ## run dbt models (silver + gold)
	docker exec -w /home/iceberg/dbt spark-iceberg dbt run

test: ## run dbt tests + Great Expectations on gold
	docker exec -w /home/iceberg/dbt spark-iceberg dbt test
	. .venv/bin/activate && python -m quality.ge_validate

demo: ## run all Iceberg feature demos
	for f in demos/0*.py; do echo "=== $$f ==="; docker exec spark-iceberg spark-submit /home/iceberg/$$f; done

query: ## open Trino CLI with the demo catalog
	docker exec -it trino trino --catalog demo
