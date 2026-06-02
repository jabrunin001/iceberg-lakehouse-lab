.PHONY: help up down clean seed ingest deps build test demo query

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

up: ## start the docker stack (+ Spark thrift server for dbt)
	docker compose up -d
	@echo "Waiting for spark-iceberg, then ensuring the Thrift Server is up (dbt needs :10000)..."
	@until docker exec spark-iceberg bash -lc 'test -x /opt/spark/sbin/start-thriftserver.sh' >/dev/null 2>&1; do sleep 2; done
	@docker exec spark-iceberg bash -lc 'ss -ltn 2>/dev/null | grep -q ":10000" || /opt/spark/sbin/start-thriftserver.sh'
	@echo "Stack up. Trino UI: http://localhost:8085 | MinIO console: http://localhost:9001"

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

deps: ## install dbt packages (run once after make up)
	cd dbt && ../.venv/bin/dbt deps --profiles-dir .

build: deps ## run dbt models (silver + gold) from host venv via thrift :10000
	cd dbt && ../.venv/bin/dbt run --profiles-dir .

test: ## run dbt tests + Great Expectations on gold
	cd dbt && ../.venv/bin/dbt test --profiles-dir .
	. .venv/bin/activate && python -m quality.ge_validate

demo: ## run all Iceberg feature demos
	for f in demos/0*.py; do echo "=== $$f ==="; docker exec spark-iceberg spark-submit /home/iceberg/$$f; done

query: ## open Trino CLI with the demo catalog
	docker exec -it trino trino --catalog demo
