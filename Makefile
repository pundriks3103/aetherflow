.PHONY: help up down logs kafka-ui events crypto-trades bronze-stream bronze-logs silver-transform silver-logs crypto-analytics metrics test recovery-test clean

help:
	@echo "AetherFlow Commands:"
	@echo "  make up            - Build and start services in the background"
	@echo "  make down          - Stop services while preserving volumes"
	@echo "  make logs          - Follow logs across all services"
	@echo "  make kafka-ui      - Start Kafka UI at http://localhost:8086"
	@echo "  make events [COUNT=<number>] - Send JSON events to Kafka"
	@echo "  make crypto-trades [DURATION=<seconds> SYMBOL=<symbol> COUNT=<messages>] - Publish live Binance trades"
	@echo "  make bronze-stream - Start the Kafka to Bronze Delta service"
	@echo "  make bronze-logs   - Follow Bronze stream logs"
	@echo "  make silver-transform - Run the Silver transformation once"
	@echo "  make crypto-analytics - Rebuild hourly Binance trade aggregates from Silver"
	@echo "  make silver-logs   - Follow Airflow logs for Silver"
	@echo "  make metrics       - Print a read-only operational metrics snapshot"
	@echo "  make test          - Run the test suite"
	@echo "  make recovery-test - Run isolated Bronze checkpoint recovery test"
	@echo "  make clean         - Remove local Python and Spark temp files"

up:
	docker compose up -d --build
	@echo "Kafka UI: http://localhost:8086"
	@echo "Spark UI: http://localhost:8080"
	@echo "Airflow:  http://localhost:8085"

down:
	docker compose down

logs:
	docker compose logs -f

kafka-ui:
	docker compose up -d kafka-ui

events:
	python3 scripts/produce_events.py $(if $(COUNT),--count $(COUNT))

crypto-trades:
	@duration="$(if $(DURATION),$(DURATION),$$(docker compose exec -T bronze-stream python3 /opt/aetherflow/scripts/aetherflow_config.py make.crypto_duration_seconds))"; docker compose exec -T -e AETHERFLOW_BINANCE_DURATION_SECONDS="$$duration" $(if $(SYMBOL),-e AETHERFLOW_BINANCE_SYMBOL=$(SYMBOL)) $(if $(COUNT),-e AETHERFLOW_BINANCE_MAX_MESSAGES=$(COUNT)) bronze-stream python3 /opt/aetherflow/scripts/produce_crypto_trades.py

bronze-stream:
	docker compose up -d bronze-stream

bronze-logs:
	docker compose logs -f bronze-stream

silver-transform:
	@dag_id="$$(docker compose exec -T airflow-scheduler python3 /opt/aetherflow/scripts/aetherflow_config.py airflow.dag_id)"; docker compose exec -T airflow-scheduler airflow dags test "$$dag_id" 2026-01-01

silver-logs:
	docker compose logs -f airflow-scheduler

crypto-analytics:
	docker compose run --build --rm --no-deps bronze-stream /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/aetherflow/scripts/crypto_market_analytics.py

metrics:
	python3 scripts/operational_metrics.py

test:
	docker compose run --build --rm --no-deps -v "$(CURDIR)/tests:/opt/aetherflow/tests:ro" bronze-stream python3 -m unittest discover -s /opt/aetherflow/tests -p 'test_*.py' -v

recovery-test:
	docker compose run --build --rm --no-deps -v "$(CURDIR)/tests:/opt/aetherflow/tests:ro" bronze-stream python3 /opt/aetherflow/tests/bronze_recovery_harness.py

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf .spark_checkpoints/

