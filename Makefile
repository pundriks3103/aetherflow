.PHONY: help up down logs kafka-ui events bronze-stream bronze-logs test clean

help:
	@echo "AetherFlow Commands:"
	@echo "  make up            - Build and start services in the background"
	@echo "  make down          - Stop services while preserving volumes"
	@echo "  make logs          - Follow logs across all services"
	@echo "  make kafka-ui      - Start Kafka UI at http://localhost:8086"
	@echo "  make events COUNT=10 - Send JSON events to Kafka"
	@echo "  make bronze-stream - Start the Kafka to Bronze Delta service"
	@echo "  make bronze-logs   - Follow Bronze stream logs"
	@echo "  make test          - Run the test suite"
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
	python3 scripts/produce_events.py --count $${COUNT:-10}

bronze-stream:
	docker compose up -d bronze-stream

bronze-logs:
	docker compose logs -f bronze-stream

test:
	pytest tests/ -v

clean:
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type d -name ".pytest_cache" -exec rm -rf {} +
	rm -rf .spark_checkpoints/

