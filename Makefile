SHELL := /bin/bash
.DEFAULT_GOAL := help
PYTHON ?= python3

ifneq (,$(wildcard .env))
include .env
export
endif

KAFKA_INTERNAL_BOOTSTRAP_SERVERS ?= kafka:9092
KAFKA_TOPICS_BIN ?= /opt/kafka/bin/kafka-topics.sh

.PHONY: help setup-venv install-producer producer test bronze bronze-all silver silver-all up down restart ps logs topics topics-list topics-describe smoke-test

help:
	@printf "Real-Time Crypto Market Intelligence Pipeline\n\n"
	@printf "Local infrastructure targets:\n"
	@printf "  make up               Start Kafka and Kafka UI\n"
	@printf "  make down             Stop local services\n"
	@printf "  make restart          Restart local services\n"
	@printf "  make ps               Show service status\n"
	@printf "  make logs             Follow Kafka and Kafka UI logs\n"
	@printf "  make topics           Create project Kafka topics\n"
	@printf "  make topics-list      List Kafka topics\n"
	@printf "  make topics-describe  Describe Kafka topics\n"
	@printf "  make smoke-test       Verify Kafka is reachable and topics exist\n"
	@printf "\nProducer targets:\n"
	@printf "  make setup-venv       Create/update .venv and install dependencies\n"
	@printf "  make install-producer Install Python producer dependencies in the active interpreter\n"
	@printf "  make producer         Run the Binance WebSocket producer\n"
	@printf "  make test             Run Python tests\n"
	@printf "\nBronze streaming targets:\n"
	@printf "  make bronze-all             Run all Bronze streams in one Spark app\n"
	@printf "\nSilver streaming targets:\n"
	@printf "  make silver-all             Run all Silver streams in one Spark app\n"

setup-venv:
	bash scripts/setup_venv.sh

install-producer:
	$(PYTHON) -m pip install -r requirements.txt

producer:
	$(PYTHON) -m producers.binance_ws_producer

test:
	$(PYTHON) -m pytest

bronze: bronze-all

bronze-all:
	$(PYTHON) -m streaming.bronze.bronze_all

silver: silver-all

silver-all:
	$(PYTHON) -m streaming.silver.silver_all

up:
	docker compose up -d

down:
	docker compose down

restart: down up

ps:
	docker compose ps

logs:
	docker compose logs -f kafka kafka-ui

topics:
	bash kafka/create_topics.sh

topics-list:
	docker compose exec -T kafka $(KAFKA_TOPICS_BIN) --bootstrap-server $(KAFKA_INTERNAL_BOOTSTRAP_SERVERS) --list

topics-describe:
	docker compose exec -T kafka $(KAFKA_TOPICS_BIN) --bootstrap-server $(KAFKA_INTERNAL_BOOTSTRAP_SERVERS) --describe

smoke-test: topics-list
