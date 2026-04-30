SHELL := /bin/bash
.DEFAULT_GOAL := help

ifneq (,$(wildcard .env))
include .env
export
endif

KAFKA_INTERNAL_BOOTSTRAP_SERVERS ?= kafka:9092
KAFKA_TOPICS_BIN ?= /opt/kafka/bin/kafka-topics.sh

.PHONY: help up down restart ps logs topics topics-list topics-describe smoke-test

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
