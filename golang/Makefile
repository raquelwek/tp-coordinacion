SHELL := /bin/bash
PWD := $(shell pwd)

up:
	mkdir -p output
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs

test:
	mkdir -p output
	rm -f ./output/*
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	go run ./verify_output.go; EXIT_CODE=$$?; \
	docker compose -f docker-compose.yaml stop -t 5; \
	docker compose -f docker-compose.yaml down; \
	exit $$EXIT_CODE
.PHONY: test

switch:
	@echo Escenarios de prueba:
	@echo "1) Un cliente, una sola réplica de cada elemento"
	@echo "2) Múltiples clientes, una sola réplica de cada elemento"
	@echo "3) Múltiples clientes, sum replicado, un solo aggregation" 
	@echo "4) Múltiples clientes, múltiples réplicas"
	@echo "5) Múltiples clientes, múltiples réplicas, nombres al azar"
	@read -p "Selecciona uno [1-5]: " option;	\
	cp ./scenarios/$${option}.yaml docker-compose.yaml
.PHONY: switch
