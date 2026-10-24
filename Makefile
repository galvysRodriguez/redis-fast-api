.PHONY: up down logs ps test lint venv submit sample dlq scale

API_URL ?= http://localhost:$(or $(API_PORT),8001)

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f api extractor indexer

ps:
	docker compose ps

venv:
	python3 -m venv .venv && .venv/bin/pip install -q -r requirements/dev.txt

test:
	.venv/bin/python -m pytest -q

lint:
	.venv/bin/ruff check pdfproc tests scripts
	.venv/bin/ruff format --check pdfproc tests scripts

# make submit FILE=reporte.pdf  (por defecto, el PDF de ejemplo)
submit:
	scripts/submit.sh $(or $(FILE),examples/informe_q3.pdf) $(API_URL)

# Regenera examples/informe_q3.pdf
sample:
	.venv/bin/python scripts/make_sample_pdf.py

# Ver mensajes en la dead-letter queue
dlq:
	docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
	  --bootstrap-server kafka:9092 --topic pdf.dlq --from-beginning --timeout-ms 5000

# make scale EXTRACTORS=3 INDEXERS=2
scale:
	docker compose up -d --no-recreate --scale extractor=$(or $(EXTRACTORS),1) --scale indexer=$(or $(INDEXERS),1)
