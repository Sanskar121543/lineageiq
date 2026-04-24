.PHONY: up down dev test lint eval seed logs neo4j-shell fmt

# ─── Stack Management ──────────────────────────────────────────────────────────

up:
	docker compose up -d
	@echo "Waiting for Neo4j..."
	@sleep 8
	$(MAKE) bootstrap

down:
	docker compose down -v

restart:
	docker compose restart backend worker

logs:
	docker compose logs -f backend worker

# ─── Development ───────────────────────────────────────────────────────────────

dev:
	docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d
	@echo "\n✓ Frontend:    http://localhost:3000"
	@echo "✓ API docs:    http://localhost:8000/docs"
	@echo "✓ Neo4j:       http://localhost:7474"
	@echo "✓ Flower:      http://localhost:5555\n"

bootstrap:
	docker compose exec backend python -c "from app.graph.schema import bootstrap_schema; import asyncio; asyncio.run(bootstrap_schema())"
	@echo "✓ Graph schema bootstrapped"

seed:
	docker compose exec backend python /app/scripts/seed_fixtures.py
	@echo "✓ Sample lineage fixtures loaded"

# ─── Testing ───────────────────────────────────────────────────────────────────

test:
	docker compose exec backend pytest tests/ -v --tb=short

test-unit:
	docker compose exec backend pytest tests/unit/ -v

test-integration:
	docker compose exec backend pytest tests/integration/ -v

# ─── Quality ───────────────────────────────────────────────────────────────────

lint:
	docker compose exec backend ruff check app/ tests/
	docker compose exec backend mypy app/ --ignore-missing-imports
	cd frontend && npm run lint

fmt:
	docker compose exec backend ruff format app/ tests/
	cd frontend && npm run format

# ─── Evaluation ────────────────────────────────────────────────────────────────

eval:
	docker compose exec backend python scripts/eval_text2cypher.py \
		--fixture fixtures/eval_queries.json \
		--output results/eval_$(shell date +%Y%m%d_%H%M%S).json

# ─── Utilities ─────────────────────────────────────────────────────────────────

neo4j-shell:
	docker compose exec neo4j cypher-shell -u neo4j -p lineageiq_dev

neo4j-reset:
	docker compose exec neo4j cypher-shell -u neo4j -p lineageiq_dev "MATCH (n) DETACH DELETE n"
	$(MAKE) bootstrap

redis-cli:
	docker compose exec redis redis-cli

build:
	docker compose build --no-cache

# ─── Debezium Connector Registration ──────────────────────────────────────────

register-debezium:
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @infra/debezium/postgres-connector.json
	@echo "✓ Debezium PostgreSQL connector registered"
