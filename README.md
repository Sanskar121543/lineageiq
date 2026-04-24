# LineageIQ

**Column-level data lineage & schema impact analysis engine.**

LineageIQ automatically infers data lineage from dbt, Spark execution plans, Kafka schema registry, and raw SQL query logs — populating a versioned Neo4j knowledge graph that lets engineers query lineage in natural language and predict the blast radius of any schema change in under two seconds.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Neo4j 5.x](https://img.shields.io/badge/neo4j-5.x-008CC1.svg)](https://neo4j.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688.svg)](https://fastapi.tiangolo.com)
[![React 18](https://img.shields.io/badge/react-18-61DAFB.svg)](https://reactjs.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## The Problem

At any company running a mature data stack — dbt + Spark + Kafka + a BI layer — nobody has a complete picture of what data flows where. Schema changes silently break downstream dashboards, ML feature pipelines, and financial reports. Engineers discover the breakage through angry Slack messages, not proactive tooling.

Manual lineage documentation doesn't scale. It's always stale. It covers tables, not columns. And it says nothing about blast radius.

LineageIQ solves this by parsing lineage **from code and metadata** — not from documentation — and making it queryable in plain English.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         SOURCE LAYER                                │
│  dbt manifest.json  │  Spark LogicalPlan  │  SQL Logs  │  Kafka SR  │
└────────────┬────────┴──────────┬──────────┴─────┬──────┴─────┬──────┘
             │                  │                 │            │
             ▼                  ▼                 ▼            ▼
┌─────────────────────────────────────────────────────────────────────┐
│                        PARSER LAYER                                 │
│   DbtManifestParser  │  SparkPlanParser  │  SqlGlotParser  │  KafkaSchemaParser  │
│                   (async Celery workers, type-partitioned pool)     │
└─────────────────────────────┬───────────────────────────────────────┘
                              │  LineageEvent (normalized)
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     LLM INFERENCE LAYER                             │
│  Undocumented transforms → GPT-4o structured output (Pydantic)     │
│  Catalog cross-validation → flag hallucinated columns              │
└─────────────────────────────┬───────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      GRAPH LAYER  (Neo4j 5)                        │
│  Nodes: Dataset · Column · Transform · Pipeline · Dashboard        │
│         · MLFeature                                                 │
│  Edges: READS_FROM · WRITES_TO · DERIVES_FROM · PART_OF · PRODUCES │
│  Versioned snapshots keyed by (commit_id, timestamp)               │
│  Debezium CDC listener → real-time schema change events            │
└──────────┬──────────────────────────────────────┬───────────────────┘
           │                                      │
           ▼                                      ▼
┌──────────────────────┐              ┌───────────────────────────────┐
│   BLAST RADIUS       │              │     TEXT2CYPHER ENGINE        │
│   ANALYZER           │              │                               │
│  Bidirectional BFS   │              │  NL → Cypher (LLM)            │
│  SLA criticality     │              │  Static schema validation      │
│  <2s on 2000+ nodes  │              │  88% accuracy / 100-Q eval    │
└──────────┬───────────┘              └──────────────┬────────────────┘
           │                                         │
           └──────────────────┬──────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      FastAPI  ·  Redis Cache                        │
└─────────────────────────────┬───────────────────────────────────────┘
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│               React 18 + React Flow + TypeScript                    │
│         Interactive lineage graph  ·  Impact report  ·  NL query   │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Key Capabilities

### 1 · Column-Level Lineage Inference
Four source parsers — dbt manifest, Spark logical plan (via SparkListener), sqlglot SQL (Redshift / BigQuery / Snowflake dialects), Kafka schema registry — each emit a normalized `LineageEvent`. For undocumented PySpark and SQL transforms, an LLM with Pydantic-validated structured output infers column-level lineage, then cross-validates every claimed column against the actual catalog schema. Hallucinated columns are flagged for human review rather than silently written to the graph.

**Result:** 91% column-level lineage coverage on 200 undocumented transforms vs. manually labeled ground truth.

### 2 · Blast Radius Analyzer
Given a proposed schema change (rename column, drop column, type cast change, table rename), a bidirectional BFS traversal finds every downstream and upstream asset. Each affected asset receives a criticality score weighted by SLA tier (P0 / P1 / P2), downstream fan-out count, and freshness cadence (streaming → hourly → daily → weekly). The ranked output report lists affected dashboards, ML features, dbt models, and pipelines with migration actions.

**Result:** <2 seconds on a 2,000+ node graph using depth-limited indexed Cypher BFS.

### 3 · Text2Cypher Natural Language Query
Engineers query the lineage graph in plain English. An LLM converts the question to Cypher using a schema-aware system prompt with 10 few-shot examples. Before execution, a static analyzer validates the generated Cypher against the known node labels and relationship types — preventing runtime errors from hallucinated schema elements.

**Result:** 88% query accuracy on a 100-question eval set validated by manual Cypher review.

### 4 · Real-Time Updates via Debezium CDC
A Debezium PostgreSQL connector listens for DDL events (ALTER TABLE, DROP COLUMN, CREATE TABLE). Each CDC event triggers an impact analysis job and updates the graph atomically — no manual refresh required.

---

## Performance

| Metric | Value | Method |
|--------|-------|--------|
| Column-level lineage coverage | 91% | vs. 200-transform manual ground truth |
| Blast radius query latency | <2s | k6 load test, 2,000+ node graph |
| Text2Cypher accuracy | 88% | 100-query eval, manual Cypher review |
| Cache hit rate (schema freeze) | ~70% | Redis 15-min TTL on impact tuples |
| Assets tracked | 200+ | dbt + Spark + Kafka + SQL |

---

## Stack

| Layer | Technology |
|-------|-----------|
| Graph DB | Neo4j 5 (Community / Enterprise) |
| SQL Parsing | sqlglot (multi-dialect) |
| LLM Orchestration | LangChain + OpenAI GPT-4o |
| Schema Validation | Pydantic v2 |
| API | FastAPI 0.109 |
| Task Queue | Celery 5 + Redis |
| CDC | Debezium + Kafka Connect |
| Frontend | React 18 + React Flow + TypeScript |
| Infra | Docker Compose (dev) / AWS ECS Fargate (prod) |
| IaC | Terraform |

---

## Quickstart

### Prerequisites

- Docker 24+ and Docker Compose v2
- OpenAI API key (for lineage inference + Text2Cypher)
- 8 GB RAM recommended (Neo4j + Kafka stack)

### 1 · Clone & configure

```bash
git clone https://github.com/yourhandle/lineageiq.git
cd lineageiq
cp .env.example .env
# Fill in OPENAI_API_KEY and any warehouse credentials in .env
```

### 2 · Start the full stack

```bash
make up          # docker compose up -d (all services)
make seed        # load sample dbt + SQL lineage fixtures
make logs        # tail all service logs
```

Services that come up:

| Service | URL |
|---------|-----|
| React UI | http://localhost:3000 |
| FastAPI docs | http://localhost:8000/docs |
| Neo4j Browser | http://localhost:7474 |
| Flower (Celery) | http://localhost:5555 |

### 3 · Ingest a dbt project

```bash
# Point at a dbt manifest.json
curl -X POST http://localhost:8000/api/v1/lineage/ingest/dbt \
  -H "Content-Type: application/json" \
  -d '{"manifest_path": "/data/jaffle_shop/manifest.json", "project_name": "jaffle_shop"}'
```

### 4 · Query the graph in natural language

```bash
curl -X POST http://localhost:8000/api/v1/query/nl \
  -H "Content-Type: application/json" \
  -d '{"question": "Which ML features depend on the orders table?"}'
```

### 5 · Run blast radius analysis

```bash
curl -X POST http://localhost:8000/api/v1/impact/blast-radius \
  -H "Content-Type: application/json" \
  -d '{
    "dataset": "orders",
    "column": "customer_id",
    "change_type": "rename",
    "new_name": "customer_key"
  }'
```

---

## Project Layout

```
lineageiq/
├── backend/
│   ├── app/
│   │   ├── main.py              # FastAPI application entry point
│   │   ├── config.py            # Settings via pydantic-settings
│   │   ├── models/lineage.py    # Pydantic domain models
│   │   ├── graph/
│   │   │   ├── client.py        # Neo4j async driver wrapper
│   │   │   ├── schema.py        # Constraints + index bootstrap
│   │   │   └── writer.py        # LineageEvent → graph writes
│   │   ├── parsers/
│   │   │   ├── base.py          # Abstract parser + LineageEvent
│   │   │   ├── dbt_parser.py    # dbt manifest.json parser
│   │   │   ├── spark_parser.py  # Spark LogicalPlan JSON parser
│   │   │   ├── sql_parser.py    # sqlglot multi-dialect parser
│   │   │   └── kafka_parser.py  # Schema Registry reader
│   │   ├── analyzers/
│   │   │   └── blast_radius.py  # BFS traversal + criticality scoring
│   │   ├── llm/
│   │   │   ├── lineage_inference.py  # LLM inference + catalog validation
│   │   │   └── text2cypher.py        # NL → validated Cypher
│   │   ├── workers/
│   │   │   ├── celery_app.py    # Celery + Redis config
│   │   │   └── tasks.py         # Async parse / ingest tasks
│   │   └── api/routes/
│   │       ├── lineage.py       # /api/v1/lineage/*
│   │       ├── impact.py        # /api/v1/impact/*
│   │       └── query.py         # /api/v1/query/*
│   └── tests/
│       ├── unit/                # Parser + analyzer unit tests
│       └── integration/         # Graph write + API integration tests
├── frontend/
│   └── src/
│       ├── components/
│       │   ├── LineageGraph.tsx       # React Flow graph
│       │   ├── BlastRadiusPanel.tsx   # Impact analysis UI
│       │   ├── NLQueryBar.tsx         # Natural language query bar
│       │   └── ImpactReport.tsx       # Ranked impact report
│       └── pages/
│           ├── Dashboard.tsx
│           └── Explorer.tsx
├── docs/
│   ├── architecture.md          # Deep-dive architecture notes
│   ├── graph-schema.md          # Full Neo4j schema reference
│   ├── api-reference.md         # OpenAPI supplement
│   └── deployment.md            # AWS ECS + Terraform guide
├── infra/
│   ├── terraform/               # ECS, RDS, ElastiCache, VPC
│   └── ecs/task-definition.json
├── scripts/
│   ├── seed_fixtures.py         # Load sample lineage data
│   └── eval_text2cypher.py      # Run Text2Cypher eval harness
├── docker-compose.yml
├── docker-compose.dev.yml
├── Makefile
└── .env.example
```

---

## Development

```bash
make dev         # hot-reload FastAPI + React dev server
make test        # pytest (unit + integration)
make lint        # ruff + mypy + eslint
make eval        # run Text2Cypher 100-query evaluation
make neo4j-shell # open cypher-shell in running container
```

### Running parsers in isolation

```python
from app.parsers.dbt_parser import DbtManifestParser

parser = DbtManifestParser()
events = parser.parse({"manifest_path": "path/to/manifest.json", "project_name": "my_project"})
for event in events:
    print(event.model_dump_json(indent=2))
```

### Extending with a new source

1. Subclass `BaseParser` in `backend/app/parsers/base.py`
2. Implement `parse(config) -> list[LineageEvent]`
3. Register the parser type in `workers/tasks.py`
4. Add a Celery queue entry if the new source is CPU-bound

---

## Deployment

See [docs/deployment.md](docs/deployment.md) for the full AWS ECS Fargate setup using the included Terraform modules.

Environment variables required in production:

```
NEO4J_URI=bolt://neo4j:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=<secret>
OPENAI_API_KEY=<secret>
REDIS_URL=redis://redis:6379/0
POSTGRES_DSN=postgresql://user:pass@host:5432/lineageiq
KAFKA_BOOTSTRAP_SERVERS=kafka:9092
SCHEMA_REGISTRY_URL=http://schema-registry:8081
```

---

## Evaluation

The `scripts/eval_text2cypher.py` harness runs 100 natural language queries against a reference Neo4j fixture and compares generated Cypher to ground-truth answers. Run it with:

```bash
python scripts/eval_text2cypher.py --fixture fixtures/eval_queries.json --output results/
```

Current baseline: **88% accuracy** (exact-match on result sets, not Cypher string equality).

---

## License

MIT
