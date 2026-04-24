![CI](https://github.com/Sanskar121543/lineageiq/actions/workflows/ci.yml/badge.svg)
# LineageIQ

**Column-level data lineage & schema impact analysis engine.**

LineageIQ automatically builds a Neo4j knowledge graph from data sources and transformation metadata, then lets engineers explore lineage visually, query it in natural language, and estimate the blast radius of schema changes.

[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)
[![Neo4j 5.x](https://img.shields.io/badge/neo4j-5.x-008CC1.svg)](https://neo4j.com/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.109-009688.svg)](https://fastapi.tiangolo.com)
[![React 18](https://img.shields.io/badge/react-18-61DAFB.svg)](https://reactjs.org/)
[![Docker](https://img.shields.io/badge/docker-compose-2496ED.svg)](https://docs.docker.com/compose/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

---

## Product Preview

### Overview Dashboard

![Dashboard](assets/dashboard.png)

### Interactive Lineage Graph

![Graph](assets/graph.png)

### Blast Radius Analyzer

![Impact](assets/impact.png)

---

## Why It Matters

Modern data stacks contain dbt models, SQL transforms, Kafka topics, dashboards, ML features, and multiple storage systems. When schemas change, downstream failures are often discovered too late.

LineageIQ solves this by:

* Building a graph of datasets, columns, transforms, dashboards, and ML features
* Tracing downstream and upstream dependencies
* Estimating the impact of schema changes
* Exposing the graph through a React UI and FastAPI backend

---

## Features

### Column-Level Lineage

Track how individual columns move through the graph, not just tables.

### Blast Radius Analysis

Given a rename, drop, or type change, LineageIQ finds affected downstream assets and ranks them by criticality.

### Natural Language Query

Ask questions about the graph in plain English.

### Real-Time Architecture

Designed around Neo4j, Redis, Kafka, PostgreSQL, Celery, and FastAPI.

### Interactive UI

Explore lineage visually with React Flow.

---

## Architecture

```text
Sources
  ├── dbt manifests
  ├── Spark execution plans
  ├── Kafka schema registry
  └── SQL query logs
        ↓
Parsers / Ingestion
        ↓
Neo4j Graph Layer
        ↓
FastAPI + Redis + Workers
        ↓
React + React Flow UI
```

---

## Benchmarks

Measured locally with Grafana k6 in Docker.

### Stress Test (Blast Radius Endpoint)

* 50 concurrent users
* 15 seconds
* 20,881 requests
* 1,389 req/s
* 100% success rate
* 35.79 ms average latency
* 44.65 ms p95 latency

### Mixed API Test

* 20 concurrent users
* 20 seconds
* 3,810 requests
* 188.90 req/s
* 100% success rate
* 105.34 ms average latency
* 271.06 ms p95 latency

**Validated under concurrent load with zero failed requests.**

---

## Tech Stack

* **Backend:** FastAPI, Python, Pydantic
* **Graph DB:** Neo4j
* **Caching:** Redis
* **Workers:** Celery
* **Streaming:** Kafka
* **Database:** PostgreSQL
* **Frontend:** React, TypeScript, React Flow
* **Containerization:** Docker, Docker Compose
* **Parsing:** sqlglot
* **Monitoring:** Flower, Prometheus instrumentation

---

## Quickstart

### Prerequisites

* Docker Desktop
* Docker Compose v2
* OpenAI API key
* 8 GB RAM recommended

### Clone Repository

```bash
git clone https://github.com/Sanskar121543/lineageiq.git
cd lineageiq
```

### Configure Environment

```bash
cp .env.example .env
```

Windows PowerShell:

```powershell
Copy-Item .env.example .env
```

Update `.env` with your values.

### Start Full Stack

```bash
docker compose up -d --build
```

### Seed Sample Data

```bash
docker compose exec backend python scripts/seed_fixtures.py
```

### Access Services

* React UI: http://localhost:3000
* FastAPI Docs: http://localhost:8000/docs
* Neo4j Browser: http://localhost:7474
* Flower: http://localhost:5555

---

## Useful Commands

```bash
docker compose up -d --build
docker compose down
docker compose ps
docker compose logs -f backend
docker compose logs -f frontend
docker compose exec backend python scripts/seed_fixtures.py
docker compose exec neo4j cypher-shell -u neo4j -p lineageiq_dev
```

---

## API Endpoints

### Stats

`GET /api/v1/stats`

### Lineage

* `GET /api/v1/lineage/graph`
* `GET /api/v1/lineage/datasets`
* `POST /api/v1/lineage/ingest/dbt`

### Impact

`POST /api/v1/impact/blast-radius`

### Query

* `POST /api/v1/query/nl`
* `GET /api/v1/query/suggestions`

---

## Project Structure

```text
lineageiq/
├── backend/
├── frontend/
├── docs/
├── infra/
├── scripts/
├── docker-compose.yml
├── docker-compose.dev.yml
├── Makefile
└── .env.example
```

---

## Development

```bash
docker compose up -d --build
docker compose exec backend pytest
docker compose -f docker-compose.dev.yml up --build
```

---

## Deployment

Production-oriented infrastructure files are included under `infra/`.

---

## License

MIT
