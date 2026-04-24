# API Reference

Interactive docs available at `http://localhost:8000/docs` (Swagger UI) and `http://localhost:8000/redoc`.

---

## Base URL

```
http://localhost:8000/api/v1
```

---

## Lineage Ingestion

### `POST /lineage/ingest/dbt`

Ingest a dbt project by parsing its `manifest.json`. Returns a task ID to poll.

**Request**
```json
{
  "manifest_path": "/data/jaffle_shop/manifest.json",
  "project_name": "jaffle_shop",
  "commit_id": "abc123"
}
```

**Response `202`**
```json
{ "task_id": "3f2e1a...", "status": "queued" }
```

---

### `POST /lineage/ingest/spark`

Ingest a Spark job's LogicalPlan JSON (captured via SparkListener).

**Request**
```json
{
  "plan": { "class": "InsertIntoHadoopFsRelationCommand", ... },
  "project_name": "analytics",
  "pipeline_name": "session_etl",
  "job_id": "application_1234_0001"
}
```

---

### `POST /lineage/ingest/sql`

Parse a raw SQL string. Falls back to LLM inference if sqlglot yields no column derivations.

**Request**
```json
{
  "sql": "SELECT o.id, o.customer_id, c.name FROM orders o JOIN customers c ON o.customer_id = c.id",
  "project_name": "reporting",
  "target_dataset": "order_summary",
  "source_datasets": ["orders", "customers"],
  "dialect": "redshift",
  "use_llm_fallback": true
}
```

---

### `POST /lineage/ingest/kafka`

Read all schemas from a Confluent Schema Registry instance.

**Request**
```json
{
  "schema_registry_url": "http://schema-registry:8081",
  "project_name": "platform",
  "topic_filter": "^user-.*"
}
```

---

### `GET /lineage/tasks/{task_id}`

Poll a parse task for completion.

**Response**
```json
{
  "task_id": "3f2e1a...",
  "status": "SUCCESS",
  "result": { "events_parsed": 12, "project_name": "jaffle_shop" }
}
```

`status` values: `PENDING` | `STARTED` | `SUCCESS` | `FAILURE` | `RETRY`

---

### `GET /lineage/graph`

Return the full lineage graph as nodes + edges for React Flow rendering.

**Query params:** `project` (optional), `limit` (default 500)

**Response**
```json
{
  "nodes": [
    { "id": "4:abc:1", "type": "Dataset", "data": { "name": "orders", "project": "jaffle_shop" } }
  ],
  "edges": [
    { "source": "4:abc:2", "target": "4:abc:1", "type": "READS_FROM", "data": {} }
  ]
}
```

---

### `GET /lineage/datasets`

List all tracked datasets.

**Query params:** `project`, `source_type`

---

### `GET /lineage/datasets/{dataset_name}/columns`

List all columns for a dataset with downstream derivation counts.

---

## Impact Analysis

### `POST /impact/blast-radius`

Analyze downstream impact of a proposed schema change. Results cached for 15 min.

**Request**
```json
{
  "dataset": "orders",
  "column": "customer_id",
  "change_type": "rename_column",
  "new_name": "customer_key"
}
```

`change_type` values: `rename_column` | `drop_column` | `type_change` | `rename_table` | `drop_table` | `add_column`

**Response**
```json
{
  "change": { "dataset": "orders", "column": "customer_id", "change_type": "rename_column" },
  "analysis_duration_ms": 187.4,
  "total_affected": 6,
  "affected_assets": [
    {
      "node_id": "4:abc:99",
      "node_type": "Dashboard",
      "name": "revenue_dashboard",
      "sla_tier": "P0",
      "downstream_fan_out": 12,
      "freshness_cadence": "hourly",
      "criticality_score": 0.8821,
      "suggested_action": "Update column reference in BI layer"
    }
  ],
  "upstream_provenance": ["raw.raw_orders", "orders", "revenue_fact"],
  "summary": "Changing orders.customer_id (rename_column) affects 6 downstream assets. 1 P0 asset. 1 dashboard."
}
```

---

### `GET /impact/p0-dashboards`

List all P0 dashboards with their immediate upstream datasets.

---

### `GET /impact/orphaned-datasets`

Find datasets with no owner and more than N downstream dependencies.

**Query params:** `min_downstream` (default 5)

---

## Natural Language Query

### `POST /query/nl`

Convert a natural language question to Cypher and execute it.

**Request**
```json
{
  "question": "Which ML features would break if I drop user_events.session_id?",
  "max_results": 100
}
```

**Response**
```json
{
  "question": "Which ML features would break if I drop user_events.session_id?",
  "cypher": {
    "cypher": "MATCH (c:Column {dataset: 'user_events', name: 'session_id'}) ...",
    "validated": true,
    "validation_errors": []
  },
  "results": [
    { "feature_name": "session_duration_feature", "sla_tier": "P1" }
  ],
  "result_count": 1,
  "duration_ms": 312.8
}
```

If Cypher validation fails, `validated` is `false`, `validation_errors` lists the issues, and `results` is empty.

---

### `POST /query/cypher`

Execute a raw read-only Cypher query directly. Write operations are blocked.

**Request**
```json
{
  "cypher": "MATCH (d:Dataset) RETURN d.name LIMIT 10",
  "parameters": {}
}
```

---

### `GET /query/suggestions`

Return pre-built example queries for the UI query bar.

---

## System

### `GET /healthz`
Liveness check. Returns `{"status": "ok"}`.

### `GET /readyz`
Readiness check. Verifies Neo4j connectivity. Returns `503` if Neo4j is unreachable.

### `GET /api/v1/stats`
Graph node and relationship counts by type.

### `GET /metrics`
Prometheus metrics endpoint (request counts, latencies, error rates).
