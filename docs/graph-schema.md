# Graph Schema Reference

## Node Labels

### Dataset
Represents a table, view, or stream — any named data artifact.

| Property | Type | Notes |
|----------|------|-------|
| `name` | string | Table/view name, e.g. `public.orders` |
| `project` | string | Owning project (dbt project name, Spark app name, etc.) |
| `source_type` | enum | `dbt` \| `spark` \| `sql` \| `kafka` \| `manual` |
| `owner` | string | Team or person responsible |
| `sla_tier` | enum | `P0` \| `P1` \| `P2` |
| `freshness_cadence` | enum | `streaming` \| `hourly` \| `daily` \| `weekly` |
| `created_at` | string | ISO timestamp of first ingestion |
| `last_seen_at` | string | ISO timestamp of most recent ingestion |
| `commit_id` | string | Git SHA or version tag of the ingestion that created/updated this node |

**Uniqueness constraint:** `(project, name)`

### Column
Represents a single column within a Dataset.

| Property | Type | Notes |
|----------|------|-------|
| `name` | string | Column name |
| `dataset` | string | Parent dataset name |
| `project` | string | Owning project |
| `data_type` | string | SQL data type (optional) |
| `created_at` | string | ISO timestamp |

**Uniqueness constraint:** `(project, dataset, name)`

### Transform
Represents a data transformation — a dbt model, Spark job, SQL script, or any compute step.

| Property | Type | Notes |
|----------|------|-------|
| `transform_id` | string | Stable unique ID (dbt node FQN, Spark job ID, SQL hash) |
| `name` | string | Human-readable name |
| `project` | string | Owning project |
| `pipeline_name` | string | Parent pipeline (optional) |
| `source_type` | enum | Parser that produced this node |
| `llm_inferred` | boolean | True if lineage was inferred by LLM rather than parsed |
| `validation_status` | enum | `ok` \| `flagged` \| `pending_review` |
| `raw_sql` | string | Original SQL or plan string (optional) |
| `commit_id` | string | Version tag |

**Uniqueness constraint:** `transform_id`

### Pipeline
Groups related Transforms into a logical data pipeline.

| Property | Type | Notes |
|----------|------|-------|
| `name` | string | Pipeline name (dbt project, Spark app, Airflow DAG) |
| `project` | string | Owning project |
| `source_type` | enum | Source system |

**Uniqueness constraint:** `(project, name)`

### Dashboard
Represents a BI dashboard or report that consumes data.

| Property | Type | Notes |
|----------|------|-------|
| `name` | string | Dashboard name |
| `sla_tier` | enum | `P0` \| `P1` \| `P2` |
| `owner` | string | Team or person |
| `freshness_cadence` | enum | How often the dashboard refreshes |

**Uniqueness constraint:** `name`

### MLFeature
Represents a machine learning feature produced from data.

| Property | Type | Notes |
|----------|------|-------|
| `name` | string | Feature name |
| `project` | string | ML project |
| `sla_tier` | enum | Criticality tier |
| `freshness_cadence` | enum | Refresh cadence |

**Uniqueness constraint:** `(project, name)`

---

## Relationship Types

### READS_FROM
`(Transform)-[:READS_FROM]->(Dataset)`

A transform reads from this dataset as a source. Properties:
- `commit_id` — version tag of the write that created this edge

### WRITES_TO
`(Transform)-[:WRITES_TO]->(Dataset)`

A transform produces / writes to this dataset. Properties:
- `commit_id` — version tag

### DERIVES_FROM
`(Column)-[:DERIVES_FROM]->(Column)`

Target column is derived from source column. This is the key relationship for column-level lineage.

Properties:
- `expression` — the SQL / PySpark expression producing the target column (nullable for passthrough)
- `is_passthrough` — true when the column is selected without transformation
- `transform_id` — the Transform that defined this derivation
- `commit_id` — version tag
- `created_at`, `last_seen_at` — timestamps

### PART_OF
`(Column)-[:PART_OF]->(Dataset)` — column belongs to a dataset
`(Transform)-[:PART_OF]->(Pipeline)` — transform is part of a pipeline

### PRODUCES
`(Dataset)-[:PRODUCES]->(MLFeature)` — dataset feeds an ML feature pipeline

---

## Cypher Patterns

### Find all downstream datasets from a source
```cypher
MATCH (start:Dataset {name: 'public.orders'})
MATCH path = (start)<-[:WRITES_TO|READS_FROM*1..6]-(t:Transform)-[:WRITES_TO]->(downstream:Dataset)
RETURN DISTINCT downstream.name AS downstream
```

### Column-level blast radius
```cypher
MATCH (c:Column {dataset: 'orders', name: 'customer_id'})
MATCH (c)<-[:DERIVES_FROM*1..6]-(affected:Column)
RETURN DISTINCT affected.dataset, affected.name
```

### Full lineage path from Kafka to dashboard
```cypher
MATCH path = (kafka:Dataset {source_type: 'kafka'})
              -[:WRITES_TO|READS_FROM*1..8]-
              (dash:Dashboard)
RETURN path
LIMIT 10
```

### LLM-flagged transforms for review
```cypher
MATCH (t:Transform)
WHERE t.llm_inferred = true AND t.validation_status = 'flagged'
RETURN t.name, t.project, t.pipeline_name, t.created_at
ORDER BY t.created_at DESC
```

### Lineage snapshot at a specific commit
```cypher
MATCH (n)
WHERE n.commit_id = 'abc123def456'
RETURN labels(n)[0] AS type, n.name AS name, n.commit_id
```

---

## Index Strategy

| Index name | Node | Properties | Purpose |
|-----------|------|-----------|---------|
| `dataset_fqn` | Dataset | `(project, name)` | Uniqueness + fast MERGE |
| `column_fqn` | Column | `(project, dataset, name)` | Uniqueness + fast MERGE |
| `column_dataset_name` | Column | `(dataset, name)` | Catalog cross-validation lookups |
| `column_dataset` | Column | `(dataset)` | Column enumeration per dataset |
| `transform_id` | Transform | `transform_id` | Uniqueness + fast MERGE |
| `transform_pipeline` | Transform | `(project, pipeline_name)` | Pipeline-scoped queries |
| `dataset_source_type` | Dataset | `source_type` | Filter by source (e.g. all Kafka topics) |
| `asset_sla_tier` | Dashboard | `sla_tier` | P0 dashboard queries |
| `mlfeature_sla_tier` | MLFeature | `sla_tier` | P0 feature queries |
| `snapshot_commit` | Snapshot | `(commit_id, project)` | Point-in-time reconstruction |
