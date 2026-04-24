# Architecture

## Design Principles

**Parse from code, not documentation.** Lineage documentation rots the moment it's written. LineageIQ infers lineage from dbt manifest files, Spark execution plans, SQL query logs, and Kafka schema registrations — all produced automatically by running systems.

**Column-level, not table-level.** Table-level lineage tells you which tables depend on other tables. Column-level lineage tells you exactly which downstream metric breaks if you rename a specific column. The blast radius analyzer operates at column granularity.

**The graph is the source of truth.** Every piece of lineage, every schema change, every impact report flows through Neo4j. The graph is versioned (via `commit_id` snapshots), queryable in natural language, and traversable in both directions.

---

## Component Breakdown

### 1 · Parser Layer

Each parser is an independent async Celery worker subscribing to a named queue:

| Parser | Queue | CPU profile | Input |
|--------|-------|-------------|-------|
| `DbtManifestParser` | `dbt` | I/O-light | `manifest.json` |
| `SparkPlanParser` | `spark` | CPU-heavy | LogicalPlan JSON |
| `SqlGlotParser` | `sql` | CPU-moderate | Raw SQL string |
| `KafkaSchemaParser` | `kafka` | I/O-bound | Schema Registry HTTP |

All parsers extend `BaseParser` and emit `LineageEvent` objects — a normalized data structure that the `GraphWriter` consumes. Adding a new source requires only a new `BaseParser` subclass and a Celery queue entry.

**sqlglot dialect handling.** `SqlGlotParser` uses sqlglot's dialect system to handle SQL differences across Redshift (double-quoted identifiers), BigQuery (backtick identifiers, `STRUCT` types), and Snowflake (case-insensitive identifiers, `FLATTEN`). The graph writer layer is completely dialect-agnostic.

### 2 · LLM Inference Layer

For undocumented SQL and PySpark transforms, `LLMLineageInference` runs a two-phase pipeline:

**Phase 1 — Structured extraction.** The SQL is sent to GPT-4o with a schema-locked system prompt that requires the model to return a Pydantic-validated JSON object. The `InferredLineage` schema specifies `source_datasets`, `target_datasets`, `column_derivations` (each with source/target column and expression), and a confidence score. Pydantic validation catches malformed JSON before it touches the graph.

**Phase 2 — Catalog cross-validation.** Every claimed source column is checked against actual `Column` nodes in Neo4j. If the LLM claims `orders.customer_id` derives from `raw.raw_orders.user_uuid`, but `user_uuid` doesn't exist in the `raw.raw_orders` catalog, that derivation is flagged as `validation_status = "flagged"`. Flagged events are written to the graph but marked for human review — they don't silently poison the lineage.

This is the key correctness property: **hallucinated column references cannot silently enter the graph.**

### 3 · Graph Layer

**Schema.** The graph has 6 node labels and 5 relationship types:

```
(Dataset)   ←—[:READS_FROM]—  (Transform)  —[:WRITES_TO]—→   (Dataset)
(Column)    —[:DERIVES_FROM]→  (Column)
(Column)    —[:PART_OF]——→     (Dataset)
(Transform) —[:PART_OF]——→     (Pipeline)
(Dataset)   —[:PRODUCES]——→    (MLFeature)
```

**Indexes.** Composite indexes on `(project, name)` for Dataset and Column support O(log n) lookups. The `DERIVES_FROM` relationship is indexed bidirectionally so blast radius BFS runs in both directions efficiently.

**Versioned snapshots.** Every `GraphWriter.write_event()` call creates a `Snapshot` node tagged with `commit_id` and `ingested_at`. To reconstruct the lineage graph as it existed at a past commit, query:

```cypher
MATCH (n)
WHERE n.commit_id = $commit_id
RETURN n
```

### 4 · Blast Radius Analyzer

The analyzer runs a depth-limited BFS from the changed entity using APOC's `apoc.path.subgraphNodes`. The `max_depth` parameter (default 8) prevents runaway queries on graphs with circular references.

**Criticality scoring.** Each affected asset is scored by:

```
score = (SLA_weight × 0.5) + (cadence_weight × 0.3) + (log(fan_out+1)/log(501) × 0.2)
```

Where:
- `SLA_weight`: P0=1.0, P1=0.6, P2=0.2
- `cadence_weight`: streaming=1.0, hourly=0.8, daily=0.5, weekly=0.2
- Fan-out is log-scaled to prevent runaway scores on high-fanout nodes

**Redis caching.** The cache key is `sha256(dataset + column + change_type)[:16]`. Cache TTL is 15 minutes. On schema-freeze days when many engineers run impact analyses, ~70% of requests are served from cache.

### 5 · Text2Cypher Engine

The NL query pipeline:

1. **LLM → Cypher.** GPT-4o receives a schema-aware system prompt listing all valid node labels, relationship types, property names, and 10 few-shot examples. The model outputs raw Cypher (no markdown, no explanation).

2. **Static validation.** A regex-based validator checks every node label and relationship type in the generated Cypher against the known schema. Hallucinated labels like `Table` or relationships like `DEPENDS_ON` are caught before Neo4j sees them. Write operations (`CREATE`, `MERGE`, `DELETE`, `SET`) are unconditionally blocked — the NL interface is read-only.

3. **Execution + formatting.** Valid Cypher is executed and results are returned as JSON arrays, rendered as tables in the React frontend.

---

## Data Flow: dbt Manifest Ingestion

```
POST /api/v1/lineage/ingest/dbt
         │
         ▼
DbtManifestParser.parse(manifest.json)
   ├── walk nodes{} → model nodes
   ├── resolve depends_on.nodes → source table names
   ├── parse compiled_sql via SqlGlotParser → ColumnDerivations
   └── fallback to column metadata for passthrough cols
         │
         ▼ list[LineageEvent]
         │
LLMLineageInference (if SqlGlotParser yields no derivations)
   ├── GPT-4o → InferredLineage (Pydantic validated)
   └── catalog cross-validation → flag hallucinated columns
         │
         ▼ LineageEvent (validated)
         │
GraphWriter.write_event()
   ├── MERGE Dataset nodes (source + target)
   ├── MERGE Transform node
   ├── MERGE READS_FROM + WRITES_TO edges
   ├── MERGE Column nodes + PART_OF edges
   ├── MERGE DERIVES_FROM edges (with expression on edge)
   ├── MERGE Pipeline node + PART_OF edge
   └── CREATE Snapshot node (commit_id + timestamp)
         │
         ▼
Neo4j (indexed, versioned)
```

---

## Performance Notes

**Neo4j BFS depth limiting.** Without `maxLevel` on APOC path queries, a deeply nested graph (e.g., 10+ transform hops) can produce O(n^k) traversals. `_MAX_BFS_DEPTH = 8` caps this. In practice, real data stacks rarely exceed 6 hops from raw source to final dashboard.

**Celery worker isolation.** Spark plan parsing is CPU-intensive (AST traversal on large plan JSON). Running it in a separate queue means a burst of Spark job submissions doesn't starve the lightweight dbt and Kafka parsers.

**sqlglot AST reuse.** sqlglot parses SQL into an AST once per call. The CTE resolution pass (`cte_map`) avoids re-parsing CTE bodies when they appear in multiple places.
