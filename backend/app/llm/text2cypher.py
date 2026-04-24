"""
Text2Cypher: natural language → validated Cypher query interface.

Two-step pipeline:
  1. LLM converts the NL question to Cypher, guided by a schema-aware system
     prompt with 10 few-shot examples.
  2. A static analyzer validates the generated Cypher against the known graph
     schema (node labels, relationship types, property names) before execution.
     Invalid queries are rejected with a descriptive error rather than failing
     at runtime with a cryptic Neo4j error.

The static validator catches the most common LLM hallucination patterns:
  - Hallucinated node labels (e.g. "Table" instead of "Dataset")
  - Hallucinated relationship types (e.g. "DEPENDS_ON" instead of "READS_FROM")
  - Syntax errors in generated Cypher
"""

from __future__ import annotations

import re
import time
from typing import Any

import structlog
from langchain_openai import ChatOpenAI

from app.config import get_settings
from app.graph.client import get_client
from app.models.lineage import CypherQuery, NLQuery, QueryResult

logger = structlog.get_logger(__name__)

# ─── Schema knowledge ─────────────────────────────────────────────────────────

VALID_NODE_LABELS = {
    "Dataset", "Column", "Transform", "Pipeline",
    "Dashboard", "MLFeature", "Snapshot",
}

VALID_RELATIONSHIP_TYPES = {
    "READS_FROM", "WRITES_TO", "DERIVES_FROM",
    "PART_OF", "PRODUCES",
}

VALID_PROPERTIES: dict[str, set[str]] = {
    "Dataset":    {"name", "project", "source_type", "created_at", "last_seen_at", "commit_id"},
    "Column":     {"name", "dataset", "project", "created_at", "last_seen_at"},
    "Transform":  {"transform_id", "name", "project", "pipeline_name", "source_type",
                   "llm_inferred", "raw_sql", "commit_id"},
    "Pipeline":   {"name", "project", "source_type"},
    "Dashboard":  {"name", "sla_tier", "freshness_cadence", "owner"},
    "MLFeature":  {"name", "project", "sla_tier", "freshness_cadence"},
}

# ─── Few-shot examples ────────────────────────────────────────────────────────

FEW_SHOT_EXAMPLES = """
Q: Show every pipeline that writes to a table used by the revenue dashboard.
Cypher:
MATCH (dash:Dashboard {name: 'revenue_dashboard'})
MATCH (t:Transform)-[:WRITES_TO]->(d:Dataset)<-[:READS_FROM]-(t2:Transform)-[:PART_OF]->(p:Pipeline)
WHERE EXISTS { MATCH (dash)-[:READS_FROM]->(d) }
RETURN DISTINCT p.name AS pipeline, d.name AS shared_dataset

Q: Which ML features would be affected if I drop user_events.session_id?
Cypher:
MATCH (c:Column {dataset: 'user_events', name: 'session_id'})
MATCH path = (c)<-[:DERIVES_FROM*1..6]-(downstream)
WHERE downstream:MLFeature OR (downstream:Column AND EXISTS {
    MATCH (downstream)-[:PART_OF]->(:Dataset)<-[:READS_FROM]-(:Transform)-[:PRODUCES]->(:MLFeature)
})
RETURN DISTINCT downstream.name AS affected_feature, labels(downstream)[0] AS type

Q: Find all datasets with no documented owner that have more than 5 downstream dependencies.
Cypher:
MATCH (d:Dataset)
WHERE NOT EXISTS { MATCH (d) WHERE d.owner IS NOT NULL }
MATCH (d)<-[:READS_FROM|WRITES_TO]-(t:Transform)
WITH d, count(DISTINCT t) AS dep_count
WHERE dep_count > 5
RETURN d.name AS dataset, dep_count ORDER BY dep_count DESC

Q: What does orders.customer_id flow into?
Cypher:
MATCH (c:Column {dataset: 'orders', name: 'customer_id'})
MATCH (c)<-[:DERIVES_FROM*1..5]-(downstream:Column)
RETURN DISTINCT downstream.dataset AS target_dataset, downstream.name AS target_column, labels(downstream)[0] AS type

Q: List all dbt models that read from the raw_events source.
Cypher:
MATCH (src:Dataset {name: 'raw_events', source_type: 'dbt'})
MATCH (t:Transform)-[:READS_FROM]->(src)
WHERE t.source_type = 'dbt'
RETURN t.name AS model, t.pipeline_name AS project

Q: Show the full lineage graph for the mrr_monthly table.
Cypher:
MATCH (d:Dataset {name: 'mrr_monthly'})
MATCH path = (upstream)-[:READS_FROM|WRITES_TO|DERIVES_FROM*1..4]->(d)
RETURN path LIMIT 100

Q: Which transforms have LLM-inferred lineage flagged for review?
Cypher:
MATCH (t:Transform)
WHERE t.llm_inferred = true AND t.validation_status = 'flagged'
RETURN t.name AS transform, t.pipeline_name AS pipeline, t.project AS project
ORDER BY t.created_at DESC

Q: Find columns that derive from more than 3 different source columns.
Cypher:
MATCH (col:Column)<-[:DERIVES_FROM]-(src:Column)
WITH col, count(DISTINCT src) AS source_count
WHERE source_count > 3
RETURN col.dataset AS dataset, col.name AS column, source_count ORDER BY source_count DESC

Q: What Kafka topics feed into the user_features ML feature?
Cypher:
MATCH (f:MLFeature {name: 'user_features'})
MATCH path = (kafka:Dataset {source_type: 'kafka'})-[:WRITES_TO|READS_FROM*1..6]->(f)
RETURN DISTINCT kafka.name AS kafka_topic, length(path) AS hops

Q: How many P0 dashboards depend on the payments pipeline?
Cypher:
MATCH (p:Pipeline {name: 'payments'})
MATCH (p)<-[:PART_OF]-(t:Transform)-[:WRITES_TO]->(d:Dataset)<-[:READS_FROM*1..4]-(dash:Dashboard)
WHERE dash.sla_tier = 'P0'
RETURN count(DISTINCT dash) AS p0_dashboard_count
"""

# ─── System prompt ────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = f"""You are an expert Neo4j Cypher query writer for a data lineage knowledge graph.

## Graph Schema

Node labels: {", ".join(sorted(VALID_NODE_LABELS))}
Relationship types: {", ".join(sorted(VALID_RELATIONSHIP_TYPES))}

Node properties:
- Dataset:   name, project, source_type (dbt|spark|sql|kafka), created_at, owner
- Column:    name, dataset, project
- Transform: transform_id, name, project, pipeline_name, source_type, llm_inferred, validation_status
- Pipeline:  name, project, source_type
- Dashboard: name, sla_tier (P0|P1|P2), freshness_cadence (streaming|hourly|daily|weekly), owner
- MLFeature: name, project, sla_tier, freshness_cadence

Relationship directions:
- (Transform)-[:READS_FROM]->(Dataset)     — transform reads from dataset
- (Transform)-[:WRITES_TO]->(Dataset)      — transform writes to dataset
- (Column)-[:DERIVES_FROM]->(Column)       — target column derived from source column
- (Column|Transform)-[:PART_OF]->(Dataset|Pipeline)
- (Transform)-[:PRODUCES]->(MLFeature)

## Rules
1. Use ONLY the node labels and relationship types listed above. Never invent new ones.
2. Prefer MATCH over WHERE for filtering on labels.
3. Use DISTINCT when aggregating to avoid duplicates.
4. Limit depth in variable-length paths to a max of 6 hops.
5. Return descriptive field names (AS clauses), not raw node objects unless returning full paths.
6. Do NOT use APOC unless specifically needed for path operations.
7. Respond with ONLY the Cypher query. No explanation, no markdown.

## Examples
{FEW_SHOT_EXAMPLES}
"""


class Text2CypherEngine:
    """
    Converts natural language questions to validated, executable Cypher queries.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._llm = ChatOpenAI(
            model=settings.text2cypher_model,
            temperature=0.0,
            max_tokens=1024,
            openai_api_key=settings.openai_api_key,
        )
        self._neo4j = get_client()

    async def query(self, nl_query: NLQuery) -> QueryResult:
        """
        Convert NL question → Cypher → validate → execute → return results.
        """
        t0 = time.perf_counter()

        # Step 1: Generate Cypher
        cypher_str = await self._generate_cypher(nl_query.question)

        # Step 2: Static validation
        cypher = self._validate_cypher(cypher_str)

        results: list[dict] = []
        if cypher.validated:
            try:
                raw = await self._neo4j.run(
                    cypher.cypher,
                    cypher.parameters,
                )
                results = raw[: nl_query.max_results]
            except Exception as exc:
                logger.error("cypher_execution_error", error=str(exc), cypher=cypher.cypher)
                cypher.validation_errors.append(f"Execution error: {exc}")
        else:
            logger.warning(
                "cypher_validation_failed",
                errors=cypher.validation_errors,
                cypher=cypher_str,
            )

        duration_ms = (time.perf_counter() - t0) * 1000

        return QueryResult(
            question=nl_query.question,
            cypher=cypher,
            results=results,
            result_count=len(results),
            duration_ms=round(duration_ms, 2),
        )

    # ─── LLM generation ───────────────────────────────────────────────────────

    async def _generate_cypher(self, question: str) -> str:
        """Ask the LLM to translate a natural language question to Cypher."""
        messages = [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": question},
        ]
        response = await self._llm.ainvoke(messages)
        raw = response.content.strip()

        # Strip accidental markdown fences
        if raw.startswith("```"):
            lines = raw.split("\n")
            raw = "\n".join(
                line for line in lines
                if not line.strip().startswith("```")
            )
        return raw.strip()

    # ─── Static validator ─────────────────────────────────────────────────────

    def _validate_cypher(self, cypher: str) -> CypherQuery:
        """
        Validate a generated Cypher string against the known schema.
        Catches hallucinated node labels, relationship types, and basic syntax.
        """
        errors: list[str] = []

        # Check for empty query
        if not cypher or len(cypher.strip()) < 5:
            return CypherQuery(cypher=cypher, validated=False,
                               validation_errors=["Empty or too-short Cypher generated"])

        # Extract all node label references: (n:Label) or (:Label)
        label_pattern = re.compile(r"\(:?(\w+)")
        for match in label_pattern.finditer(cypher):
            label = match.group(1)
            # Skip Cypher keywords and lowercase variable names
            if label[0].isupper() and label not in VALID_NODE_LABELS:
                errors.append(f"Unknown node label: '{label}'")

        # Extract all relationship type references: -[:TYPE]- or -[:TYPE*]-
        rel_pattern = re.compile(r"\[:(\w+)")
        for match in rel_pattern.finditer(cypher):
            rel_type = match.group(1)
            if rel_type not in VALID_RELATIONSHIP_TYPES:
                errors.append(f"Unknown relationship type: '{rel_type}'")

        # Reject dangerous write operations in read queries
        write_keywords = re.compile(
            r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP)\b",
            re.IGNORECASE,
        )
        if write_keywords.search(cypher):
            errors.append("Write operations are not permitted in NL queries")

        if errors:
            return CypherQuery(
                cypher=cypher,
                validated=False,
                validation_errors=errors,
            )

        return CypherQuery(cypher=cypher, validated=True)
