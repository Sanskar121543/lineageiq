"""
Bootstrap the Neo4j schema: uniqueness constraints and composite indexes.

Run once at startup via `make bootstrap` or automatically on app start.
The schema is idempotent — safe to run multiple times.
"""

from __future__ import annotations

import structlog

from app.graph.client import get_client

logger = structlog.get_logger(__name__)

# ─── Uniqueness Constraints ────────────────────────────────────────────────────
# One constraint per node type on its natural key.

CONSTRAINTS = [
    # Dataset: unique by (project, name)
    """
    CREATE CONSTRAINT dataset_fqn IF NOT EXISTS
    FOR (d:Dataset)
    REQUIRE (d.project, d.name) IS UNIQUE
    """,
    # Column: unique by (project, dataset, name)
    """
    CREATE CONSTRAINT column_fqn IF NOT EXISTS
    FOR (c:Column)
    REQUIRE (c.project, c.dataset, c.name) IS UNIQUE
    """,
    # Transform: unique by transform_id
    """
    CREATE CONSTRAINT transform_id IF NOT EXISTS
    FOR (t:Transform)
    REQUIRE t.transform_id IS UNIQUE
    """,
    # Pipeline: unique by name + project
    """
    CREATE CONSTRAINT pipeline_fqn IF NOT EXISTS
    FOR (p:Pipeline)
    REQUIRE (p.project, p.name) IS UNIQUE
    """,
    # Dashboard: unique by name
    """
    CREATE CONSTRAINT dashboard_name IF NOT EXISTS
    FOR (d:Dashboard)
    REQUIRE d.name IS UNIQUE
    """,
    # MLFeature: unique by name + project
    """
    CREATE CONSTRAINT mlfeature_fqn IF NOT EXISTS
    FOR (m:MLFeature)
    REQUIRE (m.project, m.name) IS UNIQUE
    """,
]

# ─── Composite Indexes ─────────────────────────────────────────────────────────
# Support fast lookups in BFS traversal and blast radius queries.

INDEXES = [
    # Dataset: lookup by source_type for parser-scoped queries
    """
    CREATE INDEX dataset_source_type IF NOT EXISTS
    FOR (d:Dataset) ON (d.source_type)
    """,
    # Column: lookup by dataset for column enumeration
    """
    CREATE INDEX column_dataset IF NOT EXISTS
    FOR (c:Column) ON (c.dataset)
    """,
    # Column: lookup by (dataset, name) for catalog cross-validation
    """
    CREATE INDEX column_dataset_name IF NOT EXISTS
    FOR (c:Column) ON (c.dataset, c.name)
    """,
    # Transform: lookup by project + pipeline
    """
    CREATE INDEX transform_pipeline IF NOT EXISTS
    FOR (t:Transform) ON (t.project, t.pipeline_name)
    """,
    # Snapshot tracking
    """
    CREATE INDEX snapshot_commit IF NOT EXISTS
    FOR (s:Snapshot) ON (s.commit_id, s.project)
    """,
    # SLA tier for criticality scoring queries
    """
    CREATE INDEX asset_sla_tier IF NOT EXISTS
    FOR (d:Dashboard) ON (d.sla_tier)
    """,
    """
    CREATE INDEX mlfeature_sla_tier IF NOT EXISTS
    FOR (m:MLFeature) ON (m.sla_tier)
    """,
]


async def bootstrap_schema() -> None:
    """Create all constraints and indexes. Idempotent."""
    client = get_client()

    for ddl in CONSTRAINTS:
        await client.run(ddl.strip())

    for ddl in INDEXES:
        await client.run(ddl.strip())

    logger.info(
        "neo4j_schema_bootstrapped",
        constraints=len(CONSTRAINTS),
        indexes=len(INDEXES),
    )
