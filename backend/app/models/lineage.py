"""
Core Pydantic domain models for LineageIQ.

These are the canonical data structures that flow between parsers,
the graph writer, the blast radius analyzer, and the API layer.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import uuid4

from pydantic import BaseModel, Field, field_validator


# ─── Enums ────────────────────────────────────────────────────────────────────


class NodeType(str, Enum):
    DATASET = "Dataset"
    COLUMN = "Column"
    TRANSFORM = "Transform"
    PIPELINE = "Pipeline"
    DASHBOARD = "Dashboard"
    ML_FEATURE = "MLFeature"


class RelationshipType(str, Enum):
    READS_FROM = "READS_FROM"
    WRITES_TO = "WRITES_TO"
    DERIVES_FROM = "DERIVES_FROM"
    PART_OF = "PART_OF"
    PRODUCES = "PRODUCES"


class SourceType(str, Enum):
    DBT = "dbt"
    SPARK = "spark"
    SQL = "sql"
    KAFKA = "kafka"
    MANUAL = "manual"


class ChangeType(str, Enum):
    RENAME_COLUMN = "rename_column"
    DROP_COLUMN = "drop_column"
    TYPE_CHANGE = "type_change"
    RENAME_TABLE = "rename_table"
    DROP_TABLE = "drop_table"
    ADD_COLUMN = "add_column"


class SLATier(str, Enum):
    P0 = "P0"  # Customer-facing / revenue-critical
    P1 = "P1"  # Internal business-critical
    P2 = "P2"  # Internal / experimental


class DataWarehouse(str, Enum):
    REDSHIFT = "redshift"
    BIGQUERY = "bigquery"
    SNOWFLAKE = "snowflake"
    POSTGRES = "postgres"
    GENERIC = "generic"


# ─── Column-Level Lineage ─────────────────────────────────────────────────────


class ColumnReference(BaseModel):
    """A reference to a specific column in a dataset."""

    dataset: str
    column: str
    warehouse: DataWarehouse = DataWarehouse.GENERIC

    @field_validator("dataset", "column")
    @classmethod
    def strip_quotes(cls, v: str) -> str:
        return v.strip().strip('"').strip("`")


class ColumnDerivation(BaseModel):
    """
    Maps one source column to one output column, with the transformation
    expression that produced it. This is the atomic unit of column-level lineage.
    """

    source: ColumnReference
    target: ColumnReference
    expression: str | None = None  # e.g. "COALESCE(a.customer_id, b.id)"
    is_passthrough: bool = False  # true when expression is just a rename/select


# ─── Lineage Event ────────────────────────────────────────────────────────────


class LineageEvent(BaseModel):
    """
    Normalized output of every parser. The graph writer consumes these
    and translates them into Neo4j nodes and relationships.
    """

    event_id: str = Field(default_factory=lambda: str(uuid4()))
    source_type: SourceType
    project_name: str
    commit_id: str | None = None
    ingested_at: datetime = Field(default_factory=datetime.utcnow)

    # Transform metadata
    transform_id: str  # logical identifier (e.g. dbt node fqn, spark job id)
    transform_name: str
    pipeline_name: str | None = None

    # Source → target datasets (table level)
    source_datasets: list[str] = Field(default_factory=list)
    target_datasets: list[str] = Field(default_factory=list)

    # Column-level derivations
    column_derivations: list[ColumnDerivation] = Field(default_factory=list)

    # Raw SQL / plan for LLM inference fallback
    raw_sql: str | None = None

    # Metadata
    tags: dict[str, Any] = Field(default_factory=dict)
    llm_inferred: bool = False
    validation_status: str = "ok"  # ok | flagged | pending_review


# ─── Impact Analysis ──────────────────────────────────────────────────────────


class SchemaChange(BaseModel):
    """Input to the blast radius analyzer."""

    dataset: str
    column: str | None = None
    change_type: ChangeType
    new_name: str | None = None  # for rename operations
    new_type: str | None = None  # for type_change operations


class AffectedAsset(BaseModel):
    """A single asset in the blast radius report."""

    node_id: str
    node_type: NodeType
    name: str
    sla_tier: SLATier = SLATier.P2
    downstream_fan_out: int = 0
    freshness_cadence: str = "daily"  # streaming | hourly | daily | weekly
    criticality_score: float = 0.0
    path_from_source: list[str] = Field(default_factory=list)
    suggested_action: str | None = None


class BlastRadiusReport(BaseModel):
    """Full output of a blast radius analysis run."""

    change: SchemaChange
    analysis_duration_ms: float
    total_affected: int
    affected_assets: list[AffectedAsset]
    upstream_provenance: list[str] = Field(default_factory=list)
    summary: str = ""

    @property
    def p0_count(self) -> int:
        return sum(1 for a in self.affected_assets if a.sla_tier == SLATier.P0)

    @property
    def p1_count(self) -> int:
        return sum(1 for a in self.affected_assets if a.sla_tier == SLATier.P1)


# ─── Text2Cypher ──────────────────────────────────────────────────────────────


class NLQuery(BaseModel):
    question: str
    max_results: int = 100


class CypherQuery(BaseModel):
    cypher: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    validated: bool = False
    validation_errors: list[str] = Field(default_factory=list)


class QueryResult(BaseModel):
    question: str
    cypher: CypherQuery
    results: list[dict[str, Any]]
    result_count: int
    duration_ms: float


# ─── Versioned Snapshot ────────────────────────────────────────────────────────


class LineageSnapshot(BaseModel):
    """Metadata for a versioned graph snapshot."""

    snapshot_id: str = Field(default_factory=lambda: str(uuid4()))
    commit_id: str
    project_name: str
    created_at: datetime = Field(default_factory=datetime.utcnow)
    node_count: int = 0
    relationship_count: int = 0
    source_type: SourceType
