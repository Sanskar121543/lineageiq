"""
Blast Radius Analyzer.

Given a proposed schema change (rename/drop column, type change, rename table),
performs a bidirectional BFS traversal in Neo4j to find all affected assets,
then scores each asset by a criticality formula weighted by:
  - SLA tier (P0 = 1.0, P1 = 0.6, P2 = 0.2)
  - Downstream fan-out (number of dependents)
  - Freshness cadence (streaming > hourly > daily > weekly)

The result is a ranked BlastRadiusReport with suggested migration actions.

Design: uses depth-limited Cypher BFS with APOC shortestPath for path tracing.
All (dataset, column, change_type) tuples are cached in Redis for 15 minutes.
"""

from __future__ import annotations

import hashlib
import json
import time
from typing import Any

import redis.asyncio as aioredis
import structlog

from app.config import get_settings
from app.graph.client import get_client
from app.models.lineage import (
    AffectedAsset,
    BlastRadiusReport,
    ChangeType,
    NodeType,
    SchemaChange,
    SLATier,
)

logger = structlog.get_logger(__name__)

# Criticality weights
_SLA_WEIGHTS: dict[str, float] = {"P0": 1.0, "P1": 0.6, "P2": 0.2}
_CADENCE_WEIGHTS: dict[str, float] = {
    "streaming": 1.0,
    "hourly": 0.8,
    "daily": 0.5,
    "weekly": 0.2,
}
_MAX_BFS_DEPTH = 8  # prevents runaway queries on deeply nested graphs


class BlastRadiusAnalyzer:
    """
    Analyzes the downstream and upstream impact of a proposed schema change.
    """

    def __init__(self) -> None:
        self._neo4j = get_client()
        self._settings = get_settings()
        self._redis: aioredis.Redis | None = None

    async def _get_redis(self) -> aioredis.Redis:
        if self._redis is None:
            self._redis = await aioredis.from_url(self._settings.redis_url)
        return self._redis

    async def analyze(self, change: SchemaChange) -> BlastRadiusReport:
        """
        Entry point. Returns a full BlastRadiusReport for the proposed change.
        Checks Redis cache before running the Neo4j traversal.
        """
        cache_key = self._cache_key(change)
        redis = await self._get_redis()

        # Cache hit
        cached = await redis.get(cache_key)
        if cached:
            logger.info("blast_radius_cache_hit", cache_key=cache_key)
            return BlastRadiusReport.model_validate_json(cached)

        t0 = time.perf_counter()

        # Run downstream BFS
        downstream_assets = await self._downstream_traversal(change)

        # Run upstream provenance trace
        upstream_chain = await self._upstream_provenance(change)

        # Score each affected asset
        scored = [self._score_asset(a) for a in downstream_assets]
        scored.sort(key=lambda a: a.criticality_score, reverse=True)

        duration_ms = (time.perf_counter() - t0) * 1000

        report = BlastRadiusReport(
            change=change,
            analysis_duration_ms=round(duration_ms, 2),
            total_affected=len(scored),
            affected_assets=scored,
            upstream_provenance=upstream_chain,
            summary=self._generate_summary(change, scored),
        )

        # Cache result
        await redis.set(
            cache_key,
            report.model_dump_json(),
            ex=self._settings.blast_radius_cache_ttl,
        )

        logger.info(
            "blast_radius_analyzed",
            dataset=change.dataset,
            column=change.column,
            change_type=change.change_type.value,
            affected_count=len(scored),
            duration_ms=duration_ms,
        )

        return report

    # ─── Neo4j Traversals ─────────────────────────────────────────────────────

    async def _downstream_traversal(self, change: SchemaChange) -> list[AffectedAsset]:
        """
        BFS from the changed column/dataset, following DERIVES_FROM and
        WRITES_TO/READS_FROM relationships downstream.
        """
        if change.column:
            # Column-level: start from the specific Column node
            query = """
            MATCH (start:Column {dataset: $dataset, name: $column})
            CALL apoc.path.subgraphNodes(start, {
                relationshipFilter: "DERIVES_FROM>|<PART_OF|WRITES_TO>|READS_FROM>|PRODUCES>",
                minLevel: 1,
                maxLevel: $max_depth
            }) YIELD node
            WITH node, labels(node)[0] AS node_type
            WHERE node_type IN ['Dataset', 'Column', 'Dashboard', 'MLFeature', 'Transform']
            OPTIONAL MATCH (node)<-[:DERIVES_FROM|READS_FROM|PRODUCES]-(dependent)
            WITH node, node_type, count(distinct dependent) AS fan_out
            RETURN
                elementId(node)       AS node_id,
                node_type,
                coalesce(node.name, node.dataset) AS name,
                coalesce(node.sla_tier, 'P2')     AS sla_tier,
                fan_out,
                coalesce(node.freshness_cadence, 'daily') AS freshness_cadence
            ORDER BY fan_out DESC
            LIMIT 500
            """
            params = {
                "dataset": change.dataset,
                "column": change.column,
                "max_depth": _MAX_BFS_DEPTH,
            }
        else:
            # Table-level: start from the Dataset node
            query = """
            MATCH (start:Dataset {name: $dataset})
            CALL apoc.path.subgraphNodes(start, {
                relationshipFilter: "WRITES_TO>|READS_FROM>|PRODUCES>",
                minLevel: 1,
                maxLevel: $max_depth
            }) YIELD node
            WITH node, labels(node)[0] AS node_type
            WHERE node_type IN ['Dataset', 'Dashboard', 'MLFeature', 'Transform']
            OPTIONAL MATCH (node)<-[:WRITES_TO|READS_FROM|PRODUCES]-(dependent)
            WITH node, node_type, count(distinct dependent) AS fan_out
            RETURN
                elementId(node)       AS node_id,
                node_type,
                coalesce(node.name, '') AS name,
                coalesce(node.sla_tier, 'P2') AS sla_tier,
                fan_out,
                coalesce(node.freshness_cadence, 'daily') AS freshness_cadence
            LIMIT 500
            """
            params = {"dataset": change.dataset, "max_depth": _MAX_BFS_DEPTH}

        records = await self._neo4j.run(query, params)

        assets = []
        for r in records:
            node_type_str = r.get("node_type", "Dataset")
            try:
                node_type = NodeType(node_type_str)
            except ValueError:
                node_type = NodeType.DATASET

            action = self._suggested_action(node_type, change)

            assets.append(AffectedAsset(
                node_id=str(r.get("node_id", "")),
                node_type=node_type,
                name=r.get("name", ""),
                sla_tier=SLATier(r.get("sla_tier", "P2")),
                downstream_fan_out=r.get("fan_out", 0),
                freshness_cadence=r.get("freshness_cadence", "daily"),
                suggested_action=action,
            ))

        return assets

    async def _upstream_provenance(self, change: SchemaChange) -> list[str]:
        """
        Trace upstream provenance chain for the changed entity.
        Returns a list of dataset names from raw source to the changed dataset.
        """
        query = """
        MATCH path = (leaf)-[:READS_FROM|DERIVES_FROM*1..{depth}]->(start)
        WHERE (start:Dataset {{name: $dataset}} OR start:Column {{dataset: $dataset}})
          AND NOT ()-[:READS_FROM|DERIVES_FROM]->(leaf)
        RETURN [n IN nodes(path) | coalesce(n.name, n.dataset)] AS chain
        ORDER BY length(path) DESC
        LIMIT 1
        """.format(depth=_MAX_BFS_DEPTH)

        records = await self._neo4j.run(query, {"dataset": change.dataset})
        if records:
            return records[0].get("chain", [])
        return []

    # ─── Scoring ──────────────────────────────────────────────────────────────

    def _score_asset(self, asset: AffectedAsset) -> AffectedAsset:
        """
        Criticality score = SLA weight × cadence weight × log(fan_out + 1) normalized.
        Score ∈ [0, 1].
        """
        sla_w = _SLA_WEIGHTS.get(asset.sla_tier.value, 0.2)
        cadence_w = _CADENCE_WEIGHTS.get(asset.freshness_cadence, 0.5)

        import math
        fan_out_factor = math.log(asset.downstream_fan_out + 1) / math.log(501)

        score = (sla_w * 0.5) + (cadence_w * 0.3) + (fan_out_factor * 0.2)
        asset.criticality_score = round(min(score, 1.0), 4)
        return asset

    # ─── Action suggestions ───────────────────────────────────────────────────

    def _suggested_action(self, node_type: NodeType, change: SchemaChange) -> str:
        """Return a migration action string for an affected asset."""
        actions: dict[tuple[NodeType, ChangeType], str] = {
            (NodeType.DASHBOARD, ChangeType.RENAME_COLUMN): "Update column reference in BI layer",
            (NodeType.DASHBOARD, ChangeType.DROP_COLUMN): "Remove column usage, update chart definitions",
            (NodeType.DASHBOARD, ChangeType.TYPE_CHANGE): "Verify formatting and aggregation compatibility",
            (NodeType.ML_FEATURE, ChangeType.RENAME_COLUMN): "Update feature pipeline and retrain if needed",
            (NodeType.ML_FEATURE, ChangeType.DROP_COLUMN): "Remove feature from pipeline, evaluate model impact",
            (NodeType.ML_FEATURE, ChangeType.TYPE_CHANGE): "Validate feature engineering logic for type compatibility",
            (NodeType.DATASET, ChangeType.RENAME_COLUMN): "Update downstream SELECT and JOIN references",
            (NodeType.DATASET, ChangeType.DROP_COLUMN): "Remove column from dependent transforms",
            (NodeType.TRANSFORM, ChangeType.RENAME_COLUMN): "Update SQL / PySpark transform",
            (NodeType.TRANSFORM, ChangeType.DROP_COLUMN): "Remove column from transform output",
        }
        return actions.get(
            (node_type, change.change_type),
            f"Review usage of {change.dataset}.{change.column or '*'}",
        )

    # ─── Summary generation ───────────────────────────────────────────────────

    def _generate_summary(self, change: SchemaChange, assets: list[AffectedAsset]) -> str:
        """Generate a human-readable summary of the impact."""
        p0 = sum(1 for a in assets if a.sla_tier == SLATier.P0)
        p1 = sum(1 for a in assets if a.sla_tier == SLATier.P1)
        dashboards = sum(1 for a in assets if a.node_type == NodeType.DASHBOARD)
        ml_features = sum(1 for a in assets if a.node_type == NodeType.ML_FEATURE)
        datasets = sum(1 for a in assets if a.node_type == NodeType.DATASET)

        col_ref = f".{change.column}" if change.column else ""
        parts = [
            f"Changing {change.dataset}{col_ref} ({change.change_type.value}) "
            f"affects {len(assets)} downstream assets."
        ]
        if p0:
            parts.append(f"{p0} P0 asset{'s' if p0 > 1 else ''}.")
        if p1:
            parts.append(f"{p1} P1 asset{'s' if p1 > 1 else ''}.")
        if dashboards:
            parts.append(f"{dashboards} dashboard{'s' if dashboards > 1 else ''}.")
        if ml_features:
            parts.append(f"{ml_features} ML feature pipeline{'s' if ml_features > 1 else ''}.")
        if datasets:
            parts.append(f"{datasets} downstream dataset{'s' if datasets > 1 else ''}.")

        return " ".join(parts)

    # ─── Cache key ────────────────────────────────────────────────────────────

    @staticmethod
    def _cache_key(change: SchemaChange) -> str:
        raw = json.dumps({
            "dataset": change.dataset,
            "column": change.column,
            "change_type": change.change_type.value,
        }, sort_keys=True)
        digest = hashlib.sha256(raw.encode()).hexdigest()[:16]
        return f"blast_radius:{digest}"
