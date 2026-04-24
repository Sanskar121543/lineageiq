"""
Graph writer: translates normalized LineageEvents into Neo4j nodes and relationships.

Every write is tagged with a commit_id and timestamp for versioned snapshot
reconstruction. All writes for a single LineageEvent are committed atomically.
"""

from __future__ import annotations

from datetime import datetime

import structlog

from app.graph.client import get_client
from app.models.lineage import ColumnDerivation, LineageEvent, LineageSnapshot

logger = structlog.get_logger(__name__)


class GraphWriter:
    """
    Translates LineageEvent objects into batched Neo4j MERGE operations.

    Design principles:
    - MERGE (not CREATE) everywhere — re-ingesting a manifest is idempotent.
    - Column-level derivations are written as DERIVES_FROM relationships with
      the transformation expression stored on the edge.
    - Every write carries commit_id + ingested_at for snapshot reconstruction.
    """

    def __init__(self) -> None:
        self._client = get_client()

    async def write_event(self, event: LineageEvent) -> LineageSnapshot:
        """
        Persist a full LineageEvent to Neo4j atomically.
        Returns a snapshot record with node/relationship counts.
        """
        queries: list[tuple[str, dict]] = []
        commit_id = event.commit_id or event.event_id
        ts = event.ingested_at.isoformat()

        # ── 1. Upsert source and target Dataset nodes ──────────────────────────
        for dataset_name in event.source_datasets + event.target_datasets:
            queries.append((
                """
                MERGE (d:Dataset {project: $project, name: $name})
                ON CREATE SET
                    d.source_type = $source_type,
                    d.created_at  = $ts,
                    d.commit_id   = $commit_id
                ON MATCH SET
                    d.last_seen_at = $ts,
                    d.commit_id    = $commit_id
                """,
                {
                    "project": event.project_name,
                    "name": dataset_name,
                    "source_type": event.source_type.value,
                    "ts": ts,
                    "commit_id": commit_id,
                },
            ))

        # ── 2. Upsert Transform node ───────────────────────────────────────────
        queries.append((
            """
            MERGE (t:Transform {transform_id: $transform_id})
            ON CREATE SET
                t.name          = $name,
                t.project       = $project,
                t.pipeline_name = $pipeline_name,
                t.source_type   = $source_type,
                t.llm_inferred  = $llm_inferred,
                t.raw_sql       = $raw_sql,
                t.commit_id     = $commit_id,
                t.created_at    = $ts
            ON MATCH SET
                t.last_seen_at  = $ts,
                t.commit_id     = $commit_id
            """,
            {
                "transform_id": event.transform_id,
                "name": event.transform_name,
                "project": event.project_name,
                "pipeline_name": event.pipeline_name,
                "source_type": event.source_type.value,
                "llm_inferred": event.llm_inferred,
                "raw_sql": event.raw_sql,
                "commit_id": commit_id,
                "ts": ts,
            },
        ))

        # ── 3. READS_FROM: Transform → source Datasets ────────────────────────
        for src in event.source_datasets:
            queries.append((
                """
                MATCH (t:Transform {transform_id: $tid})
                MATCH (d:Dataset {project: $project, name: $name})
                MERGE (t)-[:READS_FROM {commit_id: $commit_id}]->(d)
                """,
                {
                    "tid": event.transform_id,
                    "project": event.project_name,
                    "name": src,
                    "commit_id": commit_id,
                },
            ))

        # ── 4. WRITES_TO: Transform → target Datasets ─────────────────────────
        for tgt in event.target_datasets:
            queries.append((
                """
                MATCH (t:Transform {transform_id: $tid})
                MATCH (d:Dataset {project: $project, name: $name})
                MERGE (t)-[:WRITES_TO {commit_id: $commit_id}]->(d)
                """,
                {
                    "tid": event.transform_id,
                    "project": event.project_name,
                    "name": tgt,
                    "commit_id": commit_id,
                },
            ))

        # ── 5. Column nodes + DERIVES_FROM relationships ───────────────────────
        for deriv in event.column_derivations:
            queries.extend(self._column_derivation_queries(deriv, event, commit_id, ts))

        # ── 6. Pipeline node (if present) ─────────────────────────────────────
        if event.pipeline_name:
            queries.append((
                """
                MERGE (p:Pipeline {project: $project, name: $name})
                ON CREATE SET p.source_type = $source_type, p.created_at = $ts
                ON MATCH  SET p.last_seen_at = $ts
                """,
                {
                    "project": event.project_name,
                    "name": event.pipeline_name,
                    "source_type": event.source_type.value,
                    "ts": ts,
                },
            ))
            queries.append((
                """
                MATCH (t:Transform {transform_id: $tid})
                MATCH (p:Pipeline {project: $project, name: $pname})
                MERGE (t)-[:PART_OF]->(p)
                """,
                {
                    "tid": event.transform_id,
                    "project": event.project_name,
                    "pname": event.pipeline_name,
                },
            ))

        # ── 7. Snapshot record ─────────────────────────────────────────────────
        queries.append((
            """
            CREATE (s:Snapshot {
                snapshot_id:  $snapshot_id,
                commit_id:    $commit_id,
                project:      $project,
                source_type:  $source_type,
                event_id:     $event_id,
                created_at:   $ts
            })
            """,
            {
                "snapshot_id": f"{commit_id}_{event.event_id}",
                "commit_id": commit_id,
                "project": event.project_name,
                "source_type": event.source_type.value,
                "event_id": event.event_id,
                "ts": ts,
            },
        ))

        # ── Execute atomically ─────────────────────────────────────────────────
        await self._client.run_batch(queries)

        node_count = len(event.source_datasets) + len(event.target_datasets) + 1  # +1 transform
        rel_count = len(event.source_datasets) + len(event.target_datasets) + len(event.column_derivations)

        logger.info(
            "lineage_event_written",
            transform_id=event.transform_id,
            derivations=len(event.column_derivations),
            commit_id=commit_id,
        )

        return LineageSnapshot(
            commit_id=commit_id,
            project_name=event.project_name,
            source_type=event.source_type,
            node_count=node_count,
            relationship_count=rel_count,
        )

    def _column_derivation_queries(
        self,
        deriv: ColumnDerivation,
        event: LineageEvent,
        commit_id: str,
        ts: str,
    ) -> list[tuple[str, dict]]:
        """Generate Cypher for one ColumnDerivation (source col → target col)."""
        queries = []

        # Source column node
        queries.append((
            """
            MERGE (c:Column {project: $project, dataset: $dataset, name: $name})
            ON CREATE SET c.created_at = $ts, c.commit_id = $commit_id
            ON MATCH  SET c.last_seen_at = $ts
            """,
            {
                "project": event.project_name,
                "dataset": deriv.source.dataset,
                "name": deriv.source.column,
                "ts": ts,
                "commit_id": commit_id,
            },
        ))

        # Target column node
        queries.append((
            """
            MERGE (c:Column {project: $project, dataset: $dataset, name: $name})
            ON CREATE SET c.created_at = $ts, c.commit_id = $commit_id
            ON MATCH  SET c.last_seen_at = $ts
            """,
            {
                "project": event.project_name,
                "dataset": deriv.target.dataset,
                "name": deriv.target.column,
                "ts": ts,
                "commit_id": commit_id,
            },
        ))

        # PART_OF: Column → Dataset (source)
        queries.append((
            """
            MATCH (c:Column {project: $project, dataset: $dataset, name: $col})
            MATCH (d:Dataset {project: $project, name: $dataset})
            MERGE (c)-[:PART_OF]->(d)
            """,
            {
                "project": event.project_name,
                "dataset": deriv.source.dataset,
                "col": deriv.source.column,
            },
        ))

        # DERIVES_FROM: target Column → source Column
        queries.append((
            """
            MATCH (src:Column {project: $project, dataset: $src_ds, name: $src_col})
            MATCH (tgt:Column {project: $project, dataset: $tgt_ds, name: $tgt_col})
            MERGE (tgt)-[r:DERIVES_FROM]->(src)
            ON CREATE SET
                r.expression     = $expression,
                r.is_passthrough = $is_passthrough,
                r.transform_id   = $transform_id,
                r.commit_id      = $commit_id,
                r.created_at     = $ts
            ON MATCH SET
                r.expression     = $expression,
                r.commit_id      = $commit_id,
                r.last_seen_at   = $ts
            """,
            {
                "project": event.project_name,
                "src_ds": deriv.source.dataset,
                "src_col": deriv.source.column,
                "tgt_ds": deriv.target.dataset,
                "tgt_col": deriv.target.column,
                "expression": deriv.expression,
                "is_passthrough": deriv.is_passthrough,
                "transform_id": event.transform_id,
                "commit_id": commit_id,
                "ts": ts,
            },
        ))

        return queries
