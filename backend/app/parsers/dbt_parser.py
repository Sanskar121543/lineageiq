"""
dbt manifest.json parser.

Parses dbt manifest.json (v9+) to extract:
- Node dependency graph (refs, sources)
- Column-level lineage from column metadata and compiled SQL
- Model → pipeline (dbt project) grouping

Config keys:
    manifest_path: str  – path to manifest.json on disk
    project_name:  str  – logical project name to tag nodes
    commit_id:     str  – git SHA or version tag (optional)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import structlog

from app.models.lineage import (
    ColumnDerivation,
    ColumnReference,
    LineageEvent,
    SourceType,
)
from app.parsers.base import BaseParser
from app.parsers.sql_parser import SqlGlotParser

logger = structlog.get_logger(__name__)

# dbt node types we care about
_MODEL_TYPES = {"model", "snapshot", "seed", "analysis"}
_SOURCE_PREFIX = "source."


class DbtManifestParser(BaseParser):
    """
    Parses dbt manifest.json to produce column-level LineageEvents.

    For each model node, we:
    1. Walk depends_on.nodes to find table-level dependencies.
    2. Parse compiled_sql via SqlGlotParser to get column derivations.
    3. Fall back to column metadata (if available) for passthrough columns.
    """

    source_type = SourceType.DBT

    def __init__(self) -> None:
        self._sql_parser = SqlGlotParser()

    def parse(self, config: dict[str, Any]) -> list[LineageEvent]:
        manifest_path = Path(config["manifest_path"])
        project_name = config["project_name"]
        commit_id = config.get("commit_id")

        self._log_parse_start(manifest_path=str(manifest_path), project=project_name)

        with open(manifest_path) as f:
            manifest: dict = json.load(f)

        nodes: dict = manifest.get("nodes", {})
        sources: dict = manifest.get("sources", {})
        parent_map: dict = manifest.get("parent_map", {})

        # Register source datasets (raw tables that dbt reads from)
        source_datasets: dict[str, str] = {}
        for src_key, src_node in sources.items():
            dataset_name = f"{src_node['source_name']}.{src_node['name']}"
            source_datasets[src_key] = dataset_name

        events: list[LineageEvent] = []

        for node_key, node in nodes.items():
            if node.get("resource_type") not in _MODEL_TYPES:
                continue
            if node.get("config", {}).get("enabled") is False:
                continue

            event = self._parse_node(
                node_key=node_key,
                node=node,
                nodes=nodes,
                source_datasets=source_datasets,
                project_name=project_name,
                commit_id=commit_id,
            )
            if event:
                events.append(event)

        self._log_parse_complete(len(events), project=project_name)
        return events

    def _parse_node(
        self,
        node_key: str,
        node: dict,
        nodes: dict,
        source_datasets: dict[str, str],
        project_name: str,
        commit_id: str | None,
    ) -> LineageEvent | None:
        """Parse one dbt model node into a LineageEvent."""

        target_dataset = node.get("alias") or node.get("name", "")
        schema = node.get("schema", "")
        if schema:
            target_dataset = f"{schema}.{target_dataset}"

        # Resolve upstream datasets from depends_on.nodes
        source_tables: list[str] = []
        for dep_key in node.get("depends_on", {}).get("nodes", []):
            if dep_key.startswith(_SOURCE_PREFIX):
                src_name = source_datasets.get(dep_key)
                if src_name:
                    source_tables.append(src_name)
            elif dep_key in nodes:
                dep_node = nodes[dep_key]
                dep_name = dep_node.get("alias") or dep_node.get("name", "")
                dep_schema = dep_node.get("schema", "")
                if dep_schema:
                    dep_name = f"{dep_schema}.{dep_name}"
                source_tables.append(dep_name)

        if not source_tables and not node.get("compiled_sql"):
            return None

        # Attempt column-level lineage via compiled SQL
        column_derivations: list[ColumnDerivation] = []
        compiled_sql = node.get("compiled_sql") or node.get("compiled_code")
        if compiled_sql and source_tables:
            try:
                sql_events = self._sql_parser.parse({
                    "sql": compiled_sql,
                    "project_name": project_name,
                    "target_dataset": target_dataset,
                    "source_datasets": source_tables,
                    "dialect": "default",
                })
                if sql_events:
                    column_derivations = sql_events[0].column_derivations
            except Exception as exc:
                logger.warning(
                    "dbt_column_lineage_failed",
                    node=node_key,
                    error=str(exc),
                )

        # Fall back to column metadata for passthrough columns
        if not column_derivations:
            column_derivations = self._extract_column_metadata(
                node, target_dataset, source_tables, project_name
            )

        return LineageEvent(
            source_type=SourceType.DBT,
            project_name=project_name,
            commit_id=commit_id,
            transform_id=node_key,
            transform_name=node.get("name", node_key),
            pipeline_name=node.get("fqn", [None])[0] if node.get("fqn") else None,
            source_datasets=source_tables,
            target_datasets=[target_dataset],
            column_derivations=column_derivations,
            raw_sql=compiled_sql,
            tags={
                "dbt_resource_type": node.get("resource_type"),
                "dbt_materialized": node.get("config", {}).get("materialized"),
                "dbt_tags": node.get("tags", []),
                "dbt_package_name": node.get("package_name"),
            },
        )

    def _extract_column_metadata(
        self,
        node: dict,
        target_dataset: str,
        source_tables: list[str],
        project_name: str,
    ) -> list[ColumnDerivation]:
        """
        Build passthrough ColumnDerivations from dbt column metadata.
        Used when SQL parsing fails or compiled_sql is unavailable.
        """
        derivations: list[ColumnDerivation] = []
        columns: dict = node.get("columns", {})
        if not columns or not source_tables:
            return derivations

        primary_source = source_tables[0]

        for col_name in columns:
            derivations.append(
                ColumnDerivation(
                    source=ColumnReference(dataset=primary_source, column=col_name),
                    target=ColumnReference(dataset=target_dataset, column=col_name),
                    is_passthrough=True,
                )
            )
        return derivations
