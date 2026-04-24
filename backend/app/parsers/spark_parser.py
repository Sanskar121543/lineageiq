"""
Apache Spark LogicalPlan JSON parser.

Parses the JSON representation of a Spark LogicalPlan captured via SparkListener.
The plan is a tree of operator nodes; we walk it depth-first to extract:
  - Project (SELECT projections) → column-level derivations
  - LogicalRelation / HiveTableRelation → source tables
  - InsertIntoHadoopFsRelationCommand / SaveIntoDataSourceCommand → target tables
  - Join conditions (for lineage through joins)

The SparkListener event format this expects:
{
  "Event": "SparkListenerSQLExecutionStart",
  "physicalPlanDescription": "...",
  "sparkPlanInfo": { ... },
  "logicalPlan": { "class": "...", "children": [...], ... }
}

Config keys:
    plan:          dict   – parsed LogicalPlan JSON
    project_name:  str
    pipeline_name: str    – Spark application name
    job_id:        str    – Spark job ID
    commit_id:     str    – optional version tag
"""

from __future__ import annotations

import re
from typing import Any

import structlog

from app.models.lineage import (
    ColumnDerivation,
    ColumnReference,
    LineageEvent,
    SourceType,
)
from app.parsers.base import BaseParser

logger = structlog.get_logger(__name__)

# Spark plan node class patterns
_PROJECT_CLASSES = {"Project", "Aggregate", "Window"}
_RELATION_CLASSES = {
    "LogicalRelation",
    "HiveTableRelation",
    "DataSourceV2Relation",
    "StreamingRelation",
}
_WRITE_CLASSES = {
    "InsertIntoHadoopFsRelationCommand",
    "SaveIntoDataSourceCommand",
    "InsertIntoDataSourceCommand",
    "AppendData",
    "OverwriteByExpression",
}


class SparkPlanParser(BaseParser):
    """
    Parses a Spark LogicalPlan JSON tree into column-level LineageEvents.

    The parser performs a recursive depth-first traversal of the plan tree,
    collecting source tables from leaf relation nodes and column derivations
    from Project nodes. Each "query unit" (one write operation) becomes one
    LineageEvent.
    """

    source_type = SourceType.SPARK

    def parse(self, config: dict[str, Any]) -> list[LineageEvent]:
        plan = config["plan"]
        project_name = config["project_name"]
        pipeline_name = config.get("pipeline_name", "spark_job")
        job_id = config.get("job_id", "unknown")
        commit_id = config.get("commit_id")

        self._log_parse_start(job_id=job_id, project=project_name)

        events: list[LineageEvent] = []
        self._traverse(plan, events, project_name, pipeline_name, job_id, commit_id, [])

        self._log_parse_complete(len(events), job_id=job_id)
        return events

    def _traverse(
        self,
        node: dict,
        events: list[LineageEvent],
        project_name: str,
        pipeline_name: str,
        job_id: str,
        commit_id: str | None,
        projection_stack: list[dict],
    ) -> None:
        """DFS traversal of the plan tree. Emits an event at each write node."""
        node_class = self._node_class(node)

        if node_class in _WRITE_CLASSES:
            # Emit one LineageEvent for this write operation
            target = self._extract_write_target(node)
            sources: list[str] = []
            derivations: list[ColumnDerivation] = []

            # Walk children to collect sources and projections
            for child in node.get("children", []):
                child_sources, child_derivations = self._collect_lineage(child, target, project_name)
                sources.extend(child_sources)
                derivations.extend(child_derivations)

            events.append(LineageEvent(
                source_type=SourceType.SPARK,
                project_name=project_name,
                commit_id=commit_id,
                transform_id=f"{job_id}_{target}",
                transform_name=target,
                pipeline_name=pipeline_name,
                source_datasets=list(set(sources)),
                target_datasets=[target] if target else [],
                column_derivations=derivations,
                tags={"spark_node_class": node_class, "spark_job_id": job_id},
            ))
        else:
            for child in node.get("children", []):
                self._traverse(
                    child, events, project_name, pipeline_name,
                    job_id, commit_id, projection_stack
                )

    def _collect_lineage(
        self,
        node: dict,
        target_dataset: str,
        project_name: str,
    ) -> tuple[list[str], list[ColumnDerivation]]:
        """
        Recursively collect (source_tables, column_derivations) from a plan subtree.
        """
        node_class = self._node_class(node)
        sources: list[str] = []
        derivations: list[ColumnDerivation] = []

        if node_class in _RELATION_CLASSES:
            table_name = self._extract_relation_name(node)
            if table_name:
                sources.append(table_name)

        elif node_class in _PROJECT_CLASSES:
            projections = node.get("projectList") or node.get("aggregateExpressions", [])
            child_sources: list[str] = []
            for child in node.get("children", []):
                cs, cd = self._collect_lineage(child, target_dataset, project_name)
                child_sources.extend(cs)
                derivations.extend(cd)
            sources.extend(child_sources)

            primary_source = child_sources[0] if child_sources else "unknown"
            for proj in projections:
                deriv = self._parse_projection(proj, primary_source, target_dataset, project_name)
                if deriv:
                    derivations.append(deriv)

        else:
            for child in node.get("children", []):
                cs, cd = self._collect_lineage(child, target_dataset, project_name)
                sources.extend(cs)
                derivations.extend(cd)

        return sources, derivations

    def _parse_projection(
        self,
        proj: dict,
        source_dataset: str,
        target_dataset: str,
        project_name: str,
    ) -> ColumnDerivation | None:
        """Parse one Spark projection expression into a ColumnDerivation."""
        proj_class = self._node_class(proj)

        # Alias wraps the real expression
        if proj_class == "Alias":
            alias_name = proj.get("name") or proj.get("exprId", {}).get("jvmId")
            inner = proj.get("child") or proj.get("children", [{}])[0]
            return self._build_derivation(
                expr=inner,
                output_col=alias_name or "unknown",
                source_dataset=source_dataset,
                target_dataset=target_dataset,
                project_name=project_name,
            )

        # AttributeReference — direct passthrough
        if proj_class == "AttributeReference":
            col_name = proj.get("name", "")
            return ColumnDerivation(
                source=ColumnReference(dataset=source_dataset, column=col_name),
                target=ColumnReference(dataset=target_dataset, column=col_name),
                is_passthrough=True,
            )

        return None

    def _build_derivation(
        self,
        expr: dict,
        output_col: str,
        source_dataset: str,
        target_dataset: str,
        project_name: str,
    ) -> ColumnDerivation | None:
        """Build a ColumnDerivation from a Spark expression node."""
        expr_class = self._node_class(expr)

        if expr_class == "AttributeReference":
            src_col = expr.get("name", "")
            return ColumnDerivation(
                source=ColumnReference(dataset=source_dataset, column=src_col),
                target=ColumnReference(dataset=target_dataset, column=output_col),
                is_passthrough=src_col == output_col,
            )

        # Complex expression — extract all referenced attribute names
        referenced = self._extract_attribute_references(expr)
        if not referenced:
            return None

        expression_str = self._expr_to_string(expr)
        # Use first referenced column as the primary source (best-effort)
        return ColumnDerivation(
            source=ColumnReference(dataset=source_dataset, column=referenced[0]),
            target=ColumnReference(dataset=target_dataset, column=output_col),
            expression=expression_str,
            is_passthrough=False,
        )

    def _extract_attribute_references(self, node: dict) -> list[str]:
        """Recursively collect all AttributeReference names in an expression tree."""
        refs: list[str] = []
        if self._node_class(node) == "AttributeReference":
            name = node.get("name")
            if name:
                refs.append(name)
        for child in node.get("children", []):
            refs.extend(self._extract_attribute_references(child))
        return refs

    def _expr_to_string(self, node: dict) -> str:
        """Convert a Spark expression node to a human-readable string."""
        node_class = self._node_class(node)
        if node_class == "AttributeReference":
            return node.get("name", "?")
        if node_class == "Literal":
            return str(node.get("value", "?"))
        children = [self._expr_to_string(c) for c in node.get("children", [])]
        return f"{node_class}({', '.join(children)})"

    def _extract_write_target(self, node: dict) -> str:
        """Extract the target table name from a write command node."""
        # InsertIntoHadoopFsRelationCommand carries the path
        path = node.get("outputPath") or node.get("path", "")
        if path:
            # Extract table name from HDFS path like /user/hive/warehouse/db.db/table
            return path.rstrip("/").split("/")[-1]

        # SaveIntoDataSourceCommand stores table in options
        opts = node.get("options") or node.get("config", {})
        if isinstance(opts, dict):
            return opts.get("dbtable") or opts.get("path", "unknown")

        # DataSourceV2 carries tableIdent
        table_ident = node.get("tableIdent") or node.get("relation", {})
        if isinstance(table_ident, dict):
            parts = table_ident.get("multipartIdentifier", [])
            return ".".join(parts) if parts else "unknown"

        return "unknown"

    def _extract_relation_name(self, node: dict) -> str | None:
        """Extract the table name from a relation/leaf node."""
        # HiveTableRelation
        table_meta = node.get("tableMeta") or {}
        identifier = table_meta.get("identifier") or {}
        if identifier:
            db = identifier.get("database", "")
            table = identifier.get("table", "")
            return f"{db}.{table}" if db else table

        # LogicalRelation / DataSourceV2Relation
        catalog_table = node.get("catalogTable") or {}
        ident = catalog_table.get("identifier") or {}
        if ident:
            db = ident.get("database", "")
            table = ident.get("table", "")
            return f"{db}.{table}" if db else table

        # Fallback: look for output attributes with qualifier
        output = node.get("output", [])
        if output and isinstance(output[0], dict):
            qualifier = output[0].get("qualifier", [])
            if qualifier:
                return ".".join(qualifier)

        return None

    @staticmethod
    def _node_class(node: dict) -> str:
        """Extract the simple class name from a Spark plan node."""
        full_class = node.get("class", node.get("type", ""))
        return full_class.split(".")[-1]
