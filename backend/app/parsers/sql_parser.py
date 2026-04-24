"""
Multi-dialect SQL parser using sqlglot.

Handles Redshift, BigQuery, Snowflake, and standard SQL. Extracts
column-level lineage by walking the AST of SELECT statements, CTEs,
subqueries, and joins.

Config keys:
    sql:              str           – raw SQL string to parse
    project_name:     str           – lineage project context
    target_dataset:   str           – destination table/view
    source_datasets:  list[str]     – known upstream table names
    dialect:          str           – sqlglot dialect: redshift|bigquery|snowflake|default
    transform_id:     str           – optional stable ID (defaults to sha256 of sql)
    commit_id:        str           – optional version tag
"""

from __future__ import annotations

import hashlib
from typing import Any

import sqlglot
import sqlglot.expressions as exp
import structlog

from app.models.lineage import (
    ColumnDerivation,
    ColumnReference,
    DataWarehouse,
    LineageEvent,
    SourceType,
)
from app.parsers.base import BaseParser

logger = structlog.get_logger(__name__)

_DIALECT_MAP: dict[str, DataWarehouse] = {
    "redshift": DataWarehouse.REDSHIFT,
    "bigquery": DataWarehouse.BIGQUERY,
    "snowflake": DataWarehouse.SNOWFLAKE,
    "postgres": DataWarehouse.POSTGRES,
}


class SqlGlotParser(BaseParser):
    """
    Parses SQL strings into column-level LineageEvents using sqlglot.

    Handles:
    - Simple SELECT projections
    - CTEs (WITH clauses)
    - Subqueries aliased in FROM / JOIN
    - Column aliases
    - Function expressions (tracks top-level source columns)
    - COALESCE, CASE WHEN (multi-source columns tracked as best-effort)
    """

    source_type = SourceType.SQL

    def parse(self, config: dict[str, Any]) -> list[LineageEvent]:
        sql = config["sql"]
        project_name = config["project_name"]
        target_dataset = config.get("target_dataset", "unknown")
        source_datasets = config.get("source_datasets", [])
        dialect = config.get("dialect", "default")
        transform_id = config.get("transform_id") or self._sql_hash(sql)
        commit_id = config.get("commit_id")

        warehouse = _DIALECT_MAP.get(dialect, DataWarehouse.GENERIC)

        try:
            statements = sqlglot.parse(sql, dialect=dialect if dialect != "default" else None)
        except sqlglot.errors.ParseError as exc:
            logger.warning("sqlglot_parse_error", error=str(exc), dialect=dialect)
            return []

        all_derivations: list[ColumnDerivation] = []
        discovered_sources: set[str] = set()

        for stmt in statements:
            if stmt is None:
                continue
            if not isinstance(stmt, (exp.Select, exp.Create, exp.Insert)):
                continue

            # Resolve CTEs first — they act as virtual source tables
            cte_map: dict[str, exp.Select] = {}
            for cte in stmt.find_all(exp.CTE):
                cte_alias = cte.alias_or_name
                inner_select = cte.this
                if isinstance(inner_select, exp.Select):
                    cte_map[cte_alias.lower()] = inner_select

            # Get the final SELECT expression
            select_expr: exp.Select | None = None
            if isinstance(stmt, exp.Select):
                select_expr = stmt
            elif isinstance(stmt, (exp.Create, exp.Insert)):
                select_expr = stmt.find(exp.Select)

            if select_expr is None:
                continue

            derivations, sources = self._extract_column_derivations(
                select_expr=select_expr,
                cte_map=cte_map,
                target_dataset=target_dataset,
                project_name=project_name,
                warehouse=warehouse,
            )
            all_derivations.extend(derivations)
            discovered_sources.update(sources)

        # Merge with caller-supplied source_datasets
        all_sources = list(set(source_datasets) | discovered_sources)

        if not all_derivations and not all_sources:
            return []

        event = LineageEvent(
            source_type=SourceType.SQL,
            project_name=project_name,
            commit_id=commit_id,
            transform_id=transform_id,
            transform_name=target_dataset,
            source_datasets=all_sources,
            target_datasets=[target_dataset],
            column_derivations=all_derivations,
            raw_sql=sql,
        )
        return [event]

    # ─── Internal helpers ─────────────────────────────────────────────────────

    def _extract_column_derivations(
        self,
        select_expr: exp.Select,
        cte_map: dict[str, exp.Select],
        target_dataset: str,
        project_name: str,
        warehouse: DataWarehouse,
    ) -> tuple[list[ColumnDerivation], set[str]]:
        """
        Walk SELECT projections and build ColumnDerivation objects.
        Returns (derivations, discovered_source_table_names).
        """
        derivations: list[ColumnDerivation] = []
        discovered_sources: set[str] = set()

        # Build table alias → real table name map from FROM + JOINs
        alias_map: dict[str, str] = {}
        for table in select_expr.find_all(exp.Table):
            table_name = self._resolve_table_name(table)
            alias = table.alias or table_name
            if alias:
                alias_map[alias.lower()] = table_name
            if table_name and table_name.lower() not in cte_map:
                discovered_sources.add(table_name)

        for projection in select_expr.expressions:
            target_col = self._get_output_column_name(projection)
            if not target_col or target_col == "*":
                # SELECT * — expand using known source if possible
                for src_table in alias_map.values():
                    if src_table.lower() not in cte_map:
                        derivations.append(ColumnDerivation(
                            source=ColumnReference(
                                dataset=src_table,
                                column="*",
                            ),
                            target=ColumnReference(
                                dataset=target_dataset,
                                column="*",
                            ),
                            is_passthrough=True,
                        ))
                continue

            # Find all Column references within this projection expression
            source_cols = self._find_source_columns(projection, alias_map, cte_map)

            if not source_cols:
                # Literal or computed column with no source reference
                continue

            expression_str = projection.sql(dialect=None)

            for src_dataset, src_col in source_cols:
                is_passthrough = len(source_cols) == 1 and self._is_passthrough(projection)
                derivations.append(ColumnDerivation(
                    source=ColumnReference(dataset=src_dataset, column=src_col),
                    target=ColumnReference(dataset=target_dataset, column=target_col),
                    expression=None if is_passthrough else expression_str,
                    is_passthrough=is_passthrough,
                ))

        return derivations, discovered_sources

    def _find_source_columns(
        self,
        expr: exp.Expression,
        alias_map: dict[str, str],
        cte_map: dict[str, exp.Select],
    ) -> list[tuple[str, str]]:
        """Return [(source_table, column_name), ...] for all Column refs in expr."""
        results: list[tuple[str, str]] = []
        for col in expr.find_all(exp.Column):
            col_name = col.name
            table_alias = col.table or ""
            if table_alias.lower() in cte_map:
                # Column from a CTE — skip (CTE columns are virtual)
                continue
            real_table = alias_map.get(table_alias.lower(), table_alias) or "unknown"
            results.append((real_table, col_name))
        return results

    def _get_output_column_name(self, expr: exp.Expression) -> str | None:
        """Get the output column name for a SELECT projection."""
        if isinstance(expr, exp.Alias):
            return expr.alias
        if isinstance(expr, exp.Column):
            return expr.name
        if isinstance(expr, exp.Star):
            return "*"
        return None

    def _is_passthrough(self, expr: exp.Expression) -> bool:
        """True if the projection is a simple column reference with optional alias."""
        inner = expr.this if isinstance(expr, exp.Alias) else expr
        return isinstance(inner, exp.Column)

    def _resolve_table_name(self, table: exp.Table) -> str:
        """Resolve a Table node to its fully qualified name."""
        parts = [p for p in [table.db, table.name] if p]
        return ".".join(parts) if parts else table.name or ""

    @staticmethod
    def _sql_hash(sql: str) -> str:
        return hashlib.sha256(sql.encode()).hexdigest()[:16]
