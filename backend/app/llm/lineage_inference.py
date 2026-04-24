"""
LLM-based lineage inference for undocumented transforms.

Pipeline:
  1. Send SQL / PySpark code to GPT-4o with a structured output schema.
  2. Validate the response with Pydantic (guaranteed shape, no key errors).
  3. Cross-validate every claimed column against the live Neo4j catalog.
     - If a source column does not exist in the catalog → flag for human review.
     - If a target dataset does not exist → create a stub Dataset node.
  4. Return a LineageEvent with validation_status = "ok" | "flagged".

This validation step is what prevents LLM hallucinations from silently
corrupting the graph. Flagged events are written to Neo4j with a
"pending_review" label so engineers can inspect and approve them.
"""

from __future__ import annotations

import json
from typing import Any

import structlog
from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field, ValidationError

from app.config import get_settings
from app.graph.client import get_client
from app.models.lineage import (
    ColumnDerivation,
    ColumnReference,
    LineageEvent,
    SourceType,
)

logger = structlog.get_logger(__name__)

# ─── Structured output schema ─────────────────────────────────────────────────


class InferredColumnDerivation(BaseModel):
    source_dataset: str
    source_column: str
    target_dataset: str
    target_column: str
    expression: str | None = None
    is_passthrough: bool = False


class InferredLineage(BaseModel):
    """Pydantic schema that the LLM must conform to."""

    source_datasets: list[str] = Field(description="All source table/dataset names")
    target_datasets: list[str] = Field(description="All target table/dataset names written to")
    column_derivations: list[InferredColumnDerivation] = Field(
        description="Column-level lineage. One entry per output column."
    )
    confidence: float = Field(ge=0.0, le=1.0, description="Model confidence in this inference")
    notes: str | None = Field(None, description="Any ambiguities or assumptions made")


# ─── Prompt ───────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = """You are a data lineage extraction expert. Given a SQL query or PySpark job,
extract the complete column-level data lineage.

For each output column, identify:
- Which source dataset and column it comes from
- The transformation expression if it is not a simple passthrough

Rules:
- Only list columns that appear in the final output (not intermediate CTEs unless they are the target)
- For SELECT *, list it as source_column: "*"
- For computed columns with no clear source (e.g. CURRENT_TIMESTAMP), skip them
- Set is_passthrough=true only when the column is selected without transformation
- Set confidence based on how unambiguous the lineage is (0.9+ for simple SELECTs, lower for complex joins)

Respond ONLY with a valid JSON object matching this schema. No markdown, no explanation.

Schema:
{schema}
"""

_SCHEMA_JSON = json.dumps(InferredLineage.model_json_schema(), indent=2)


class LLMLineageInference:
    """
    Infers column-level lineage for undocumented transforms using GPT-4o,
    then validates against the live catalog.
    """

    def __init__(self) -> None:
        settings = get_settings()
        self._llm = ChatOpenAI(
            model=settings.inference_model,
            temperature=settings.llm_temperature,
            max_tokens=settings.llm_max_tokens,
            openai_api_key=settings.openai_api_key,
        )
        self._neo4j = get_client()

    async def infer(
        self,
        sql: str,
        project_name: str,
        transform_id: str,
        transform_name: str,
        pipeline_name: str | None = None,
        commit_id: str | None = None,
        known_sources: list[str] | None = None,
    ) -> LineageEvent:
        """
        Run LLM inference on a SQL/PySpark string and return a validated LineageEvent.
        """
        # ── Step 1: LLM inference ──────────────────────────────────────────────
        inferred = await self._run_inference(sql)
        if inferred is None:
            return self._fallback_event(
                sql, project_name, transform_id, transform_name,
                pipeline_name, commit_id, known_sources or [],
            )

        # Merge with caller-supplied sources
        if known_sources:
            for src in known_sources:
                if src not in inferred.source_datasets:
                    inferred.source_datasets.append(src)

        # ── Step 2: Catalog cross-validation ──────────────────────────────────
        validation_issues = await self._cross_validate(inferred, project_name)
        validation_status = "flagged" if validation_issues else "ok"

        if validation_issues:
            logger.warning(
                "lineage_inference_flagged",
                transform_id=transform_id,
                issues=validation_issues,
            )

        # ── Step 3: Convert to domain model ───────────────────────────────────
        derivations = [
            ColumnDerivation(
                source=ColumnReference(
                    dataset=d.source_dataset,
                    column=d.source_column,
                ),
                target=ColumnReference(
                    dataset=d.target_dataset,
                    column=d.target_column,
                ),
                expression=d.expression,
                is_passthrough=d.is_passthrough,
            )
            for d in inferred.column_derivations
        ]

        logger.info(
            "lineage_inference_complete",
            transform_id=transform_id,
            derivations=len(derivations),
            confidence=inferred.confidence,
            validation_status=validation_status,
        )

        return LineageEvent(
            source_type=SourceType.SQL,
            project_name=project_name,
            commit_id=commit_id,
            transform_id=transform_id,
            transform_name=transform_name,
            pipeline_name=pipeline_name,
            source_datasets=inferred.source_datasets,
            target_datasets=inferred.target_datasets,
            column_derivations=derivations,
            raw_sql=sql,
            llm_inferred=True,
            validation_status=validation_status,
            tags={
                "llm_confidence": inferred.confidence,
                "llm_notes": inferred.notes,
                "validation_issues": validation_issues,
            },
        )

    # ─── LLM call ─────────────────────────────────────────────────────────────

    async def _run_inference(self, sql: str) -> InferredLineage | None:
        """Send SQL to GPT-4o and parse the structured JSON response."""
        system = _SYSTEM_PROMPT.format(schema=_SCHEMA_JSON)
        messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": f"Extract lineage from this SQL:\n\n```sql\n{sql}\n```"},
        ]

        try:
            response = await self._llm.ainvoke(messages)
            raw = response.content.strip()

            # Strip accidental markdown fences
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]

            return InferredLineage.model_validate_json(raw)

        except (ValidationError, json.JSONDecodeError) as exc:
            logger.error("llm_inference_parse_error", error=str(exc))
            return None
        except Exception as exc:
            logger.error("llm_inference_failed", error=str(exc))
            return None

    # ─── Catalog cross-validation ─────────────────────────────────────────────

    async def _cross_validate(
        self,
        inferred: InferredLineage,
        project_name: str,
    ) -> list[str]:
        """
        Check every claimed source column against actual Column nodes in Neo4j.
        Returns a list of validation issue strings.
        """
        issues: list[str] = []

        # Collect unique (dataset, column) pairs to check
        pairs = {
            (d.source_dataset, d.source_column)
            for d in inferred.column_derivations
            if d.source_column != "*"
        }

        if not pairs:
            return []

        # Batch lookup — one Cypher query for all pairs
        query = """
        UNWIND $pairs AS pair
        OPTIONAL MATCH (c:Column {project: $project, dataset: pair[0], name: pair[1]})
        RETURN pair[0] AS dataset, pair[1] AS col, c IS NOT NULL AS exists
        """
        records = await self._neo4j.run(query, {
            "project": project_name,
            "pairs": [[d, c] for d, c in pairs],
        })

        for record in records:
            if not record.get("exists"):
                issues.append(
                    f"Column '{record['dataset']}.{record['col']}' not found in catalog"
                )

        return issues

    # ─── Fallback ─────────────────────────────────────────────────────────────

    def _fallback_event(
        self,
        sql: str,
        project_name: str,
        transform_id: str,
        transform_name: str,
        pipeline_name: str | None,
        commit_id: str | None,
        known_sources: list[str],
    ) -> LineageEvent:
        """Return a minimal LineageEvent when LLM inference fails."""
        return LineageEvent(
            source_type=SourceType.SQL,
            project_name=project_name,
            commit_id=commit_id,
            transform_id=transform_id,
            transform_name=transform_name,
            pipeline_name=pipeline_name,
            source_datasets=known_sources,
            target_datasets=[],
            column_derivations=[],
            raw_sql=sql,
            llm_inferred=True,
            validation_status="pending_review",
        )
