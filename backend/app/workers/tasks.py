"""
Celery tasks. Each task wraps one parser or analyzer invocation.

All tasks follow the same pattern:
  1. Parse config / validate input
  2. Instantiate the parser or analyzer
  3. Run the parsing / analysis
  4. Write results to Neo4j via GraphWriter
  5. Return a summary dict (serializable)

Tasks are idempotent — re-running with the same input is safe because
the graph writer uses MERGE everywhere.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import structlog

from app.workers.celery_app import celery_app

logger = structlog.get_logger(__name__)


def _run_async(coro):
    """Run a coroutine in a new event loop (Celery workers are sync)."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ─── dbt ──────────────────────────────────────────────────────────────────────

@celery_app.task(name="app.workers.tasks.parse_dbt_manifest", bind=True, max_retries=3)
def parse_dbt_manifest(self, config: dict[str, Any]) -> dict:
    """
    Parse a dbt manifest.json and write lineage to Neo4j.

    config keys:
        manifest_path: str
        project_name:  str
        commit_id:     str  (optional)
    """
    from app.graph.writer import GraphWriter
    from app.parsers.dbt_parser import DbtManifestParser

    try:
        parser = DbtManifestParser()
        events = parser.parse(config)

        writer = GraphWriter()
        snapshots = [_run_async(writer.write_event(e)) for e in events]

        return {
            "status": "ok",
            "events_parsed": len(events),
            "project_name": config.get("project_name"),
        }
    except Exception as exc:
        logger.error("parse_dbt_manifest_failed", error=str(exc), config=config)
        raise self.retry(exc=exc, countdown=10)


# ─── Spark ────────────────────────────────────────────────────────────────────

@celery_app.task(name="app.workers.tasks.parse_spark_plan", bind=True, max_retries=3)
def parse_spark_plan(self, config: dict[str, Any]) -> dict:
    """
    Parse a Spark LogicalPlan JSON and write lineage to Neo4j.

    config keys:
        plan:          dict   – parsed plan JSON
        project_name:  str
        pipeline_name: str
        job_id:        str
        commit_id:     str  (optional)
    """
    from app.graph.writer import GraphWriter
    from app.parsers.spark_parser import SparkPlanParser

    try:
        parser = SparkPlanParser()
        events = parser.parse(config)

        writer = GraphWriter()
        [_run_async(writer.write_event(e)) for e in events]

        return {
            "status": "ok",
            "events_parsed": len(events),
            "job_id": config.get("job_id"),
        }
    except Exception as exc:
        logger.error("parse_spark_plan_failed", error=str(exc))
        raise self.retry(exc=exc, countdown=15)


# ─── SQL ──────────────────────────────────────────────────────────────────────

@celery_app.task(name="app.workers.tasks.parse_sql", bind=True, max_retries=3)
def parse_sql(self, config: dict[str, Any]) -> dict:
    """
    Parse a SQL string and write lineage to Neo4j.
    Falls back to LLM inference if sqlglot yields no derivations.

    config keys:
        sql:              str
        project_name:     str
        target_dataset:   str
        source_datasets:  list[str]
        dialect:          str  (optional, default: 'default')
        transform_id:     str  (optional)
        commit_id:        str  (optional)
        use_llm_fallback: bool (optional, default: true)
    """
    from app.graph.writer import GraphWriter
    from app.parsers.sql_parser import SqlGlotParser

    try:
        parser = SqlGlotParser()
        events = parser.parse(config)

        # LLM fallback: if sqlglot produced no column derivations
        use_llm = config.get("use_llm_fallback", True)
        if use_llm and (not events or not events[0].column_derivations):
            infer_task = infer_llm_lineage.delay(config)
            return {
                "status": "llm_fallback_queued",
                "inference_task_id": infer_task.id,
            }

        writer = GraphWriter()
        [_run_async(writer.write_event(e)) for e in events]

        return {"status": "ok", "events_parsed": len(events)}
    except Exception as exc:
        logger.error("parse_sql_failed", error=str(exc))
        raise self.retry(exc=exc, countdown=10)


# ─── Kafka ────────────────────────────────────────────────────────────────────

@celery_app.task(name="app.workers.tasks.parse_kafka_schemas", bind=True, max_retries=3)
def parse_kafka_schemas(self, config: dict[str, Any]) -> dict:
    """
    Read all schemas from Confluent Schema Registry and write to Neo4j.

    config keys:
        schema_registry_url: str
        project_name:        str
        topic_filter:        str  (optional regex)
        commit_id:           str  (optional)
    """
    from app.graph.writer import GraphWriter
    from app.parsers.kafka_parser import KafkaSchemaParser

    try:
        parser = KafkaSchemaParser()
        events = parser.parse(config)

        writer = GraphWriter()
        [_run_async(writer.write_event(e)) for e in events]

        return {"status": "ok", "topics_parsed": len(events)}
    except Exception as exc:
        logger.error("parse_kafka_schemas_failed", error=str(exc))
        raise self.retry(exc=exc, countdown=10)


# ─── LLM inference ────────────────────────────────────────────────────────────

@celery_app.task(name="app.workers.tasks.infer_llm_lineage", bind=True, max_retries=2)
def infer_llm_lineage(self, config: dict[str, Any]) -> dict:
    """
    Run LLM lineage inference on a SQL string and write to Neo4j.
    Used as fallback when sqlglot yields no column derivations.
    """
    from app.graph.writer import GraphWriter
    from app.llm.lineage_inference import LLMLineageInference

    async def _run():
        inference = LLMLineageInference()
        event = await inference.infer(
            sql=config["sql"],
            project_name=config["project_name"],
            transform_id=config.get("transform_id", "llm_inferred"),
            transform_name=config.get("target_dataset", "unknown"),
            pipeline_name=config.get("pipeline_name"),
            commit_id=config.get("commit_id"),
            known_sources=config.get("source_datasets", []),
        )
        writer = GraphWriter()
        snapshot = await writer.write_event(event)
        return event.validation_status

    try:
        status = _run_async(_run())
        return {"status": "ok", "validation_status": status}
    except Exception as exc:
        logger.error("infer_llm_lineage_failed", error=str(exc))
        raise self.retry(exc=exc, countdown=30)


# ─── Blast radius re-analysis ─────────────────────────────────────────────────

@celery_app.task(name="app.workers.tasks.rerun_blast_radius")
def rerun_blast_radius(dataset: str, column: str | None = None) -> dict:
    """
    Invalidate the Redis cache and rerun blast radius analysis for a dataset.
    Triggered after a CDC event modifies schema metadata.
    """
    import redis as sync_redis
    from app.config import get_settings

    settings = get_settings()
    r = sync_redis.from_url(settings.redis_url)

    # Delete all cached blast radius keys for this dataset
    pattern = "blast_radius:*"
    keys = r.keys(pattern)
    if keys:
        r.delete(*keys)

    logger.info("blast_radius_cache_invalidated", dataset=dataset, keys_deleted=len(keys))
    return {"status": "ok", "cache_keys_deleted": len(keys)}


# ─── Debezium CDC event processor ────────────────────────────────────────────

@celery_app.task(name="app.workers.tasks.process_cdc_event")
def process_cdc_event(event: dict[str, Any]) -> dict:
    """
    Process a Debezium CDC event (schema change from PostgreSQL).

    Expected event format:
    {
        "op": "c" | "u" | "d",
        "source": {"table": "...", "schema": "..."},
        "after": {"ddl": "ALTER TABLE ...", "dataset": "...", "column": "..."}
    }

    Triggers:
    1. Graph update for the changed schema entity.
    2. Blast radius cache invalidation.
    """
    op = event.get("op")
    after = event.get("after", {})
    source = event.get("source", {})

    dataset = after.get("dataset") or source.get("table", "unknown")
    column = after.get("column")
    ddl = after.get("ddl", "")

    logger.info(
        "cdc_event_received",
        op=op,
        dataset=dataset,
        column=column,
        ddl=ddl[:100],
    )

    # Invalidate blast radius cache
    rerun_blast_radius.delay(dataset=dataset, column=column)

    # If it's a DDL change (ALTER TABLE), ingest via SQL parser
    if ddl and "ALTER" in ddl.upper():
        parse_sql.delay({
            "sql": ddl,
            "project_name": source.get("db", "cdc"),
            "target_dataset": dataset,
            "source_datasets": [],
            "use_llm_fallback": False,
        })

    return {"status": "ok", "dataset": dataset, "op": op}
