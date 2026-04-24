"""
Celery application factory.

Parser workers are organized into named queues by source type so they
can be scaled independently:
  - dbt:   lightweight I/O (JSON file reads)
  - spark: CPU-heavy (AST traversal on large plan JSONs)
  - sql:   CPU-moderate (sqlglot parsing + LLM inference)
  - kafka: I/O-bound (HTTP calls to Schema Registry)
  - default: blast radius re-analysis, CDC events
"""

from celery import Celery

from app.config import get_settings

settings = get_settings()

celery_app = Celery(
    "lineageiq",
    broker=settings.celery_broker_url,
    backend=settings.celery_result_backend,
    include=["app.workers.tasks"],
)

celery_app.conf.update(
    # Serialization
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,

    # Routing: each parser type gets its own queue
    task_routes={
        "app.workers.tasks.parse_dbt_manifest":    {"queue": "dbt"},
        "app.workers.tasks.parse_spark_plan":       {"queue": "spark"},
        "app.workers.tasks.parse_sql":              {"queue": "sql"},
        "app.workers.tasks.parse_kafka_schemas":    {"queue": "kafka"},
        "app.workers.tasks.infer_llm_lineage":      {"queue": "sql"},
        "app.workers.tasks.rerun_blast_radius":     {"queue": "default"},
        "app.workers.tasks.process_cdc_event":      {"queue": "default"},
    },

    # Retry policy
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    task_max_retries=3,

    # Worker tuning
    worker_prefetch_multiplier=1,   # prevent starving slow tasks
    task_soft_time_limit=120,       # 2 min soft limit
    task_time_limit=180,            # 3 min hard kill

    # Result expiry
    result_expires=3600,            # 1 hour

    # Beat schedule for periodic re-ingestion (optional)
    beat_schedule={},
)
