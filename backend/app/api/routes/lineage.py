"""
/api/v1/lineage/* — lineage ingestion endpoints.

All ingestion is async: the endpoint validates the request, dispatches a
Celery task, and returns a task ID. Clients poll /tasks/{task_id} for status.
"""

from __future__ import annotations

from typing import Any

from celery.result import AsyncResult
from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from app.workers.celery_app import celery_app
from app.workers.tasks import (
    parse_dbt_manifest,
    parse_kafka_schemas,
    parse_spark_plan,
    parse_sql,
)

router = APIRouter(prefix="/lineage", tags=["Lineage Ingestion"])


# ─── Request models ───────────────────────────────────────────────────────────

class DbtIngestRequest(BaseModel):
    manifest_path: str = Field(..., description="Absolute path to dbt manifest.json")
    project_name: str
    commit_id: str | None = None


class SparkIngestRequest(BaseModel):
    plan: dict[str, Any] = Field(..., description="Parsed Spark LogicalPlan JSON")
    project_name: str
    pipeline_name: str = "spark_job"
    job_id: str = "unknown"
    commit_id: str | None = None


class SqlIngestRequest(BaseModel):
    sql: str = Field(..., description="Raw SQL string to parse")
    project_name: str
    target_dataset: str
    source_datasets: list[str] = Field(default_factory=list)
    dialect: str = "default"
    transform_id: str | None = None
    commit_id: str | None = None
    use_llm_fallback: bool = True


class KafkaIngestRequest(BaseModel):
    schema_registry_url: str
    project_name: str
    topic_filter: str | None = None
    commit_id: str | None = None


class TaskResponse(BaseModel):
    task_id: str
    status: str = "queued"


class TaskStatusResponse(BaseModel):
    task_id: str
    status: str
    result: dict | None = None


# ─── Endpoints ────────────────────────────────────────────────────────────────

@router.post("/ingest/dbt", response_model=TaskResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_dbt(request: DbtIngestRequest) -> TaskResponse:
    """
    Ingest a dbt project by parsing its manifest.json.
    Returns a task ID to poll for completion.
    """
    task = parse_dbt_manifest.delay(request.model_dump())
    return TaskResponse(task_id=task.id)


@router.post("/ingest/spark", response_model=TaskResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_spark(request: SparkIngestRequest) -> TaskResponse:
    """Ingest a Spark job's LogicalPlan JSON."""
    task = parse_spark_plan.delay(request.model_dump())
    return TaskResponse(task_id=task.id)


@router.post("/ingest/sql", response_model=TaskResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_sql(request: SqlIngestRequest) -> TaskResponse:
    """Parse a raw SQL string. Falls back to LLM inference if sqlglot yields no derivations."""
    task = parse_sql.delay(request.model_dump())
    return TaskResponse(task_id=task.id)


@router.post("/ingest/kafka", response_model=TaskResponse, status_code=status.HTTP_202_ACCEPTED)
async def ingest_kafka(request: KafkaIngestRequest) -> TaskResponse:
    """Read all schemas from a Confluent Schema Registry instance."""
    task = parse_kafka_schemas.delay(request.model_dump())
    return TaskResponse(task_id=task.id)


@router.get("/tasks/{task_id}", response_model=TaskStatusResponse)
async def get_task_status(task_id: str) -> TaskStatusResponse:
    """Poll a parse task for completion status."""
    result = AsyncResult(task_id, app=celery_app)
    state = result.state

    task_result = None
    if state == "SUCCESS":
        task_result = result.result
    elif state == "FAILURE":
        task_result = {"error": str(result.result)}

    return TaskStatusResponse(task_id=task_id, status=state, result=task_result)


@router.get("/graph")
async def get_lineage_graph(
    project: str | None = None,
    limit: int = 500,
):
    """
    Return the full lineage graph for a project as nodes + edges,
    suitable for rendering in React Flow.
    """
    from app.graph.client import get_client

    client = get_client()

    where = "WHERE d.project = $project" if project else ""
    params = {"project": project, "limit": limit} if project else {"limit": limit}

    nodes_query = f"""
    MATCH (n)
    WHERE n:Dataset OR n:Column OR n:Transform OR n:Dashboard OR n:MLFeature OR n:Pipeline
    {where.replace('d.project', 'n.project')}
    RETURN elementId(n) AS id, labels(n)[0] AS type, properties(n) AS props
    LIMIT $limit
    """

    edges_query = f"""
    MATCH (a)-[r]->(b)
    WHERE (a:Dataset OR a:Column OR a:Transform OR a:Dashboard OR a:MLFeature OR a:Pipeline)
      AND (b:Dataset OR b:Column OR b:Transform OR b:Dashboard OR b:MLFeature OR b:Pipeline)
    RETURN elementId(a) AS source, elementId(b) AS target, type(r) AS rel_type,
           properties(r) AS props
    LIMIT $limit
    """

    nodes = await client.run(nodes_query, params)
    edges = await client.run(edges_query, params)

    return {
        "nodes": [{"id": n["id"], "type": n["type"], "data": n["props"]} for n in nodes],
        "edges": [
            {
                "source": e["source"],
                "target": e["target"],
                "type": e["rel_type"],
                "data": e["props"],
            }
            for e in edges
        ],
    }


@router.get("/datasets")
async def list_datasets(project: str | None = None, source_type: str | None = None):
    """List all tracked datasets with optional filtering."""
    from app.graph.client import get_client

    client = get_client()
    conditions = []
    params = {}

    if project:
        conditions.append("d.project = $project")
        params["project"] = project
    if source_type:
        conditions.append("d.source_type = $source_type")
        params["source_type"] = source_type

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
    MATCH (d:Dataset) {where}
    OPTIONAL MATCH (d)<-[:READS_FROM|WRITES_TO]-(t:Transform)
    WITH d, count(DISTINCT t) AS transform_count
    RETURN d.name AS name, d.project AS project, d.source_type AS source_type,
           transform_count, d.created_at AS created_at
    ORDER BY transform_count DESC
    """

    return await client.run(query, params)


@router.get("/datasets/{dataset_name}/columns")
async def get_dataset_columns(dataset_name: str, project: str | None = None):
    """List all columns for a dataset with their downstream derivation counts."""
    from app.graph.client import get_client

    client = get_client()
    params: dict = {"dataset": dataset_name}
    project_filter = "AND c.project = $project" if project else ""
    if project:
        params["project"] = project

    query = f"""
    MATCH (c:Column {{dataset: $dataset}}) {project_filter}
    OPTIONAL MATCH (c)<-[:DERIVES_FROM]-(downstream:Column)
    RETURN c.name AS column, count(DISTINCT downstream) AS downstream_count
    ORDER BY downstream_count DESC
    """
    return await client.run(query, params)
