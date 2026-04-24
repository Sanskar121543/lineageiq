"""
Integration tests for the FastAPI routes.

These tests require a live Neo4j instance (provided by docker compose).
Run via: make test-integration

They use httpx.AsyncClient against the real FastAPI app and a real
(but isolated) Neo4j database. Each test class manages its own cleanup
to leave the database in a consistent state.
"""

from __future__ import annotations

import pytest
import httpx
from httpx import AsyncClient

# ─── App fixture ──────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
async def client():
    """Async HTTP client pointed at the running FastAPI app."""
    from app.main import app
    async with AsyncClient(app=app, base_url="http://test") as c:
        yield c


@pytest.fixture(autouse=True, scope="module")
async def bootstrap():
    """Ensure Neo4j schema is bootstrapped before integration tests run."""
    from app.graph.client import get_client
    from app.graph.schema import bootstrap_schema
    neo4j = get_client()
    await neo4j.connect()
    await bootstrap_schema()
    yield
    # Cleanup: remove test nodes
    await neo4j.run(
        "MATCH (n) WHERE n.project = 'integration_test' DETACH DELETE n"
    )


# ─── Health check ─────────────────────────────────────────────────────────────


class TestHealth:
    async def test_healthz(self, client: AsyncClient):
        r = await client.get("/healthz")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    async def test_readyz_with_neo4j(self, client: AsyncClient):
        r = await client.get("/readyz")
        # Either 200 (connected) or 503 (Neo4j not running in this environment)
        assert r.status_code in (200, 503)


# ─── Lineage ingestion ────────────────────────────────────────────────────────


class TestLineageIngestion:
    async def test_ingest_sql_queues_task(self, client: AsyncClient):
        r = await client.post("/api/v1/lineage/ingest/sql", json={
            "sql": "SELECT id, name FROM users",
            "project_name": "integration_test",
            "target_dataset": "user_dim",
            "source_datasets": ["users"],
            "use_llm_fallback": False,
        })
        assert r.status_code == 202
        body = r.json()
        assert "task_id" in body
        assert body["status"] == "queued"

    async def test_ingest_sql_invalid_body_rejected(self, client: AsyncClient):
        r = await client.post("/api/v1/lineage/ingest/sql", json={
            "project_name": "integration_test",
            # missing required 'sql' field
        })
        assert r.status_code == 422

    async def test_get_lineage_graph(self, client: AsyncClient):
        r = await client.get("/api/v1/lineage/graph")
        assert r.status_code == 200
        body = r.json()
        assert "nodes" in body
        assert "edges" in body
        assert isinstance(body["nodes"], list)
        assert isinstance(body["edges"], list)

    async def test_get_lineage_graph_filtered_by_project(self, client: AsyncClient):
        r = await client.get("/api/v1/lineage/graph", params={"project": "integration_test"})
        assert r.status_code == 200

    async def test_list_datasets(self, client: AsyncClient):
        r = await client.get("/api/v1/lineage/datasets")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_task_status_unknown_id(self, client: AsyncClient):
        r = await client.get("/api/v1/lineage/tasks/nonexistent-task-id-00000")
        assert r.status_code == 200
        # Celery returns PENDING for unknown task IDs
        assert r.json()["status"] in ("PENDING", "FAILURE")


# ─── Impact analysis ──────────────────────────────────────────────────────────


class TestImpactAnalysis:
    async def test_blast_radius_nonexistent_dataset(self, client: AsyncClient):
        """Blast radius on a nonexistent dataset should return empty results, not 500."""
        r = await client.post("/api/v1/impact/blast-radius", json={
            "dataset": "nonexistent_table_xyz",
            "column": "some_col",
            "change_type": "rename_column",
        })
        assert r.status_code == 200
        body = r.json()
        assert body["total_affected"] == 0
        assert isinstance(body["affected_assets"], list)

    async def test_blast_radius_missing_dataset_field(self, client: AsyncClient):
        r = await client.post("/api/v1/impact/blast-radius", json={
            "change_type": "drop_column",
            # missing 'dataset'
        })
        assert r.status_code == 422

    async def test_blast_radius_invalid_change_type(self, client: AsyncClient):
        r = await client.post("/api/v1/impact/blast-radius", json={
            "dataset": "orders",
            "change_type": "teleport_column",  # not a valid ChangeType
        })
        assert r.status_code == 422

    async def test_list_p0_dashboards(self, client: AsyncClient):
        r = await client.get("/api/v1/impact/p0-dashboards")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_orphaned_datasets(self, client: AsyncClient):
        r = await client.get("/api/v1/impact/orphaned-datasets")
        assert r.status_code == 200
        assert isinstance(r.json(), list)


# ─── NL Query ─────────────────────────────────────────────────────────────────


class TestNLQuery:
    async def test_suggestions_returns_list(self, client: AsyncClient):
        r = await client.get("/api/v1/query/suggestions")
        assert r.status_code == 200
        body = r.json()
        assert "suggestions" in body
        assert len(body["suggestions"]) >= 5

    async def test_raw_cypher_read_query(self, client: AsyncClient):
        r = await client.post("/api/v1/query/cypher", json={
            "cypher": "MATCH (n:Dataset) RETURN n.name AS name LIMIT 5"
        })
        assert r.status_code == 200
        body = r.json()
        assert "results" in body
        assert "count" in body

    async def test_raw_cypher_write_blocked(self, client: AsyncClient):
        r = await client.post("/api/v1/query/cypher", json={
            "cypher": "CREATE (n:Dataset {name: 'injected'})"
        })
        assert r.status_code == 403

    async def test_raw_cypher_delete_blocked(self, client: AsyncClient):
        r = await client.post("/api/v1/query/cypher", json={
            "cypher": "MATCH (n) DETACH DELETE n"
        })
        assert r.status_code == 403

    async def test_raw_cypher_empty_blocked(self, client: AsyncClient):
        r = await client.post("/api/v1/query/cypher", json={"cypher": ""})
        assert r.status_code == 400

    async def test_nl_query_missing_question(self, client: AsyncClient):
        r = await client.post("/api/v1/query/nl", json={})
        assert r.status_code == 422


# ─── Stats ────────────────────────────────────────────────────────────────────


class TestStats:
    async def test_graph_stats_shape(self, client: AsyncClient):
        r = await client.get("/api/v1/stats")
        assert r.status_code == 200
        body = r.json()
        assert "nodes" in body
        assert "relationships" in body
        assert isinstance(body["nodes"], dict)
        assert isinstance(body["relationships"], dict)
