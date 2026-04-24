"""
LineageIQ FastAPI application.

Startup sequence:
  1. Connect to Neo4j and bootstrap schema constraints + indexes.
  2. Register Prometheus metrics middleware.
  3. Mount all route modules under /api/v1.
  4. Expose /healthz and /readyz endpoints for container health checks.
"""

from __future__ import annotations

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

from app.api.routes import impact, lineage, query
from app.config import get_settings
from app.graph.client import get_client
from app.graph.schema import bootstrap_schema

logger = structlog.get_logger(__name__)
settings = get_settings()

app = FastAPI(
    title="LineageIQ",
    description="Column-level data lineage & schema impact analysis engine",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# ─── CORS ─────────────────────────────────────────────────────────────────────

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Prometheus ───────────────────────────────────────────────────────────────

Instrumentator().instrument(app).expose(app, endpoint="/metrics")

# ─── Routes ───────────────────────────────────────────────────────────────────

app.include_router(lineage.router, prefix="/api/v1")
app.include_router(impact.router, prefix="/api/v1")
app.include_router(query.router, prefix="/api/v1")


# ─── Lifecycle ────────────────────────────────────────────────────────────────

@app.on_event("startup")
async def startup() -> None:
    client = get_client()
    await client.connect()
    await bootstrap_schema()
    logger.info("lineageiq_started", environment=settings.environment)


@app.on_event("shutdown")
async def shutdown() -> None:
    client = get_client()
    await client.close()
    logger.info("lineageiq_stopped")


# ─── Health checks ────────────────────────────────────────────────────────────

@app.get("/healthz", tags=["Health"])
async def health():
    """Liveness check — returns 200 if the process is running."""
    return {"status": "ok"}


@app.get("/readyz", tags=["Health"])
async def readiness():
    """
    Readiness check — verifies Neo4j connectivity.
    Container orchestrators use this to gate traffic.
    """
    try:
        client = get_client()
        await client.run("RETURN 1 AS ping")
        return {"status": "ready", "neo4j": "connected"}
    except Exception as exc:
        from fastapi import HTTPException
        raise HTTPException(status_code=503, detail=f"Neo4j not ready: {exc}")


@app.get("/api/v1/stats", tags=["Stats"])
async def graph_stats():
    """Return high-level graph statistics."""
    client = get_client()
    query = """
    MATCH (n)
    RETURN labels(n)[0] AS node_type, count(*) AS count
    ORDER BY count DESC
    """
    node_counts = await client.run(query, {})

    edge_query = """
    MATCH ()-[r]->()
    RETURN type(r) AS rel_type, count(*) AS count
    ORDER BY count DESC
    """
    edge_counts = await client.run(edge_query, {})

    return {
        "nodes": {r["node_type"]: r["count"] for r in node_counts},
        "relationships": {r["rel_type"]: r["count"] for r in edge_counts},
    }
