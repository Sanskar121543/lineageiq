"""
/api/v1/impact/* — blast radius and schema impact analysis endpoints.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.analyzers.blast_radius import BlastRadiusAnalyzer
from app.models.lineage import BlastRadiusReport, ChangeType, SchemaChange

router = APIRouter(prefix="/impact", tags=["Impact Analysis"])

_analyzer = BlastRadiusAnalyzer()


@router.post("/blast-radius", response_model=BlastRadiusReport)
async def blast_radius(change: SchemaChange) -> BlastRadiusReport:
    """
    Analyze the downstream blast radius of a proposed schema change.

    Returns a ranked list of affected assets with criticality scores,
    SLA tier breakdowns, and suggested migration actions.

    Results are cached in Redis for 15 minutes keyed by (dataset, column, change_type).
    """
    return await _analyzer.analyze(change)


@router.get("/assets/{dataset_name}")
async def get_asset_metadata(dataset_name: str):
    """
    Return metadata for a dataset node including SLA tier, owner,
    freshness cadence, and downstream fan-out count.
    """
    from app.graph.client import get_client

    client = get_client()
    query = """
    MATCH (d:Dataset {name: $name})
    OPTIONAL MATCH (d)<-[:READS_FROM|WRITES_TO]-(t:Transform)
    WITH d, count(DISTINCT t) AS transform_count
    OPTIONAL MATCH (d)<-[:READS_FROM*1..3]-(dash:Dashboard)
    WITH d, transform_count, count(DISTINCT dash) AS dashboard_count
    RETURN
        d.name            AS name,
        d.project         AS project,
        d.source_type     AS source_type,
        d.owner           AS owner,
        d.sla_tier        AS sla_tier,
        d.freshness_cadence AS freshness_cadence,
        transform_count,
        dashboard_count
    LIMIT 1
    """
    results = await client.run(query, {"name": dataset_name})
    if not results:
        raise HTTPException(status_code=404, detail=f"Dataset '{dataset_name}' not found")
    return results[0]


@router.get("/p0-dashboards")
async def list_p0_dashboards():
    """List all P0 dashboards and their immediate upstream datasets."""
    from app.graph.client import get_client

    client = get_client()
    query = """
    MATCH (dash:Dashboard {sla_tier: 'P0'})
    OPTIONAL MATCH (dash)<-[:PRODUCES|READS_FROM]-(t:Transform)-[:READS_FROM]->(d:Dataset)
    RETURN dash.name AS dashboard, dash.owner AS owner,
           collect(DISTINCT d.name) AS upstream_datasets
    ORDER BY dash.name
    """
    return await client.run(query, {})


@router.get("/orphaned-datasets")
async def find_orphaned_datasets(min_downstream: int = 5):
    """
    Find datasets with no documented owner and more than N downstream dependencies.
    These are operational risk hotspots.
    """
    from app.graph.client import get_client

    client = get_client()
    query = """
    MATCH (d:Dataset)
    WHERE d.owner IS NULL OR d.owner = ''
    MATCH (d)<-[:READS_FROM|WRITES_TO]-(t:Transform)
    WITH d, count(DISTINCT t) AS dep_count
    WHERE dep_count >= $min_downstream
    RETURN d.name AS dataset, d.project AS project,
           d.source_type AS source_type, dep_count
    ORDER BY dep_count DESC
    """
    return await client.run(query, {"min_downstream": min_downstream})


@router.get("/lineage-history/{dataset_name}")
async def lineage_history(dataset_name: str, limit: int = 10):
    """
    Return snapshot history for a dataset — useful for incident post-mortems.
    Shows what the lineage looked like at each commit.
    """
    from app.graph.client import get_client

    client = get_client()
    query = """
    MATCH (s:Snapshot)
    WHERE s.project IS NOT NULL
    OPTIONAL MATCH (d:Dataset {name: $name})
    WHERE d.commit_id = s.commit_id
    RETURN s.commit_id AS commit_id, s.created_at AS created_at,
           s.source_type AS source_type, s.project AS project
    ORDER BY s.created_at DESC
    LIMIT $limit
    """
    return await client.run(query, {"name": dataset_name, "limit": limit})
