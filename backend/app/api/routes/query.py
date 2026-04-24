"""
/api/v1/query/* — Text2Cypher natural language query interface.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from app.llm.text2cypher import Text2CypherEngine
from app.models.lineage import NLQuery, QueryResult

router = APIRouter(prefix="/query", tags=["Natural Language Query"])

_engine = Text2CypherEngine()


@router.post("/nl", response_model=QueryResult)
async def natural_language_query(query: NLQuery) -> QueryResult:
    """
    Convert a natural language question to Cypher and execute it against
    the lineage graph.

    The generated Cypher is validated against the known schema before
    execution. If validation fails, the response includes the validation
    errors and no graph query is run.

    Example questions:
    - "Which ML features depend on the orders table?"
    - "Show every pipeline that writes to a table used by the revenue dashboard"
    - "Find all datasets with no owner and more than 5 downstream dependencies"
    """
    return await _engine.query(query)


@router.post("/cypher")
async def raw_cypher_query(query: dict):
    """
    Execute a raw read-only Cypher query directly.
    Write operations (CREATE, MERGE, DELETE, SET) are blocked.
    """
    from app.graph.client import get_client
    from app.llm.text2cypher import VALID_NODE_LABELS, VALID_RELATIONSHIP_TYPES
    import re

    cypher = query.get("cypher", "").strip()
    if not cypher:
        raise HTTPException(status_code=400, detail="No Cypher provided")

    write_pattern = re.compile(r"\b(CREATE|MERGE|DELETE|DETACH|SET|REMOVE|DROP)\b", re.IGNORECASE)
    if write_pattern.search(cypher):
        raise HTTPException(status_code=403, detail="Write operations are not permitted")

    client = get_client()
    try:
        results = await client.run(cypher, query.get("parameters", {}))
        return {"results": results, "count": len(results)}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"Cypher error: {exc}")


@router.get("/suggestions")
async def query_suggestions():
    """
    Return a set of pre-built example queries to seed the UI query bar.
    """
    return {
        "suggestions": [
            "Show every pipeline that writes to a table used by the revenue dashboard",
            "Which ML features would be affected if I drop user_events.session_id?",
            "Find all datasets with no documented owner and more than 5 downstream dependencies",
            "What does orders.customer_id flow into?",
            "List all dbt models that read from the raw_events source",
            "Which transforms have LLM-inferred lineage flagged for review?",
            "Find columns derived from more than 3 source columns",
            "What Kafka topics feed into the user_features ML feature?",
            "How many P0 dashboards depend on the payments pipeline?",
            "Show the full lineage graph for the mrr_monthly table",
        ]
    }
