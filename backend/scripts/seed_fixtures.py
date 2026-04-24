#!/usr/bin/env python3
"""
Seed the Neo4j graph with a realistic sample lineage graph for demo/testing.

Creates a full data stack lineage:
  raw.events (Kafka) → spark_session_etl → analytics.sessions
  analytics.sessions + raw.users → dbt:user_activity → reporting.user_activity
  reporting.user_activity → revenue_dashboard (P0)
  analytics.sessions → ml_feature:session_duration
  raw.orders → dbt:orders → dbt:revenue_fact → finance_dashboard (P0)
  finance_dashboard feeds 2 ML features

Run with:
    python scripts/seed_fixtures.py
or
    make seed
"""

import asyncio
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from app.graph.client import get_client
from app.graph.writer import GraphWriter
from app.graph.schema import bootstrap_schema
from app.models.lineage import (
    ColumnDerivation,
    ColumnReference,
    LineageEvent,
    SourceType,
)


SAMPLE_EVENTS: list[LineageEvent] = [

    # ── Kafka source: raw events topic ────────────────────────────────────────
    LineageEvent(
        source_type=SourceType.KAFKA,
        project_name="platform",
        commit_id="seed_v1",
        transform_id="kafka_schema_raw-events-value_1",
        transform_name="schema:raw-events-value",
        pipeline_name="kafka",
        source_datasets=[],
        target_datasets=["kafka.raw-events"],
        column_derivations=[
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="user_id"),
                target=ColumnReference(dataset="kafka.raw-events", column="user_id"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="session_id"),
                target=ColumnReference(dataset="kafka.raw-events", column="session_id"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="event_type"),
                target=ColumnReference(dataset="kafka.raw-events", column="event_type"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="timestamp"),
                target=ColumnReference(dataset="kafka.raw-events", column="timestamp"),
                is_passthrough=True,
            ),
        ],
        tags={"kafka_topic": "raw-events"},
    ),

    # ── Spark: session ETL (Kafka → analytics.sessions) ───────────────────────
    LineageEvent(
        source_type=SourceType.SPARK,
        project_name="platform",
        commit_id="seed_v1",
        transform_id="spark_session_etl_job_001",
        transform_name="session_etl",
        pipeline_name="spark_streaming",
        source_datasets=["kafka.raw-events"],
        target_datasets=["analytics.sessions"],
        column_derivations=[
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="session_id"),
                target=ColumnReference(dataset="analytics.sessions", column="session_id"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="user_id"),
                target=ColumnReference(dataset="analytics.sessions", column="user_id"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="timestamp"),
                target=ColumnReference(dataset="analytics.sessions", column="session_start"),
                expression="MIN(timestamp) OVER (PARTITION BY session_id)",
                is_passthrough=False,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="timestamp"),
                target=ColumnReference(dataset="analytics.sessions", column="session_end"),
                expression="MAX(timestamp) OVER (PARTITION BY session_id)",
                is_passthrough=False,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="kafka.raw-events", column="timestamp"),
                target=ColumnReference(dataset="analytics.sessions", column="duration_seconds"),
                expression="DATEDIFF(session_end, session_start)",
                is_passthrough=False,
            ),
        ],
    ),

    # ── dbt: orders model ─────────────────────────────────────────────────────
    LineageEvent(
        source_type=SourceType.DBT,
        project_name="jaffle_shop",
        commit_id="seed_v1",
        transform_id="model.jaffle_shop.orders",
        transform_name="orders",
        pipeline_name="jaffle_shop",
        source_datasets=["raw.raw_orders", "raw.raw_customers"],
        target_datasets=["public.orders"],
        column_derivations=[
            ColumnDerivation(
                source=ColumnReference(dataset="raw.raw_orders", column="id"),
                target=ColumnReference(dataset="public.orders", column="order_id"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="raw.raw_orders", column="customer_id"),
                target=ColumnReference(dataset="public.orders", column="customer_id"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="raw.raw_orders", column="amount"),
                target=ColumnReference(dataset="public.orders", column="amount"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="raw.raw_orders", column="status"),
                target=ColumnReference(dataset="public.orders", column="status"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="raw.raw_customers", column="name"),
                target=ColumnReference(dataset="public.orders", column="customer_name"),
                is_passthrough=True,
            ),
        ],
        tags={"dbt_resource_type": "model", "dbt_materialized": "table"},
    ),

    # ── dbt: revenue_fact ─────────────────────────────────────────────────────
    LineageEvent(
        source_type=SourceType.DBT,
        project_name="jaffle_shop",
        commit_id="seed_v1",
        transform_id="model.jaffle_shop.revenue_fact",
        transform_name="revenue_fact",
        pipeline_name="jaffle_shop",
        source_datasets=["public.orders"],
        target_datasets=["public.revenue_fact"],
        column_derivations=[
            ColumnDerivation(
                source=ColumnReference(dataset="public.orders", column="customer_id"),
                target=ColumnReference(dataset="public.revenue_fact", column="customer_id"),
                is_passthrough=True,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="public.orders", column="amount"),
                target=ColumnReference(dataset="public.revenue_fact", column="total_revenue"),
                expression="SUM(amount)",
                is_passthrough=False,
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="public.orders", column="order_id"),
                target=ColumnReference(dataset="public.revenue_fact", column="order_count"),
                expression="COUNT(DISTINCT order_id)",
                is_passthrough=False,
            ),
        ],
        tags={"dbt_resource_type": "model", "dbt_materialized": "table"},
    ),
]

# ── Dashboard and MLFeature nodes (written directly as Cypher) ────────────────

ASSET_CYPHER = [
    # Dashboards
    """
    MERGE (d:Dashboard {name: 'revenue_dashboard'})
    SET d.sla_tier = 'P0',
        d.owner = 'data-platform-team',
        d.freshness_cadence = 'hourly',
        d.project = 'platform'
    """,
    """
    MERGE (d:Dashboard {name: 'finance_executive_dashboard'})
    SET d.sla_tier = 'P0',
        d.owner = 'finance-team',
        d.freshness_cadence = 'daily',
        d.project = 'platform'
    """,
    """
    MERGE (d:Dashboard {name: 'product_analytics_dashboard'})
    SET d.sla_tier = 'P1',
        d.owner = 'product-team',
        d.freshness_cadence = 'daily',
        d.project = 'platform'
    """,
    # ML Features
    """
    MERGE (m:MLFeature {name: 'session_duration_feature', project: 'platform'})
    SET m.sla_tier = 'P1',
        m.freshness_cadence = 'hourly'
    """,
    """
    MERGE (m:MLFeature {name: 'customer_ltv_feature', project: 'jaffle_shop'})
    SET m.sla_tier = 'P0',
        m.freshness_cadence = 'daily'
    """,
    # Dashboard → Dataset READS_FROM edges
    """
    MATCH (d:Dashboard {name: 'revenue_dashboard'})
    MATCH (ds:Dataset {name: 'public.revenue_fact'})
    MERGE (d)-[:READS_FROM]->(ds)
    """,
    """
    MATCH (d:Dashboard {name: 'finance_executive_dashboard'})
    MATCH (ds:Dataset {name: 'public.revenue_fact'})
    MERGE (d)-[:READS_FROM]->(ds)
    """,
    """
    MATCH (d:Dashboard {name: 'product_analytics_dashboard'})
    MATCH (ds:Dataset {name: 'analytics.sessions'})
    MERGE (d)-[:READS_FROM]->(ds)
    """,
    # MLFeature PRODUCES edges
    """
    MATCH (m:MLFeature {name: 'session_duration_feature'})
    MATCH (ds:Dataset {name: 'analytics.sessions'})
    MERGE (ds)-[:PRODUCES]->(m)
    """,
    """
    MATCH (m:MLFeature {name: 'customer_ltv_feature'})
    MATCH (ds:Dataset {name: 'public.revenue_fact'})
    MERGE (ds)-[:PRODUCES]->(m)
    """,
]


async def seed():
    client = get_client()
    await client.connect()
    await bootstrap_schema()

    writer = GraphWriter()

    print("Writing lineage events…")
    for event in SAMPLE_EVENTS:
        snapshot = await writer.write_event(event)
        print(f"  ✓ {event.transform_name} ({snapshot.node_count} nodes, {snapshot.relationship_count} rels)")

    print("Writing asset nodes and edges…")
    for cypher in ASSET_CYPHER:
        await client.run(cypher.strip())
    print(f"  ✓ {len(ASSET_CYPHER)} asset operations complete")

    # Summary
    count = await client.run("MATCH (n) RETURN count(n) AS total")
    rels  = await client.run("MATCH ()-[r]->() RETURN count(r) AS total")
    print(f"\n✓ Seed complete: {count[0]['total']} nodes, {rels[0]['total']} relationships")


if __name__ == "__main__":
    asyncio.run(seed())
