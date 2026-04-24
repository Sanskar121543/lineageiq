"""
Shared pytest fixtures for LineageIQ test suite.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

# ─── Sample data factories ────────────────────────────────────────────────────


@pytest.fixture
def sample_dbt_manifest() -> dict:
    """Minimal dbt manifest.json structure for parser tests."""
    return {
        "metadata": {"dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v9/manifest.json"},
        "nodes": {
            "model.jaffle_shop.orders": {
                "name": "orders",
                "alias": "orders",
                "schema": "public",
                "resource_type": "model",
                "package_name": "jaffle_shop",
                "fqn": ["jaffle_shop", "orders"],
                "config": {"materialized": "table", "enabled": True},
                "depends_on": {
                    "nodes": ["source.jaffle_shop.raw.raw_orders", "model.jaffle_shop.customers"]
                },
                "compiled_sql": """
                    SELECT
                        o.id AS order_id,
                        o.customer_id,
                        c.name AS customer_name,
                        o.amount,
                        o.status,
                        CURRENT_TIMESTAMP AS loaded_at
                    FROM raw.raw_orders o
                    JOIN public.customers c ON o.customer_id = c.id
                """,
                "columns": {
                    "order_id": {"name": "order_id"},
                    "customer_id": {"name": "customer_id"},
                    "amount": {"name": "amount"},
                },
                "tags": ["finance"],
            },
            "model.jaffle_shop.customers": {
                "name": "customers",
                "alias": "customers",
                "schema": "public",
                "resource_type": "model",
                "package_name": "jaffle_shop",
                "fqn": ["jaffle_shop", "customers"],
                "config": {"materialized": "view", "enabled": True},
                "depends_on": {"nodes": ["source.jaffle_shop.raw.raw_customers"]},
                "compiled_sql": "SELECT id, name, email FROM raw.raw_customers",
                "columns": {},
                "tags": [],
            },
        },
        "sources": {
            "source.jaffle_shop.raw.raw_orders": {
                "name": "raw_orders",
                "source_name": "raw",
                "resource_type": "source",
            },
            "source.jaffle_shop.raw.raw_customers": {
                "name": "raw_customers",
                "source_name": "raw",
                "resource_type": "source",
            },
        },
    }


@pytest.fixture
def sample_spark_plan() -> dict:
    """Minimal Spark LogicalPlan JSON for parser tests."""
    return {
        "class": "InsertIntoHadoopFsRelationCommand",
        "outputPath": "/user/hive/warehouse/analytics.db/sessions",
        "children": [
            {
                "class": "Project",
                "projectList": [
                    {
                        "class": "Alias",
                        "name": "session_id",
                        "child": {
                            "class": "AttributeReference",
                            "name": "id",
                        },
                    },
                    {
                        "class": "Alias",
                        "name": "user_id",
                        "child": {
                            "class": "AttributeReference",
                            "name": "user_id",
                        },
                    },
                    {
                        "class": "Alias",
                        "name": "duration_seconds",
                        "child": {
                            "class": "Subtract",
                            "children": [
                                {"class": "AttributeReference", "name": "end_time"},
                                {"class": "AttributeReference", "name": "start_time"},
                            ],
                        },
                    },
                ],
                "children": [
                    {
                        "class": "LogicalRelation",
                        "output": [
                            {"class": "AttributeReference", "name": "id", "qualifier": ["raw", "events"]},
                            {"class": "AttributeReference", "name": "user_id", "qualifier": ["raw", "events"]},
                            {"class": "AttributeReference", "name": "start_time", "qualifier": ["raw", "events"]},
                            {"class": "AttributeReference", "name": "end_time", "qualifier": ["raw", "events"]},
                        ],
                        "catalogTable": {
                            "identifier": {"database": "raw", "table": "events"},
                        },
                    }
                ],
            }
        ],
    }


@pytest.fixture
def sample_sql_cte() -> str:
    return """
    WITH ranked_orders AS (
        SELECT
            customer_id,
            order_id,
            amount,
            ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC) AS rn
        FROM orders
    )
    SELECT
        customer_id,
        order_id AS latest_order_id,
        amount AS latest_order_amount
    FROM ranked_orders
    WHERE rn = 1
    """


@pytest.fixture
def sample_avro_schema() -> str:
    import json
    return json.dumps({
        "type": "record",
        "name": "UserEvent",
        "namespace": "com.example",
        "fields": [
            {"name": "user_id", "type": "string"},
            {"name": "event_type", "type": "string"},
            {"name": "session_id", "type": ["null", "string"], "default": None},
            {"name": "timestamp", "type": "long"},
            {"name": "properties", "type": {"type": "map", "values": "string"}},
        ],
    })
