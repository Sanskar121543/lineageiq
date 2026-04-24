"""
Unit tests for all four source parsers.

These tests are fully offline — no Neo4j, no LLM, no Kafka.
They validate that parsers emit correctly shaped LineageEvents
from known fixture inputs.
"""

from __future__ import annotations

import json
import tempfile
from pathlib import Path

import pytest

from app.models.lineage import SourceType
from app.parsers.dbt_parser import DbtManifestParser
from app.parsers.kafka_parser import KafkaSchemaParser
from app.parsers.spark_parser import SparkPlanParser
from app.parsers.sql_parser import SqlGlotParser


# ─── SQL Parser ───────────────────────────────────────────────────────────────


class TestSqlGlotParser:
    def setup_method(self):
        self.parser = SqlGlotParser()

    def test_simple_select(self):
        sql = "SELECT a.id AS user_id, a.name FROM users a"
        events = self.parser.parse({
            "sql": sql,
            "project_name": "test",
            "target_dataset": "user_dim",
            "source_datasets": ["users"],
        })
        assert len(events) == 1
        event = events[0]
        assert event.source_type == SourceType.SQL
        assert "users" in event.source_datasets
        assert "user_dim" in event.target_datasets

    def test_column_derivations_extracted(self):
        sql = "SELECT id, email, UPPER(name) AS name_upper FROM customers"
        events = self.parser.parse({
            "sql": sql,
            "project_name": "test",
            "target_dataset": "customer_clean",
            "source_datasets": ["customers"],
        })
        assert events
        derivations = events[0].column_derivations
        col_names = [d.target.column for d in derivations]
        assert "id" in col_names or "email" in col_names

    def test_passthrough_flagged(self):
        sql = "SELECT id, name FROM products"
        events = self.parser.parse({
            "sql": sql,
            "project_name": "test",
            "target_dataset": "products_clean",
            "source_datasets": ["products"],
        })
        derivations = events[0].column_derivations
        passthrough = [d for d in derivations if d.is_passthrough]
        assert len(passthrough) > 0

    def test_cte_handling(self, sample_sql_cte):
        events = self.parser.parse({
            "sql": sample_sql_cte,
            "project_name": "test",
            "target_dataset": "latest_orders",
            "source_datasets": ["orders"],
        })
        assert events
        assert events[0].source_datasets  # sources should be identified

    def test_empty_sql_returns_empty(self):
        events = self.parser.parse({
            "sql": "-- just a comment",
            "project_name": "test",
            "target_dataset": "x",
        })
        assert events == []

    def test_multi_dialect_snowflake(self):
        sql = 'SELECT "user_id", "email" FROM "PUBLIC"."USERS"'
        events = self.parser.parse({
            "sql": sql,
            "project_name": "test",
            "target_dataset": "user_staging",
            "source_datasets": [],
            "dialect": "snowflake",
        })
        # Should not raise; may produce events or empty list
        assert isinstance(events, list)


# ─── dbt Parser ───────────────────────────────────────────────────────────────


class TestDbtManifestParser:
    def setup_method(self):
        self.parser = DbtManifestParser()

    def test_parses_two_models(self, sample_dbt_manifest, tmp_path):
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(sample_dbt_manifest))

        events = self.parser.parse({
            "manifest_path": str(manifest_file),
            "project_name": "jaffle_shop",
            "commit_id": "abc123",
        })

        assert len(events) == 2
        names = [e.transform_name for e in events]
        assert "orders" in names
        assert "customers" in names

    def test_source_type_is_dbt(self, sample_dbt_manifest, tmp_path):
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(sample_dbt_manifest))

        events = self.parser.parse({
            "manifest_path": str(manifest_file),
            "project_name": "jaffle_shop",
        })
        for e in events:
            assert e.source_type == SourceType.DBT

    def test_commit_id_propagated(self, sample_dbt_manifest, tmp_path):
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(sample_dbt_manifest))

        events = self.parser.parse({
            "manifest_path": str(manifest_file),
            "project_name": "jaffle_shop",
            "commit_id": "deadbeef",
        })
        for e in events:
            assert e.commit_id == "deadbeef"

    def test_source_datasets_resolved(self, sample_dbt_manifest, tmp_path):
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(sample_dbt_manifest))

        events = self.parser.parse({
            "manifest_path": str(manifest_file),
            "project_name": "jaffle_shop",
        })
        orders_event = next(e for e in events if e.transform_name == "orders")
        assert any("raw_orders" in s for s in orders_event.source_datasets)

    def test_disabled_model_skipped(self, sample_dbt_manifest, tmp_path):
        sample_dbt_manifest["nodes"]["model.jaffle_shop.orders"]["config"]["enabled"] = False
        manifest_file = tmp_path / "manifest.json"
        manifest_file.write_text(json.dumps(sample_dbt_manifest))

        events = self.parser.parse({
            "manifest_path": str(manifest_file),
            "project_name": "jaffle_shop",
        })
        names = [e.transform_name for e in events]
        assert "orders" not in names


# ─── Spark Parser ─────────────────────────────────────────────────────────────


class TestSparkPlanParser:
    def setup_method(self):
        self.parser = SparkPlanParser()

    def test_parses_write_command(self, sample_spark_plan):
        events = self.parser.parse({
            "plan": sample_spark_plan,
            "project_name": "analytics",
            "pipeline_name": "session_etl",
            "job_id": "job_001",
        })
        assert len(events) == 1
        assert events[0].source_type == SourceType.SPARK

    def test_target_dataset_extracted(self, sample_spark_plan):
        events = self.parser.parse({
            "plan": sample_spark_plan,
            "project_name": "analytics",
            "pipeline_name": "session_etl",
            "job_id": "job_001",
        })
        assert "sessions" in events[0].target_datasets[0]

    def test_source_dataset_extracted(self, sample_spark_plan):
        events = self.parser.parse({
            "plan": sample_spark_plan,
            "project_name": "analytics",
            "pipeline_name": "session_etl",
            "job_id": "job_001",
        })
        assert any("events" in s for s in events[0].source_datasets)

    def test_column_derivations_present(self, sample_spark_plan):
        events = self.parser.parse({
            "plan": sample_spark_plan,
            "project_name": "analytics",
            "pipeline_name": "session_etl",
            "job_id": "job_001",
        })
        assert len(events[0].column_derivations) > 0

    def test_empty_plan_returns_empty(self):
        events = self.parser.parse({
            "plan": {"class": "LocalRelation", "children": []},
            "project_name": "test",
            "pipeline_name": "test",
            "job_id": "x",
        })
        assert events == []


# ─── Kafka Parser ─────────────────────────────────────────────────────────────


class TestKafkaSchemaParser:
    def setup_method(self):
        self.parser = KafkaSchemaParser()

    def test_parse_avro_fields(self, sample_avro_schema):
        cols = self.parser._parse_avro_fields(json.loads(sample_avro_schema))
        field_names = [c["name"] for c in cols]
        assert "user_id" in field_names
        assert "event_type" in field_names
        assert "session_id" in field_names

    def test_nullable_detection(self, sample_avro_schema):
        cols = self.parser._parse_avro_fields(json.loads(sample_avro_schema))
        session_col = next(c for c in cols if c["name"] == "session_id")
        assert session_col["nullable"] is True

    def test_non_nullable_detection(self, sample_avro_schema):
        cols = self.parser._parse_avro_fields(json.loads(sample_avro_schema))
        user_col = next(c for c in cols if c["name"] == "user_id")
        assert user_col["nullable"] is False

    def test_json_schema_fields(self):
        schema_str = json.dumps({
            "type": "object",
            "properties": {
                "id": {"type": "integer"},
                "name": {"type": "string"},
                "optional_field": {"type": ["string", "null"]},
            },
            "required": ["id", "name"],
        })
        cols = self.parser._parse_json_schema_fields(json.loads(schema_str))
        assert any(c["name"] == "id" and not c["nullable"] for c in cols)
        assert any(c["name"] == "optional_field" and c["nullable"] for c in cols)

    def test_topic_name_extraction(self):
        """Subject names with -value suffix should strip to topic name."""
        subject = "user-events-value"
        schema_payload = {
            "id": 1,
            "version": 1,
            "schemaType": "AVRO",
            "schema": json.dumps({
                "type": "record",
                "name": "UserEvent",
                "fields": [{"name": "id", "type": "string"}],
            }),
        }
        event = self.parser._schema_to_event(
            subject=subject,
            schema_payload=schema_payload,
            project_name="test",
            commit_id=None,
        )
        assert event is not None
        assert "kafka.user-events" in event.target_datasets[0]
