"""
Kafka Schema Registry parser.

Reads Avro and JSON schemas from the Confluent Schema Registry REST API
and creates Dataset + Column nodes representing Kafka topics.

This parser does not produce DERIVES_FROM relationships (Kafka topics are
source data, not transforms). The lineage from a Kafka topic to a downstream
table is captured when the consumer's SQL/Spark job is parsed.

Config keys:
    schema_registry_url: str  – base URL of Schema Registry
    project_name:        str
    topic_filter:        str  – optional regex to filter topic names
    commit_id:           str  – optional version tag
"""

from __future__ import annotations

import re
from typing import Any

import httpx
import structlog

from app.models.lineage import (
    ColumnDerivation,
    ColumnReference,
    LineageEvent,
    SourceType,
)
from app.parsers.base import BaseParser

logger = structlog.get_logger(__name__)

# Map Avro / JSON Schema types to friendly names
_AVRO_TYPE_MAP: dict[str, str] = {
    "string": "string",
    "int": "integer",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "bytes": "bytes",
    "null": "null",
}


class KafkaSchemaParser(BaseParser):
    """
    Reads all schemas from the Confluent Schema Registry and emits a
    LineageEvent per topic, carrying the schema's fields as Column nodes.

    Uses httpx for synchronous HTTP calls (Celery workers are sync by default).
    """

    source_type = SourceType.KAFKA

    def parse(self, config: dict[str, Any]) -> list[LineageEvent]:
        registry_url = config.get("schema_registry_url", "http://localhost:8081")
        project_name = config["project_name"]
        topic_filter = config.get("topic_filter")
        commit_id = config.get("commit_id")

        self._log_parse_start(registry_url=registry_url, project=project_name)

        subjects = self._fetch_subjects(registry_url)
        if topic_filter:
            pattern = re.compile(topic_filter)
            subjects = [s for s in subjects if pattern.search(s)]

        events: list[LineageEvent] = []
        for subject in subjects:
            schema = self._fetch_latest_schema(registry_url, subject)
            if not schema:
                continue
            event = self._schema_to_event(subject, schema, project_name, commit_id)
            if event:
                events.append(event)

        self._log_parse_complete(len(events), registry_url=registry_url)
        return events

    def _fetch_subjects(self, registry_url: str) -> list[str]:
        """GET /subjects — returns all registered schema subjects."""
        try:
            resp = httpx.get(f"{registry_url}/subjects", timeout=10)
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.error("schema_registry_subjects_failed", error=str(exc))
            return []

    def _fetch_latest_schema(self, registry_url: str, subject: str) -> dict | None:
        """GET /subjects/{subject}/versions/latest — returns the latest schema."""
        try:
            resp = httpx.get(
                f"{registry_url}/subjects/{subject}/versions/latest",
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as exc:
            logger.warning("schema_registry_fetch_failed", subject=subject, error=str(exc))
            return None

    def _schema_to_event(
        self,
        subject: str,
        schema_payload: dict,
        project_name: str,
        commit_id: str | None,
    ) -> LineageEvent | None:
        """Convert a Schema Registry subject into a LineageEvent."""
        schema_type = schema_payload.get("schemaType", "AVRO")
        schema_str = schema_payload.get("schema", "{}")
        schema_id = schema_payload.get("id", 0)
        version = schema_payload.get("version", 1)

        # Derive topic name from subject (strip -key / -value suffix)
        topic_name = subject
        for suffix in ("-value", "-key"):
            if subject.endswith(suffix):
                topic_name = subject[: -len(suffix)]
                break

        columns = self._extract_columns(schema_str, schema_type)
        if not columns:
            return None

        # For Kafka sources, the "transform" is the schema registration itself
        return LineageEvent(
            source_type=SourceType.KAFKA,
            project_name=project_name,
            commit_id=commit_id,
            transform_id=f"kafka_schema_{subject}_{version}",
            transform_name=f"schema:{subject}",
            pipeline_name="kafka",
            source_datasets=[],  # Kafka topics are source leaves
            target_datasets=[f"kafka.{topic_name}"],
            column_derivations=[
                ColumnDerivation(
                    source=ColumnReference(dataset=f"kafka.{topic_name}", column=col["name"]),
                    target=ColumnReference(dataset=f"kafka.{topic_name}", column=col["name"]),
                    is_passthrough=True,
                )
                for col in columns
            ],
            tags={
                "kafka_subject": subject,
                "kafka_schema_id": schema_id,
                "kafka_schema_version": version,
                "kafka_schema_type": schema_type,
                "kafka_topic": topic_name,
                "columns": columns,
            },
        )

    def _extract_columns(self, schema_str: str, schema_type: str) -> list[dict]:
        """
        Extract field definitions from an Avro or JSON Schema string.
        Returns [{"name": str, "type": str, "nullable": bool}, ...].
        """
        import json

        try:
            schema = json.loads(schema_str)
        except (json.JSONDecodeError, TypeError):
            return []

        if schema_type == "AVRO":
            return self._parse_avro_fields(schema)
        elif schema_type == "JSON":
            return self._parse_json_schema_fields(schema)
        return []

    def _parse_avro_fields(self, schema: dict) -> list[dict]:
        """Extract fields from an Avro Record schema."""
        fields = schema.get("fields", [])
        result = []
        for field in fields:
            avro_type = field.get("type", "string")
            nullable = False

            # Union with null: ["null", "string"] → nullable string
            if isinstance(avro_type, list):
                nullable = "null" in avro_type
                avro_type = next((t for t in avro_type if t != "null"), "string")

            if isinstance(avro_type, dict):
                # Nested record or complex type
                friendly_type = avro_type.get("type", "record")
            else:
                friendly_type = _AVRO_TYPE_MAP.get(avro_type, avro_type)

            result.append({
                "name": field.get("name", ""),
                "type": friendly_type,
                "nullable": nullable,
                "doc": field.get("doc"),
            })
        return result

    def _parse_json_schema_fields(self, schema: dict) -> list[dict]:
        """Extract fields from a JSON Schema object."""
        properties = schema.get("properties", {})
        required = set(schema.get("required", []))
        result = []
        for name, prop in properties.items():
            json_type = prop.get("type", "string")
            if isinstance(json_type, list):
                nullable = "null" in json_type
                json_type = next((t for t in json_type if t != "null"), "string")
            else:
                nullable = name not in required

            result.append({
                "name": name,
                "type": json_type,
                "nullable": nullable,
                "description": prop.get("description"),
            })
        return result
