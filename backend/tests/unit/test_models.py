"""
Unit tests for Pydantic domain models and LineageEvent validation.
No external dependencies required.
"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.models.lineage import (
    AffectedAsset,
    BlastRadiusReport,
    ChangeType,
    ColumnDerivation,
    ColumnReference,
    LineageEvent,
    NodeType,
    NLQuery,
    SchemaChange,
    SLATier,
    SourceType,
)


class TestColumnReference:
    def test_strips_double_quotes(self):
        ref = ColumnReference(dataset='"orders"', column='"customer_id"')
        assert ref.dataset == "orders"
        assert ref.column == "customer_id"

    def test_strips_backticks(self):
        ref = ColumnReference(dataset="`orders`", column="`id`")
        assert ref.dataset == "orders"
        assert ref.column == "id"

    def test_strips_whitespace(self):
        ref = ColumnReference(dataset="  orders  ", column="  id  ")
        assert ref.dataset == "orders"
        assert ref.column == "id"


class TestLineageEvent:
    def test_default_event_id_generated(self):
        e1 = LineageEvent(
            source_type=SourceType.SQL,
            project_name="test",
            transform_id="t1",
            transform_name="t1",
        )
        e2 = LineageEvent(
            source_type=SourceType.SQL,
            project_name="test",
            transform_id="t2",
            transform_name="t2",
        )
        assert e1.event_id != e2.event_id

    def test_validation_status_defaults_to_ok(self):
        e = LineageEvent(
            source_type=SourceType.DBT,
            project_name="test",
            transform_id="t1",
            transform_name="t1",
        )
        assert e.validation_status == "ok"

    def test_llm_inferred_defaults_false(self):
        e = LineageEvent(
            source_type=SourceType.SPARK,
            project_name="test",
            transform_id="t1",
            transform_name="t1",
        )
        assert e.llm_inferred is False

    def test_serializes_to_json(self):
        e = LineageEvent(
            source_type=SourceType.SQL,
            project_name="test",
            transform_id="t1",
            transform_name="transform_one",
            source_datasets=["raw.orders"],
            target_datasets=["clean.orders"],
            column_derivations=[
                ColumnDerivation(
                    source=ColumnReference(dataset="raw.orders", column="id"),
                    target=ColumnReference(dataset="clean.orders", column="order_id"),
                    is_passthrough=False,
                    expression="CAST(id AS VARCHAR)",
                )
            ],
        )
        json_str = e.model_dump_json()
        assert "transform_one" in json_str
        assert "raw.orders" in json_str

    def test_source_type_enum_validated(self):
        with pytest.raises(ValidationError):
            LineageEvent(
                source_type="invalid_source",  # not a valid SourceType
                project_name="test",
                transform_id="t1",
                transform_name="t1",
            )


class TestSchemaChange:
    def test_valid_rename_column(self):
        sc = SchemaChange(
            dataset="orders",
            column="customer_id",
            change_type=ChangeType.RENAME_COLUMN,
            new_name="customer_key",
        )
        assert sc.dataset == "orders"
        assert sc.change_type == ChangeType.RENAME_COLUMN

    def test_valid_drop_table(self):
        sc = SchemaChange(dataset="deprecated_table", change_type=ChangeType.DROP_TABLE)
        assert sc.column is None

    def test_invalid_change_type_rejected(self):
        with pytest.raises(ValidationError):
            SchemaChange(dataset="orders", change_type="teleport")


class TestBlastRadiusReport:
    def _make_report(self) -> BlastRadiusReport:
        change = SchemaChange(dataset="orders", column="id", change_type=ChangeType.DROP_COLUMN)
        assets = [
            AffectedAsset(
                node_id="1", node_type=NodeType.DASHBOARD, name="revenue_dash",
                sla_tier=SLATier.P0, criticality_score=0.9,
            ),
            AffectedAsset(
                node_id="2", node_type=NodeType.ML_FEATURE, name="ltv_feature",
                sla_tier=SLATier.P1, criticality_score=0.6,
            ),
            AffectedAsset(
                node_id="3", node_type=NodeType.DATASET, name="orders_clean",
                sla_tier=SLATier.P2, criticality_score=0.2,
            ),
        ]
        return BlastRadiusReport(
            change=change,
            analysis_duration_ms=42.5,
            total_affected=3,
            affected_assets=assets,
            summary="Test summary",
        )

    def test_p0_count_property(self):
        report = self._make_report()
        assert report.p0_count == 1

    def test_p1_count_property(self):
        report = self._make_report()
        assert report.p1_count == 1

    def test_total_affected(self):
        report = self._make_report()
        assert report.total_affected == 3

    def test_round_trips_json(self):
        report = self._make_report()
        json_str = report.model_dump_json()
        reloaded = BlastRadiusReport.model_validate_json(json_str)
        assert reloaded.total_affected == 3
        assert reloaded.p0_count == 1


class TestNLQuery:
    def test_defaults(self):
        q = NLQuery(question="test question")
        assert q.max_results == 100

    def test_empty_question_accepted(self):
        # Model itself doesn't enforce non-empty; API layer handles that
        q = NLQuery(question="")
        assert q.question == ""

    def test_max_results_customizable(self):
        q = NLQuery(question="test", max_results=50)
        assert q.max_results == 50


class TestSLATier:
    def test_p0_is_highest(self):
        weights = {"P0": 1.0, "P1": 0.6, "P2": 0.2}
        assert weights[SLATier.P0] > weights[SLATier.P1] > weights[SLATier.P2]

    def test_valid_values(self):
        assert SLATier("P0") == SLATier.P0
        assert SLATier("P1") == SLATier.P1
        assert SLATier("P2") == SLATier.P2

    def test_invalid_value_rejected(self):
        with pytest.raises(ValueError):
            SLATier("P3")
