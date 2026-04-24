"""
Unit tests for blast radius scoring and Text2Cypher static validation.
These tests do NOT require a live Neo4j or Redis instance.
"""

from __future__ import annotations

import math
import pytest

from app.analyzers.blast_radius import BlastRadiusAnalyzer, _SLA_WEIGHTS, _CADENCE_WEIGHTS
from app.llm.text2cypher import Text2CypherEngine
from app.models.lineage import (
    AffectedAsset,
    ChangeType,
    NodeType,
    SchemaChange,
    SLATier,
)


# ─── Criticality scoring ──────────────────────────────────────────────────────


class TestCriticalityScoring:
    def setup_method(self):
        self.analyzer = BlastRadiusAnalyzer()

    def _make_asset(
        self,
        node_type=NodeType.DASHBOARD,
        sla_tier=SLATier.P0,
        fan_out=10,
        cadence="hourly",
    ) -> AffectedAsset:
        return AffectedAsset(
            node_id="test-id",
            node_type=node_type,
            name="test_asset",
            sla_tier=sla_tier,
            downstream_fan_out=fan_out,
            freshness_cadence=cadence,
        )

    def test_p0_scores_higher_than_p2(self):
        p0 = self._make_asset(sla_tier=SLATier.P0, fan_out=1, cadence="daily")
        p2 = self._make_asset(sla_tier=SLATier.P2, fan_out=1, cadence="daily")
        scored_p0 = self.analyzer._score_asset(p0)
        scored_p2 = self.analyzer._score_asset(p2)
        assert scored_p0.criticality_score > scored_p2.criticality_score

    def test_streaming_scores_higher_than_weekly(self):
        streaming = self._make_asset(sla_tier=SLATier.P1, fan_out=5, cadence="streaming")
        weekly = self._make_asset(sla_tier=SLATier.P1, fan_out=5, cadence="weekly")
        s = self.analyzer._score_asset(streaming)
        w = self.analyzer._score_asset(weekly)
        assert s.criticality_score > w.criticality_score

    def test_high_fan_out_increases_score(self):
        low = self._make_asset(sla_tier=SLATier.P1, fan_out=1, cadence="daily")
        high = self._make_asset(sla_tier=SLATier.P1, fan_out=100, cadence="daily")
        s_low = self.analyzer._score_asset(low)
        s_high = self.analyzer._score_asset(high)
        assert s_high.criticality_score > s_low.criticality_score

    def test_score_bounded_zero_to_one(self):
        extreme = self._make_asset(sla_tier=SLATier.P0, fan_out=500, cadence="streaming")
        scored = self.analyzer._score_asset(extreme)
        assert 0.0 <= scored.criticality_score <= 1.0

    def test_zero_fan_out_still_scores(self):
        asset = self._make_asset(fan_out=0)
        scored = self.analyzer._score_asset(asset)
        assert scored.criticality_score >= 0.0

    def test_score_is_deterministic(self):
        asset = self._make_asset(sla_tier=SLATier.P1, fan_out=20, cadence="hourly")
        s1 = self.analyzer._score_asset(asset)
        asset2 = self._make_asset(sla_tier=SLATier.P1, fan_out=20, cadence="hourly")
        s2 = self.analyzer._score_asset(asset2)
        assert s1.criticality_score == s2.criticality_score


# ─── Summary generation ───────────────────────────────────────────────────────


class TestSummaryGeneration:
    def setup_method(self):
        self.analyzer = BlastRadiusAnalyzer()

    def test_summary_mentions_asset_count(self):
        change = SchemaChange(dataset="orders", column="customer_id", change_type=ChangeType.RENAME_COLUMN)
        assets = [
            AffectedAsset(node_id="1", node_type=NodeType.DASHBOARD, name="d1", sla_tier=SLATier.P0),
            AffectedAsset(node_id="2", node_type=NodeType.ML_FEATURE, name="m1", sla_tier=SLATier.P1),
        ]
        summary = self.analyzer._generate_summary(change, assets)
        assert "2" in summary
        assert "P0" in summary

    def test_summary_mentions_change_type(self):
        change = SchemaChange(dataset="payments", change_type=ChangeType.DROP_TABLE)
        summary = self.analyzer._generate_summary(change, [])
        assert "drop_table" in summary or "payments" in summary


# ─── Cache key ────────────────────────────────────────────────────────────────


class TestCacheKey:
    def test_same_input_same_key(self):
        change1 = SchemaChange(dataset="orders", column="id", change_type=ChangeType.RENAME_COLUMN)
        change2 = SchemaChange(dataset="orders", column="id", change_type=ChangeType.RENAME_COLUMN)
        assert BlastRadiusAnalyzer._cache_key(change1) == BlastRadiusAnalyzer._cache_key(change2)

    def test_different_column_different_key(self):
        c1 = SchemaChange(dataset="orders", column="id", change_type=ChangeType.RENAME_COLUMN)
        c2 = SchemaChange(dataset="orders", column="amount", change_type=ChangeType.RENAME_COLUMN)
        assert BlastRadiusAnalyzer._cache_key(c1) != BlastRadiusAnalyzer._cache_key(c2)

    def test_different_change_type_different_key(self):
        c1 = SchemaChange(dataset="orders", column="id", change_type=ChangeType.RENAME_COLUMN)
        c2 = SchemaChange(dataset="orders", column="id", change_type=ChangeType.DROP_COLUMN)
        assert BlastRadiusAnalyzer._cache_key(c1) != BlastRadiusAnalyzer._cache_key(c2)


# ─── Text2Cypher static validator ────────────────────────────────────────────


class TestCypherValidator:
    def setup_method(self):
        # We test only the validator — no LLM calls needed
        from app.config import get_settings
        from unittest.mock import patch
        with patch.object(get_settings(), "openai_api_key", "dummy"):
            self.engine = Text2CypherEngine.__new__(Text2CypherEngine)
            self.engine._validate_cypher = Text2CypherEngine._validate_cypher.__get__(
                self.engine, Text2CypherEngine
            )

    def test_valid_cypher_passes(self):
        cypher = "MATCH (d:Dataset) RETURN d.name LIMIT 10"
        result = self.engine._validate_cypher(cypher)
        assert result.validated is True
        assert not result.validation_errors

    def test_valid_with_relationship_passes(self):
        cypher = "MATCH (t:Transform)-[:READS_FROM]->(d:Dataset) RETURN t.name, d.name"
        result = self.engine._validate_cypher(cypher)
        assert result.validated is True

    def test_hallucinated_label_rejected(self):
        cypher = "MATCH (t:Table) RETURN t.name"
        result = self.engine._validate_cypher(cypher)
        assert result.validated is False
        assert any("Table" in e for e in result.validation_errors)

    def test_hallucinated_relationship_rejected(self):
        cypher = "MATCH (a:Dataset)-[:DEPENDS_ON]->(b:Dataset) RETURN a.name"
        result = self.engine._validate_cypher(cypher)
        assert result.validated is False
        assert any("DEPENDS_ON" in e for e in result.validation_errors)

    def test_write_operation_rejected(self):
        cypher = "MATCH (d:Dataset) WHERE d.name = 'x' DELETE d"
        result = self.engine._validate_cypher(cypher)
        assert result.validated is False

    def test_merge_rejected(self):
        cypher = "MERGE (d:Dataset {name: 'new_table'})"
        result = self.engine._validate_cypher(cypher)
        assert result.validated is False

    def test_empty_query_rejected(self):
        result = self.engine._validate_cypher("")
        assert result.validated is False

    def test_multiple_valid_labels_pass(self):
        cypher = """
        MATCH (t:Transform)-[:READS_FROM]->(d:Dataset)
        MATCH (c:Column)-[:PART_OF]->(d)
        RETURN t.name, d.name, c.name
        """
        result = self.engine._validate_cypher(cypher)
        assert result.validated is True
