"""
Integration tests for the GraphWriter against a live Neo4j instance.

Tests that LineageEvents are correctly persisted as nodes and relationships,
that MERGE is idempotent, and that versioned snapshots are created.

Requires: Neo4j running (docker compose up neo4j)
"""

from __future__ import annotations

import pytest

from app.graph.client import get_client
from app.graph.writer import GraphWriter
from app.models.lineage import (
    ColumnDerivation,
    ColumnReference,
    LineageEvent,
    SourceType,
)


@pytest.fixture(scope="module")
async def neo4j():
    client = get_client()
    await client.connect()
    yield client
    # cleanup test nodes
    await client.run(
        "MATCH (n) WHERE n.project = 'writer_test' DETACH DELETE n"
    )


@pytest.fixture
def writer():
    return GraphWriter()


@pytest.fixture
def sample_event() -> LineageEvent:
    return LineageEvent(
        source_type=SourceType.SQL,
        project_name="writer_test",
        commit_id="test_commit_abc",
        transform_id="test_transform_001",
        transform_name="test_transform",
        pipeline_name="test_pipeline",
        source_datasets=["writer_test.source_table"],
        target_datasets=["writer_test.target_table"],
        column_derivations=[
            ColumnDerivation(
                source=ColumnReference(dataset="writer_test.source_table", column="id"),
                target=ColumnReference(dataset="writer_test.target_table", column="record_id"),
                is_passthrough=False,
                expression="CAST(id AS VARCHAR)",
            ),
            ColumnDerivation(
                source=ColumnReference(dataset="writer_test.source_table", column="name"),
                target=ColumnReference(dataset="writer_test.target_table", column="name"),
                is_passthrough=True,
            ),
        ],
    )


class TestGraphWriter:
    async def test_write_event_creates_dataset_nodes(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            "MATCH (d:Dataset {project: 'writer_test'}) RETURN d.name AS name"
        )
        names = {r["name"] for r in results}
        assert "writer_test.source_table" in names
        assert "writer_test.target_table" in names

    async def test_write_event_creates_transform_node(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            "MATCH (t:Transform {transform_id: 'test_transform_001'}) RETURN t.name AS name"
        )
        assert len(results) == 1
        assert results[0]["name"] == "test_transform"

    async def test_write_event_creates_column_nodes(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            "MATCH (c:Column {project: 'writer_test'}) RETURN c.name AS name, c.dataset AS ds"
        )
        col_names = {r["name"] for r in results}
        assert "id" in col_names
        assert "name" in col_names
        assert "record_id" in col_names

    async def test_write_event_creates_derives_from_relationship(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            """
            MATCH (src:Column {name: 'id', project: 'writer_test'})
                  <-[r:DERIVES_FROM]-
                  (tgt:Column {name: 'record_id', project: 'writer_test'})
            RETURN r.expression AS expr, r.is_passthrough AS passthrough
            """
        )
        assert len(results) == 1
        assert results[0]["expr"] == "CAST(id AS VARCHAR)"
        assert results[0]["passthrough"] is False

    async def test_passthrough_column_flagged_correctly(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            """
            MATCH (src:Column {name: 'name', project: 'writer_test'})
                  <-[r:DERIVES_FROM]-
                  (tgt:Column {name: 'name', project: 'writer_test'})
            RETURN r.is_passthrough AS passthrough
            """
        )
        assert len(results) == 1
        assert results[0]["passthrough"] is True

    async def test_write_event_is_idempotent(self, neo4j, writer, sample_event):
        """Writing the same event twice should not create duplicate nodes."""
        await writer.write_event(sample_event)
        await writer.write_event(sample_event)

        results = await neo4j.run(
            "MATCH (t:Transform {transform_id: 'test_transform_001'}) RETURN count(t) AS c"
        )
        assert results[0]["c"] == 1

    async def test_write_event_creates_snapshot(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            "MATCH (s:Snapshot {commit_id: 'test_commit_abc'}) RETURN s.project AS project"
        )
        assert len(results) >= 1
        assert results[0]["project"] == "writer_test"

    async def test_write_event_creates_pipeline_node(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            "MATCH (p:Pipeline {name: 'test_pipeline', project: 'writer_test'}) RETURN p.name AS name"
        )
        assert len(results) == 1

    async def test_write_event_creates_reads_from_relationship(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            """
            MATCH (t:Transform {transform_id: 'test_transform_001'})
                  -[:READS_FROM]->
                  (d:Dataset {name: 'writer_test.source_table'})
            RETURN t.name AS transform, d.name AS dataset
            """
        )
        assert len(results) == 1

    async def test_write_event_creates_writes_to_relationship(self, neo4j, writer, sample_event):
        await writer.write_event(sample_event)

        results = await neo4j.run(
            """
            MATCH (t:Transform {transform_id: 'test_transform_001'})
                  -[:WRITES_TO]->
                  (d:Dataset {name: 'writer_test.target_table'})
            RETURN t.name AS transform
            """
        )
        assert len(results) == 1

    async def test_llm_inferred_flag_propagated(self, neo4j, writer):
        event = LineageEvent(
            source_type=SourceType.SQL,
            project_name="writer_test",
            commit_id="test_commit_llm",
            transform_id="test_llm_transform",
            transform_name="llm_transform",
            source_datasets=["writer_test.llm_source"],
            target_datasets=["writer_test.llm_target"],
            column_derivations=[],
            llm_inferred=True,
            validation_status="flagged",
        )
        await writer.write_event(event)

        results = await neo4j.run(
            """
            MATCH (t:Transform {transform_id: 'test_llm_transform'})
            RETURN t.llm_inferred AS inferred, t.validation_status AS status
            """
        )
        assert results[0]["inferred"] is True
        assert results[0]["status"] == "flagged"
