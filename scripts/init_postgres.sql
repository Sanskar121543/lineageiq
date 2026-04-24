-- LineageIQ PostgreSQL initialization
-- Creates the schema change tracking table that Debezium monitors.

CREATE SCHEMA IF NOT EXISTS lineageiq;

-- Schema change event log (Debezium watches this table)
CREATE TABLE IF NOT EXISTS lineageiq.schema_changes (
    id          BIGSERIAL PRIMARY KEY,
    dataset     TEXT NOT NULL,
    column_name TEXT,
    change_type TEXT NOT NULL,   -- ALTER_TABLE | DROP_COLUMN | ADD_COLUMN | RENAME_COLUMN
    ddl         TEXT,
    source_db   TEXT,
    occurred_at TIMESTAMPTZ DEFAULT NOW(),
    processed   BOOLEAN DEFAULT FALSE
);

-- Catalog: known datasets and their columns (cross-validation source)
CREATE TABLE IF NOT EXISTS lineageiq.catalog_datasets (
    id         BIGSERIAL PRIMARY KEY,
    project    TEXT NOT NULL,
    name       TEXT NOT NULL,
    source_type TEXT NOT NULL,
    owner      TEXT,
    sla_tier   TEXT DEFAULT 'P2',
    freshness_cadence TEXT DEFAULT 'daily',
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (project, name)
);

CREATE TABLE IF NOT EXISTS lineageiq.catalog_columns (
    id         BIGSERIAL PRIMARY KEY,
    project    TEXT NOT NULL,
    dataset    TEXT NOT NULL,
    name       TEXT NOT NULL,
    data_type  TEXT,
    nullable   BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (project, dataset, name)
);

-- Enable logical replication for Debezium
ALTER TABLE lineageiq.schema_changes REPLICA IDENTITY FULL;
ALTER TABLE lineageiq.catalog_datasets REPLICA IDENTITY FULL;
ALTER TABLE lineageiq.catalog_columns REPLICA IDENTITY FULL;
