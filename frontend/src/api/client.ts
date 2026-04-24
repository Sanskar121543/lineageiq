import axios from "axios";

const BASE = import.meta.env.VITE_API_BASE_URL ?? "http://localhost:8000";

export const api = axios.create({
  baseURL: `${BASE}/api/v1`,
  headers: { "Content-Type": "application/json" },
});

// ─── Types ────────────────────────────────────────────────────────────────────

export interface GraphNode {
  id: string;
  type: "Dataset" | "Column" | "Transform" | "Pipeline" | "Dashboard" | "MLFeature";
  data: Record<string, unknown>;
}

export interface GraphEdge {
  source: string;
  target: string;
  type: "READS_FROM" | "WRITES_TO" | "DERIVES_FROM" | "PART_OF" | "PRODUCES";
  data: Record<string, unknown>;
}

export interface LineageGraph {
  nodes: GraphNode[];
  edges: GraphEdge[];
}

export interface AffectedAsset {
  node_id: string;
  node_type: string;
  name: string;
  sla_tier: "P0" | "P1" | "P2";
  downstream_fan_out: number;
  freshness_cadence: string;
  criticality_score: number;
  path_from_source: string[];
  suggested_action: string | null;
}

export interface BlastRadiusReport {
  change: {
    dataset: string;
    column: string | null;
    change_type: string;
  };
  analysis_duration_ms: number;
  total_affected: number;
  affected_assets: AffectedAsset[];
  upstream_provenance: string[];
  summary: string;
}

export interface QueryResult {
  question: string;
  cypher: {
    cypher: string;
    validated: boolean;
    validation_errors: string[];
  };
  results: Record<string, unknown>[];
  result_count: number;
  duration_ms: number;
}

export interface GraphStats {
  nodes: Record<string, number>;
  relationships: Record<string, number>;
}

// ─── API calls ────────────────────────────────────────────────────────────────

export const fetchLineageGraph = (project?: string): Promise<LineageGraph> =>
  api.get("/lineage/graph", { params: { project, limit: 1000 } }).then((r) => r.data);

export const fetchBlastRadius = (
  dataset: string,
  column: string | null,
  changeType: string
): Promise<BlastRadiusReport> =>
  api
    .post("/impact/blast-radius", { dataset, column, change_type: changeType })
    .then((r) => r.data);

export const submitNLQuery = (question: string): Promise<QueryResult> =>
  api.post("/query/nl", { question, max_results: 100 }).then((r) => r.data);

export const fetchQuerySuggestions = (): Promise<{ suggestions: string[] }> =>
  api.get("/query/suggestions").then((r) => r.data);

export const fetchGraphStats = (): Promise<GraphStats> =>
  api.get("/stats").then((r) => r.data);

export const fetchDatasets = (project?: string) =>
  api.get("/lineage/datasets", { params: { project } }).then((r) => r.data);

export const ingestDbt = (manifestPath: string, projectName: string, commitId?: string) =>
  api
    .post("/lineage/ingest/dbt", {
      manifest_path: manifestPath,
      project_name: projectName,
      commit_id: commitId,
    })
    .then((r) => r.data);
