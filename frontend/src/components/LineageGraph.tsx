import { useEffect, useState, useCallback } from "react";
import ReactFlow, {
  Background,
  Controls,
  MiniMap,
  useNodesState,
  useEdgesState,
  type Node,
  type Edge,
  type NodeTypes,
  BackgroundVariant,
  MarkerType,
} from "reactflow";
import "reactflow/dist/style.css";
import { fetchLineageGraph, type GraphNode } from "../api/client";
// ─── Color map ────────────────────────────────────────────────────────────────

const NODE_TYPE_COLORS: Record<string, string> = {
  Dataset:   "#4fffb0",
  Column:    "#4da6ff",
  Transform: "#b87fff",
  Pipeline:  "#f7b731",
  Dashboard: "#ff6b9d",
  MLFeature: "#ff9f4a",
};

const EDGE_COLORS: Record<string, string> = {
  READS_FROM:   "#4da6ff",
  WRITES_TO:    "#4fffb0",
  DERIVES_FROM: "#b87fff",
  PART_OF:      "#f7b731",
  PRODUCES:     "#ff9f4a",
};

// ─── Custom node renderer ─────────────────────────────────────────────────────

function GraphNodeComponent({ data }: { data: Record<string, unknown> }) {
  const nodeType = data._nodeType as string ?? "Dataset";
  const color = NODE_TYPE_COLORS[nodeType] ?? "#888";
  const name = (data.name as string) ?? (data.dataset as string) ?? "unknown";
  const project = data.project as string | undefined;
  const sourceType = data.source_type as string | undefined;

  return (
    <div style={{
      minWidth: 140,
      maxWidth: 200,
      padding: "8px 12px",
      background: "var(--bg-elevated)",
      border: `1px solid ${color}44`,
      borderLeft: `3px solid ${color}`,
      borderRadius: 6,
      cursor: "pointer",
      transition: "border-color 0.15s, box-shadow 0.15s",
    }}
    onMouseEnter={(e) => {
      (e.currentTarget as HTMLDivElement).style.borderColor = color;
      (e.currentTarget as HTMLDivElement).style.boxShadow = `0 0 12px ${color}30`;
    }}
    onMouseLeave={(e) => {
      (e.currentTarget as HTMLDivElement).style.borderColor = `${color}44`;
      (e.currentTarget as HTMLDivElement).style.boxShadow = "none";
    }}
    >
      {/* Type badge */}
      <div style={{
        fontSize: 9,
        fontFamily: "var(--font-mono)",
        color: color,
        letterSpacing: "0.1em",
        textTransform: "uppercase",
        marginBottom: 3,
        opacity: 0.9,
      }}>
        {nodeType}
        {sourceType && <span style={{ opacity: 0.6 }}> · {sourceType}</span>}
      </div>
      {/* Name */}
      <div style={{
        fontSize: 12,
        fontWeight: 600,
        color: "var(--text-primary)",
        fontFamily: "var(--font-mono)",
        overflow: "hidden",
        textOverflow: "ellipsis",
        whiteSpace: "nowrap",
      }}>
        {name}
      </div>
      {/* Project */}
      {project && (
        <div style={{
          fontSize: 10,
          color: "var(--text-muted)",
          marginTop: 2,
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
        }}>
          {project}
        </div>
      )}
    </div>
  );
}

const nodeTypes: NodeTypes = {
  lineageNode: GraphNodeComponent,
};

// ─── Layout: simple hierarchical (left→right by type) ─────────────────────────

const TYPE_ORDER = ["Dataset", "Transform", "Column", "Pipeline", "Dashboard", "MLFeature"];

function layoutNodes(rawNodes: GraphNode[]): { x: number; y: number }[] {
  const typeGroups: Record<string, GraphNode[]> = {};
  for (const n of rawNodes) {
    if (!typeGroups[n.type]) typeGroups[n.type] = [];
    typeGroups[n.type].push(n);
  }

  const positions: { x: number; y: number }[] = new Array(rawNodes.length);
  const xSpacing = 260;
  const ySpacing = 80;

  let col = 0;
  for (const type of TYPE_ORDER) {
    const group = typeGroups[type] ?? [];
    group.forEach((node, rowIdx) => {
      const idx = rawNodes.indexOf(node);
      positions[idx] = { x: col * xSpacing, y: rowIdx * ySpacing };
    });
    if (group.length > 0) col++;
  }

  return positions;
}

// ─── Main component ───────────────────────────────────────────────────────────

interface Props {
  onNodeClick?: (nodeType: string, name: string, dataset?: string) => void;
}

export default function LineageGraph({ onNodeClick }: Props) {
  const [nodes, setNodes, onNodesChange] = useNodesState([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [nodeCount, setNodeCount] = useState(0);

  useEffect(() => {
    fetchLineageGraph()
      .then((graph) => {
        const positions = layoutNodes(graph.nodes);

        const rfNodes: Node[] = graph.nodes.map((n, i) => ({
          id: n.id,
          type: "lineageNode",
          position: positions[i] ?? { x: 0, y: 0 },
          data: { ...n.data, _nodeType: n.type, _id: n.id },
        }));

        const rfEdges: Edge[] = graph.edges.map((e, i) => ({
          id: `e-${i}`,
          source: e.source,
          target: e.target,
          label: e.type,
          labelStyle: {
            fill: "var(--text-muted)",
            fontFamily: "var(--font-mono)",
            fontSize: 9,
          },
          labelBgStyle: {
            fill: "var(--bg-base)",
            fillOpacity: 0.8,
          },
          style: {
            stroke: EDGE_COLORS[e.type] ?? "#4a5068",
            strokeWidth: 1.5,
            opacity: 0.7,
          },
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: EDGE_COLORS[e.type] ?? "#4a5068",
            width: 16,
            height: 16,
          },
          animated: e.type === "DERIVES_FROM",
        }));

        setNodes(rfNodes);
        setEdges(rfEdges);
        setNodeCount(rfNodes.length);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const handleNodeClick = useCallback(
    (_: unknown, node: Node) => {
      const data = node.data as Record<string, unknown>;
      const nodeType = data._nodeType as string;
      const name = (data.name as string) ?? (data.dataset as string) ?? "";
      const dataset = data.dataset as string | undefined;
      onNodeClick?.(nodeType, name, dataset);
    },
    [onNodeClick]
  );

  if (loading) {
    return (
      <div style={{
        height: "100%",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "var(--bg-base)",
        color: "var(--text-muted)",
        fontFamily: "var(--font-mono)",
        fontSize: 13,
        gap: 10,
      }}>
        <span className="spin">◌</span>
        Loading lineage graph…
      </div>
    );
  }

  if (error) {
    return (
      <div style={{
        height: "100%",
        display: "flex",
        alignItems: "center",
        justifyContent: "center",
        background: "var(--bg-base)",
        padding: 40,
      }}>
        <div style={{
          maxWidth: 400,
          padding: "20px 24px",
          background: "var(--danger-dim)",
          border: "1px solid var(--danger)",
          borderRadius: "var(--radius-md)",
          color: "var(--danger)",
          fontFamily: "var(--font-mono)",
          fontSize: 13,
          lineHeight: 1.6,
        }}>
          <div style={{ fontWeight: 600, marginBottom: 4 }}>Graph unavailable</div>
          {error}
          <div style={{ color: "var(--text-muted)", marginTop: 8, fontSize: 11 }}>
            Make sure the backend and Neo4j are running: <code>make up</code>
          </div>
        </div>
      </div>
    );
  }

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {/* Node count badge */}
      {nodeCount > 0 && (
        <div style={{
          position: "absolute",
          bottom: 60,
          left: 16,
          zIndex: 5,
          padding: "4px 10px",
          background: "var(--bg-surface)",
          border: "1px solid var(--border)",
          borderRadius: 20,
          fontSize: 11,
          fontFamily: "var(--font-mono)",
          color: "var(--text-muted)",
        }}>
          {nodeCount} nodes · {edges.length} edges
        </div>
      )}

      {/* Legend */}
      <div style={{
        position: "absolute",
        top: 72,
        left: 16,
        zIndex: 5,
        background: "var(--bg-surface)",
        border: "1px solid var(--border)",
        borderRadius: "var(--radius-md)",
        padding: "10px 14px",
        display: "flex",
        flexDirection: "column",
        gap: 5,
      }}>
        {Object.entries(NODE_TYPE_COLORS).map(([type, color]) => (
          <div key={type} style={{ display: "flex", alignItems: "center", gap: 8 }}>
            <div style={{ width: 8, height: 8, borderRadius: 2, background: color, flexShrink: 0 }} />
            <span style={{ fontSize: 10, fontFamily: "var(--font-mono)", color: "var(--text-muted)" }}>{type}</span>
          </div>
        ))}
      </div>

      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        onNodeClick={handleNodeClick}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{ padding: 0.15 }}
        minZoom={0.1}
        maxZoom={2.5}
        style={{ background: "var(--bg-base)" }}
      >
        <Background
          variant={BackgroundVariant.Dots}
          gap={24}
          size={1}
          color="#1e2330"
        />
        <Controls />
        <MiniMap
          nodeColor={(n) => {
            const nodeType = (n.data as Record<string, unknown>)._nodeType as string;
            return NODE_TYPE_COLORS[nodeType] ?? "#888";
          }}
          maskColor="rgba(10, 11, 14, 0.85)"
        />
      </ReactFlow>
    </div>
  );
}
