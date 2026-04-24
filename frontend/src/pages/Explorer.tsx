import { useState, useCallback } from "react";
import LineageGraph from "../components/LineageGraph";
import NLQueryBar from "../components/NLQueryBar";
import BlastRadiusPanel from "../components/BlastRadiusPanel";

type RightPanel = "query" | "blast" | null;

export default function Explorer() {
  const [rightPanel, setRightPanel] = useState<RightPanel>("query");
  const [selectedDataset, setSelectedDataset] = useState<string | null>(null);
  const [selectedColumn, setSelectedColumn] = useState<string | null>(null);

  const handleNodeClick = useCallback((nodeType: string, name: string, dataset?: string) => {
    if (nodeType === "Column") {
      setSelectedDataset(dataset ?? name);
      setSelectedColumn(name);
    } else if (nodeType === "Dataset") {
      setSelectedDataset(name);
      setSelectedColumn(null);
    }
    setRightPanel("blast");
  }, []);

  return (
    <div style={{ display: "flex", height: "100vh", width: "100%", overflow: "hidden" }}>

      {/* ── Graph canvas ── */}
      <div style={{ flex: 1, position: "relative", overflow: "hidden" }}>
        {/* Top toolbar */}
        <div style={{
          position: "absolute",
          top: 16,
          left: 16,
          right: 16,
          zIndex: 5,
          display: "flex",
          alignItems: "center",
          gap: 8,
        }}>
          <div style={{
            padding: "6px 14px",
            background: "var(--bg-surface)",
            border: "1px solid var(--border)",
            borderRadius: "var(--radius-md)",
            fontSize: 11,
            fontFamily: "var(--font-mono)",
            color: "var(--text-muted)",
            letterSpacing: "0.06em",
          }}>
            LINEAGE GRAPH
          </div>

          <div style={{ flex: 1 }} />

          {/* Panel toggle buttons */}
          {(["query", "blast"] as RightPanel[]).map((p) => (
            <button
              key={p}
              onClick={() => setRightPanel(rightPanel === p ? null : p)}
              style={{
                padding: "6px 14px",
                background: rightPanel === p ? "var(--accent-dim)" : "var(--bg-surface)",
                border: rightPanel === p ? "1px solid var(--accent)" : "1px solid var(--border)",
                borderRadius: "var(--radius-md)",
                color: rightPanel === p ? "var(--accent)" : "var(--text-secondary)",
                cursor: "pointer",
                fontSize: 11,
                fontFamily: "var(--font-mono)",
                letterSpacing: "0.06em",
                textTransform: "uppercase",
                transition: "all 0.15s",
              }}
            >
              {p === "query" ? "NL Query" : "Blast Radius"}
            </button>
          ))}
        </div>

        <LineageGraph onNodeClick={handleNodeClick} />
      </div>

      {/* ── Right panel ── */}
      {rightPanel && (
        <div style={{
          width: 400,
          borderLeft: "1px solid var(--border)",
          background: "var(--bg-surface)",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
          flexShrink: 0,
        }}>
          {/* Panel header */}
          <div style={{
            padding: "14px 20px",
            borderBottom: "1px solid var(--border)",
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
          }}>
            <div style={{
              fontSize: 11,
              fontFamily: "var(--font-mono)",
              color: "var(--text-muted)",
              letterSpacing: "0.1em",
              textTransform: "uppercase",
            }}>
              {rightPanel === "query" ? "Natural Language Query" : "Blast Radius Analyzer"}
            </div>
            <button
              onClick={() => setRightPanel(null)}
              style={{
                background: "none",
                border: "none",
                color: "var(--text-muted)",
                cursor: "pointer",
                fontSize: 16,
                lineHeight: 1,
                padding: "2px 4px",
              }}
            >
              ×
            </button>
          </div>

          {/* Panel body */}
          <div style={{ flex: 1, overflow: "hidden" }}>
            {rightPanel === "query" && <NLQueryBar />}
            {rightPanel === "blast" && (
              <BlastRadiusPanel
                initialDataset={selectedDataset ?? undefined}
                initialColumn={selectedColumn ?? undefined}
              />
            )}
          </div>
        </div>
      )}
    </div>
  );
}
