import { useEffect, useState } from "react";
import { fetchGraphStats, type GraphStats } from "../api/client";

const NODE_COLORS: Record<string, string> = {
  Dataset:   "var(--node-dataset)",
  Column:    "var(--node-column)",
  Transform: "var(--node-transform)",
  Pipeline:  "var(--node-pipeline)",
  Dashboard: "var(--node-dashboard)",
  MLFeature: "var(--node-mlfeature)",
};

const REL_COLORS: Record<string, string> = {
  READS_FROM:   "var(--info)",
  WRITES_TO:    "var(--accent)",
  DERIVES_FROM: "var(--node-transform)",
  PART_OF:      "var(--node-pipeline)",
  PRODUCES:     "var(--node-mlfeature)",
};

export default function Dashboard() {
  const [stats, setStats] = useState<GraphStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    fetchGraphStats()
      .then(setStats)
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  }, []);

  const totalNodes = stats
    ? Object.values(stats.nodes).reduce((a, b) => a + b, 0)
    : 0;
  const totalRels = stats
    ? Object.values(stats.relationships).reduce((a, b) => a + b, 0)
    : 0;

  return (
    <div style={{
      height: "100%",
      overflowY: "auto",
      padding: "32px 40px",
      background: "var(--bg-base)",
    }}>

      {/* Header */}
      <div style={{ marginBottom: 40 }}>
        <div style={{
          fontFamily: "var(--font-mono)",
          fontSize: 11,
          color: "var(--accent)",
          letterSpacing: "0.12em",
          textTransform: "uppercase",
          marginBottom: 8,
        }}>
          LineageIQ / Overview
        </div>
        <h1 style={{
          fontSize: 32,
          fontWeight: 800,
          color: "var(--text-primary)",
          letterSpacing: "-0.03em",
          lineHeight: 1.1,
        }}>
          Knowledge Graph
        </h1>
        <p style={{ color: "var(--text-secondary)", marginTop: 8, fontSize: 14, maxWidth: 480 }}>
          Column-level data lineage across dbt, Spark, Kafka, and SQL sources.
        </p>
      </div>

      {loading && (
        <div style={{ color: "var(--text-muted)", fontFamily: "var(--font-mono)", fontSize: 13 }}>
          <span className="spin" style={{ display: "inline-block", marginRight: 8 }}>◌</span>
          Connecting to graph…
        </div>
      )}

      {error && (
        <div style={{
          padding: "12px 16px",
          background: "var(--danger-dim)",
          border: "1px solid var(--danger)",
          borderRadius: "var(--radius-md)",
          color: "var(--danger)",
          fontFamily: "var(--font-mono)",
          fontSize: 13,
        }}>
          {error} — is the backend running?
        </div>
      )}

      {stats && (
        <div className="fade-in">
          {/* Top stat cards */}
          <div style={{ display: "grid", gridTemplateColumns: "repeat(4, 1fr)", gap: 16, marginBottom: 32 }}>
            <StatCard label="Total Nodes" value={totalNodes} accent="var(--accent)" />
            <StatCard label="Relationships" value={totalRels} accent="var(--info)" />
            <StatCard
              label="Datasets"
              value={stats.nodes["Dataset"] ?? 0}
              accent="var(--node-dataset)"
            />
            <StatCard
              label="Columns"
              value={stats.nodes["Column"] ?? 0}
              accent="var(--node-column)"
            />
          </div>

          {/* Node breakdown */}
          <div style={{ display: "grid", gridTemplateColumns: "1fr 1fr", gap: 24 }}>
            <Section title="Node Types">
              {Object.entries(stats.nodes).map(([label, count]) => (
                <BarRow
                  key={label}
                  label={label}
                  count={count}
                  max={Math.max(...Object.values(stats.nodes))}
                  color={NODE_COLORS[label] ?? "var(--text-secondary)"}
                />
              ))}
            </Section>

            <Section title="Relationship Types">
              {Object.entries(stats.relationships).map(([type, count]) => (
                <BarRow
                  key={type}
                  label={type}
                  count={count}
                  max={Math.max(...Object.values(stats.relationships))}
                  color={REL_COLORS[type] ?? "var(--text-secondary)"}
                  mono
                />
              ))}
            </Section>
          </div>

          {/* Quick start */}
          <div style={{ marginTop: 32 }}>
            <Section title="Quick Actions">
              <div style={{ display: "grid", gridTemplateColumns: "repeat(3, 1fr)", gap: 12 }}>
                {[
                  { label: "Explore lineage graph", desc: "Visualize nodes and relationships interactively", page: "explorer" },
                  { label: "Run NL query", desc: "Ask questions in plain English about your data", page: "explorer" },
                  { label: "Blast radius check", desc: "Predict impact of a schema change before applying it", page: "explorer" },
                ].map((action) => (
                  <div
                    key={action.label}
                    style={{
                      padding: "16px 18px",
                      background: "var(--bg-elevated)",
                      border: "1px solid var(--border)",
                      borderRadius: "var(--radius-md)",
                      cursor: "pointer",
                      transition: "border-color 0.15s",
                    }}
                    onMouseEnter={(e) => {
                      (e.currentTarget as HTMLDivElement).style.borderColor = "var(--accent)";
                    }}
                    onMouseLeave={(e) => {
                      (e.currentTarget as HTMLDivElement).style.borderColor = "var(--border)";
                    }}
                  >
                    <div style={{ fontSize: 13, fontWeight: 600, color: "var(--text-primary)", marginBottom: 4 }}>
                      {action.label}
                    </div>
                    <div style={{ fontSize: 12, color: "var(--text-secondary)", lineHeight: 1.5 }}>
                      {action.desc}
                    </div>
                  </div>
                ))}
              </div>
            </Section>
          </div>
        </div>
      )}
    </div>
  );
}

function StatCard({ label, value, accent }: { label: string; value: number; accent: string }) {
  return (
    <div style={{
      padding: "20px 24px",
      background: "var(--bg-surface)",
      border: "1px solid var(--border)",
      borderRadius: "var(--radius-md)",
    }}>
      <div style={{
        fontSize: 28,
        fontWeight: 800,
        fontFamily: "var(--font-mono)",
        color: accent,
        lineHeight: 1,
        marginBottom: 6,
      }}>
        {value.toLocaleString()}
      </div>
      <div style={{ fontSize: 12, color: "var(--text-secondary)", textTransform: "uppercase", letterSpacing: "0.08em" }}>
        {label}
      </div>
    </div>
  );
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div style={{
      background: "var(--bg-surface)",
      border: "1px solid var(--border)",
      borderRadius: "var(--radius-md)",
      overflow: "hidden",
    }}>
      <div style={{
        padding: "12px 20px",
        borderBottom: "1px solid var(--border)",
        fontSize: 11,
        fontWeight: 600,
        letterSpacing: "0.1em",
        textTransform: "uppercase",
        color: "var(--text-muted)",
        fontFamily: "var(--font-mono)",
      }}>
        {title}
      </div>
      <div style={{ padding: "12px 20px", display: "flex", flexDirection: "column", gap: 8 }}>
        {children}
      </div>
    </div>
  );
}

function BarRow({
  label, count, max, color, mono = false,
}: {
  label: string; count: number; max: number; color: string; mono?: boolean;
}) {
  const pct = max > 0 ? (count / max) * 100 : 0;
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 10 }}>
      <div style={{
        width: 120,
        fontSize: 12,
        color: "var(--text-secondary)",
        fontFamily: mono ? "var(--font-mono)" : "var(--font-ui)",
        flexShrink: 0,
        whiteSpace: "nowrap",
        overflow: "hidden",
        textOverflow: "ellipsis",
      }}>
        {label}
      </div>
      <div style={{
        flex: 1,
        height: 6,
        background: "var(--bg-elevated)",
        borderRadius: 3,
        overflow: "hidden",
      }}>
        <div style={{
          width: `${pct}%`,
          height: "100%",
          background: color,
          borderRadius: 3,
          transition: "width 0.6s cubic-bezier(0.16, 1, 0.3, 1)",
          opacity: 0.85,
        }} />
      </div>
      <div style={{
        width: 40,
        textAlign: "right",
        fontSize: 12,
        fontFamily: "var(--font-mono)",
        color: "var(--text-muted)",
      }}>
        {count}
      </div>
    </div>
  );
}
