import { useState, useEffect } from "react";
import { fetchBlastRadius, type BlastRadiusReport, type AffectedAsset } from "../api/client";

const CHANGE_TYPES = [
  { value: "rename_column",  label: "Rename column" },
  { value: "drop_column",    label: "Drop column" },
  { value: "type_change",    label: "Type change" },
  { value: "rename_table",   label: "Rename table" },
  { value: "drop_table",     label: "Drop table" },
];

const SLA_COLORS: Record<string, string> = {
  P0: "var(--danger)",
  P1: "var(--warn)",
  P2: "var(--info)",
};

interface Props {
  initialDataset?: string;
  initialColumn?: string;
}

export default function BlastRadiusPanel({ initialDataset = "", initialColumn = "" }: Props) {
  const [dataset, setDataset]     = useState(initialDataset);
  const [column, setColumn]       = useState(initialColumn);
  const [changeType, setChangeType] = useState("rename_column");
  const [loading, setLoading]     = useState(false);
  const [report, setReport]       = useState<BlastRadiusReport | null>(null);
  const [error, setError]         = useState<string | null>(null);

  // Auto-populate when parent passes selection from graph click
  useEffect(() => {
    if (initialDataset) setDataset(initialDataset);
    if (initialColumn)  setColumn(initialColumn);
  }, [initialDataset, initialColumn]);

  const run = async () => {
    if (!dataset.trim() || loading) return;
    setLoading(true);
    setError(null);
    setReport(null);
    try {
      const r = await fetchBlastRadius(
        dataset.trim(),
        column.trim() || null,
        changeType,
      );
      setReport(r);
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const scored = report?.affected_assets ?? [];
  const p0 = scored.filter((a) => a.sla_tier === "P0");
  const p1 = scored.filter((a) => a.sla_tier === "P1");

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%", overflow: "hidden" }}>
      {/* Form */}
      <div style={{ padding: 16, borderBottom: "1px solid var(--border)", display: "flex", flexDirection: "column", gap: 8 }}>
        <Input
          label="Dataset"
          value={dataset}
          onChange={setDataset}
          placeholder="e.g. orders"
          mono
        />
        <Input
          label="Column (optional)"
          value={column}
          onChange={setColumn}
          placeholder="e.g. customer_id"
          mono
        />
        <div>
          <label style={{ fontSize: 10, fontFamily: "var(--font-mono)", color: "var(--text-muted)", letterSpacing: "0.08em", display: "block", marginBottom: 4 }}>
            CHANGE TYPE
          </label>
          <select
            value={changeType}
            onChange={(e) => setChangeType(e.target.value)}
            style={{
              width: "100%",
              padding: "7px 10px",
              background: "var(--bg-elevated)",
              border: "1px solid var(--border)",
              borderRadius: "var(--radius-sm)",
              color: "var(--text-primary)",
              fontFamily: "var(--font-mono)",
              fontSize: 12,
              outline: "none",
              cursor: "pointer",
            }}
          >
            {CHANGE_TYPES.map((ct) => (
              <option key={ct.value} value={ct.value} style={{ background: "var(--bg-elevated)" }}>
                {ct.label}
              </option>
            ))}
          </select>
        </div>
        <button
          onClick={run}
          disabled={!dataset.trim() || loading}
          style={{
            padding: "8px",
            background: dataset.trim() && !loading ? "var(--accent)" : "var(--bg-hover)",
            border: "none",
            borderRadius: "var(--radius-sm)",
            color: dataset.trim() && !loading ? "var(--bg-base)" : "var(--text-muted)",
            cursor: dataset.trim() && !loading ? "pointer" : "default",
            fontFamily: "var(--font-mono)",
            fontWeight: 700,
            fontSize: 12,
            letterSpacing: "0.04em",
            transition: "all 0.15s",
          }}
        >
          {loading ? "Analyzing…" : "Analyze Impact"}
        </button>
      </div>

      {/* Results */}
      <div style={{ flex: 1, overflowY: "auto", padding: 16, display: "flex", flexDirection: "column", gap: 12 }}>

        {error && (
          <div className="fade-in" style={{
            padding: "10px 14px",
            background: "var(--danger-dim)",
            border: "1px solid var(--danger)",
            borderRadius: "var(--radius-md)",
            color: "var(--danger)",
            fontFamily: "var(--font-mono)",
            fontSize: 12,
          }}>
            {error}
          </div>
        )}

        {report && (
          <div className="fade-in" style={{ display: "flex", flexDirection: "column", gap: 12 }}>
            {/* Summary */}
            <div style={{
              padding: "12px 14px",
              background: p0.length > 0 ? "var(--danger-dim)" : p1.length > 0 ? "var(--warn-dim)" : "var(--bg-elevated)",
              border: `1px solid ${p0.length > 0 ? "var(--danger)" : p1.length > 0 ? "var(--warn)" : "var(--border)"}`,
              borderRadius: "var(--radius-md)",
            }}>
              <div style={{ fontSize: 13, color: "var(--text-primary)", lineHeight: 1.5, marginBottom: 8 }}>
                {report.summary}
              </div>
              {/* Tier badges */}
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap" }}>
                {[
                  { tier: "P0", count: p0.length },
                  { tier: "P1", count: p1.length },
                  { tier: "P2", count: scored.length - p0.length - p1.length },
                ].filter((t) => t.count > 0).map(({ tier, count }) => (
                  <div key={tier} style={{
                    padding: "2px 10px",
                    background: SLA_COLORS[tier] + "22",
                    border: `1px solid ${SLA_COLORS[tier]}`,
                    borderRadius: 12,
                    fontSize: 11,
                    fontFamily: "var(--font-mono)",
                    color: SLA_COLORS[tier],
                  }}>
                    {count} {tier}
                  </div>
                ))}
                <div style={{
                  padding: "2px 10px",
                  background: "var(--bg-hover)",
                  border: "1px solid var(--border)",
                  borderRadius: 12,
                  fontSize: 11,
                  fontFamily: "var(--font-mono)",
                  color: "var(--text-muted)",
                }}>
                  {report.analysis_duration_ms.toFixed(0)}ms
                </div>
              </div>
            </div>

            {/* Upstream provenance */}
            {report.upstream_provenance.length > 0 && (
              <div>
                <SectionLabel>Upstream provenance</SectionLabel>
                <div style={{
                  padding: "8px 12px",
                  background: "var(--bg-elevated)",
                  border: "1px solid var(--border)",
                  borderRadius: 6,
                  fontFamily: "var(--font-mono)",
                  fontSize: 11,
                  color: "var(--text-secondary)",
                  lineHeight: 1.8,
                }}>
                  {report.upstream_provenance.join(" → ")}
                </div>
              </div>
            )}

            {/* Affected assets */}
            {scored.length > 0 && (
              <div>
                <SectionLabel>Affected assets ({scored.length})</SectionLabel>
                <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
                  {scored.map((asset) => (
                    <AssetCard key={asset.node_id} asset={asset} />
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function Input({
  label, value, onChange, placeholder, mono = false,
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  mono?: boolean;
}) {
  return (
    <div>
      <label style={{ fontSize: 10, fontFamily: "var(--font-mono)", color: "var(--text-muted)", letterSpacing: "0.08em", display: "block", marginBottom: 4 }}>
        {label.toUpperCase()}
      </label>
      <input
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        style={{
          width: "100%",
          padding: "7px 10px",
          background: "var(--bg-elevated)",
          border: "1px solid var(--border)",
          borderRadius: "var(--radius-sm)",
          color: "var(--text-primary)",
          fontFamily: mono ? "var(--font-mono)" : "var(--font-ui)",
          fontSize: 12,
          outline: "none",
          transition: "border-color 0.15s",
        }}
        onFocus={(e) => { (e.target as HTMLInputElement).style.borderColor = "var(--accent)"; }}
        onBlur={(e) => { (e.target as HTMLInputElement).style.borderColor = "var(--border)"; }}
      />
    </div>
  );
}

function SectionLabel({ children }: { children: React.ReactNode }) {
  return (
    <div style={{
      fontSize: 10,
      fontFamily: "var(--font-mono)",
      color: "var(--text-muted)",
      letterSpacing: "0.1em",
      textTransform: "uppercase",
      marginBottom: 6,
    }}>
      {children}
    </div>
  );
}

function AssetCard({ asset }: { asset: AffectedAsset }) {
  const slaColor = SLA_COLORS[asset.sla_tier] ?? "var(--info)";
  const score = asset.criticality_score;

  return (
    <div style={{
      padding: "10px 12px",
      background: "var(--bg-elevated)",
      border: "1px solid var(--border)",
      borderLeft: `3px solid ${slaColor}`,
      borderRadius: 6,
    }}>
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: 4 }}>
        <div style={{
          fontSize: 12,
          fontWeight: 600,
          fontFamily: "var(--font-mono)",
          color: "var(--text-primary)",
          overflow: "hidden",
          textOverflow: "ellipsis",
          whiteSpace: "nowrap",
          flex: 1,
        }}>
          {asset.name}
        </div>
        <div style={{ display: "flex", gap: 4, marginLeft: 8, flexShrink: 0 }}>
          <Badge color={slaColor}>{asset.sla_tier}</Badge>
          <Badge color="var(--text-muted)">{asset.node_type}</Badge>
        </div>
      </div>

      {/* Criticality bar */}
      <div style={{ marginBottom: 6 }}>
        <div style={{ height: 3, background: "var(--bg-hover)", borderRadius: 2, overflow: "hidden" }}>
          <div style={{
            width: `${score * 100}%`,
            height: "100%",
            background: score > 0.7 ? "var(--danger)" : score > 0.4 ? "var(--warn)" : "var(--info)",
            borderRadius: 2,
            transition: "width 0.5s ease",
          }} />
        </div>
        <div style={{ fontSize: 9, fontFamily: "var(--font-mono)", color: "var(--text-muted)", marginTop: 2 }}>
          criticality {(score * 100).toFixed(0)}% · {asset.freshness_cadence} · {asset.downstream_fan_out} downstream
        </div>
      </div>

      {asset.suggested_action && (
        <div style={{
          fontSize: 11,
          color: "var(--text-secondary)",
          lineHeight: 1.4,
          borderTop: "1px solid var(--border)",
          paddingTop: 6,
          marginTop: 2,
        }}>
          {asset.suggested_action}
        </div>
      )}
    </div>
  );
}

function Badge({ children, color }: { children: React.ReactNode; color: string }) {
  return (
    <div style={{
      padding: "1px 6px",
      background: color + "22",
      border: `1px solid ${color}66`,
      borderRadius: 4,
      fontSize: 9,
      fontFamily: "var(--font-mono)",
      color: color,
      letterSpacing: "0.06em",
    }}>
      {children}
    </div>
  );
}
