import { useState, useEffect, useRef } from "react";
import { submitNLQuery, fetchQuerySuggestions, type QueryResult } from "../api/client";

export default function NLQueryBar() {
  const [question, setQuestion] = useState("");
  const [loading, setLoading]   = useState(false);
  const [result, setResult]     = useState<QueryResult | null>(null);
  const [error, setError]       = useState<string | null>(null);
  const [suggestions, setSuggestions] = useState<string[]>([]);
  const textareaRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    fetchQuerySuggestions()
      .then((d) => setSuggestions(d.suggestions))
      .catch(() => {});
  }, []);

  const submit = async () => {
    if (!question.trim() || loading) return;
    setLoading(true);
    setError(null);
    setResult(null);
    try {
      const r = await submitNLQuery(question.trim());
      setResult(r);
    } catch (e: unknown) {
      setError((e as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const handleKey = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (e.key === "Enter" && (e.metaKey || e.ctrlKey)) submit();
  };

  return (
    <div style={{
      display: "flex",
      flexDirection: "column",
      height: "100%",
      overflow: "hidden",
    }}>
      {/* Input area */}
      <div style={{ padding: 16, borderBottom: "1px solid var(--border)" }}>
        <div style={{
          background: "var(--bg-elevated)",
          border: "1px solid var(--border)",
          borderRadius: "var(--radius-md)",
          overflow: "hidden",
          transition: "border-color 0.15s",
        }}
        onFocusCapture={(e) => {
          (e.currentTarget as HTMLDivElement).style.borderColor = "var(--accent)";
        }}
        onBlurCapture={(e) => {
          (e.currentTarget as HTMLDivElement).style.borderColor = "var(--border)";
        }}
        >
          <textarea
            ref={textareaRef}
            value={question}
            onChange={(e) => setQuestion(e.target.value)}
            onKeyDown={handleKey}
            placeholder="Ask about your data lineage…"
            rows={3}
            style={{
              width: "100%",
              background: "transparent",
              border: "none",
              outline: "none",
              resize: "none",
              padding: "12px 14px",
              color: "var(--text-primary)",
              fontFamily: "var(--font-ui)",
              fontSize: 13,
              lineHeight: 1.5,
            }}
          />
          <div style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "6px 10px 8px",
          }}>
            <span style={{ fontSize: 10, color: "var(--text-muted)", fontFamily: "var(--font-mono)" }}>
              ⌘↵ to run
            </span>
            <button
              onClick={submit}
              disabled={!question.trim() || loading}
              style={{
                padding: "5px 14px",
                background: question.trim() && !loading ? "var(--accent)" : "var(--bg-hover)",
                border: "none",
                borderRadius: 4,
                color: question.trim() && !loading ? "var(--bg-base)" : "var(--text-muted)",
                cursor: question.trim() && !loading ? "pointer" : "default",
                fontFamily: "var(--font-mono)",
                fontSize: 11,
                fontWeight: 600,
                transition: "all 0.15s",
              }}
            >
              {loading ? "Running…" : "Run"}
            </button>
          </div>
        </div>
      </div>

      {/* Scrollable result area */}
      <div style={{ flex: 1, overflowY: "auto", padding: 16, display: "flex", flexDirection: "column", gap: 12 }}>

        {/* Suggestions */}
        {!result && !loading && suggestions.length > 0 && (
          <div>
            <div style={{
              fontSize: 10,
              fontFamily: "var(--font-mono)",
              color: "var(--text-muted)",
              letterSpacing: "0.1em",
              textTransform: "uppercase",
              marginBottom: 8,
            }}>
              Example queries
            </div>
            <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
              {suggestions.slice(0, 6).map((s) => (
                <button
                  key={s}
                  onClick={() => setQuestion(s)}
                  style={{
                    textAlign: "left",
                    background: "var(--bg-elevated)",
                    border: "1px solid var(--border)",
                    borderRadius: 6,
                    padding: "8px 12px",
                    color: "var(--text-secondary)",
                    cursor: "pointer",
                    fontSize: 12,
                    lineHeight: 1.4,
                    transition: "all 0.15s",
                  }}
                  onMouseEnter={(e) => {
                    (e.currentTarget as HTMLButtonElement).style.borderColor = "var(--accent)";
                    (e.currentTarget as HTMLButtonElement).style.color = "var(--text-primary)";
                  }}
                  onMouseLeave={(e) => {
                    (e.currentTarget as HTMLButtonElement).style.borderColor = "var(--border)";
                    (e.currentTarget as HTMLButtonElement).style.color = "var(--text-secondary)";
                  }}
                >
                  {s}
                </button>
              ))}
            </div>
          </div>
        )}

        {/* Error */}
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

        {/* Results */}
        {result && (
          <div className="fade-in">
            {/* Validation warning */}
            {!result.cypher.validated && (
              <div style={{
                padding: "8px 12px",
                background: "var(--warn-dim)",
                border: "1px solid var(--warn)",
                borderRadius: 6,
                color: "var(--warn)",
                fontSize: 11,
                fontFamily: "var(--font-mono)",
                marginBottom: 10,
              }}>
                ⚠ Cypher validation failed: {result.cypher.validation_errors.join(", ")}
              </div>
            )}

            {/* Generated Cypher */}
            <div style={{ marginBottom: 12 }}>
              <div style={{
                fontSize: 10,
                fontFamily: "var(--font-mono)",
                color: "var(--text-muted)",
                letterSpacing: "0.1em",
                textTransform: "uppercase",
                marginBottom: 4,
              }}>
                Generated Cypher · {result.duration_ms.toFixed(0)}ms
              </div>
              <pre style={{
                padding: "10px 14px",
                background: "var(--bg-elevated)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                fontSize: 11,
                fontFamily: "var(--font-mono)",
                color: "var(--node-transform)",
                overflowX: "auto",
                lineHeight: 1.5,
                whiteSpace: "pre-wrap",
                wordBreak: "break-all",
              }}>
                {result.cypher.cypher}
              </pre>
            </div>

            {/* Results table */}
            {result.results.length > 0 ? (
              <div>
                <div style={{
                  fontSize: 10,
                  fontFamily: "var(--font-mono)",
                  color: "var(--text-muted)",
                  letterSpacing: "0.1em",
                  textTransform: "uppercase",
                  marginBottom: 6,
                }}>
                  {result.result_count} result{result.result_count !== 1 ? "s" : ""}
                </div>
                <ResultTable rows={result.results} />
              </div>
            ) : (
              <div style={{
                padding: "12px 14px",
                background: "var(--bg-elevated)",
                border: "1px solid var(--border)",
                borderRadius: 6,
                color: "var(--text-muted)",
                fontFamily: "var(--font-mono)",
                fontSize: 12,
              }}>
                No results returned.
              </div>
            )}
          </div>
        )}
      </div>
    </div>
  );
}

function ResultTable({ rows }: { rows: Record<string, unknown>[] }) {
  if (rows.length === 0) return null;
  const cols = Object.keys(rows[0]);

  return (
    <div style={{ overflowX: "auto", borderRadius: 6, border: "1px solid var(--border)" }}>
      <table style={{ width: "100%", borderCollapse: "collapse", fontSize: 11, fontFamily: "var(--font-mono)" }}>
        <thead>
          <tr style={{ background: "var(--bg-elevated)", borderBottom: "1px solid var(--border)" }}>
            {cols.map((c) => (
              <th key={c} style={{
                padding: "6px 10px",
                textAlign: "left",
                color: "var(--text-muted)",
                fontWeight: 600,
                whiteSpace: "nowrap",
                letterSpacing: "0.06em",
              }}>
                {c}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              style={{ borderBottom: i < rows.length - 1 ? "1px solid var(--border)" : "none" }}
            >
              {cols.map((c) => (
                <td key={c} style={{
                  padding: "6px 10px",
                  color: "var(--text-secondary)",
                  maxWidth: 180,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                }}>
                  {String(row[c] ?? "")}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
