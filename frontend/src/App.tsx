import { useState } from "react";
import Explorer from "./pages/Explorer";
import Dashboard from "./pages/Dashboard";
import "./index.css";

type Page = "explorer" | "dashboard";

const NAV_ITEMS: { id: Page; label: string; icon: string }[] = [
  { id: "dashboard", label: "Overview",  icon: "⬡" },
  { id: "explorer",  label: "Explorer",  icon: "◈" },
];

export default function App() {
  const [page, setPage] = useState<Page>("dashboard");

  return (
    <div style={{ display: "flex", height: "100vh", width: "100vw", overflow: "hidden" }}>
      {/* ── Sidebar ── */}
      <nav style={{
        width: 56,
        background: "var(--bg-surface)",
        borderRight: "1px solid var(--border)",
        display: "flex",
        flexDirection: "column",
        alignItems: "center",
        padding: "16px 0",
        gap: 4,
        flexShrink: 0,
        zIndex: 10,
      }}>
        {/* Logo mark */}
        <div style={{
          width: 32,
          height: 32,
          borderRadius: 6,
          background: "var(--accent-dim)",
          border: "1px solid var(--accent)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          color: "var(--accent)",
          fontFamily: "var(--font-mono)",
          fontWeight: 700,
          fontSize: 14,
          marginBottom: 16,
          flexShrink: 0,
        }}>
          LQ
        </div>

        {NAV_ITEMS.map((item) => (
          <button
            key={item.id}
            onClick={() => setPage(item.id)}
            title={item.label}
            style={{
              width: 40,
              height: 40,
              border: page === item.id
                ? "1px solid var(--accent)"
                : "1px solid transparent",
              borderRadius: 8,
              background: page === item.id ? "var(--accent-dim)" : "transparent",
              color: page === item.id ? "var(--accent)" : "var(--text-muted)",
              cursor: "pointer",
              fontSize: 16,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              transition: "all 0.15s",
            }}
            onMouseEnter={(e) => {
              if (page !== item.id) {
                (e.currentTarget as HTMLButtonElement).style.background = "var(--bg-hover)";
                (e.currentTarget as HTMLButtonElement).style.color = "var(--text-primary)";
              }
            }}
            onMouseLeave={(e) => {
              if (page !== item.id) {
                (e.currentTarget as HTMLButtonElement).style.background = "transparent";
                (e.currentTarget as HTMLButtonElement).style.color = "var(--text-muted)";
              }
            }}
          >
            {item.icon}
          </button>
        ))}
      </nav>

      {/* ── Main content ── */}
      <main style={{ flex: 1, overflow: "hidden", position: "relative" }}>
        {page === "dashboard" && <Dashboard />}
        {page === "explorer"  && <Explorer />}
      </main>
    </div>
  );
}
