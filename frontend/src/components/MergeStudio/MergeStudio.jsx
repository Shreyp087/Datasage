import React, { useState, useEffect, useCallback } from "react";
import { datasetsApi } from "../../api/datasets";
import { mergeApi } from "../../api/merge";

// ── Palette & constants ───────────────────────────────────────────────────────
const C = {
  bg:        "#0B0F1A",
  surface:   "#111827",
  surfaceHi: "#1A2236",
  border:    "#1E2D45",
  borderHi:  "#2E4568",
  accent:    "#3B82F6",
  accentDim: "#1D4ED8",
  green:     "#10B981",
  amber:     "#F59E0B",
  red:       "#EF4444",
  muted:     "#64748B",
  text:      "#E2E8F0",
  textDim:   "#94A3B8",
};

const STRATEGY_LABELS = {
  exact:      "Exact match",
  casefold:   "Casefold (lowercase + trim)",
  numeric:    "Numeric cast",
  date:       "Date comparison",
  id_strip:   "ID strip (EMP-007 -> 7)",
  slug:       "Slug (remove non-alphanumeric)",
};

const JOIN_TYPE_LABELS = {
  inner: "INNER — only matched rows",
  left:  "LEFT  — all left rows + matches",
  right: "RIGHT — all right rows + matches",
  outer: "OUTER — all rows from both",
};

// ── Utilities ─────────────────────────────────────────────────────────────────
function confidenceColor(score) {
  if (score >= 75) return C.green;
  if (score >= 45) return C.amber;
  return C.red;
}

function confidenceLabel(score) {
  if (score >= 75) return "High";
  if (score >= 45) return "Medium";
  return "Low";
}

function pct(n, total) {
  if (!total) return "0%";
  return `${Math.round((n / total) * 100)}%`;
}

// ── Sub-components ────────────────────────────────────────────────────────────

function Badge({ label, color = C.accent }) {
  return (
    <span
      style={{
        background: color + "22",
        color,
        border: `1px solid ${color}44`,
        borderRadius: 4,
        padding: "1px 7px",
        fontSize: 11,
        fontFamily: "monospace",
        fontWeight: 600,
        letterSpacing: "0.03em",
      }}
    >
      {label}
    </span>
  );
}

function ConfidenceBar({ score }) {
  const color = confidenceColor(score);
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
      <div
        style={{
          flex: 1,
          height: 6,
          background: C.border,
          borderRadius: 3,
          overflow: "hidden",
        }}
      >
        <div
          style={{
            width: `${score}%`,
            height: "100%",
            background: color,
            borderRadius: 3,
            transition: "width 0.6s ease",
          }}
        />
      </div>
      <span style={{ color, fontWeight: 700, fontSize: 13, minWidth: 42 }}>
        {score.toFixed(0)}%
      </span>
      <span style={{ color: C.muted, fontSize: 11 }}>
        {confidenceLabel(score)}
      </span>
    </div>
  );
}

function DatasetSelector({ datasets, value, onChange, label, side }) {
  const sideColor = side === "left" ? C.accent : C.green;
  return (
    <div style={{ flex: 1 }}>
      <div
        style={{
          fontSize: 11,
          color: sideColor,
          fontWeight: 700,
          letterSpacing: "0.08em",
          textTransform: "uppercase",
          marginBottom: 6,
        }}
      >
        {label}
      </div>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value)}
        style={{
          width: "100%",
          background: C.surfaceHi,
          border: `1.5px solid ${value ? sideColor + "55" : C.border}`,
          borderRadius: 8,
          color: C.text,
          padding: "10px 12px",
          fontSize: 14,
          outline: "none",
          cursor: "pointer",
          appearance: "none",
          backgroundImage: `url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='12' height='8' viewBox='0 0 12 8'%3E%3Cpath d='M1 1l5 5 5-5' stroke='%2364748B' stroke-width='1.5' fill='none'/%3E%3C/svg%3E")`,
          backgroundRepeat: "no-repeat",
          backgroundPosition: "right 12px center",
          paddingRight: 32,
        }}
      >
        <option value="">Select dataset…</option>
        {datasets.map((d) => (
          <option key={d.id} value={d.id}>
            {d.name} ({d.row_count?.toLocaleString() || "?"} rows)
          </option>
        ))}
      </select>
    </div>
  );
}

function SignalBars({ signals }) {
  if (!signals?.length) return null;
  return (
    <div style={{ marginTop: 10 }}>
      {signals.map((s, i) => {
        const signalPts =
          typeof s.score_pts === "number"
            ? s.score_pts
            : typeof s.weight === "number"
              ? s.weight * 100
              : 0;
        return (
          <div
            key={i}
            style={{
              display: "flex",
              alignItems: "center",
              gap: 8,
              marginBottom: 5,
            }}
          >
            <div
              style={{
                width: 80,
                fontSize: 10,
                color: C.muted,
                textAlign: "right",
                flexShrink: 0,
              }}
            >
              {s.type.replace(/_/g, " ")}
            </div>
            <div
              style={{
                flex: 1,
                height: 4,
                background: C.border,
                borderRadius: 2,
                overflow: "hidden",
              }}
            >
              <div
                style={{
                  width: `${Math.min(100, signalPts)}%`,
                  height: "100%",
                  background: `linear-gradient(90deg, ${C.accent}, ${C.green})`,
                  borderRadius: 2,
                }}
              />
            </div>
            <div
              style={{
                fontSize: 10,
                color: C.textDim,
                width: 200,
                flexShrink: 0,
                whiteSpace: "nowrap",
                overflow: "hidden",
                textOverflow: "ellipsis",
              }}
            >
              {s.description}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function CandidateCard({ candidate, isSelected, onSelect, onPreview }) {
  const cc = confidenceColor(candidate.confidence);
  const outputRows = candidate.est_output_rows ?? candidate.merged_rows ?? 0;
  return (
    <div
      onClick={() => onSelect(candidate)}
      style={{
        background: isSelected ? C.surfaceHi : C.surface,
        border: `1.5px solid ${isSelected ? cc : C.border}`,
        borderRadius: 10,
        padding: "14px 16px",
        cursor: "pointer",
        transition: "all 0.15s",
        boxShadow: isSelected ? `0 0 0 2px ${cc}22` : "none",
      }}
    >
      {/* Header row */}
      <div
        style={{
          display: "flex",
          justifyContent: "space-between",
          alignItems: "flex-start",
          marginBottom: 10,
        }}
      >
        <div>
          <div
            style={{
              fontFamily: "monospace",
              fontSize: 14,
              fontWeight: 700,
              color: C.text,
              marginBottom: 3,
            }}
          >
            <span style={{ color: C.accent }}>{candidate.left_col}</span>
            <span style={{ color: C.muted, margin: "0 6px" }}>↔</span>
            <span style={{ color: C.green }}>{candidate.right_col}</span>
          </div>
          <div style={{ display: "flex", gap: 5, flexWrap: "wrap" }}>
            <Badge label={candidate.strategy} color={C.accent} />
            <Badge label={candidate.join_type.toUpperCase()} color={C.green} />
          </div>
        </div>
        <div style={{ textAlign: "right", flexShrink: 0, marginLeft: 12 }}>
          <div
            style={{
              fontSize: 22,
              fontWeight: 800,
              color: cc,
              lineHeight: 1,
            }}
          >
            {candidate.confidence.toFixed(0)}
          </div>
          <div style={{ fontSize: 10, color: C.muted }}>/ 100</div>
        </div>
      </div>

      {/* Confidence bar */}
      <ConfidenceBar score={candidate.confidence} />

      {/* Match stats */}
      <div
        style={{
          display: "flex",
          gap: 16,
          marginTop: 10,
          fontSize: 12,
          color: C.textDim,
        }}
      >
        <span>
          <span style={{ color: C.text, fontWeight: 600 }}>
            {candidate.match_count.toLocaleString()}
          </span>{" "}
          matches
        </span>
        <span>
          <span style={{ color: C.accent }}>
            {candidate.left_match_pct.toFixed(0)}%
          </span>{" "}
          of left
        </span>
        <span>
          <span style={{ color: C.green }}>
            {candidate.right_match_pct.toFixed(0)}%
          </span>{" "}
          of right
        </span>
        <span>
          ~
          <span style={{ color: C.text, fontWeight: 600 }}>
            {outputRows.toLocaleString()}
          </span>{" "}
          output rows
        </span>
      </div>

      {/* Signal bars */}
      {isSelected && <SignalBars signals={candidate.signals} />}

      {/* Preview button */}
      {isSelected && (
        <button
          onClick={(e) => {
            e.stopPropagation();
            onPreview(candidate);
          }}
          style={{
            marginTop: 12,
            background: C.accent,
            color: "#fff",
            border: "none",
            borderRadius: 6,
            padding: "6px 14px",
            fontSize: 12,
            fontWeight: 600,
            cursor: "pointer",
          }}
        >
          Preview merge →
        </button>
      )}
    </div>
  );
}

function PreviewTable({ data, columns }) {
  if (!data?.length) return null;
  const cols = columns?.slice(0, 12) || [];
  return (
    <div
      style={{
        overflowX: "auto",
        borderRadius: 8,
        border: `1px solid ${C.border}`,
        marginTop: 12,
      }}
    >
      <table
        style={{
          width: "100%",
          borderCollapse: "collapse",
          fontSize: 12,
          fontFamily: "monospace",
        }}
      >
        <thead>
          <tr style={{ background: C.surfaceHi }}>
            {cols.map((c) => (
              <th
                key={c}
                style={{
                  padding: "8px 12px",
                  textAlign: "left",
                  color: C.textDim,
                  fontWeight: 600,
                  borderBottom: `1px solid ${C.border}`,
                  whiteSpace: "nowrap",
                }}
              >
                {c}
              </th>
            ))}
            {columns?.length > 12 && (
              <th
                style={{
                  padding: "8px 12px",
                  color: C.muted,
                  borderBottom: `1px solid ${C.border}`,
                }}
              >
                +{columns.length - 12} more
              </th>
            )}
          </tr>
        </thead>
        <tbody>
          {data.slice(0, 20).map((row, i) => (
            <tr
              key={i}
              style={{
                background: i % 2 === 0 ? C.surface : C.surfaceHi,
                transition: "background 0.1s",
              }}
            >
              {cols.map((c) => (
                <td
                  key={c}
                  style={{
                    padding: "7px 12px",
                    color: row[c] === "" ? C.muted : C.text,
                    borderBottom: `1px solid ${C.border}22`,
                    maxWidth: 180,
                    overflow: "hidden",
                    textOverflow: "ellipsis",
                    whiteSpace: "nowrap",
                  }}
                >
                  {row[c] === "" ? <em style={{ color: C.muted }}>null</em> : row[c]}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StatBox({ label, value, sub, color = C.text }) {
  return (
    <div
      style={{
        background: C.surfaceHi,
        border: `1px solid ${C.border}`,
        borderRadius: 8,
        padding: "12px 16px",
        textAlign: "center",
      }}
    >
      <div style={{ fontSize: 22, fontWeight: 800, color }}>{value}</div>
      <div style={{ fontSize: 11, color: C.textDim, marginTop: 2 }}>{label}</div>
      {sub && (
        <div style={{ fontSize: 10, color: C.muted, marginTop: 1 }}>{sub}</div>
      )}
    </div>
  );
}

function WarningBanner({ warnings }) {
  if (!warnings?.length) return null;
  return (
    <div
      style={{
        background: C.amber + "11",
        border: `1px solid ${C.amber}33`,
        borderRadius: 8,
        padding: "10px 14px",
        marginTop: 12,
      }}
    >
      <div
        style={{
          fontSize: 12,
          fontWeight: 700,
          color: C.amber,
          marginBottom: 4,
        }}
      >
        ⚠ Warnings
      </div>
      {warnings.map((w, i) => (
        <div key={i} style={{ fontSize: 12, color: C.textDim, marginTop: 2 }}>
          • {w}
        </div>
      ))}
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export default function MergeStudio() {
  const [datasets, setDatasets]   = useState([]);
  const [leftId,   setLeftId]     = useState("");
  const [rightId,  setRightId]    = useState("");
  const [loading,  setLoading]    = useState(false);
  const [error,    setError]      = useState(null);

  // Detection results
  const [detected,    setDetected]    = useState(null);
  const [selected,    setSelected]    = useState(null);
  const [customLeft,  setCustomLeft]  = useState("");
  const [customRight, setCustomRight] = useState("");
  const [strategy,    setStrategy]    = useState("casefold");
  const [joinType,    setJoinType]    = useState("left");

  // Preview
  const [preview,    setPreview]    = useState(null);
  const [previewing, setPreviewing] = useState(false);

  // Apply
  const [applying,    setApplying]    = useState(false);
  const [applyResult, setApplyResult] = useState(null);
  const [outputName,  setOutputName]  = useState("");

  // Step state
  const [step, setStep] = useState(1); // 1=select, 2=detect, 3=configure, 4=done

  // Load datasets on mount
  useEffect(() => {
    datasetsApi
      .list()
      .then((res) => setDatasets((res.data || []).filter((d) => d.status === "complete")))
      .catch(() => {});
  }, []);

  // Update custom cols when detected
  useEffect(() => {
    if (selected) {
      setCustomLeft(selected.left_col);
      setCustomRight(selected.right_col);
      setStrategy(selected.strategy);
      setJoinType(selected.join_type);
    }
  }, [selected]);

  const handleDetect = useCallback(async () => {
    if (!leftId || !rightId) return;
    setLoading(true);
    setError(null);
    setDetected(null);
    setSelected(null);
    setPreview(null);
    setApplyResult(null);
    try {
      const { data } = await mergeApi.detect({
        left_dataset_id: leftId,
        right_dataset_id: rightId,
        top_n: 8,
      });
      setDetected(data);
      if (data.candidates?.length > 0) {
        setSelected(data.candidates[0]);
      }
      setStep(2);
    } catch (e) {
      setError(e?.response?.data?.detail || e?.message || "Failed to detect merge candidates.");
    } finally {
      setLoading(false);
    }
  }, [leftId, rightId]);

  const handlePreview = useCallback(async (cand) => {
    setPreviewing(true);
    setPreview(null);
    try {
      const { data } = await mergeApi.preview({
        left_dataset_id: leftId,
        right_dataset_id: rightId,
        left_col: cand?.left_col || customLeft,
        right_col: cand?.right_col || customRight,
        strategy: cand?.strategy || strategy,
        join_type: joinType,
        preview_rows: 25,
      });
      setPreview(data);
      setStep(3);
    } catch (e) {
      setError(e?.response?.data?.detail || e?.message || "Failed to preview merge.");
    } finally {
      setPreviewing(false);
    }
  }, [leftId, rightId, customLeft, customRight, strategy, joinType]);

  const handleApply = useCallback(async () => {
    setApplying(true);
    setError(null);
    try {
      const { data } = await mergeApi.apply({
        left_dataset_id: leftId,
        right_dataset_id: rightId,
        left_col: customLeft,
        right_col: customRight,
        strategy,
        join_type: joinType,
        output_name: outputName || undefined,
      });
      setApplyResult(data);
      setStep(4);
    } catch (e) {
      setError(e?.response?.data?.detail || e?.message || "Failed to apply merge.");
    } finally {
      setApplying(false);
    }
  }, [leftId, rightId, customLeft, customRight, strategy, joinType, outputName]);

  // ── Layout ─────────────────────────────────────────────────────────────────

  const leftDs  = datasets.find((d) => d.id === leftId);
  const rightDs = datasets.find((d) => d.id === rightId);

  return (
    <div
      style={{
        minHeight: "100vh",
        background: C.bg,
        color: C.text,
        fontFamily: "'Inter', 'Segoe UI', system-ui, sans-serif",
        padding: "0 0 80px",
      }}
    >
      {/* ── Header ─────────────────────────────────────────────────────────── */}
      <div
        style={{
          borderBottom: `1px solid ${C.border}`,
          padding: "20px 32px",
          display: "flex",
          justifyContent: "space-between",
          alignItems: "center",
          background: C.surface,
          position: "sticky",
          top: 0,
          zIndex: 10,
        }}
      >
        <div>
          <div
            style={{
              fontSize: 20,
              fontWeight: 800,
              letterSpacing: "-0.02em",
              display: "flex",
              alignItems: "center",
              gap: 10,
            }}
          >
            <span style={{ fontSize: 22 }}>⟳</span>
            <span>Merge Studio</span>
            <Badge label="AUTO-JOIN" color={C.accent} />
          </div>
          <div style={{ fontSize: 12, color: C.muted, marginTop: 2 }}>
            Intelligently joins two datasets — detects keys, strategies, and quality automatically
          </div>
        </div>

        {/* Step indicator */}
        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
          {["Select", "Detect", "Configure", "Done"].map((s, i) => {
            const active  = step === i + 1;
            const done    = step > i + 1;
            return (
              <div
                key={s}
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: 6,
                }}
              >
                <div
                  style={{
                    width: 24,
                    height: 24,
                    borderRadius: "50%",
                    background: done ? C.green : active ? C.accent : C.border,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                    fontSize: 11,
                    fontWeight: 700,
                    color: done || active ? "#fff" : C.muted,
                  }}
                >
                  {done ? "✓" : i + 1}
                </div>
                <span
                  style={{
                    fontSize: 12,
                    color: active ? C.text : done ? C.green : C.muted,
                    fontWeight: active ? 700 : 400,
                  }}
                >
                  {s}
                </span>
                {i < 3 && (
                  <span style={{ color: C.border, fontSize: 14, marginLeft: -2 }}>›</span>
                )}
              </div>
            );
          })}
        </div>
      </div>

      <div style={{ maxWidth: 1100, margin: "0 auto", padding: "28px 32px" }}>
        {/* ── Error banner ──────────────────────────────────────────────────── */}
        {error && (
          <div
            style={{
              background: C.red + "11",
              border: `1px solid ${C.red}33`,
              borderRadius: 8,
              padding: "10px 16px",
              marginBottom: 20,
              fontSize: 13,
              color: C.red,
            }}
          >
            ✕ {error}
          </div>
        )}

        {/* ── Step 1: Dataset Selection ──────────────────────────────────────── */}
        <div
          style={{
            background: C.surface,
            border: `1px solid ${C.border}`,
            borderRadius: 12,
            padding: "20px 24px",
            marginBottom: 20,
          }}
        >
          <div
            style={{
              fontSize: 13,
              fontWeight: 700,
              color: C.textDim,
              marginBottom: 16,
              letterSpacing: "0.05em",
              textTransform: "uppercase",
            }}
          >
            Step 1 — Select Datasets to Merge
          </div>

          <div style={{ display: "flex", gap: 16, alignItems: "flex-end" }}>
            <DatasetSelector
              datasets={datasets}
              value={leftId}
              onChange={setLeftId}
              label="Left Dataset (base)"
              side="left"
            />

            <div
              style={{
                fontSize: 20,
                color: C.muted,
                paddingBottom: 12,
                flexShrink: 0,
              }}
            >
              +
            </div>

            <DatasetSelector
              datasets={datasets}
              value={rightId}
              onChange={setRightId}
              label="Right Dataset (join)"
              side="right"
            />

            <button
              onClick={handleDetect}
              disabled={!leftId || !rightId || leftId === rightId || loading}
              style={{
                background:
                  !leftId || !rightId || leftId === rightId || loading
                    ? C.border
                    : `linear-gradient(135deg, ${C.accent}, ${C.accentDim})`,
                color: !leftId || !rightId || leftId === rightId ? C.muted : "#fff",
                border: "none",
                borderRadius: 8,
                padding: "10px 22px",
                fontSize: 14,
                fontWeight: 700,
                cursor:
                  !leftId || !rightId || leftId === rightId ? "not-allowed" : "pointer",
                flexShrink: 0,
                whiteSpace: "nowrap",
                transition: "all 0.2s",
                boxShadow:
                  leftId && rightId && leftId !== rightId && !loading
                    ? `0 4px 20px ${C.accent}44`
                    : "none",
              }}
            >
              {loading ? "⟳ Detecting…" : "Auto-Detect Joins →"}
            </button>
          </div>

          {leftDs && rightDs && (
            <div
              style={{
                marginTop: 12,
                display: "flex",
                gap: 16,
                fontSize: 12,
                color: C.muted,
              }}
            >
              <span>
                <span style={{ color: C.accent }}>●</span>{" "}
                {leftDs.name}: {leftDs.row_count?.toLocaleString()} rows ×{" "}
                {leftDs.col_count} cols
              </span>
              <span>
                <span style={{ color: C.green }}>●</span>{" "}
                {rightDs.name}: {rightDs.row_count?.toLocaleString()} rows ×{" "}
                {rightDs.col_count} cols
              </span>
            </div>
          )}
        </div>

        {/* ── Step 2: Candidates ────────────────────────────────────────────── */}
        {detected && (
          <div
            style={{
              background: C.surface,
              border: `1px solid ${C.border}`,
              borderRadius: 12,
              padding: "20px 24px",
              marginBottom: 20,
            }}
          >
            <div
              style={{
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                marginBottom: 16,
              }}
            >
              <div
                style={{
                  fontSize: 13,
                  fontWeight: 700,
                  color: C.textDim,
                  letterSpacing: "0.05em",
                  textTransform: "uppercase",
                }}
              >
                Step 2 — Detected Join Candidates
              </div>
              <span style={{ fontSize: 12, color: C.muted }}>
                {detected.candidates.length} candidates found
                {" · "}
                click to expand signals
              </span>
            </div>

            {detected.candidates.length === 0 ? (
              <div
                style={{
                  padding: "24px",
                  textAlign: "center",
                  color: C.muted,
                  fontSize: 14,
                }}
              >
                No join candidates found. Try manually selecting columns below.
              </div>
            ) : (
              <div
                style={{
                  display: "grid",
                  gridTemplateColumns: "repeat(auto-fill, minmax(320px, 1fr))",
                  gap: 12,
                }}
              >
                {detected.candidates.map((c, i) => (
                  <CandidateCard
                    key={i}
                    candidate={c}
                    isSelected={selected?.left_col === c.left_col && selected?.right_col === c.right_col}
                    onSelect={setSelected}
                    onPreview={handlePreview}
                  />
                ))}
              </div>
            )}

            {/* Manual override */}
            <div
              style={{
                marginTop: 16,
                paddingTop: 16,
                borderTop: `1px solid ${C.border}`,
              }}
            >
              <div
                style={{
                  fontSize: 12,
                  color: C.muted,
                  marginBottom: 10,
                  fontWeight: 600,
                }}
              >
                MANUAL OVERRIDE — customize join columns
              </div>
              <div style={{ display: "flex", gap: 12, flexWrap: "wrap", alignItems: "flex-end" }}>
                <div>
                  <div style={{ fontSize: 11, color: C.accent, marginBottom: 4 }}>Left column</div>
                  <select
                    value={customLeft}
                    onChange={(e) => setCustomLeft(e.target.value)}
                    style={{
                      background: C.surfaceHi,
                      border: `1px solid ${C.border}`,
                      borderRadius: 6,
                      color: C.text,
                      padding: "7px 10px",
                      fontSize: 13,
                      outline: "none",
                    }}
                  >
                    <option value="">Select…</option>
                    {detected.left_cols.map((c) => (
                      <option key={c} value={c}>{c}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <div style={{ fontSize: 11, color: C.green, marginBottom: 4 }}>Right column</div>
                  <select
                    value={customRight}
                    onChange={(e) => setCustomRight(e.target.value)}
                    style={{
                      background: C.surfaceHi,
                      border: `1px solid ${C.border}`,
                      borderRadius: 6,
                      color: C.text,
                      padding: "7px 10px",
                      fontSize: 13,
                      outline: "none",
                    }}
                  >
                    <option value="">Select…</option>
                    {detected.right_cols.map((c) => (
                      <option key={c} value={c}>{c}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <div style={{ fontSize: 11, color: C.textDim, marginBottom: 4 }}>Strategy</div>
                  <select
                    value={strategy}
                    onChange={(e) => setStrategy(e.target.value)}
                    style={{
                      background: C.surfaceHi,
                      border: `1px solid ${C.border}`,
                      borderRadius: 6,
                      color: C.text,
                      padding: "7px 10px",
                      fontSize: 13,
                      outline: "none",
                    }}
                  >
                    {Object.entries(STRATEGY_LABELS).map(([v, l]) => (
                      <option key={v} value={v}>{l}</option>
                    ))}
                  </select>
                </div>

                <div>
                  <div style={{ fontSize: 11, color: C.textDim, marginBottom: 4 }}>Join type</div>
                  <select
                    value={joinType}
                    onChange={(e) => setJoinType(e.target.value)}
                    style={{
                      background: C.surfaceHi,
                      border: `1px solid ${C.border}`,
                      borderRadius: 6,
                      color: C.text,
                      padding: "7px 10px",
                      fontSize: 13,
                      outline: "none",
                    }}
                  >
                    {Object.entries(JOIN_TYPE_LABELS).map(([v, l]) => (
                      <option key={v} value={v}>{l}</option>
                    ))}
                  </select>
                </div>

                <button
                  onClick={() => handlePreview(null)}
                  disabled={!customLeft || !customRight || previewing}
                  style={{
                    background: C.surfaceHi,
                    border: `1px solid ${C.borderHi}`,
                    borderRadius: 6,
                    color: C.text,
                    padding: "7px 14px",
                    fontSize: 13,
                    cursor: !customLeft || !customRight ? "not-allowed" : "pointer",
                    fontWeight: 600,
                  }}
                >
                  {previewing ? "⟳" : "Preview →"}
                </button>
              </div>
            </div>
          </div>
        )}

        {/* ── Step 3: Preview & Apply ────────────────────────────────────────── */}
        {preview && (
          <div
            style={{
              background: C.surface,
              border: `1px solid ${C.border}`,
              borderRadius: 12,
              padding: "20px 24px",
              marginBottom: 20,
            }}
          >
            <div
              style={{
                fontSize: 13,
                fontWeight: 700,
                color: C.textDim,
                marginBottom: 16,
                letterSpacing: "0.05em",
                textTransform: "uppercase",
              }}
            >
              Step 3 — Preview Merge & Apply
            </div>

            {/* Stats grid */}
            <div
              style={{
                display: "grid",
                gridTemplateColumns: "repeat(5, 1fr)",
                gap: 10,
                marginBottom: 16,
              }}
            >
              <StatBox
                label="Total output rows"
                value={preview.total_rows.toLocaleString()}
                color={C.text}
              />
              <StatBox
                label="Matched rows"
                value={preview.matched_rows.toLocaleString()}
                sub={pct(preview.matched_rows, preview.total_rows)}
                color={C.green}
              />
              <StatBox
                label="Left only"
                value={preview.left_only.toLocaleString()}
                sub={joinType === "inner" ? "dropped" : "kept"}
                color={preview.left_only > 0 ? C.amber : C.muted}
              />
              <StatBox
                label="Right only"
                value={preview.right_only.toLocaleString()}
                sub={joinType === "inner" ? "dropped" : "kept"}
                color={preview.right_only > 0 ? C.amber : C.muted}
              />
              <StatBox
                label="Output columns"
                value={preview.columns?.length || 0}
                color={C.accent}
              />
            </div>

            <WarningBanner warnings={preview.warnings} />

            {/* Data preview table */}
            <PreviewTable data={preview.preview} columns={preview.columns} />

            {/* Apply section */}
            <div
              style={{
                marginTop: 20,
                paddingTop: 20,
                borderTop: `1px solid ${C.border}`,
                display: "flex",
                gap: 12,
                alignItems: "center",
                flexWrap: "wrap",
              }}
            >
              <div style={{ flex: 1, minWidth: 240 }}>
                <div style={{ fontSize: 11, color: C.textDim, marginBottom: 4 }}>
                  Output dataset name (optional)
                </div>
                <input
                  value={outputName}
                  onChange={(e) => setOutputName(e.target.value)}
                  placeholder={`${leftDs?.name || "left"} + ${rightDs?.name || "right"} [${joinType}]`}
                  style={{
                    width: "100%",
                    background: C.surfaceHi,
                    border: `1px solid ${C.border}`,
                    borderRadius: 6,
                    color: C.text,
                    padding: "8px 12px",
                    fontSize: 13,
                    outline: "none",
                    boxSizing: "border-box",
                  }}
                />
              </div>
              <button
                onClick={handleApply}
                disabled={applying}
                style={{
                  background: applying
                    ? C.border
                    : `linear-gradient(135deg, ${C.green}, #059669)`,
                  color: applying ? C.muted : "#fff",
                  border: "none",
                  borderRadius: 8,
                  padding: "10px 24px",
                  fontSize: 14,
                  fontWeight: 700,
                  cursor: applying ? "not-allowed" : "pointer",
                  whiteSpace: "nowrap",
                  boxShadow: applying ? "none" : `0 4px 20px ${C.green}44`,
                  transition: "all 0.2s",
                }}
              >
                {applying ? "⟳ Merging…" : "✓ Apply Merge & Save"}
              </button>
            </div>
          </div>
        )}

        {/* ── Step 4: Success ────────────────────────────────────────────────── */}
        {applyResult && (
          <div
            style={{
              background: C.green + "0D",
              border: `1.5px solid ${C.green}44`,
              borderRadius: 12,
              padding: "24px 28px",
              textAlign: "center",
            }}
          >
            <div style={{ fontSize: 36, marginBottom: 8 }}>✓</div>
            <div
              style={{
                fontSize: 20,
                fontWeight: 800,
                color: C.green,
                marginBottom: 6,
              }}
            >
              Merge Complete
            </div>
            <div
              style={{ fontSize: 14, color: C.textDim, marginBottom: 20 }}
            >
              {applyResult.output_name} — saved as new dataset
            </div>
            <div
              style={{
                display: "flex",
                gap: 16,
                justifyContent: "center",
                flexWrap: "wrap",
                marginBottom: 20,
              }}
            >
              <StatBox
                label="Total rows"
                value={applyResult.merged_rows.toLocaleString()}
                color={C.text}
              />
              <StatBox
                label="Matched"
                value={applyResult.matched_rows.toLocaleString()}
                color={C.green}
              />
              <StatBox
                label="Left only"
                value={applyResult.left_only_rows.toLocaleString()}
                color={C.amber}
              />
              <StatBox
                label="Right only"
                value={applyResult.right_only_rows.toLocaleString()}
                color={C.amber}
              />
            </div>
            <WarningBanner warnings={applyResult.warnings} />
            <div
              style={{
                marginTop: 20,
                display: "flex",
                gap: 12,
                justifyContent: "center",
              }}
            >
              <a
                href={`/dataset/${applyResult.output_dataset_id}`}
                style={{
                  background: C.accent,
                  color: "#fff",
                  textDecoration: "none",
                  borderRadius: 8,
                  padding: "10px 20px",
                  fontSize: 13,
                  fontWeight: 700,
                }}
              >
                Open merged dataset →
              </a>
              <button
                onClick={() => {
                  setStep(1);
                  setDetected(null);
                  setSelected(null);
                  setPreview(null);
                  setApplyResult(null);
                  setOutputName("");
                }}
                style={{
                  background: "transparent",
                  border: `1px solid ${C.border}`,
                  color: C.textDim,
                  borderRadius: 8,
                  padding: "10px 20px",
                  fontSize: 13,
                  cursor: "pointer",
                }}
              >
                Start new merge
              </button>
            </div>
          </div>
        )}

        {/* ── Empty state ───────────────────────────────────────────────────── */}
        {!detected && !loading && (
          <div
            style={{
              textAlign: "center",
              padding: "48px 0",
              color: C.muted,
            }}
          >
            <div style={{ fontSize: 40, marginBottom: 12, opacity: 0.4 }}>⟳</div>
            <div style={{ fontSize: 16, marginBottom: 6 }}>
              Select two datasets to auto-detect join keys
            </div>
            <div style={{ fontSize: 13 }}>
              The engine scores column pairs using name similarity,
              semantic keywords, dtype compatibility, and value overlap
            </div>
          </div>
        )}

        {loading && (
          <div
            style={{
              textAlign: "center",
              padding: "48px 0",
              color: C.accent,
              fontSize: 15,
            }}
          >
            <div
              style={{
                fontSize: 36,
                marginBottom: 12,
                animation: "spin 1s linear infinite",
              }}
            >
              ⟳
            </div>
            Analysing column pairs and computing overlap…
            <style>{`@keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }`}</style>
          </div>
        )}
      </div>
    </div>
  );
}
