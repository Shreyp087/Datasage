import json
from datetime import datetime
from html import escape
from typing import Any, Dict, List

def compress_for_agents(json_summary: Dict[str, Any]) -> Dict[str, Any]:
    """
    Compresses the full EDA JSON down to a strict subset for LLM context windows (< 4000 tokens).
    """
    def r(val):
        return round(val, 3) if isinstance(val, float) else val

    compressed = {
        "shape": json_summary.get("shape"),
        "domain": json_summary.get("domain"),
        "dataset_quality_score": r(json_summary.get("dataset_quality_score")),
        "warnings": json_summary.get("warnings", []),
        "high_correlations": [
            {k: r(v) for k, v in h.items()} for h in json_summary.get("high_correlations", [])
        ],
        "columns": []
    }
    
    for col in json_summary.get("columns", []):
        c_comp = {
            "name": col.get("name"),
            "role": col.get("role"),
            "dtype": col.get("dtype"),
            "null_pct": r(col.get("null_pct")),
            "distribution_type": col.get("distribution_type"),
            "outlier_pct": r(col.get("outlier_pct")),
        }
        
        # Max 3 top values
        top_vals = col.get("top_5_values", [])[:3]
        if top_vals:
            c_comp["top_3_values"] = top_vals
            
        compressed["columns"].append(c_comp)
        
    return compressed

def _log_get(log: Any, key: str, default: Any = None) -> Any:
    if isinstance(log, dict):
        return log.get(key, default)
    return getattr(log, key, default)


def _format_null(null_pct: float) -> tuple[str, str]:
    if null_pct > 0.30:
        return "critical", f"⛔ {null_pct * 100:.1f}%"
    if null_pct >= 0.05:
        return "warning-val", f"⚠️ {null_pct * 100:.1f}%"
    return "ok", f"✅ {null_pct * 100:.1f}%"


def _format_outlier(outlier_pct: float) -> tuple[str, str]:
    if outlier_pct > 0.10:
        return "critical", f"🔴 {outlier_pct * 100:.1f}%"
    if outlier_pct >= 0.01:
        return "warning-val", f"🟡 {outlier_pct * 100:.1f}%"
    return "ok", f"🟢 {outlier_pct * 100:.1f}%"


def _quality_label(score: float) -> tuple[str, str]:
    if score >= 91:
        return "excellent", "Excellent"
    if score >= 71:
        return "good", "Good"
    if score >= 41:
        return "fair", "Fair"
    return "poor", "Poor"


def _describe_processing_change(log: Any) -> str:
    step = str(_log_get(log, "step_name", "pipeline"))
    action = str(_log_get(log, "action", "updated"))
    column = _log_get(log, "column_name")
    reason = _log_get(log, "reason")
    after_value = _log_get(log, "after_value", {})
    if not isinstance(after_value, dict):
        after_value = {}

    col_text = f"`{column}`" if column else "dataset"

    if action == "impute":
        method = after_value.get("method", "imputation")
        fill = after_value.get("fill_val", "calculated value")
        return f"Filled missing values in {col_text} using {method} (value: {fill})."
    if action == "add_indicator":
        return f"Added a missingness indicator column for {col_text} to preserve null signal."
    if action == "flag_outliers":
        count = after_value.get("count", 0)
        pct = after_value.get("pct", 0)
        method = after_value.get("method", "IQR")
        return f"Flagged {count} potential outliers in {col_text} ({float(pct) * 100:.1f}%) using {method}."
    if action == "drop_duplicates":
        removed = after_value.get("rows_removed", after_value.get("count", "multiple"))
        return f"Removed {removed} duplicate records to reduce repeated observations."
    if action == "rename_column":
        new_name = after_value.get("new_name", "normalized name")
        return f"Renamed {col_text} to `{new_name}` for consistent schema naming."
    if action == "coerce_type":
        new_dtype = after_value.get("dtype", "supported type")
        return f"Converted {col_text} to `{new_dtype}` to stabilize downstream analysis."
    if action == "skip_column":
        return f"Skipped {col_text} in {step} due to invalid/statistically unsafe values."
    if reason:
        return str(reason)
    return f"Updated {col_text} during {step} ({action})."


def _plot_src(plot: str) -> str:
    if plot.startswith("data:image"):
        return plot
    return f"data:image/png;base64,{plot}"


def build_html_report(
    json_summary: dict,
    processing_logs: list,
    plots: dict,
    dataset_name: str,
) -> str:
    shape = json_summary.get("shape", {})
    quality_score = float(json_summary.get("dataset_quality_score", 0) or 0)
    columns = json_summary.get("columns", []) or []
    correlations = json_summary.get("high_correlations", []) or []
    warnings = json_summary.get("warnings", []) or []
    domain = str(json_summary.get("domain", "General"))
    q_class, q_label = _quality_label(quality_score)

    critical_cols = [
        c for c in columns if float(c.get("null_pct", 0) or 0) > 0.30 or float(c.get("outlier_pct", 0) or 0) > 0.10
    ]
    warning_cols = [
        c for c in columns if 0.05 < float(c.get("null_pct", 0) or 0) <= 0.30 or 0.01 < float(c.get("outlier_pct", 0) or 0) <= 0.10
    ]
    clean_cols = [
        c for c in columns if float(c.get("null_pct", 0) or 0) <= 0.05 and float(c.get("outlier_pct", 0) or 0) <= 0.01
    ]

    css = """
    :root {
      --primary: #1a1a2e;
      --surface: #16213e;
      --card: #0f3460;
      --accent: #e94560;
      --success: #4ade80;
      --warning: #fbbf24;
      --danger: #f87171;
      --text: #e2e8f0;
      --muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: system-ui, sans-serif;
      background: var(--primary);
      color: var(--text);
      padding: 2rem;
      line-height: 1.6;
    }
    .report-header {
      background: linear-gradient(135deg, var(--card), var(--surface));
      border-radius: 12px;
      padding: 2rem;
      margin-bottom: 2rem;
      border-left: 4px solid var(--accent);
    }
    .quality-gauge {
      display: inline-flex;
      align-items: center;
      gap: 1rem;
      background: rgba(255,255,255,0.05);
      padding: 1rem 1.5rem;
      border-radius: 8px;
    }
    .gauge-score {
      font-size: 2.5rem;
      font-weight: 700;
      min-width: 80px;
      text-align: center;
    }
    .gauge-score.excellent { color: var(--success); }
    .gauge-score.good { color: #86efac; }
    .gauge-score.fair { color: var(--warning); }
    .gauge-score.poor { color: var(--danger); }
    .stats-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
      gap: 1rem;
      margin-bottom: 2rem;
    }
    .stat-card {
      background: var(--surface);
      border-radius: 8px;
      padding: 1.25rem;
      text-align: center;
      border: 1px solid rgba(255,255,255,0.05);
    }
    .stat-value {
      font-size: 1.75rem;
      font-weight: 700;
      color: var(--accent);
    }
    .stat-label {
      font-size: 0.75rem;
      color: var(--muted);
      text-transform: uppercase;
      letter-spacing: 0.05em;
      margin-top: 0.25rem;
    }
    .section {
      margin-bottom: 2rem;
    }
    .section-title {
      font-size: 1.1rem;
      font-weight: 600;
      color: var(--text);
      padding: 0.75rem 1rem;
      background: var(--surface);
      border-radius: 8px 8px 0 0;
      border-bottom: 2px solid var(--accent);
      display: flex;
      align-items: center;
      gap: 0.5rem;
    }
    .columns-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(300px, 1fr));
      gap: 1rem;
      margin-top: 1rem;
    }
    .column-card {
      background: var(--surface);
      border-radius: 8px;
      padding: 1.25rem;
      border: 1px solid rgba(255,255,255,0.05);
      transition: border-color 0.2s;
    }
    .column-card.has-issues { border-color: var(--danger); }
    .column-card.has-warnings { border-color: var(--warning); }
    .column-card.clean { border-color: var(--success); }
    .col-name {
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
      font-size: 0.9rem;
      font-weight: 700;
      color: var(--accent);
      margin-bottom: 0.75rem;
      padding-bottom: 0.5rem;
      border-bottom: 1px solid rgba(255,255,255,0.1);
    }
    .col-meta {
      display: flex;
      flex-wrap: wrap;
      gap: 0.4rem;
      margin-bottom: 0.75rem;
    }
    .badge {
      padding: 0.2rem 0.5rem;
      border-radius: 4px;
      font-size: 0.7rem;
      font-weight: 600;
      text-transform: uppercase;
    }
    .badge-dtype { background: var(--card); color: #93c5fd; }
    .badge-role { background: #1e3a5f; color: #a5b4fc; }
    .metric-row {
      display: flex;
      justify-content: space-between;
      gap: 0.8rem;
      padding: 0.3rem 0;
      font-size: 0.85rem;
      border-bottom: 1px solid rgba(255,255,255,0.04);
    }
    .metric-label { color: var(--muted); }
    .metric-value {
      font-weight: 600;
      text-align: right;
      max-width: 65%;
      word-break: break-word;
    }
    .critical { color: var(--danger); }
    .warning-val { color: var(--warning); }
    .ok { color: var(--success); }
    .info { color: #93c5fd; }
    .mini-bar-container { margin-top: 0.75rem; }
    .mini-bar-label {
      display: flex;
      justify-content: space-between;
      font-size: 0.75rem;
      color: var(--muted);
      margin-bottom: 0.2rem;
      gap: 0.5rem;
    }
    .mini-bar-track {
      height: 6px;
      background: rgba(255,255,255,0.1);
      border-radius: 3px;
      margin-bottom: 0.4rem;
      overflow: hidden;
    }
    .mini-bar-fill {
      height: 100%;
      background: var(--accent);
      border-radius: 3px;
      transition: width 0.3s;
    }
    .alert {
      padding: 1rem 1.25rem;
      border-radius: 8px;
      margin-bottom: 1rem;
      display: flex;
      align-items: flex-start;
      gap: 0.75rem;
    }
    .alert-danger { background: rgba(248,113,113,0.1); border-left: 3px solid var(--danger); }
    .alert-warning { background: rgba(251,191,36,0.1); border-left: 3px solid var(--warning); }
    .alert-success { background: rgba(74,222,128,0.1); border-left: 3px solid var(--success); }
    .correlation-table {
      width: 100%;
      border-collapse: collapse;
      margin-top: 1rem;
    }
    .correlation-table th {
      background: var(--card);
      padding: 0.75rem;
      text-align: left;
      font-size: 0.8rem;
      text-transform: uppercase;
      letter-spacing: 0.05em;
      color: var(--muted);
    }
    .correlation-table td {
      padding: 0.75rem;
      border-bottom: 1px solid rgba(255,255,255,0.05);
      font-size: 0.85rem;
    }
    .correlation-table tr:hover td { background: rgba(255,255,255,0.02); }
    .code { color: var(--accent); font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
    .log-wrap {
      background: var(--surface);
      border-radius: 0 0 8px 8px;
      padding: 1rem;
    }
    .log-item {
      display: flex;
      gap: 1rem;
      padding: 0.6rem 0;
      border-bottom: 1px solid rgba(255,255,255,0.04);
      font-size: 0.82rem;
      align-items: flex-start;
    }
    .log-step {
      color: var(--accent);
      font-weight: 600;
      min-width: 120px;
      font-family: ui-monospace, SFMono-Regular, Menlo, monospace;
    }
    .log-action { color: var(--text); flex: 1; }
    .log-reason { color: var(--muted); max-width: 40%; }
    .plots-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(320px, 1fr));
      gap: 1rem;
      margin-top: 1rem;
    }
    .plot-card {
      background: var(--surface);
      border-radius: 8px;
      padding: 1rem;
      border: 1px solid rgba(255,255,255,0.05);
    }
    .plot-img {
      width: 100%;
      max-width: 800px;
      border-radius: 6px;
      display: block;
      margin: 0.6rem auto 0;
    }
    @media print {
      body { background: white; color: black; padding: 1rem; }
      .column-card, .stat-card, .plot-card { break-inside: avoid; border: 1px solid #ddd; }
      .section-title { border-bottom-color: #777; background: #f3f4f6; color: #111; }
      .report-header { border-left-color: #444; }
    }
    """

    html_parts: list[str] = []
    html_parts.append("<!DOCTYPE html>")
    html_parts.append("<html><head><meta charset='UTF-8'>")
    html_parts.append(f"<title>EDA Report - {escape(dataset_name)}</title>")
    html_parts.append(f"<style>{css}</style></head><body>")

    html_parts.append(
        f"""
        <div class="report-header">
            <h1 style="font-size:1.75rem; margin-bottom:0.5rem;">
                📊 EDA Report — {escape(dataset_name)}
            </h1>
            <p style="color:#94a3b8; margin-bottom:1.5rem;">
                Domain: <strong style="color:#e94560">{escape(domain)}</strong> &nbsp;|&nbsp;
                Generated: <strong>{datetime.utcnow().strftime('%B %d, %Y at %H:%M UTC')}</strong>
            </p>
            <div class="quality-gauge">
                <div>
                    <div class="gauge-score {q_class}">{quality_score:.0f}</div>
                    <div style="font-size:0.75rem;color:#94a3b8;">out of 100</div>
                </div>
                <div>
                    <div style="font-weight:700;font-size:1.1rem;">{q_label} Quality</div>
                    <div style="font-size:0.8rem;color:#94a3b8;">Dataset Quality Score</div>
                </div>
            </div>
        </div>
        """
    )

    html_parts.append(
        f"""
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-value">{int(shape.get('rows', 0)):,}</div>
                <div class="stat-label">Total Rows</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{int(shape.get('cols', 0))}</div>
                <div class="stat-label">Columns</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color:#f87171">{len(critical_cols)}</div>
                <div class="stat-label">Critical Columns</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color:#fbbf24">{len(warning_cols)}</div>
                <div class="stat-label">Warning Columns</div>
            </div>
            <div class="stat-card">
                <div class="stat-value" style="color:#4ade80">{len(clean_cols)}</div>
                <div class="stat-label">Clean Columns</div>
            </div>
            <div class="stat-card">
                <div class="stat-value">{float(json_summary.get('memory_mb', 0) or 0):.1f} MB</div>
                <div class="stat-label">Memory Usage</div>
            </div>
        </div>
        """
    )

    if warnings:
        html_parts.append('<div class="section">')
        html_parts.append('<div class="section-title">⚠️ Dataset-Level Warnings</div>')
        for warning in warnings:
            html_parts.append(f'<div class="alert alert-warning">⚠️ {escape(str(warning))}</div>')
        html_parts.append("</div>")
    else:
        html_parts.append('<div class="section">')
        html_parts.append('<div class="section-title">✅ Dataset-Level Status</div>')
        html_parts.append('<div class="alert alert-success">No global data-quality warnings were generated.</div>')
        html_parts.append("</div>")

    if plots:
        html_parts.append('<div class="section">')
        html_parts.append('<div class="section-title">🖼️ Visual Diagnostics</div>')
        html_parts.append('<div class="plots-grid">')
        for name, plot in plots.items():
            html_parts.append(
                f"""
                <div class="plot-card">
                    <div style="font-weight:600; color:#e2e8f0;">
                        {escape(name.replace('_', ' ').title())}
                    </div>
                    <img class="plot-img" src="{_plot_src(plot)}" alt="{escape(name)}" loading="lazy" />
                </div>
                """
            )
        html_parts.append("</div></div>")

    html_parts.append('<div class="section"><div class="section-title">🗂️ Column Profiles</div><div class="columns-grid">')

    row_total = int(shape.get("rows", 0) or 0)
    for col in columns:
        null_pct = float(col.get("null_pct", 0) or 0)
        outlier_pct = float(col.get("outlier_pct", 0) or 0)

        if null_pct > 0.30 or outlier_pct > 0.10:
            card_class = "has-issues"
        elif null_pct > 0.05 or outlier_pct > 0.01:
            card_class = "has-warnings"
        else:
            card_class = "clean"

        null_class, null_str = _format_null(null_pct)
        out_class, out_str = _format_outlier(outlier_pct)
        null_count = int(col.get("null_count", round(null_pct * row_total)))
        dist = str(col.get("distribution_type", "unknown"))
        unique_count = int(col.get("unique_count", 0) or 0)
        unique_pct = float(col.get("unique_pct", 0) or 0)

        html_parts.append(
            f"""
            <div class="column-card {card_class}">
                <div class="col-name">{escape(str(col.get('name', 'unknown')))}</div>
                <div class="col-meta">
                    <span class="badge badge-dtype">{escape(str(col.get('dtype', 'unknown')))}</span>
                    <span class="badge badge-role">{escape(str(col.get('role', 'feature')))}</span>
                    <span class="badge" style="background:#1a3a2a;color:#86efac;">{escape(dist)}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Missing Values</span>
                    <span class="metric-value {null_class}">{null_str} ({null_count:,} rows)</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Unique Values</span>
                    <span class="metric-value info">{unique_count:,} ({unique_pct * 100:.1f}%)</span>
                </div>
            """
        )

        if col.get("mean") is not None:
            html_parts.append(
                f"""
                <div class="metric-row">
                    <span class="metric-label">Mean / Median</span>
                    <span class="metric-value">{float(col.get('mean', 0)):.3f} / {float(col.get('median', 0)):.3f}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Std Dev</span>
                    <span class="metric-value">{float(col.get('std', 0) or 0):.3f}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Range</span>
                    <span class="metric-value">{escape(str(col.get('min')))} → {escape(str(col.get('max')))}</span>
                </div>
                <div class="metric-row">
                    <span class="metric-label">Outliers</span>
                    <span class="metric-value {out_class}">{out_str} ({int(col.get('outlier_count', 0) or 0):,} rows)</span>
                </div>
                """
            )

        top_vals = col.get("top_5_values", []) or []
        if top_vals and col.get("role") != "id_col":
            html_parts.append('<div class="mini-bar-container"><div style="font-size:0.72rem;color:#94a3b8;margin-bottom:0.4rem;">TOP VALUES</div>')
            max_count = 1
            for item in top_vals[:4]:
                if isinstance(item, dict):
                    max_count = max(max_count, int(item.get("count", 0) or 0))
            for item in top_vals[:4]:
                if not isinstance(item, dict):
                    continue
                value = str(item.get("value", "n/a"))
                count = int(item.get("count", 0) or 0)
                pct_w = (count / max_count * 100.0) if max_count else 0.0
                html_parts.append(
                    f"""
                    <div class="mini-bar-label">
                        <span>{escape(value[:24])}</span>
                        <span>{count:,}</span>
                    </div>
                    <div class="mini-bar-track"><div class="mini-bar-fill" style="width:{pct_w:.0f}%"></div></div>
                    """
                )
            html_parts.append("</div>")

        html_parts.append("</div>")

    html_parts.append("</div></div>")

    if correlations:
        html_parts.append('<div class="section"><div class="section-title">🔗 High Correlations</div>')
        html_parts.append('<table class="correlation-table"><thead><tr><th>Column A</th><th>Column B</th><th>Correlation</th><th>Concern Level</th></tr></thead><tbody>')
        for corr in correlations:
            value = float(corr.get("correlation", 0) or 0)
            abs_value = abs(value)
            if abs_value > 0.95:
                badge = f'<span class="critical">⛔ {abs_value:.3f} — Severe multicollinearity</span>'
            elif abs_value >= 0.85:
                badge = f'<span class="warning-val">⚠️ {abs_value:.3f} — High correlation</span>'
            else:
                badge = f'<span class="ok">✅ {abs_value:.3f}</span>'
            html_parts.append(
                f"""
                <tr>
                    <td><span class="code">{escape(str(corr.get('col1', 'N/A')))}</span></td>
                    <td><span class="code">{escape(str(corr.get('col2', 'N/A')))}</span></td>
                    <td>{value:.4f}</td>
                    <td>{badge}</td>
                </tr>
                """
            )
        html_parts.append("</tbody></table></div>")

    if processing_logs:
        html_parts.append('<div class="section"><div class="section-title">📋 Processing Log</div><div class="log-wrap">')
        for log in processing_logs[:200]:
            severity = str(_log_get(log, "severity", "info")).lower()
            severity_color = {"error": "#f87171", "warning": "#fbbf24", "info": "#94a3b8"}.get(severity, "#94a3b8")
            step = str(_log_get(log, "step_name", "step")).upper()
            action = _describe_processing_change(log)
            raw_reason = _log_get(log, "reason") or ""
            reason_text = escape(str(raw_reason)) if raw_reason else "Auto-generated by pipeline rule."
            html_parts.append(
                f"""
                <div class="log-item">
                    <span class="log-step" style="color:{severity_color}">[{escape(step)}]</span>
                    <span class="log-action">{escape(action)}</span>
                    <span class="log-reason">{reason_text}</span>
                </div>
                """
            )
        html_parts.append("</div></div>")

    html_parts.append("</body></html>")
    return "".join(html_parts)
