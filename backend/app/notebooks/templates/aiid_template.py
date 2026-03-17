AIID_TEMPLATE = {
    "title": "AI Incident Database — Reproducible Research Notebook",
    "description": (
        "PS5-style AIID Insight Studio template for decision storytelling across "
        "trend, harm, sector, deployer, developer, and risk-intersection views. "
        "Designed for policy teams, journalists, and researchers using repeatable "
        "snapshot analysis in DataSage."
    ),
    "domain": "ai_incidents",
    "is_template": True,
    "is_public": True,
    "tags": [
        "aiid",
        "ai-safety",
        "reproducible",
        "policy",
        "research",
        "insight-studio",
    ],
    "cells": [
        {
            "id": "cell_001",
            "type": "text",
            "title": "📋 AIID PS5 Insight Studio",
            "content": """
## AIID PS5 Insight Studio

One-click, audience-ready notebook flow for **policy teams, journalists, and researchers**.

### What this in-app template delivers
- Snapshot KPI overview
- Incident momentum by year
- Harm and sector concentration
- Risk intersection heatmaps
- Deployer and developer accountability views
- Detailed narrative summary with coverage indicators

### Important scope note
The DataSage notebook runner executes declarative analysis cells, not arbitrary Python cells.
For advanced archive profiling, PK/FK relationship inference, and standalone HTML briefing generation,
use the companion notebook:
- `AIID_Research_Notebook.ipynb` (repo root)
- `notebooks/AIID_PS5_Insight_Studio.ipynb`

### Responsible interpretation
1. Reporting bias is structural: AIID captures documented incidents, not all incidents.
2. Classification coverage varies across snapshots and fields.
3. Recent periods can be undercounted due to reporting and curation lag.
4. Entity attribution reflects available reporting, not legal determination.

### Snapshot Info
- **Source:** https://incidentdatabase.ai/research/snapshots/
- **Snapshot Date:** {snapshot_date}
            """.strip(),
        },
        {
            "id": "cell_002",
            "type": "analysis",
            "title": "📊 Snapshot KPI Overview",
            "description": "Core snapshot totals and baseline indicators for decision context.",
            "analysis_type": "summary",
            "config": {},
            "assumption": "Counts reflect incidents available in this uploaded snapshot.",
            "limitation": "Snapshot coverage is incomplete relative to the full universe of AI harms.",
        },
        {
            "id": "cell_003",
            "type": "analysis",
            "title": "📈 AI Incident Momentum (Yearly)",
            "description": "Year-over-year incident volume trend for longitudinal risk narrative.",
            "analysis_type": "trend",
            "config": {
                "x_field": "year",
                "chart_type": "line",
                "filters": {},
            },
            "assumption": "Incident year is parsed consistently from incident date metadata.",
            "limitation": "Most recent year may be incomplete due to lag in reporting and curation.",
        },
        {
            "id": "cell_004",
            "type": "analysis",
            "title": "⚠️ Harm Type Distribution",
            "description": "Most frequent harm types in the current snapshot.",
            "analysis_type": "distribution",
            "config": {
                "field": "harm_type",
                "chart_type": "bar",
                "top_n": 12,
            },
            "assumption": "Harm labels are normalized to comparable categories.",
            "limitation": "Unclassified incidents are excluded from this breakdown.",
        },
        {
            "id": "cell_005",
            "type": "analysis",
            "title": "🏭 Sector Exposure Distribution",
            "description": "Top deployment sectors associated with reported incidents.",
            "analysis_type": "distribution",
            "config": {
                "field": "sector_of_deployment",
                "chart_type": "horizontal_bar",
                "top_n": 12,
            },
            "assumption": "Sector labels are sufficiently consistent for grouping.",
            "limitation": "Multi-sector incidents can appear in more than one category.",
        },
        {
            "id": "cell_006",
            "type": "analysis",
            "title": "🏢 Top Alleged Deployers",
            "description": "Organizations most frequently named as deployers in incidents.",
            "analysis_type": "top_n",
            "config": {
                "field": "allegeddeployerofaisystem_primary",
                "n": 15,
                "chart_type": "horizontal_bar",
            },
            "assumption": "Primary deployer field captures the most salient attribution per incident.",
            "limitation": "Media visibility can amplify counts for large organizations.",
        },
        {
            "id": "cell_007",
            "type": "analysis",
            "title": "🔥 Risk Interaction Matrix (Harm × Sector)",
            "description": "Cross-tab of dominant harms by deployment sector.",
            "analysis_type": "heatmap",
            "config": {
                "row_field": "harm_type",
                "col_field": "sector_of_deployment",
                "top_n": 10,
            },
            "assumption": "Incidents with both fields populated are representative enough for signal.",
            "limitation": "Sparse classification can hide true cross-domain risk interactions.",
        },
        {
            "id": "cell_008",
            "type": "analysis",
            "title": "📅 Harm Trend Over Time",
            "description": "How dominant harm categories change across years.",
            "analysis_type": "trend",
            "config": {
                "x_field": "year",
                "group_by": "harm_type",
                "chart_type": "line",
                "filters": {},
            },
            "assumption": "Harm taxonomy is stable enough for longitudinal grouping.",
            "limitation": "Taxonomy and labeling practices can shift between snapshot vintages.",
        },
        {
            "id": "cell_009",
            "type": "analysis",
            "title": "🧪 Top Alleged Developers",
            "description": "Organizations most frequently named as developers tied to incidents.",
            "analysis_type": "top_n",
            "config": {
                "field": "allegeddeveloperofaisystem_primary",
                "n": 15,
                "chart_type": "horizontal_bar",
            },
            "assumption": "Primary developer attribution captures high-signal actor mapping.",
            "limitation": "Missing developer fields reduce completeness of accountability views.",
        },
        {
            "id": "cell_010",
            "type": "analysis",
            "title": "🗓️ Incident Rhythm by Month",
            "description": "Month-of-year incident pattern for communications and monitoring cadence.",
            "analysis_type": "distribution",
            "config": {
                "field": "month",
                "chart_type": "bar",
                "top_n": 12,
            },
            "assumption": "Incident dates parse reliably into month values.",
            "limitation": "Historical records with missing dates reduce seasonality fidelity.",
        },
        {
            "id": "cell_011",
            "type": "analysis",
            "title": "📰 Reporting Source Footprint",
            "description": "Most frequent reporting source domains linked to incidents.",
            "analysis_type": "distribution",
            "config": {
                "field": "report_sources",
                "chart_type": "horizontal_bar",
                "top_n": 15,
            },
            "assumption": "Report source metadata reflects external visibility of incidents.",
            "limitation": "Source concentration can bias which incident types are publicly documented.",
        },
        {
            "id": "cell_012",
            "type": "analysis",
            "title": "🧭 Responsibility Matrix (Deployer × Harm)",
            "description": "Cross-map major deployers against common harm categories.",
            "analysis_type": "heatmap",
            "config": {
                "row_field": "allegeddeployerofaisystem_primary",
                "col_field": "harm_type",
                "top_n": 10,
            },
            "assumption": "Top deployer and harm categories capture the strongest accountability signals.",
            "limitation": "This matrix is indicative, not causal or legal attribution evidence.",
        },
        {
            "id": "cell_013",
            "type": "analysis",
            "title": "🧠 Executive Narrative Summary",
            "description": "Consolidated narrative of trend, concentration, and coverage patterns.",
            "analysis_type": "detailed_summary",
            "config": {"top_n": 10},
            "assumption": "Narrative reflects observed snapshot metadata and current field coverage.",
            "limitation": "Interpret outputs as directional evidence under reporting constraints.",
        },
        {
            "id": "cell_014",
            "type": "text",
            "title": "📝 Sample Incident Titles",
            "description": "Randomized sample of incident titles for qualitative context.",
            "analysis_type": "text_sample",
            "config": {
                "field": "title",
                "n": 10,
            },
        },
        {
            "id": "cell_015",
            "type": "text",
            "title": "🔁 Reproducibility & Citation",
            "content": """
## Reproducing This Analysis

### In DataSage
1. Upload an AIID snapshot dataset
2. Open notebooks and select this AIID template
3. Run all cells
4. Export notebook output as HTML or Jupyter

### Full standalone PS5 notebook (advanced flow)
Use the companion notebook files for archive-level profiling and standalone visual brief generation:
- `AIID_Research_Notebook.ipynb`
- `notebooks/AIID_PS5_Insight_Studio.ipynb`

### Data citation
> McGregor, S. (2021) Preventing Repeated Real World AI Failures by
> Cataloging Incidents: The AI Incident Database.
> In Proceedings of the Thirty-Third Annual Conference on Innovative
> Applications of Artificial Intelligence (IAAI-21).

### Snapshot used
This analysis was run against AIID snapshot date **{snapshot_date}**.
Snapshot URL: {snapshot_url}

### Generated
{generated_date}
            """.strip(),
        },
    ],
}
