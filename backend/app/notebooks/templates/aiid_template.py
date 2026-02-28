AIID_TEMPLATE = {
    "title": "AI Incident Database — Reproducible Research Notebook",
    "description": (
        "A reusable analytical framework for exploring the AI Incident Database. "
        "Run this on any AIID snapshot to reproduce the full analysis. "
        "Covers trends, harm types, deployers, sectors, and responsible "
        "interpretation notes. Built with DataSage."
    ),
    "domain": "ai_incidents",
    "is_template": True,
    "is_public": True,
    "tags": ["aiid", "ai-safety", "reproducible", "policy", "research"],
    "cells": [
        {
            "id": "cell_001",
            "type": "text",
            "title": "📋 About This Notebook",
            "content": """
## AI Incident Database — Reproducible Research Notebook

**Dataset:** AI Incident Database (AIID) — https://incidentdatabase.ai
**Citation:** McGregor, S. (2021) Preventing Repeated Real World AI Failures
by Cataloging Incidents: The AI Incident Database. IAAI-21.

### What This Notebook Does
This notebook provides a standardized analytical framework for
understanding trends in documented AI failures and harms.
It is designed to be **reproducible** — run it on any AIID snapshot
and you will get the same analysis updated to that snapshot's data.

### ⚠️ Responsible Interpretation Notes
1. **Reporting bias exists.** Incidents in AIID are those that received
   media coverage. Less-publicized harms are underrepresented.
2. **This is not a comprehensive census** of AI failures — only those
   discovered and submitted to the database.
3. **Categories are evolving.** Taxonomy classifications may change
   across snapshots. Compare snapshots with caution.
4. **Correlation ≠ causation.** Trends in the data reflect documentation
   patterns as much as underlying incident rates.
5. **Entity attribution is complex.** The line between deployer and
   developer is not always clear-cut.

### Snapshot Info
- **Source:** https://incidentdatabase.ai/research/snapshots/
- **Format:** CSV extracted from tar.bz2 weekly backup
- **Snapshot Date:** {snapshot_date}
            """.strip(),
        },
        {
            "id": "cell_002",
            "type": "analysis",
            "title": "📊 Dataset Overview",
            "description": "High-level summary of this AIID snapshot",
            "analysis_type": "summary",
            "config": {},
            "assumption": "Counts reflect only submitted and accepted incidents.",
            "limitation": "Does not represent the full universe of AI harms.",
        },
        {
            "id": "cell_003",
            "type": "analysis",
            "title": "📈 Incidents Per Year",
            "description": (
                "How many AI incidents were reported each year? "
                "Shows growth trend in AI-related harms over time."
            ),
            "analysis_type": "trend",
            "config": {
                "x_field": "year",
                "chart_type": "line",
                "filters": {},
            },
            "assumption": (
                "Incident date reflects the date of the AI-related event, "
                "not the submission date."
            ),
            "limitation": (
                "Recent years may appear lower due to reporting lag — "
                "incidents take time to be submitted and accepted."
            ),
        },
        {
            "id": "cell_004",
            "type": "analysis",
            "title": "⚠️ Incidents by Harm Type",
            "description": (
                "What types of harm are most commonly documented? "
                "Uses CSET taxonomy classifications."
            ),
            "analysis_type": "distribution",
            "config": {
                "field": "harm_type",
                "chart_type": "bar",
                "top_n": 15,
            },
            "assumption": "Harm type classification uses CSET taxonomy.",
            "limitation": (
                "Many incidents have no harm type classification — "
                "unclassified incidents are excluded from this chart."
            ),
        },
        {
            "id": "cell_005",
            "type": "analysis",
            "title": "🏭 Incidents by Sector of Deployment",
            "description": (
                "Which industries are seeing the most AI incidents? "
                "Helps identify high-risk sectors."
            ),
            "analysis_type": "distribution",
            "config": {
                "field": "sector_of_deployment",
                "chart_type": "horizontal_bar",
                "top_n": 12,
            },
            "assumption": "Sector classification uses CSET taxonomy.",
            "limitation": "Multi-sector incidents are counted in each applicable sector.",
        },
        {
            "id": "cell_006",
            "type": "analysis",
            "title": "🏢 Top Alleged Deployers",
            "description": (
                "Which organizations are most frequently named as "
                "deployers of AI systems involved in incidents?"
            ),
            "analysis_type": "top_n",
            "config": {
                "field": "allegeddeployerofaisystem_primary",
                "n": 15,
                "chart_type": "horizontal_bar",
            },
            "assumption": (
                "Deployer attribution is based on incident reports, "
                "not legal determinations."
            ),
            "limitation": (
                "Large organizations have more coverage in media, "
                "introducing reporting bias toward well-known companies."
            ),
        },
        {
            "id": "cell_007",
            "type": "analysis",
            "title": "🔥 Harm Type × Sector Heatmap",
            "description": (
                "Which harm types appear most in which sectors? "
                "Reveals sector-specific risk patterns."
            ),
            "analysis_type": "heatmap",
            "config": {
                "row_field": "harm_type",
                "col_field": "sector_of_deployment",
                "top_n": 8,
            },
            "assumption": "Only incidents with both fields classified are shown.",
            "limitation": (
                "Classification coverage is partial — "
                "interpret patterns as indicative, not definitive."
            ),
        },
        {
            "id": "cell_008",
            "type": "analysis",
            "title": "📅 Trend by Harm Type Over Time",
            "description": (
                "How has the mix of harm types changed year over year? "
                "Shows whether certain harms are increasing or declining."
            ),
            "analysis_type": "trend",
            "config": {
                "x_field": "year",
                "group_by": "harm_type",
                "chart_type": "line",
                "filters": {},
            },
            "assumption": "Uses CSET harm type taxonomy.",
            "limitation": "Pre-2018 data is sparse — focus on 2018 onwards.",
        },
        {
            "id": "cell_009",
            "type": "text",
            "title": "📝 Sample Incident Titles",
            "description": "A random sample of incident titles for context",
            "analysis_type": "text_sample",
            "config": {
                "field": "title",
                "n": 10,
            },
        },
        {
            "id": "cell_010",
            "type": "text",
            "title": "🔁 Reproducibility & Citation",
            "content": """
## Reproducing This Analysis

### Steps to Reproduce
1. Download an AIID snapshot from: https://incidentdatabase.ai/research/snapshots/
2. Upload to DataSage and select domain: **AI Incidents (AIID)**
3. Navigate to Notebooks → select **AIID Research Template**
4. Click **Run All Cells**
5. Export as HTML, PDF, or .ipynb

### Data Citation
> McGregor, S. (2021) Preventing Repeated Real World AI Failures by
> Cataloging Incidents: The AI Incident Database.
> In Proceedings of the Thirty-Third Annual Conference on Innovative
> Applications of Artificial Intelligence (IAAI-21).

### Snapshot Used
This analysis was run against the AIID snapshot dated **{snapshot_date}**.
Downloaded from: {snapshot_url}

### Assumptions & Limitations Summary
| Cell | Key Assumption | Key Limitation |
|------|----------------|----------------|
| Incidents Per Year | Date = event date | Reporting lag in recent years |
| Harm Type | CSET taxonomy | Partial classification coverage |
| Sector | CSET taxonomy | Multi-sector incidents counted multiple times |
| Top Deployers | Media attribution | Bias toward well-known organizations |
| Heatmap | Both fields classified | Partial coverage |

### Tool Citation
> DataSage Reproducible Notebook System.
> AI Incident Database Analysis.
> Generated: {generated_date}
            """.strip(),
        },
    ],
}
