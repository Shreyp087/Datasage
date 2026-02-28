import json
import logging
from .base_agent import BaseDataAgent

logger = logging.getLogger(__name__)

class QualityInspectorAgent(BaseDataAgent):
    def __init__(self):
        super().__init__()
        self.agent_name = "Quality Inspector"
        self.agent_role = "QA"
        self.agent_icon = "🔍"
        self.temperature = 0.2

    def get_system_prompt(self, eda_json: dict, domain: str, processing_logs: list) -> str:
        return f"""You are a senior data quality engineer. You receive an EDA summary of a {domain} dataset. 
Analyze it and return a structured quality report. Be specific about column names. 
Do not hallucinate column names — only reference columns present in the provided JSON."""

    def get_user_prompt(self, eda_json: dict, domain: str, processing_logs: list) -> str:
        return f"""Dataset domain: {domain}
EDA Summary: {json.dumps(eda_json, indent=2)}
Processing issues found: {json.dumps(processing_logs[:50])}

Return JSON with this exact structure:
{{
  "overall_quality_grade": "A/B/C/D/F",
  "quality_score": 0-100,
  "critical_issues": [{{"column": "string", "issue": "string", "impact": "string", "fix": "string"}}],
  "warnings": [{{"column": "string", "warning": "string", "recommendation": "string"}}],
  "data_readiness": "production_ready | needs_cleaning | major_issues",
  "summary": "string"
}}"""

    def json_to_markdown(self, data: dict, meta: dict | None = None) -> str:
        grade = str(data.get("overall_quality_grade", "N/A")).upper()
        score = data.get("quality_score", 0)
        readiness = str(data.get("data_readiness", "needs_cleaning"))

        grade_emoji = {"A": "🏆", "B": "✅", "C": "🟡", "D": "🔴", "F": "⛔"}.get(grade, "❓")
        score_str = self.format_quality_score(score)
        readiness_map = {
            "production_ready": "🟢 Production Ready",
            "needs_cleaning": "🟡 Needs Cleaning",
            "major_issues": "🔴 Major Issues Found",
        }
        readiness_str = readiness_map.get(readiness, readiness.replace("_", " ").title())

        critical = data.get("critical_issues", []) or []
        warnings = data.get("warnings", []) or []

        positive_findings: list[str] = []
        if not critical:
            positive_findings.append("No blocking quality failures were detected.")
        if len(warnings) <= 2:
            positive_findings.append("Most columns are usable with light cleanup.")
        if self._to_float(score) >= 71:
            positive_findings.append("Overall quality score indicates the dataset is in good shape.")
        if not positive_findings:
            positive_findings.append("The dataset has salvageable signal once highlighted issues are fixed.")

        recommendations: list[str] = []
        for issue in critical:
            fix = str(issue.get("fix", "")).strip()
            col = self._code_col(issue.get("column", "N/A"))
            if fix:
                recommendations.append(f"**Fix** — Apply `{fix}` to {col}.")
        for warning in warnings:
            reco = str(warning.get("recommendation", "")).strip()
            col = self._code_col(warning.get("column", "N/A"))
            if reco:
                recommendations.append(f"**Improve** — {reco} for {col}.")
        if not recommendations:
            recommendations.append("**Proceed** — Start modeling and monitor drift after deployment.")

        md = self._header(meta)
        md += "\n### 📊 Summary\n"
        md += self._summary(
            data.get("summary"),
            "This dataset is generally usable, with a small number of quality checks that should be reviewed first.",
        )
        md += "\n\n---\n\n"
        md += "### 📈 Key Metrics\n\n"
        md += "| Metric | Value |\n"
        md += "|--------|-------|\n"
        md += f"| **Quality Grade** | {grade_emoji} Grade {grade} |\n"
        md += f"| **Quality Score** | {score_str} |\n"
        md += f"| **Data Readiness** | {readiness_str} |\n"
        md += f"| **Critical Issues** | {len(critical)} |\n"
        md += f"| **Warnings** | {len(warnings)} |\n\n"

        if critical:
            md += "### 🚨 Critical Issues\n\n"
            md += "| Issue | Column | Impact | Fix |\n"
            md += "|-------|--------|--------|-----|\n"
            for issue in critical:
                col = self._code_col(issue.get("column", "N/A"))
                issue_text = str(issue.get("issue", "Quality issue detected"))
                impact = str(issue.get("impact", "Could affect training reliability"))
                fix = str(issue.get("fix", "Investigate and clean column values"))
                null_str = self.format_null_pct(issue.get("null_pct")) if issue.get("null_pct") is not None else ""
                outlier_str = self.format_outlier_pct(issue.get("outlier_pct")) if issue.get("outlier_pct") is not None else ""
                severity_context = " ".join(x for x in [null_str, outlier_str] if x).strip()
                issue_with_context = f"{issue_text} ({severity_context})" if severity_context else issue_text
                md += f"| {issue_with_context} | {col} | {impact} | {fix} |\n"
            md += "\n"

        if warnings:
            md += "### ⚠️ Warnings\n\n"
            md += "| Warning | Column | Recommendation |\n"
            md += "|---------|--------|----------------|\n"
            for warning in warnings:
                col = self._code_col(warning.get("column", "General"))
                warning_text = str(warning.get("warning", "Review this column"))
                recommendation = str(warning.get("recommendation", "Apply targeted cleaning"))
                md += f"| {warning_text} | {col} | {recommendation} |\n"
            md += "\n"

        md += "### ✅ What Looks Good\n"
        for finding in positive_findings:
            md += f"- {finding}\n"
        md += "\n---\n\n"
        md += "### 💡 Recommendations\n"
        for idx, recommendation in enumerate(recommendations[:6], start=1):
            md += f"{idx}. {recommendation}\n"

        return md
