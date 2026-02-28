import json
import logging
from .base_agent import BaseDataAgent

logger = logging.getLogger(__name__)

class StatisticalAnalystAgent(BaseDataAgent):
    def __init__(self):
        super().__init__()
        self.agent_name = "Statistical Analyst"
        self.agent_role = "Stats"
        self.agent_icon = "📐"
        self.temperature = 0.2

    def get_system_prompt(self, eda_json: dict, domain: str, processing_logs: list) -> str:
        return """You are an Expert Statistical Analyst.
Analyze statistical properties, flag multicollinearity, and assess distributions."""

    def get_user_prompt(self, eda_json: dict, domain: str, processing_logs: list) -> str:
        return f"""EDA Summary: {json.dumps(eda_json)}

Return ONLY valid JSON with this exact structure:
{{
  "distribution_issues": [{{"column": "str", "issue": "str", "recommendation": "str"}}],
  "multicollinearity_warnings": [{{"col1": "str", "col2": "str", "correlation": 0.0, "action": "str"}}],
  "class_imbalance": {{"detected": true|false, "column": "str", "ratio": "str", "suggestion": "str"}},
  "normalization_needed": [{{"column": "str", "current_range": "str", "suggested_method": "str"}}],
  "statistical_summary": "str"
}}"""

    def json_to_markdown(self, data: dict, meta: dict | None = None) -> str:
        dist_issues = data.get("distribution_issues", []) or []
        correlations = data.get("multicollinearity_warnings", []) or []
        class_imbalance = data.get("class_imbalance", {}) or {}
        normalization_needed = data.get("normalization_needed", []) or []

        severe_corr = [
            item
            for item in correlations
            if abs(self._to_float(item.get("correlation"), default=0.0)) > 0.95
        ]
        high_corr = [
            item
            for item in correlations
            if 0.85 <= abs(self._to_float(item.get("correlation"), default=0.0)) <= 0.95
        ]

        positive_findings: list[str] = []
        if not correlations:
            positive_findings.append("No problematic correlation clusters were detected.")
        if not normalization_needed:
            positive_findings.append("Most numeric features already appear scale-compatible.")
        if not class_imbalance.get("detected"):
            positive_findings.append("Class distribution appears balanced for baseline modeling.")
        if not positive_findings:
            positive_findings.append("Statistical issues are fixable with standard preprocessing.")

        recommendations: list[str] = []
        for item in severe_corr[:5]:
            col1 = self._code_col(item.get("col1", "N/A"))
            col2 = self._code_col(item.get("col2", "N/A"))
            action = str(item.get("action", "Drop one feature or use regularization"))
            recommendations.append(f"**Reduce** — Resolve multicollinearity between {col1} and {col2} ({action}).")
        for item in normalization_needed[:5]:
            col = self._code_col(item.get("column", "N/A"))
            method = str(item.get("suggested_method", "standardization"))
            recommendations.append(f"**Scale** — Apply **{method}** to {col}.")
        if class_imbalance.get("detected"):
            recommendations.append(
                f"**Rebalance** — Address skew in {self._code_col(class_imbalance.get('column', 'target'))} "
                f"using stratified split and class weights."
            )
        if not recommendations:
            recommendations.append("**Proceed** — Continue with baseline model experiments and monitor residual drift.")

        md = self._header(meta)
        md += "\n### 📊 Summary\n"
        md += self._summary(
            data.get("statistical_summary"),
            "This statistical review highlights distribution shape, correlation risk, and scaling needs.",
        )
        md += "\n\n---\n\n"
        md += "### 📈 Key Metrics\n\n"
        md += "| Metric | Value |\n"
        md += "|--------|-------|\n"
        md += f"| **Distribution Issues** | {'⚠️ ' + str(len(dist_issues)) if dist_issues else '✅ 0'} |\n"
        md += f"| **Severe Correlations** | {'⛔ ' + str(len(severe_corr)) if severe_corr else '✅ 0'} |\n"
        md += f"| **High Correlations** | {'⚠️ ' + str(len(high_corr)) if high_corr else '✅ 0'} |\n"
        imbalance_status = (
            self.format_class_imbalance(class_imbalance.get("ratio", "1:1"))
            if class_imbalance.get("detected")
            else "🟢 Balanced"
        )
        md += f"| **Class Balance** | {imbalance_status} |\n"
        md += f"| **Normalization Needed** | {'🟡 ' + str(len(normalization_needed)) if normalization_needed else '✅ 0'} |\n\n"

        critical_rows = []
        for item in severe_corr:
            critical_rows.append(
                {
                    "issue": f"Severe correlation ({self.format_correlation(item.get('correlation'))})",
                    "column": f"{item.get('col1', 'N/A')} ↔ {item.get('col2', 'N/A')}",
                    "impact": "Model coefficients can become unstable and misleading.",
                    "fix": str(item.get("action", "Drop one of the two columns or regularize.")),
                }
            )
        if class_imbalance.get("detected") and "🔴" in self.format_class_imbalance(class_imbalance.get("ratio")):
            critical_rows.append(
                {
                    "issue": f"Severe class imbalance ({self.format_class_imbalance(class_imbalance.get('ratio'))})",
                    "column": str(class_imbalance.get("column", "target")),
                    "impact": "The model may ignore minority classes and miss rare events.",
                    "fix": str(class_imbalance.get("suggestion", "Use weighting/SMOTE and evaluate with recall/F1.")),
                }
            )

        if critical_rows:
            md += "### 🚨 Critical Issues\n\n"
            md += "| Issue | Column | Impact | Fix |\n"
            md += "|-------|--------|--------|-----|\n"
            for row in critical_rows:
                md += (
                    f"| {row['issue']} | {self._code_col(row['column'])} | "
                    f"{row['impact']} | {row['fix']} |\n"
                )
            md += "\n"

        warning_rows = []
        for item in dist_issues:
            warning_rows.append(
                {
                    "warning": str(item.get("issue", "Distribution irregularity")),
                    "column": str(item.get("column", "N/A")),
                    "recommendation": str(item.get("recommendation", "Review transformation strategy")),
                }
            )
        for item in high_corr:
            warning_rows.append(
                {
                    "warning": f"High correlation ({self.format_correlation(item.get('correlation'))})",
                    "column": f"{item.get('col1', 'N/A')} ↔ {item.get('col2', 'N/A')}",
                    "recommendation": str(item.get("action", "Consider feature reduction before modeling.")),
                }
            )
        for item in normalization_needed:
            warning_rows.append(
                {
                    "warning": "Feature scaling recommended",
                    "column": str(item.get("column", "N/A")),
                    "recommendation": (
                        f"Apply {item.get('suggested_method', 'standardization')} "
                        f"(range: {item.get('current_range', 'n/a')})."
                    ),
                }
            )

        if warning_rows:
            md += "### ⚠️ Warnings\n\n"
            md += "| Warning | Column | Recommendation |\n"
            md += "|---------|--------|----------------|\n"
            for row in warning_rows[:15]:
                md += (
                    f"| {row['warning']} | {self._code_col(row['column'])} | "
                    f"{row['recommendation']} |\n"
                )
            md += "\n"

        md += "### ✅ What Looks Good\n"
        for finding in positive_findings:
            md += f"- {finding}\n"
        md += "\n---\n\n### 💡 Recommendations\n"
        for idx, rec in enumerate(recommendations[:8], start=1):
            md += f"{idx}. {rec}\n"

        return md
