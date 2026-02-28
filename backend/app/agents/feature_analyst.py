import json
import logging
from .base_agent import BaseDataAgent

logger = logging.getLogger(__name__)

class FeatureAnalystAgent(BaseDataAgent):
    def __init__(self):
        super().__init__()
        self.agent_name = "Feature Analyst"
        self.agent_role = "SME"
        self.agent_icon = "🧬"
        self.temperature = 0.4

    def get_system_prompt(self, eda_json: dict, domain: str, processing_logs: list) -> str:
        return f"""You are an Expert Data Scientist specializing in {domain} data.
Your task is to interpret what each feature means in the domain context and flag domain-specific anomalies or PII.

Domain Rules:
- healthcare: reference normal ranges for common features (age, bmi, blood_pressure, etc.)
- education: flag grade distributions, identify at-risk indicators
- finance: flag volatility, skew as potential risk signals
- ALWAYS: flag features that may contain PII (Names, Emails, SSN, Addresses)."""

    def get_user_prompt(self, eda_json: dict, domain: str, processing_logs: list) -> str:
        return f"""EDA Summary: {json.dumps(eda_json)}

Return JSON with this exact structure:
{{
  "feature_interpretations": [{{"column": "str", "interpretation": "str", "domain_concern": true, "concern_detail": "str"}}],
  "pii_flags": [{{"column": "str", "pii_type": "name|email|phone|ssn|address|other"}}],
  "recommended_features_for_ml": ["str"],
  "features_to_drop": [{{"column": "str", "reason": "str"}}],
  "domain_summary": "str"
}}"""

    def json_to_markdown(self, data: dict, meta: dict | None = None) -> str:
        pii_flags = data.get("pii_flags", []) or []
        interpretations = data.get("feature_interpretations", []) or []
        recommended = data.get("recommended_features_for_ml", []) or []
        to_drop = data.get("features_to_drop", []) or []

        pii_risk = {
            "ssn": "🔴 Critical",
            "address": "🔴 High",
            "email": "🟡 Medium",
            "phone": "🟡 Medium",
            "name": "🟡 Medium",
            "other": "🟡 Medium",
        }

        domain_concerns = [item for item in interpretations if item.get("domain_concern")]
        positive_findings: list[str] = []
        if not pii_flags:
            positive_findings.append("No obvious PII patterns were flagged.")
        if recommended:
            positive_findings.append(
                f"{len(recommended)} columns appear useful for modeling based on feature semantics."
            )
        if not domain_concerns:
            positive_findings.append("No major domain-specific anomalies were detected.")
        if not positive_findings:
            positive_findings.append("Several features are still usable after targeted cleanup.")

        recommendations: list[str] = []
        for flag in pii_flags:
            col = self._code_col(flag.get("column", "N/A"))
            pii_type = str(flag.get("pii_type", "other")).lower()
            recommendations.append(
                f"**Protect** — Mask or remove {col} because it appears to contain `{pii_type}` data."
            )
        for item in to_drop:
            col = self._code_col(item.get("column", "N/A"))
            reason = str(item.get("reason", "Low signal or risk"))
            recommendations.append(f"**Remove** — Drop {col} ({reason}).")
        if recommended:
            top_cols = ", ".join(self._code_col(col) for col in recommended[:6])
            recommendations.append(f"**Prioritize** — Start model experiments with {top_cols}.")
        if not recommendations:
            recommendations.append("**Review** — Validate feature semantics with a domain expert before training.")

        md = self._header(meta)
        md += "\n### 📊 Summary\n"
        md += self._summary(
            data.get("domain_summary"),
            "This feature-level review highlights which columns are most useful, risky, or potentially sensitive.",
        )
        md += "\n\n---\n\n"
        md += "### 📈 Key Metrics\n\n"
        md += "| Metric | Value |\n"
        md += "|--------|-------|\n"
        md += f"| **PII Flags** | {'🔴 ' + str(len(pii_flags)) if pii_flags else '🟢 0'} |\n"
        md += f"| **Domain Concerns** | {'⚠️ ' + str(len(domain_concerns)) if domain_concerns else '✅ 0'} |\n"
        md += f"| **Recommended Features** | 🟢 {len(recommended)} |\n"
        md += f"| **Suggested Drops** | {'🟡 ' + str(len(to_drop)) if to_drop else '✅ 0'} |\n\n"

        if pii_flags:
            md += "### 🚨 Critical Issues\n\n"
            md += "| Issue | Column | Impact | Fix |\n"
            md += "|-------|--------|--------|-----|\n"
            for flag in pii_flags:
                col = self._code_col(flag.get("column", "N/A"))
                pii_type = str(flag.get("pii_type", "other")).lower()
                risk = pii_risk.get(pii_type, "🟡 Medium")
                md += (
                    f"| PII detected ({pii_type.upper()} - {risk}) | {col} | "
                    f"Sharing/training may violate privacy policy | Mask, hash, or drop this column |\n"
                )
            md += "\n"

        warnings: list[dict[str, str]] = []
        for item in domain_concerns:
            warnings.append(
                {
                    "column": str(item.get("column", "N/A")),
                    "warning": str(item.get("interpretation", "Domain concern detected")),
                    "recommendation": str(
                        item.get("concern_detail", "Validate this feature with domain constraints before modeling.")
                    ),
                }
            )
        for item in to_drop:
            warnings.append(
                {
                    "column": str(item.get("column", "N/A")),
                    "warning": "Low-value or problematic feature",
                    "recommendation": str(item.get("reason", "Drop from training dataset")),
                }
            )

        if warnings:
            md += "### ⚠️ Warnings\n\n"
            md += "| Warning | Column | Recommendation |\n"
            md += "|---------|--------|----------------|\n"
            for warning in warnings[:12]:
                md += (
                    f"| {warning['warning']} | {self._code_col(warning['column'])} | "
                    f"{warning['recommendation']} |\n"
                )
            md += "\n"

        md += "### ✅ What Looks Good\n"
        for finding in positive_findings:
            md += f"- {finding}\n"
        md += "\n"

        if recommended:
            md += "### 🧪 Recommended Features\n\n"
            md += ", ".join(self._code_col(col) for col in recommended)
            md += "\n\n"

        md += "---\n\n### 💡 Recommendations\n"
        for idx, recommendation in enumerate(recommendations[:8], start=1):
            md += f"{idx}. {recommendation}\n"

        return md
