import json
import logging
from .base_agent import BaseDataAgent

logger = logging.getLogger(__name__)

class MLAdvisorAgent(BaseDataAgent):
    def __init__(self):
        super().__init__()
        self.agent_name = "ML Advisor"
        self.agent_role = "ML"
        self.agent_icon = "🤖"
        self.temperature = 0.4

    def get_system_prompt(self, combined_context: dict) -> str:
        return """You are a Principal Machine Learning Advisor.
Synthesize findings from the Quality Inspector, Feature Analyst, and Statistical Analyst into an ML readiness assessment."""

    def get_user_prompt(self, combined_context: dict) -> str:
        return f"""Context from previous agents: {json.dumps(combined_context)}

Return ONLY valid JSON with this exact structure:
{{
  "ml_readiness_score": 0-10,
  "suggested_problem_types": ["classification", "regression", "clustering", "time_series", "nlp"],
  "suggested_algorithms": [{{"algorithm": "str", "reason": "str", "priority": "high/medium/low"}}],
  "blockers": ["str"],
  "quick_wins": ["str"],
  "estimated_preprocessing_effort": "low/medium/high",
  "readiness_summary": "str"
}}"""

    def json_to_markdown(self, data: dict, meta: dict | None = None) -> str:
        score = self._to_float(data.get("ml_readiness_score", 0), default=0.0)
        filled = max(0, min(10, int(round(score))))
        bar = "█" * filled + "░" * (10 - filled)

        if score >= 8:
            score_badge = f"🟢 {score:.1f}/10 — Ready for ML"
        elif score >= 5:
            score_badge = f"🟡 {score:.1f}/10 — Needs Preparation"
        else:
            score_badge = f"🔴 {score:.1f}/10 — Not Ready"

        effort = str(data.get("estimated_preprocessing_effort", "medium")).lower()
        effort_map = {"low": "🟢 Low", "medium": "🟡 Medium", "high": "🔴 High"}
        effort_badge = effort_map.get(effort, effort.replace("_", " ").title())

        blockers = data.get("blockers", []) or []
        quick_wins = data.get("quick_wins", []) or []
        algorithms = data.get("suggested_algorithms", []) or []
        problem_types = data.get("suggested_problem_types", []) or []

        positive_findings: list[str] = []
        if not blockers:
            positive_findings.append("No major blockers are preventing immediate model prototyping.")
        if quick_wins:
            positive_findings.append("There are quick, high-impact improvements that can raise readiness fast.")
        if algorithms:
            positive_findings.append("Several algorithm families already fit the data profile.")
        if not positive_findings:
            positive_findings.append("The dataset can become ML-ready after a focused cleanup sprint.")

        recommendations: list[str] = []
        for blocker in blockers[:5]:
            recommendations.append(f"**Unblock** — {blocker}")
        for win in quick_wins[:5]:
            recommendations.append(f"**Implement** — {win}")
        for algo in algorithms[:3]:
            name = str(algo.get("algorithm", "Model"))
            reason = str(algo.get("reason", "Matches data structure"))
            recommendations.append(f"**Test** — Benchmark **{name}** first because it {reason.lower()}.")
        if not recommendations:
            recommendations.append("**Start** — Run baseline experiments and iterate with error analysis.")

        md = self._header(meta)
        md += "\n### 📊 Summary\n"
        md += self._summary(
            data.get("readiness_summary"),
            "This dataset is close to machine-learning readiness and needs targeted preprocessing to reduce risk.",
        )
        md += "\n\n---\n\n"
        md += "### 📈 Key Metrics\n\n"
        md += "| Metric | Value |\n"
        md += "|--------|-------|\n"
        md += f"| **ML Readiness Score** | {score_badge} |\n"
        md += f"| **Readiness Gauge** | `{bar}` ({score:.1f}/10) |\n"
        md += f"| **Preprocessing Effort** | {effort_badge} |\n"
        md += f"| **Blockers** | {'⛔ ' + str(len(blockers)) if blockers else '✅ 0'} |\n"
        md += f"| **Quick Wins** | {'⚡ ' + str(len(quick_wins)) if quick_wins else '✅ 0'} |\n\n"

        if blockers:
            md += "### 🚨 Critical Issues\n\n"
            md += "| Issue | Column | Impact | Fix |\n"
            md += "|-------|--------|--------|-----|\n"
            for blocker in blockers:
                md += (
                    f"| {blocker} | {self._code_col('Dataset-wide')} | "
                    f"Prevents reliable model training or validation | Resolve blocker before full training |\n"
                )
            md += "\n"

        warnings: list[dict[str, str]] = []
        for algo in algorithms:
            warnings.append(
                {
                    "warning": f"Algorithm fit: {algo.get('algorithm', 'Model')}",
                    "column": "Dataset-wide",
                    "recommendation": str(algo.get("reason", "Evaluate during benchmarking")),
                }
            )
        for ptype in problem_types:
            warnings.append(
                {
                    "warning": f"Suggested problem type: {ptype}",
                    "column": "Target definition",
                    "recommendation": "Validate this framing against your business question and labels.",
                }
            )

        if warnings:
            md += "### ⚠️ Warnings\n\n"
            md += "| Warning | Column | Recommendation |\n"
            md += "|---------|--------|----------------|\n"
            for row in warnings[:10]:
                md += (
                    f"| {row['warning']} | {self._code_col(row['column'])} | "
                    f"{row['recommendation']} |\n"
                )
            md += "\n"

        md += "### ✅ What Looks Good\n"
        for finding in positive_findings:
            md += f"- {finding}\n"
        md += "\n"

        if algorithms:
            md += "### 🧪 Recommended Algorithms\n\n"
            md += "| Priority | Algorithm | Why It Fits |\n"
            md += "|----------|-----------|-------------|\n"
            priority_icon = {"high": "🥇 High", "medium": "🥈 Medium", "low": "🥉 Low"}
            for item in algorithms:
                icon = priority_icon.get(str(item.get("priority", "medium")).lower(), "🥈 Medium")
                md += (
                    f"| {icon} | **{item.get('algorithm', 'Model')}** | "
                    f"{item.get('reason', 'Good baseline candidate')} |\n"
                )
            md += "\n"

        if problem_types:
            labels = {
                "classification": "🏷️ Classification",
                "regression": "📈 Regression",
                "clustering": "🔵 Clustering",
                "time_series": "📅 Time Series",
                "nlp": "📝 NLP",
            }
            md += "### 🎯 Suitable Problem Types\n"
            md += " | ".join(labels.get(str(item), str(item)) for item in problem_types)
            md += "\n\n"

        md += "---\n\n### 💡 Recommendations\n"
        for idx, rec in enumerate(recommendations[:8], start=1):
            md += f"{idx}. {rec}\n"

        return md
