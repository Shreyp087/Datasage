from dataclasses import dataclass
from datetime import datetime, timezone
import json
import logging
import re
from typing import Any, Optional

from app.core.llm_client import LLMClient

logger = logging.getLogger(__name__)

@dataclass
class AgentOutput:
    agent_name: str
    agent_role: str
    structured_json: Optional[dict]
    report_markdown: str
    tokens_used: int
    model_used: str
    provider: str

class BaseDataAgent:
    def __init__(self):
        self.llm = LLMClient()
        self.agent_name = "Base Agent"
        self.agent_role = "Base Role"
        self.agent_icon = "🤖"
        self.temperature = 0.3

    def run(self, *args, **kwargs) -> AgentOutput:
        try:
            system_prompt = self.get_system_prompt(*args, **kwargs)
            user_prompt = self.get_user_prompt(*args, **kwargs)
        except TypeError:
            # Fallback wrapper for parameter matching
            system_prompt = self.get_system_prompt(**kwargs) if not args else self.get_system_prompt(*args)
            user_prompt = self.get_user_prompt(**kwargs) if not args else self.get_user_prompt(*args, **kwargs)
            
        try:
            response = self.llm.complete(
                system_prompt=system_prompt,
                user_prompt=user_prompt,
                max_tokens=2000,
                temperature=self.temperature,
                response_format="json"
            )
            
            parsed = self.llm.parse_json_response(response)
            meta = self.build_report_meta(
                args=args,
                kwargs=kwargs,
                parsed=parsed,
                model_used=response.model_used,
                provider=response.provider,
            )
            
            return AgentOutput(
                agent_name=self.agent_name,
                agent_role=self.agent_role,
                structured_json=parsed,
                report_markdown=self.json_to_markdown(parsed, meta=meta),
                tokens_used=response.total_tokens,
                model_used=response.model_used,
                provider=response.provider
            )
        except Exception as e:
            logger.error(f"{self.agent_name} failed during LLM execution: {e}")
            return AgentOutput(
                agent_name=self.agent_name,
                agent_role=self.agent_role,
                structured_json={"error": str(e)},
                report_markdown=(
                    f"## {self.agent_icon} {self.agent_name} — Report Error\n\n"
                    f"### 📊 Summary\n"
                    f"The report generator could not complete this analysis. "
                    f"Please retry after fixing the pipeline/runtime issue.\n\n"
                    f"### 🚨 Critical Issues\n"
                    f"| Issue | Impact | Fix |\n"
                    f"|-------|--------|-----|\n"
                    f"| Agent execution failed | No recommendations were generated | Check worker/API logs and retry |\n\n"
                    f"### ⚠️ Details\n"
                    f"`{str(e)}`\n"
                ),
                tokens_used=0,
                model_used="unknown",
                provider=self.llm.provider
            )

    def get_system_prompt(self, *args, **kwargs) -> str:
        raise NotImplementedError
    
    def get_user_prompt(self, *args, **kwargs) -> str:
        raise NotImplementedError

    def build_report_meta(
        self,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        parsed: dict[str, Any],
        model_used: str,
        provider: str,
    ) -> dict[str, str]:
        payload: dict[str, Any] = {}
        if args and isinstance(args[0], dict):
            payload = args[0]
        elif isinstance(kwargs.get("eda_json"), dict):
            payload = kwargs["eda_json"]
        elif isinstance(kwargs.get("combined_context"), dict):
            payload = kwargs["combined_context"]

        domain = kwargs.get("domain")
        if not domain and len(args) > 1 and isinstance(args[1], str):
            domain = args[1]
        if not domain and isinstance(payload.get("domain"), str):
            domain = payload.get("domain")
        if not domain and isinstance(parsed.get("domain"), str):
            domain = parsed.get("domain")

        dataset_name = (
            kwargs.get("dataset_name")
            or payload.get("dataset_name")
            or payload.get("dataset")
            or parsed.get("dataset_name")
            or "Dataset"
        )

        return {
            "agent_name": self.agent_name,
            "agent_icon": self.agent_icon,
            "dataset_name": str(dataset_name),
            "domain": str(domain or "general"),
            "model_used": model_used,
            "provider": provider,
            "analyzed_at": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        }

    def _domain_badge(self, domain: str) -> str:
        d = str(domain or "general").lower()
        badges = {
            "healthcare": "🏥 Healthcare",
            "finance": "💹 Finance",
            "education": "🎓 Education",
            "ecommerce": "🛒 E-Commerce",
            "ai_incidents": "⚠️ AI Incidents",
            "general": "📦 General",
            "other": "🧭 Other",
        }
        return badges.get(d, f"📦 {domain}")

    def _to_float(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _as_percent(self, value: Any) -> float:
        num = self._to_float(value, default=0.0)
        if num <= 1:
            return num * 100.0
        return num

    def format_null_pct(self, value: Any) -> str:
        pct = self._as_percent(value)
        if pct > 30:
            return f"⛔ {pct:.1f}%"
        if pct >= 5:
            return f"⚠️ {pct:.1f}%"
        return f"✅ {pct:.1f}%"

    def format_outlier_pct(self, value: Any) -> str:
        pct = self._as_percent(value)
        if pct > 10:
            return f"🔴 {pct:.1f}%"
        if pct >= 1:
            return f"🟡 {pct:.1f}%"
        return f"🟢 {pct:.1f}%"

    def format_quality_score(self, value: Any) -> str:
        score = self._to_float(value, default=0.0)
        if score >= 91:
            return f"✨ {score:.0f}/100 — Excellent"
        if score >= 71:
            return f"🟢 {score:.0f}/100 — Good"
        if score >= 41:
            return f"🟡 {score:.0f}/100 — Fair"
        return f"🔴 {score:.0f}/100 — Poor"

    def format_correlation(self, value: Any) -> str:
        corr = abs(self._to_float(value, default=0.0))
        if corr > 0.95:
            return f"⛔ {corr:.3f} — Severe multicollinearity"
        if corr >= 0.85:
            return f"⚠️ {corr:.3f} — High correlation"
        return f"✅ {corr:.3f}"

    def format_class_imbalance(self, ratio: Any) -> str:
        ratio_str = str(ratio or "1:1")
        parsed = re.findall(r"\d+(?:\.\d+)?", ratio_str)
        if len(parsed) >= 2:
            major = self._to_float(parsed[0], default=1.0)
            minor = max(self._to_float(parsed[1], default=1.0), 1.0)
            ratio_val = major / minor
            if ratio_val > 10:
                return f"🔴 Severely Imbalanced ({ratio_str})"
            if ratio_val >= 3:
                return f"🟡 Moderately Imbalanced ({ratio_str})"
            return f"🟢 Balanced ({ratio_str})"
        return f"🟢 Balanced ({ratio_str})" if ratio_str in {"1:1", "1"} else f"⚠️ {ratio_str}"

    def _code_col(self, value: Any) -> str:
        return f"`{str(value or 'N/A')}`"

    def _header(self, meta: Optional[dict[str, str]] = None) -> str:
        meta = meta or {}
        dataset_name = meta.get("dataset_name", "Dataset")
        domain_badge = self._domain_badge(meta.get("domain", "general"))
        analyzed_at = meta.get("analyzed_at", datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"))
        model = meta.get("model_used", "unknown")
        return (
            f"## {self.agent_icon} {self.agent_name} — {dataset_name}\n"
            f"**Domain:** {domain_badge} | **Analyzed:** {analyzed_at} | **Model:** {model}\n\n"
            f"---\n"
        )

    def _summary(self, text: Any, fallback: str) -> str:
        summary = str(text or "").strip()
        if summary:
            return summary
        return fallback

    def json_to_markdown(self, data: dict, meta: Optional[dict[str, str]] = None) -> str:
        summary = self._summary(
            data.get("summary") if isinstance(data, dict) else None,
            "This report is available, but the model did not return a structured summary.",
        )
        key_points: list[str] = []
        if isinstance(data, dict):
            for key, value in data.items():
                if isinstance(value, (str, int, float, bool)) and key != "summary":
                    key_points.append(f"- **{key.replace('_', ' ').title()}**: {value}")

        markdown = self._header(meta)
        markdown += f"\n### 📊 Summary\n{summary}\n\n---\n"
        markdown += "### ✅ What Looks Good\n"
        if key_points:
            markdown += "\n".join(key_points[:5]) + "\n"
        else:
            markdown += "- Basic report generation completed successfully.\n"
        markdown += "\n---\n### 💡 Recommendations\n1. **Review** — Validate this report against domain expectations.\n"
        return markdown
