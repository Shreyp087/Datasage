import asyncio
import json
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.llm_client import LLMClient
from app.models.models import AgentReport, Dataset, DatasetStatusEnum

from .base_agent import AgentOutput
from .feature_analyst import FeatureAnalystAgent
from .ml_advisor import MLAdvisorAgent
from .quality_inspector import QualityInspectorAgent
from .statistical_analyst import StatisticalAnalystAgent

logger = logging.getLogger(__name__)


class AgentOrchestrator:
    def run_all_agents(self, eda_json: dict, domain: str, processing_logs: list[str]) -> list[AgentOutput]:
        agents_123 = [
            QualityInspectorAgent(),
            FeatureAnalystAgent(),
            StatisticalAnalystAgent(),
        ]

        results: list[AgentOutput] = []
        errors: list[dict[str, str]] = []

        with ThreadPoolExecutor(max_workers=3) as executor:
            futures = {
                executor.submit(agent.run, eda_json, domain, processing_logs): agent.agent_name
                for agent in agents_123
            }

            for future in as_completed(futures):
                agent_name = futures[future]
                try:
                    result = future.result(timeout=120)
                    results.append(result)
                    logger.info("%s completed (%s tokens)", agent_name, result.tokens_used)
                except Exception as exc:
                    logger.exception("%s failed", agent_name)
                    errors.append({"agent": agent_name, "error": str(exc)})

        if results:
            try:
                combined_context = {
                    "domain": domain,
                    "dataset_name": eda_json.get("dataset_name", "Dataset"),
                    "quality_report": next(
                        (
                            r.structured_json
                            for r in results
                            if r.agent_name == "Quality Inspector"
                        ),
                        {},
                    ),
                    "feature_report": next(
                        (
                            r.structured_json
                            for r in results
                            if r.agent_name == "Feature Analyst"
                        ),
                        {},
                    ),
                    "statistical_report": next(
                        (
                            r.structured_json
                            for r in results
                            if r.agent_name == "Statistical Analyst"
                        ),
                        {},
                    ),
                }
                ml_advisor = MLAdvisorAgent()
                ml_result = ml_advisor.run(combined_context)
                results.append(ml_result)
                logger.info("ML Advisor completed (%s tokens)", ml_result.tokens_used)
            except Exception as exc:
                logger.exception("ML Advisor failed")
                errors.append({"agent": "ML Advisor", "error": str(exc)})

        if results:
            try:
                synthesis = self._synthesize_report(
                    eda_json=eda_json,
                    domain=domain,
                    processing_logs=processing_logs,
                    agent_results=results,
                )
                results.append(synthesis)
            except Exception:
                logger.exception("Synthesizer failed")

        if errors:
            logger.warning("Agent orchestration completed with %d errors", len(errors))
        return results

    def _synthesize_report(
        self,
        eda_json: dict,
        domain: str,
        processing_logs: list[str],
        agent_results: list[AgentOutput],
    ) -> AgentOutput:
        client = LLMClient()
        prompt_payload = {
            "domain": domain,
            "dataset_name": eda_json.get("dataset_name", "Dataset"),
            "eda_json": eda_json,
            "processing_logs": processing_logs[:30],
            "agent_results": [result.structured_json for result in agent_results if result.structured_json],
        }
        response = client.complete(
            system_prompt=(
                "You are a senior data science editor writing for non-technical researchers and students. "
                "Return markdown only (no JSON) using sections: "
                "Summary, Critical Issues, Warnings, What Looks Good, Key Metrics, Recommendations. "
                "Use plain English and include visual badges/icons for severity."
            ),
            user_prompt=json.dumps(prompt_payload, default=str),
            max_tokens=1800,
            temperature=0.3,
            response_format="text",
        )

        synthesis_markdown = response.text.strip() or (
            "## 📄 Report Synthesizer — Dataset\n\n"
            "### 📊 Summary\nUnable to generate synthesis text from agent outputs.\n"
        )

        return AgentOutput(
            agent_name="Report Synthesizer",
            agent_role="Synthesis",
            structured_json={
                "summary_generated": True,
                "source_agents": [result.agent_name for result in agent_results],
            },
            report_markdown=synthesis_markdown,
            tokens_used=response.total_tokens,
            model_used=response.model_used,
            provider=response.provider,
        )


async def run_all_agents(
    dataset_id: uuid.UUID,
    eda_json: dict,
    domain: str,
    logs: list[str],
    db: AsyncSession,
) -> None:
    """
    Backward-compatible async wrapper used by legacy callers/tests.
    """
    orchestrator = AgentOrchestrator()
    results = await asyncio.to_thread(orchestrator.run_all_agents, eda_json, domain, logs)

    reports = [
        AgentReport(
            dataset_id=dataset_id,
            agent_name=result.agent_name,
            agent_role=result.agent_role,
            structured_json=result.structured_json,
            report_markdown=result.report_markdown,
            tokens_used=result.tokens_used,
            model_used=result.model_used,
            provider=result.provider,
        )
        for result in results
    ]
    if reports:
        db.add_all(reports)

    dataset = await db.get(Dataset, dataset_id)
    if dataset:
        dataset.status = DatasetStatusEnum.complete

    await db.commit()
