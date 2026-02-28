import pytest
import asyncio
from unittest.mock import Mock, patch

from app.core.llm_client import LLMClient, LLMResponse
from app.core.exceptions import LLMError, LLMParseError
from app.agents.base_agent import BaseDataAgent
from app.agents.quality_inspector import QualityInspectorAgent
from app.agents.feature_analyst import FeatureAnalystAgent
from app.agents.statistical_analyst import StatisticalAnalystAgent
from app.agents.ml_advisor import MLAdvisorAgent
import app.agents.orchestrator

# --- 1. Test both providers via parameterization and mocked completion ---

@pytest.mark.parametrize("provider", ["openai", "anthropic"])
def test_llm_client_initialization(provider):
    with patch("app.core.llm_client.settings") as mock_settings:
        mock_settings.llm_provider = provider
        client = LLMClient(provider=provider)
        assert client.provider == provider

@pytest.mark.parametrize("provider", ["openai", "anthropic"])
def test_quality_inspector_mocked(provider):
    agent = QualityInspectorAgent()
    agent.llm.provider = provider
    
    mock_response = LLMResponse(
        text='{"overall_quality_grade": "A", "quality_score": 95, "critical_issues": [], "warnings": [], "data_readiness": "ready", "summary": "Good"}',
        input_tokens=10, output_tokens=20, total_tokens=30, model_used="test-model", provider=provider
    )
    
    with patch.object(LLMClient, "complete", return_value=mock_response):
        out = agent.run({"dataset_quality_score": 90}, "generic", [])
        
        assert out.agent_name == "Quality Inspector"
        assert out.provider == provider
        assert out.structured_json["overall_quality_grade"] == "A"

# --- 2. Test JSON parsing fallback logic ---

def test_json_parse_fallback():
    client = LLMClient(provider="openai")
    
    # 2a. Valid JSON
    res1 = LLMResponse('{"a": 1}', 0, 0, 0, "m", "p")
    assert client.parse_json_response(res1) == {"a": 1}
    
    # 2b. Markdown fenced JSON
    res2 = LLMResponse('Here is the json:\n```json\n{"b": 2}\n```\nDone.', 0, 0, 0, "m", "p")
    assert client.parse_json_response(res2) == {"b": 2}
    
    # 2c. Regex fallback for inline JSON block
    res3 = LLMResponse('Wait, the answer is {"c": 3} and that is all.', 0, 0, 0, "m", "p")
    assert client.parse_json_response(res3) == {"c": 3}
    
    # 2d. Incomplete/Malformed JSON fails gracefully (Parse Error)
    res4 = LLMResponse('{"broken": ', 0, 0, 0, "m", "p")
    with pytest.raises(LLMParseError):
        client.parse_json_response(res4)

# --- 3. Test Retry Logic ---

@patch("app.core.llm_client.time.sleep") # intercept sleep to make tests fast
def test_llm_retry_success(mock_sleep):
    client = LLMClient(provider="openai")
    
    call_counts = {"count": 0}
    
    def fake_complete_openai(*args, **kwargs):
        call_counts["count"] += 1
        if call_counts["count"] < 3:
            raise Exception("Temporary API Error")
        return LLMResponse('{"success": true}', 5, 5, 10, "m", "p")
        
    with patch.object(client, "_complete_openai", side_effect=fake_complete_openai):
        res = client.complete("sys", "user", response_format="json")
        assert call_counts["count"] == 3
        assert res.total_tokens == 10

@patch("app.core.llm_client.time.sleep")
def test_llm_error_raised(mock_sleep):
    client = LLMClient(provider="openai")
    
    def fake_complete_openai(*args, **kwargs):
        raise Exception("Fatal API Error")
        
    with patch.object(client, "_complete_openai", side_effect=fake_complete_openai):
        with pytest.raises(LLMError) as exc_info:
            client.complete("sys", "user")
        
        assert exc_info.value.retries == 3
        assert exc_info.value.provider == "openai"

# --- 4. Test Token Tracking Sums ---
        
@pytest.mark.asyncio
async def test_orchestrator_token_tracking():
    # Since orchestrator logic sums tokens from AgentOutputs, we mock out all 4 agents
    
    mock_q = Mock(return_value=Mock(structured_json={"q":1}, report_markdown="", tokens_used=10, model_used="", provider="", agent_name="", agent_role=""))
    mock_f = Mock(return_value=Mock(structured_json={"f":2}, report_markdown="", tokens_used=20, model_used="", provider="", agent_name="", agent_role=""))
    mock_s = Mock(return_value=Mock(structured_json={"s":3}, report_markdown="", tokens_used=30, model_used="", provider="", agent_name="", agent_role=""))
    mock_m = Mock(return_value=Mock(structured_json={"m":4}, report_markdown="", tokens_used=40, model_used="", provider="", agent_name="", agent_role=""))
    
    with patch("app.agents.orchestrator.QualityInspectorAgent.run", mock_q):
        with patch("app.agents.orchestrator.FeatureAnalystAgent.run", mock_f):
            with patch("app.agents.orchestrator.StatisticalAnalystAgent.run", mock_s):
                with patch("app.agents.orchestrator.MLAdvisorAgent.run", mock_m):
                    
                    # We also mock synthesize_final_report purely for tokens
                    with patch("app.agents.orchestrator.synthesize_final_report", return_value={"markdown": "", "tokens": 100, "model": "", "provider": ""}):
                        
                        from app.agents.orchestrator import run_all_agents
                        from unittest.mock import AsyncMock
                        
                        mock_db = AsyncMock()
                        
                        import uuid
                        await run_all_agents(uuid.uuid4(), {}, "domain", [], mock_db)
                        
                        # db.add_all should be called with 5 instances of AgentReport
                        # The synthetic sum of tokens inside DB should be exactly 10+20+30+40+100 = 200
                        
                        mock_db.add_all.assert_called_once()
                        reports = mock_db.add_all.call_args[0][0]
                        assert len(reports) == 5
                        
                        assert reports[0].tokens_used == 10
                        assert reports[1].tokens_used == 20
                        assert reports[2].tokens_used == 30
                        assert reports[3].tokens_used == 40
                        assert reports[4].tokens_used == 100
