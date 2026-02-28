import os
import time
import json
import logging
import asyncio
from typing import Optional
from dataclasses import dataclass

from .config import settings
from .exceptions import LLMError, LLMParseError

logger = logging.getLogger(__name__)

@dataclass
class LLMResponse:
    text: str
    input_tokens: int
    output_tokens: int
    total_tokens: int
    model_used: str
    provider: str

@dataclass
class LLMMessage:
    role: str
    content: str


class LLMClient:
    def __init__(self, provider: Optional[str] = None):
        self.provider = provider or settings.llm_provider
        if self.provider not in ["openai", "anthropic"]:
            raise ValueError(f"Unsupported LLM provider: {self.provider}")

    def complete(
        self,
        system_prompt: str,
        user_prompt: str,
        max_tokens: int = 2000,
        temperature: float = 0.3,
        response_format: str = "text"
    ) -> LLMResponse:
        retries = 3
        backoff = [1, 2, 4]
        
        for attempt in range(retries):
            start_time = time.time()
            try:
                if self.provider == "openai":
                    response = self._complete_openai(system_prompt, user_prompt, max_tokens, temperature, response_format)
                else:
                    response = self._complete_anthropic(system_prompt, user_prompt, max_tokens, temperature, response_format)
                
                duration = time.time() - start_time
                logger.info(f"LLM Call Success | Provider: {response.provider} | Model: {response.model_used} | Tokens: {response.total_tokens} | Duration: {duration*1000:.0f}ms")
                return response
                
            except Exception as e:
                import requests
                from openai import RateLimitError as OpenAIRateLimitError, APITimeoutError as OpenAITimeoutError
                from anthropic import RateLimitError as AnthropicRateLimitError, APITimeoutError as AnthropicTimeoutError

                duration = time.time() - start_time
                error_msg = str(e)
                logger.warning(f"LLM Call Failed (Attempt {attempt + 1}/{retries}) | Provider: {self.provider} | Duration: {duration*1000:.0f}ms | Error: {error_msg}")
                
                if attempt == retries - 1:
                    raise LLMError(f"LLM completion failed after {retries} retries: {error_msg}", provider=self.provider, retries=retries) from e

                # Determine backoff
                sleep_time = backoff[attempt]
                if isinstance(e, (OpenAIRateLimitError, AnthropicRateLimitError)) or "429" in error_msg:
                    sleep_time = 10
                
                time.sleep(sleep_time)
                
        raise LLMError("Unexpected LLM failure", provider=self.provider, retries=retries)

    def _complete_openai(self, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float, response_format: str) -> LLMResponse:
        from openai import OpenAI
        client = OpenAI(api_key=settings.openai_api_key)
        
        kwargs = {
            "model": settings.openai_model,
            "max_tokens": max_tokens,
            "temperature": temperature,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        }
        
        if response_format == "json":
            kwargs["response_format"] = {"type": "json_object"}
            kwargs["messages"][0]["content"] += "\nReturn ONLY valid JSON. No markdown, no explanation."
            
        res = client.chat.completions.create(**kwargs)
        
        return LLMResponse(
            text=res.choices[0].message.content,
            input_tokens=res.usage.prompt_tokens,
            output_tokens=res.usage.completion_tokens,
            total_tokens=res.usage.total_tokens,
            model_used=res.model,
            provider="openai"
        )

    def _complete_anthropic(self, system_prompt: str, user_prompt: str, max_tokens: int, temperature: float, response_format: str) -> LLMResponse:
        import anthropic
        client = anthropic.Anthropic(api_key=settings.anthropic_api_key)
        
        system = system_prompt
        if response_format == "json":
            system += "\nReturn ONLY valid JSON. No markdown, no explanation outside the JSON."
            
        res = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=system,
            messages=[{"role": "user", "content": user_prompt}]
        )
        
        return LLMResponse(
            text=res.content[0].text,
            input_tokens=res.usage.input_tokens,
            output_tokens=res.usage.output_tokens,
            total_tokens=res.usage.input_tokens + res.usage.output_tokens,
            model_used=res.model,
            provider="anthropic"
        )

    def parse_json_response(self, response: LLMResponse) -> dict:
        text = response.text.strip()
        
        # 1. Direct parse
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            pass
            
        # 2. Strip Markdown Fences
        if text.startswith("```"):
            lines = text.split("\n")
            if len(lines) >= 2 and lines[0].startswith("```"):
                lines = lines[1:]
                if lines[-1].strip() == "```":
                    lines = lines[:-1]
                stripped_text = "\n".join(lines)
                try:
                    return json.loads(stripped_text)
                except json.JSONDecodeError:
                    pass
                    
        # 3. Regex block extract
        import re
        match = re.search(r'\{.*\}', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group(0))
            except json.JSONDecodeError:
                pass
                
        raise LLMParseError(f"Failed to parse LLM response into JSON. Model Output:\n{text}", raw_response=text)
