from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from typing import TypeVar

import openai

from llm.errors import LLMConfigurationError
from models.messages import LLMResponse, Message
from utils.logging import get_logger
from utils.retry import async_retry

logger = get_logger(__name__)

T = TypeVar("T")


class AzureOpenAIProvider:
    """Azure OpenAI provider with JSON schema structured output."""

    def __init__(
        self,
        endpoint: str,
        api_key: str,
        deployment: str = "gpt-4o",
        api_version: str = "2024-08-01-preview",
        rate_limiter=None,
    ) -> None:
        self._endpoint = endpoint.strip()
        self._api_key = api_key.strip()
        self._client = openai.AsyncAzureOpenAI(
            azure_endpoint=self._endpoint,
            api_key=self._api_key,
            api_version=api_version,
        )
        self._deployment = deployment
        self._rate_limiter = rate_limiter

    def model_name(self) -> str:
        return self._deployment

    def supports_prompt_caching(self) -> bool:
        return False

    def _ensure_configured(self) -> None:
        missing: list[str] = []
        if not self._endpoint:
            missing.append("AZURE_OPENAI_ENDPOINT")
        if not self._api_key:
            missing.append("AZURE_OPENAI_API_KEY")
        if not self._deployment:
            missing.append("AZURE_OPENAI_DEPLOYMENT")
        if not missing:
            return
        raise LLMConfigurationError(
            "Azure OpenAI access is not configured. Missing: "
            + ", ".join(missing)
            + ". Set them in ~/.weisiong/.env or a sidecar env file next to WeiSiong.app."
        )

    @async_retry(max_attempts=3, retryable_exceptions=(openai.APIError,))
    async def complete(
        self,
        system_prompt: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse:
        self._ensure_configured()
        if self._rate_limiter:
            await self._rate_limiter.acquire()

        oai_messages = [{"role": "system", "content": system_prompt}]
        oai_messages += [{"role": m.role, "content": m.content} for m in messages]

        t0 = time.monotonic()
        response = await self._client.chat.completions.create(
            model=self._deployment,
            messages=oai_messages,
            temperature=temperature,
            max_tokens=max_tokens,
        )
        latency_ms = (time.monotonic() - t0) * 1000

        content = response.choices[0].message.content or ""
        usage = response.usage

        return LLMResponse(
            content=content,
            input_tokens=usage.prompt_tokens if usage else 0,
            output_tokens=usage.completion_tokens if usage else 0,
            latency_ms=latency_ms,
        )

    async def stream_complete(
        self,
        system_prompt: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]:
        self._ensure_configured()
        if self._rate_limiter:
            await self._rate_limiter.acquire()

        oai_messages = [{"role": "system", "content": system_prompt}]
        oai_messages += [{"role": m.role, "content": m.content} for m in messages]

        stream = await self._client.chat.completions.create(
            model=self._deployment,
            messages=oai_messages,
            temperature=temperature,
            max_tokens=max_tokens,
            stream=True,
        )

        async for chunk in stream:
            for choice in chunk.choices:
                delta = getattr(choice, "delta", None)
                if delta is None:
                    continue
                text = getattr(delta, "content", None)
                if text:
                    yield text

    @async_retry(max_attempts=3, retryable_exceptions=(openai.APIError,))
    async def complete_structured(
        self,
        system_prompt: str,
        messages: list[Message],
        response_model: type[T],
        temperature: float = 0.0,
    ) -> T:
        self._ensure_configured()
        if self._rate_limiter:
            await self._rate_limiter.acquire()

        oai_messages = [{"role": "system", "content": system_prompt}]
        oai_messages += [{"role": m.role, "content": m.content} for m in messages]

        response = await self._client.beta.chat.completions.parse(
            model=self._deployment,
            messages=oai_messages,
            temperature=temperature,
            response_format=response_model,
        )
        return response.choices[0].message.parsed
