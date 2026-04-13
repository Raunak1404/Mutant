from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator
from typing import TypeVar

import anthropic

from llm.errors import LLMConfigurationError
from models.messages import LLMResponse, Message
from utils.logging import get_logger
from utils.retry import async_retry

logger = get_logger(__name__)

T = TypeVar("T")


class ClaudeProvider:
    """Anthropic Claude provider with prompt caching and structured output."""

    def __init__(
        self,
        api_key: str,
        model: str = "claude-3-5-sonnet-20241022",
        rate_limiter=None,
    ) -> None:
        self._api_key = api_key.strip()
        self._client = anthropic.AsyncAnthropic(
            api_key=self._api_key,
            max_retries=5,
            timeout=anthropic.Timeout(60.0, connect=10.0),
        )
        self._model = model
        self._rate_limiter = rate_limiter

    async def close(self) -> None:
        """Close the underlying httpx/aiohttp session."""
        await self._client.close()

    def model_name(self) -> str:
        return self._model

    def supports_prompt_caching(self) -> bool:
        return True

    def _ensure_configured(self) -> None:
        if self._api_key:
            return
        raise LLMConfigurationError(
            "Anthropic API access is not configured. Set ANTHROPIC_API_KEY in "
            "~/.weisiong/.env, a sidecar env file next to WeiSiong.app, or the project .env."
        )

    @async_retry(max_attempts=3, retryable_exceptions=(anthropic.APIError,))
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

        anthropic_messages = [
            {"role": m.role, "content": m.content}
            for m in messages
            if m.role != "system"
        ]

        t0 = time.monotonic()
        response = await self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=anthropic_messages,
        )
        latency_ms = (time.monotonic() - t0) * 1000

        content = response.content[0].text if response.content else ""
        usage = response.usage

        if self._rate_limiter and hasattr(response, "headers"):
            await self._rate_limiter.adapt_from_headers(dict(response.headers))

        return LLMResponse(
            content=content,
            input_tokens=usage.input_tokens,
            output_tokens=usage.output_tokens,
            latency_ms=latency_ms,
            cached=getattr(usage, "cache_read_input_tokens", 0) > 0,
        )

    @async_retry(max_attempts=3, retryable_exceptions=(anthropic.APIError,))
    async def complete_structured(
        self,
        system_prompt: str,
        messages: list[Message],
        response_model: type[T],
        temperature: float = 0.0,
    ) -> T:
        """Use tool-use to enforce structured JSON output."""
        self._ensure_configured()
        if self._rate_limiter:
            await self._rate_limiter.acquire()

        schema = response_model.model_json_schema()
        tool_name = response_model.__name__

        anthropic_messages = [
            {"role": m.role, "content": m.content}
            for m in messages
            if m.role != "system"
        ]

        response = await self._client.messages.create(
            model=self._model,
            max_tokens=4096,
            temperature=temperature,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[
                {
                    "name": tool_name,
                    "description": f"Return structured output as {tool_name}",
                    "input_schema": schema,
                }
            ],
            tool_choice={"type": "tool", "name": tool_name},
            messages=anthropic_messages,
        )

        for block in response.content:
            if block.type == "tool_use" and block.name == tool_name:
                return response_model.model_validate(block.input)

        raise ValueError(f"Claude did not return tool_use block for {tool_name}")

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

        anthropic_messages = [
            {"role": m.role, "content": m.content}
            for m in messages
            if m.role != "system"
        ]

        stream = await self._client.messages.create(
            model=self._model,
            max_tokens=max_tokens,
            temperature=temperature,
            system=[
                {
                    "type": "text",
                    "text": system_prompt,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            messages=anthropic_messages,
            stream=True,
        )

        async for event in stream:
            if event.type != "content_block_delta":
                continue
            delta = getattr(event, "delta", None)
            if getattr(delta, "type", None) != "text_delta":
                continue
            text = getattr(delta, "text", "")
            if text:
                yield text
