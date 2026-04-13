from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, TypeVar, runtime_checkable

from models.messages import LLMResponse, Message

T = TypeVar("T")


@runtime_checkable
class LLMProvider(Protocol):
    async def complete(
        self,
        system_prompt: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> LLMResponse: ...

    async def stream_complete(
        self,
        system_prompt: str,
        messages: list[Message],
        temperature: float = 0.0,
        max_tokens: int = 4096,
    ) -> AsyncIterator[str]: ...

    async def complete_structured(
        self,
        system_prompt: str,
        messages: list[Message],
        response_model: type[T],
        temperature: float = 0.0,
    ) -> T: ...

    def supports_prompt_caching(self) -> bool: ...

    def model_name(self) -> str: ...
