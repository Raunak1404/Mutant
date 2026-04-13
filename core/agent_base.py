from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from llm.provider import LLMProvider
from models.messages import Message
from models.results import RuleMetadata
from utils.logging import get_logger

logger = get_logger(__name__)


class AgentBase(ABC):
    """Base class for all agents. Holds shared LLM provider and logging."""

    def __init__(self, llm: LLMProvider, job_id: str) -> None:
        self.llm = llm
        self.job_id = job_id

    def _make_message(self, content: str, role: str = "user") -> Message:
        return Message(role=role, content=content)

    @abstractmethod
    async def run(self, *args: Any, **kwargs: Any) -> Any:
        """Execute the agent's primary task."""
