from __future__ import annotations


class LLMConfigurationError(RuntimeError):
    """Raised when the configured LLM provider is missing required settings."""

