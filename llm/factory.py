from __future__ import annotations

from config.settings import Settings
from llm.provider import LLMProvider


def create_llm_provider(settings: Settings, rate_limiter=None) -> LLMProvider:
    if settings.LLM_PROVIDER == "azure_openai":
        from llm.azure_openai_provider import AzureOpenAIProvider
        return AzureOpenAIProvider(
            endpoint=settings.AZURE_OPENAI_ENDPOINT,
            api_key=settings.AZURE_OPENAI_API_KEY,
            deployment=settings.AZURE_OPENAI_DEPLOYMENT,
            api_version=settings.AZURE_OPENAI_API_VERSION,
            rate_limiter=rate_limiter,
        )

    from llm.claude_provider import ClaudeProvider
    return ClaudeProvider(
        api_key=settings.ANTHROPIC_API_KEY,
        model=settings.ANTHROPIC_MODEL,
        rate_limiter=rate_limiter,
    )
