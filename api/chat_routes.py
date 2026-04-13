from __future__ import annotations

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import get_llm, get_session
from chat.chat_handler import (
    apply_confirmed_changes,
    handle_chat_message,
    load_step_context,
    stream_chat_message,
)
from chat.chat_models import (
    ChatConfirmRequest,
    ChatHistoryItem,
    ChatRequest,
    ChatResponse,
    StepRuleSummary,
)
from db.models import ChatMessage
from llm.errors import LLMConfigurationError
from utils.logging import get_logger

chat_router = APIRouter(prefix="/chat", tags=["chat"])
logger = get_logger(__name__)


@chat_router.post("/message", response_model=ChatResponse)
async def send_message(
    request: ChatRequest,
    session: AsyncSession = Depends(get_session),
    llm=Depends(get_llm),
):
    """Send a user message and get an AI response with optional proposed changes."""
    try:
        return await handle_chat_message(session, llm, request.session_id, request.message)
    except LLMConfigurationError as exc:
        return ChatResponse(
            session_id=request.session_id,
            message=str(exc),
            proposed_changes=[],
            needs_confirmation=False,
            questions=[],
        )
    except Exception as exc:
        logger.error("chat_message_error", session_id=request.session_id, error=str(exc))
        raise


@chat_router.post("/message/stream")
async def stream_message(
    request: ChatRequest,
    session: AsyncSession = Depends(get_session),
    llm=Depends(get_llm),
):
    """Stream the assistant response for a user message over SSE."""

    async def event_stream():
        try:
            async for chunk in stream_chat_message(session, llm, request.session_id, request.message):
                yield chunk
        except LLMConfigurationError as exc:
            payload = ChatResponse(
                session_id=request.session_id,
                message=str(exc),
                proposed_changes=[],
                needs_confirmation=False,
                questions=[],
            )
            yield f"event: result\ndata: {payload.model_dump_json()}\n\n"
        except Exception as exc:
            logger.error("chat_stream_error", session_id=request.session_id, error=str(exc))
            payload = ChatResponse(
                session_id=request.session_id,
                message=f"Error: {exc}",
                proposed_changes=[],
                needs_confirmation=False,
                questions=[],
            )
            yield f"event: result\ndata: {payload.model_dump_json()}\n\n"

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@chat_router.post("/confirm", response_model=ChatResponse)
async def confirm_changes(
    request: ChatConfirmRequest,
    session: AsyncSession = Depends(get_session),
    llm=Depends(get_llm),
):
    """Confirm and apply the most recently proposed changes in a session."""
    try:
        return await apply_confirmed_changes(session, llm, request.session_id, request.job_id)
    except LLMConfigurationError as exc:
        return ChatResponse(
            session_id=request.session_id,
            message=str(exc),
            proposed_changes=[],
            needs_confirmation=False,
            questions=[],
        )
    except Exception as exc:
        logger.error("chat_confirm_error", session_id=request.session_id, error=str(exc))
        raise


@chat_router.get("/history", response_model=list[ChatHistoryItem])
async def get_history(
    session_id: str = Query(...),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
):
    """Retrieve chat history for a session."""
    result = await session.execute(
        select(ChatMessage)
        .where(ChatMessage.session_id == session_id)
        .order_by(ChatMessage.created_at.asc())
        .limit(limit)
    )
    rows = result.scalars().all()
    return [
        ChatHistoryItem(
            role=row.role,
            content=row.content,
            metadata_json=row.metadata_json,
            created_at=row.created_at.isoformat(),
        )
        for row in rows
    ]


@chat_router.get("/rules", response_model=list[StepRuleSummary])
async def get_rules(
    session: AsyncSession = Depends(get_session),
):
    """Get current rules and code function names for all steps."""
    return await load_step_context(session)
