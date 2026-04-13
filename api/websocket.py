from __future__ import annotations

import asyncio
import json

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from redis.asyncio import Redis

from api.dependencies import get_redis
from core.progress import ProgressPublisher
from utils.logging import get_logger

logger = get_logger(__name__)

ws_router = APIRouter(tags=["websocket"])


@ws_router.websocket("/jobs/{job_id}/stream")
async def stream_progress(websocket: WebSocket, job_id: str):
    """Stream real-time job progress events via Redis pub/sub with history replay."""
    await websocket.accept()
    redis = get_redis()
    channel = f"job:progress:{job_id}"
    progress = ProgressPublisher(redis)
    close_normally = False

    async with redis.pubsub() as pubsub:
        await pubsub.subscribe(channel)
        logger.info("ws_subscribed", job_id=job_id, channel=channel)

        try:
            # Replay any events that were published before we subscribed
            terminal = False
            for event_json in await progress.get_history(job_id):
                await websocket.send_text(event_json)
                try:
                    parsed = json.loads(event_json)
                    if parsed.get("event_type") in ("completed", "failed"):
                        terminal = True
                        close_normally = True
                        break
                except json.JSONDecodeError:
                    pass

            if terminal:
                return

            # Now listen for live events
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue

                data = message["data"]
                if isinstance(data, bytes):
                    data = data.decode()

                await websocket.send_text(data)

                # Close stream on terminal events
                try:
                    parsed = json.loads(data)
                    if parsed.get("event_type") in ("completed", "failed"):
                        close_normally = True
                        break
                except json.JSONDecodeError:
                    pass

        except WebSocketDisconnect:
            logger.info("ws_disconnected", job_id=job_id)
        except asyncio.CancelledError:
            pass
        finally:
            if close_normally:
                try:
                    await websocket.close(code=1000)
                except Exception:
                    logger.debug("ws_close_skipped", job_id=job_id)
            await pubsub.unsubscribe(channel)
            logger.info("ws_unsubscribed", job_id=job_id)
