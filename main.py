from __future__ import annotations

from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI

from api.chat_routes import chat_router
from api.dependencies import set_singletons
from api.routes import router
from api.websocket import ws_router
from config.settings import Settings
from db.code_versions import seed_code_from_files
from db.rules import seed_rules_from_files
from runtime.bootstrap import close_runtime_services, create_runtime_services
from runtime.paths import app_root
from tasks.broker import broker
from utils.logging import configure_logging, get_logger

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = Settings()
    configure_logging(debug=settings.DEBUG)
    logger.info(
        "startup",
        provider=settings.LLM_PROVIDER,
        storage=settings.STORAGE_BACKEND,
        job_runner=settings.JOB_RUNNER,
        use_redis=settings.USE_REDIS,
        execution_service=settings.EXECUTION_SERVICE_URL,
    )

    services = await create_runtime_services(settings)

    # Seed rules and native step code from the resolved packaged steps directory.
    async with services.session_factory() as session:
        await seed_rules_from_files(session, settings.STEPS_CODE_DIR)
        await seed_code_from_files(session, settings.STEPS_CODE_DIR)

    # Wire dependencies
    set_singletons(
        services.redis,
        services.session_factory,
        services.storage,
        services.llm,
        services.cache,
    )

    if settings.JOB_RUNNER == "taskiq":
        await broker.startup()

    logger.info("startup_complete")
    yield

    if settings.JOB_RUNNER == "taskiq":
        await broker.shutdown()
    await close_runtime_services(services)
    logger.info("shutdown_complete")


def create_app() -> FastAPI:
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import FileResponse
    from fastapi.staticfiles import StaticFiles

    resource_root = app_root()
    static_dir = resource_root / "static"
    index_file = static_dir / "index.html"

    app = FastAPI(
        title="Mutant Agentic Excel Processor",
        version="1.0.0",
        description="Agentic AI system for Excel data transformation with dual LLM support",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)
    app.include_router(chat_router)
    app.include_router(ws_router)

    # Serve frontend
    @app.get("/")
    async def serve_frontend():
        return FileResponse(index_file)

    app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

    return app


app = create_app()


if __name__ == "__main__":
    settings = Settings()
    uvicorn.run(
        "main:app",
        host=settings.API_HOST,
        port=settings.API_PORT,
        reload=settings.DEBUG,
    )
