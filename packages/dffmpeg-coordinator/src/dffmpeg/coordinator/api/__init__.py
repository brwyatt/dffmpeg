import asyncio
from contextlib import asynccontextmanager
from logging import getLogger
from typing import Optional

from fastapi import FastAPI
from fastapi.responses import RedirectResponse
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from dffmpeg.coordinator.api.routes import dashboard, health, job, worker
from dffmpeg.coordinator.config import CoordinatorConfig, load_config
from dffmpeg.coordinator.db import DB
from dffmpeg.coordinator.janitor import Janitor
from dffmpeg.coordinator.transports import TransportManager

logger = getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.
    Handles startup and shutdown events, including database and transport setup.
    """
    config: CoordinatorConfig = app.state.config
    app.state.db = DB(config=config.database)
    await app.state.db.setup_all()

    app.state.transports = TransportManager(config=config.transports, app=app)
    await app.state.transports.setup_all()

    janitor = Janitor(
        worker_repo=app.state.db.workers,
        job_repo=app.state.db.jobs,
        transports=app.state.transports,
        config=config.janitor,
    )
    janitor_task = asyncio.create_task(janitor.start())

    yield

    janitor_task.cancel()
    try:
        await janitor_task
    except asyncio.CancelledError:
        pass


def create_app(config: Optional[CoordinatorConfig] = None) -> FastAPI:
    """
    App factory function.
    """
    if config is None:
        config = load_config()

    app = FastAPI(
        title="dffmpeg Coordinator",
        lifespan=lifespan,
        docs_url="/docs" if config.dev_mode else None,
        redoc_url="/redoc" if config.dev_mode else None,
        openapi_url="/openapi.json" if config.dev_mode else None,
    )
    app.state.config = config

    if config.trusted_proxies:
        app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=config.trusted_proxies)

    # Include routers
    app.include_router(health.router)
    app.include_router(worker.router)
    app.include_router(job.router)
    app.include_router(dashboard.router)

    @app.get("/", include_in_schema=False)
    async def root_redirect():
        if config.web_dashboard_enabled:
            return RedirectResponse(url="/status")
        return RedirectResponse(url="/health")

    return app
