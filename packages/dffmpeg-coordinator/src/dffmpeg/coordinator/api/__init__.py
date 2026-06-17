import asyncio
import gc
import signal
import threading
from contextlib import asynccontextmanager
from logging import getLogger
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware
from uvicorn.server import Server

from dffmpeg.coordinator.api.routes import admin, dashboard, health, job, metrics, worker
from dffmpeg.coordinator.config import CoordinatorConfig, load_config
from dffmpeg.coordinator.db import DB
from dffmpeg.coordinator.janitor import Janitor
from dffmpeg.coordinator.transports import TransportManager

logger = getLogger(__name__)


async def _execute_shutdown_sequence(app: FastAPI):
    app.state.shutting_down = True

    # Stop Janitor immediately
    try:
        await app.state.janitor.stop()
    except asyncio.CancelledError:
        pass

    delay = app.state.config.shutdown_delay_seconds
    if delay > 0:
        logger.info(f"Draining coordinator. Sleeping for {delay} seconds...")
        await asyncio.sleep(delay)

    await app.state.transports.drain_all()


def _setup_signal_interceptors(app: FastAPI):
    def trigger_shutdown(sig):
        if getattr(app.state, "shutdown_triggered", False):
            return
        app.state.shutdown_triggered = True
        logger.info("Caught shutdown signal. Starting graceful drain before Uvicorn stops...")
        asyncio.create_task(graceful_shutdown(sig))

    async def graceful_shutdown(sig):
        await _execute_shutdown_sequence(app)

        # Now trigger Uvicorn's shutdown by finding the server instance
        for obj in gc.get_objects():
            if isinstance(obj, Server):
                obj.should_exit = True
                break

    # Register our signal handlers to intercept Uvicorn's shutdown
    if threading.current_thread() is threading.main_thread():
        loop = asyncio.get_running_loop()
        try:
            loop.add_signal_handler(signal.SIGINT, lambda: trigger_shutdown(signal.SIGINT))
            loop.add_signal_handler(signal.SIGTERM, lambda: trigger_shutdown(signal.SIGTERM))
        except NotImplementedError:
            # add_signal_handler is not implemented on Windows
            pass


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Lifespan context manager for the FastAPI application.
    Handles startup and shutdown events, including database and transport setup.
    """
    config: CoordinatorConfig = app.state.config

    app.state.shutting_down = False
    app.state.shutdown_triggered = False

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
    app.state.janitor = janitor
    await janitor.start()

    _setup_signal_interceptors(app)

    yield

    if not app.state.shutdown_triggered:
        app.state.shutdown_triggered = True
        await _execute_shutdown_sequence(app)


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

    @app.middleware("http")
    async def add_custom_headers(request: Request, call_next):
        response = await call_next(request)

        # Prevent caching globally for ALL endpoints
        response.headers["Cache-Control"] = "no-cache, no-store, must-revalidate"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"

        # Add real-time streaming optimizations based on Content-Type
        content_type = response.headers.get("content-type", "")
        if "application/x-ndjson" in content_type:
            response.headers["Connection"] = "keep-alive"
            response.headers["X-Accel-Buffering"] = "no"

        return response

    if config.trusted_proxies:
        app.add_middleware(ProxyHeadersMiddleware, trusted_hosts=config.trusted_proxies)

    # Include routers
    app.include_router(health.router)
    app.include_router(worker.router)
    app.include_router(job.router)
    app.include_router(dashboard.router)
    app.include_router(metrics.router)
    app.include_router(admin.router)

    @app.get("/", include_in_schema=False)
    async def root_redirect():
        if config.web_dashboard_enabled:
            return RedirectResponse(url="/status")
        return RedirectResponse(url="/health")

    return app
