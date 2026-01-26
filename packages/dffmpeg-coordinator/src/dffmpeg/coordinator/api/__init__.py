from contextlib import asynccontextmanager
from logging import getLogger
from typing import Optional

from fastapi import FastAPI

from dffmpeg.coordinator.api.routes import health, job, test, worker
from dffmpeg.coordinator.config import CoordinatorConfig, load_config
from dffmpeg.coordinator.db import DB
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

    yield


def create_app(config: Optional[CoordinatorConfig] = None) -> FastAPI:
    """
    App factory function.
    """
    if config is None:
        config = load_config()

    app = FastAPI(title="dffmpeg Coordinator", lifespan=lifespan)
    app.state.config = config

    # Include routers
    app.include_router(health.router)
    app.include_router(worker.router)
    app.include_router(job.router)
    app.include_router(test.router)

    return app
