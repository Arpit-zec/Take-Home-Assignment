import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.api import router
from app.config import Settings
from app.logging_config import configure_logging
from app.storage import Storage

logger = logging.getLogger(__name__)


def create_app(storage: Storage | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        configure_logging()
        app.state.storage = storage or Storage(Settings())
        app.state.storage.indexes()
        try:
            yield
        finally:
            if storage is None:
                app.state.storage.close()

    app = FastAPI(title="Ecommerce Cohort Insights", version="0.1.0", lifespan=lifespan)
    if storage is not None:
        app.state.storage = storage
    app.include_router(router)

    async def dependency_error(request: Request, exc: Exception) -> JSONResponse:
        logger.error(
            "dependency_unavailable",
            exc_info=exc,
            extra={"method": request.method, "path": request.url.path},
        )
        return JSONResponse(
            status_code=503,
            content={"detail": "Service temporarily unavailable; retry shortly"},
            headers={"Retry-After": "5"},
        )

    app.add_exception_handler(PyMongoError, dependency_error)
    app.add_exception_handler(RedisError, dependency_error)

    @app.get("/health")
    def health() -> JSONResponse:
        checks: dict[str, str] = {}
        try:
            app.state.storage.mongo.admin.command("ping")
            checks["mongodb"] = "ok"
        except PyMongoError:
            checks["mongodb"] = "unavailable"
        try:
            app.state.storage.redis.ping()
            checks["redis"] = "ok"
        except RedisError:
            checks["redis"] = "unavailable"
        healthy = all(value == "ok" for value in checks.values())
        return JSONResponse(
            status_code=200 if healthy else 503,
            content={"status": "ok" if healthy else "degraded", **checks},
        )

    return app


app = create_app()
