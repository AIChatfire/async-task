"""gateway-api 应用（§11 协议面 / §19 部署 / §17 探针）。

无状态、多副本；真相全在 Postgres。启动时做三件小事：装配日志、确保队列存在、
预热模板注册表（让"未知 alias"在第一个请求之前就变成确定行为）。
"""

from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from ..config import get_settings
from ..db.base import healthy as db_healthy
from ..db.base import dispose_engine, init_schema
from ..infra.redis import close_redis, ping as redis_ping
from ..observability.logging import configure_logging
from ..observability.metrics import render_metrics
from .container import configure_container, get_container
from .routes import callbacks, router

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    configure_logging()
    container = get_container()
    if settings.is_sqlite:
        # 开发/测试：直接建表；生产必须走 alembic upgrade head
        await init_schema()
    try:
        for queue in ("submit:default", "cancel:default", "compensate:default", "finalize:default"):
            await container.bus.ensure_queue(queue)
    except Exception as exc:  # noqa: BLE001 - 队列不可用不应阻止进程起来（就绪探针会报）
        logger.warning("queue warmup failed: %s", exc)
    logger.info("gateway-api started env=%s submit_mode=%s", settings.app_env, settings.submit_mode)
    try:
        yield
    finally:
        await container.limiter.aclose() if hasattr(container.limiter, "aclose") else None
        await close_redis()
        await dispose_engine()


def create_app() -> FastAPI:
    settings = get_settings()
    app = FastAPI(
        title="Async Gateway",
        version="0.1.0",
        description=(
            "通用排队异步网关：给既有上游套一层，上游零改动。"
            "对外承诺三语义（提交/查询/取消），认证为上游凭证透传。"
        ),
        lifespan=lifespan,
        docs_url="/docs" if settings.app_env != "prod" else None,
        redoc_url=None,
    )
    app.include_router(router)
    app.include_router(callbacks)

    @app.exception_handler(Exception)
    async def _unhandled(request: Request, exc: Exception) -> JSONResponse:  # pragma: no cover
        logger.exception("unhandled error on %s %s", request.method, request.url.path)
        return JSONResponse(status_code=500, content={"detail": "internal error"})

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz() -> JSONResponse:
        db_ok = await db_healthy()
        cache_ok = await redis_ping()
        payload = {"db": db_ok, "cache": cache_ok}
        status = 200 if db_ok else 503
        return JSONResponse(status_code=status, content=payload)

    @app.get("/metrics", include_in_schema=False)
    async def metrics() -> PlainTextResponse:
        return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4")

    return app


app = create_app()


__all__ = ["app", "configure_container", "create_app"]
