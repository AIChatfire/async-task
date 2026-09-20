"""Redis 客户端（§10 broker 与缓存）。

Redis **不是**真相源：丢 Redis 不丢任务（真相在 Postgres，靠 ``next_poll_at``
重建投递）。因此这里的连接失败都不应让受理失败——除了刻意要背压的场景。
"""

from __future__ import annotations

from typing import Any

import redis.asyncio as aioredis

from ..config import get_settings

_client: aioredis.Redis | None = None


def get_redis() -> aioredis.Redis:
    global _client
    if _client is None:
        settings = get_settings()
        _client = aioredis.Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            health_check_interval=30,
            socket_keepalive=True,
        )
    return _client


async def close_redis() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
    _client = None


async def ping() -> bool:
    try:
        return bool(await get_redis().ping())
    except Exception:  # pragma: no cover - 探针
        return False


def set_client(client: Any) -> None:
    """测试注入用。"""
    global _client
    _client = client
