"""任务请求数据的**短生命周期**存放（create 请求体 / 受理响应存档）。

2026-09-21 裁定（用户指令「转存只走外部 minio、不配置自动不转存、精简架构」）：

* **对象存储只服务结果转存**（见 ``infra/object_store.py``）——外部 MinIO、可选件；
* 受理路径需要跨进程共享的两份**小对象**放在这里：Redis（与队列/凭证同一套基础设施，
  本来就在关键路径上），**不再占用对象存储**：

  - ``req:{tenant}:{task_id}``     —— 受理时的 create 请求体（worker 提交时重建请求）；
  - ``reqresp:{tenant}:{task_id}`` —— 受理/上游原生响应存档（幂等重放要返回同一份）。

TTL 策略（不引入新配置项）：

* 请求体：按任务 deadline（同凭证口径）——worker 在 deadline 内会提交，之后不再需要；
* 响应存档：取幂等窗口（``idempotency_window_seconds``）——重放要在窗口内拿回同一份响应。

读不到时的口径：**不猜内容**。worker 提交前若请求体不存在（如 Redis 数据丢失），
任务显式失败（``ErrorCode.REQUEST_LOST``，不可重试），绝不拿空体去调上游 ——
空体只会被上游判成客户端的错（4xx），把基础设施事故伪装成用户错误。
"""

from __future__ import annotations

import json
from typing import Any, Protocol

from ..config import get_settings
from .redis import get_redis


class RequestDataNotFound(Exception):
    """请求数据不存在（未写入、已过期或已清理）。"""


class RequestStore(Protocol):
    async def put_json(self, key: str, payload: Any, ttl_seconds: int) -> None: ...
    async def get_bytes(self, key: str) -> bytes: ...
    async def drop(self, key: str) -> None: ...


def request_key(tenant: str, task_id: str) -> str:
    """create 请求体的键。"""
    return f"req:{tenant}:{task_id}"


def response_key(tenant: str, task_id: str) -> str:
    """受理/上游原生响应存档的键（幂等重放必须返回**同一份**响应）。"""
    return f"reqresp:{tenant}:{task_id}"


def body_ttl_seconds() -> int:
    """请求体 TTL：覆盖整个任务 deadline（同凭证口径）。"""
    return min(3600, max(300, get_settings().task_deadline_seconds + 300))


def response_ttl_seconds() -> int:
    """响应存档 TTL：幂等窗口内重放必须能取回同一份响应。"""
    return max(60, int(get_settings().idempotency_window_seconds))


class RedisRequestStore:
    """Redis 实现（decode_responses=True ⇒ 存取按字符串）。"""

    async def put_json(self, key: str, payload: Any, ttl_seconds: int) -> None:
        blob = json.dumps(payload, ensure_ascii=False, default=str)
        await get_redis().set(key, blob, ex=max(60, ttl_seconds))

    async def get_bytes(self, key: str) -> bytes:
        raw = await get_redis().get(key)
        if raw is None:
            raise RequestDataNotFound(key)
        return raw.encode() if isinstance(raw, str) else raw

    async def drop(self, key: str) -> None:
        await get_redis().delete(key)


class MemoryRequestStore:
    """进程内实现（测试 / 单机联调）。"""

    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def put_json(self, key: str, payload: Any, ttl_seconds: int) -> None:
        self.values[key] = json.dumps(payload, ensure_ascii=False, default=str)

    async def get_bytes(self, key: str) -> bytes:
        if key not in self.values:
            raise RequestDataNotFound(key)
        return self.values[key].encode()

    async def drop(self, key: str) -> None:
        self.values.pop(key, None)


_store: RequestStore | None = None


def get_request_store() -> RequestStore:
    global _store
    if _store is None:
        settings = get_settings()
        if not settings.is_test and settings.redis_url:
            _store = RedisRequestStore()
        else:
            _store = MemoryRequestStore()
    return _store


def set_request_store(store: RequestStore | None) -> None:
    global _store
    _store = store
