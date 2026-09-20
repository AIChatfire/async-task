"""数据面凭证的**短生命周期**存放（架构缺口的落地裁定，见 docs/IMPLEMENTATION.md）。

文档要求两件事同时成立：

1. 数据面凭证"随 Authorization 头透传，网关不存 key、不落盘"（§18.2）；
2. worker 在**受理之后的另一个进程里**要向上游发起提交与轮询（§10 进程划分）。

这两条在字面上有冲突：受理请求处理完，header 就没了，而 worker 之后还要用。

本模块给出的落地裁定是**折中而非绕过**：

* 凭证进入 **Redis（内存语义）**，键为 ``cred:{task_id}``，**带 TTL**（受理时按
  deadline 计算，默认不超过 30 分钟），**不写入 Postgres、不落磁盘、不进日志**；
* 任务进入终态（或转人工确认、或转存结束）时**立即删除**；
* 提供 ``none`` 后端：把 ``AG_CREDENTIAL_CHANNEL=none`` 打开后，worker 拿不到凭证，
  提交/轮询只能由客户端请求驱动（透传模式的原教旨形态）——用于需要"绝对不驻留"的场景。

该裁定需要在架构评审上确认（列入 docs/IMPLEMENTATION.md 的"待确认决策"）。
"""

from __future__ import annotations

from typing import Protocol

from redis.exceptions import RedisError

from ..config import get_settings
from .redis import get_redis


class NoCredential(Exception):
    """凭证不存在（未受理、已过期或已清理）。"""


class CredentialStore(Protocol):
    async def put(self, task_id: str, header_value: str, ttl_seconds: int) -> None: ...
    async def get(self, task_id: str) -> str | None: ...
    async def drop(self, task_id: str) -> None: ...


def credential_ttl_seconds() -> int:
    return min(3600, max(300, get_settings().task_deadline_seconds + 300))


def _key(task_id: str) -> str:
    return f"cred:{task_id}"


class RedisEphemeralCredentials:
    async def put(self, task_id: str, header_value: str, ttl_seconds: int) -> None:
        if not header_value:
            return
        await get_redis().set(_key(task_id), header_value, ex=max(60, ttl_seconds))

    async def get(self, task_id: str) -> str | None:
        try:
            return await get_redis().get(_key(task_id))
        except RedisError:  # pragma: no cover - Redis 抖动时不误判为"无凭证"
            raise

    async def drop(self, task_id: str) -> None:
        await get_redis().delete(_key(task_id))


class MemoryEphemeralCredentials:
    def __init__(self) -> None:
        self.values: dict[str, str] = {}

    async def put(self, task_id: str, header_value: str, ttl_seconds: int) -> None:
        self.values[task_id] = header_value

    async def get(self, task_id: str) -> str | None:
        return self.values.get(task_id)

    async def drop(self, task_id: str) -> None:
        self.values.pop(task_id, None)


class NullCredentials:
    """原教旨透传：不驻留任何凭证，worker 侧因此无法自行发起上游调用。"""

    async def put(self, task_id: str, header_value: str, ttl_seconds: int) -> None:
        return None

    async def get(self, task_id: str) -> str | None:
        return None

    async def drop(self, task_id: str) -> None:
        return None


_store: CredentialStore | None = None


def get_credential_store() -> CredentialStore:
    global _store
    if _store is None:
        settings = get_settings()
        if not settings.is_test and settings.redis_url:
            _store = RedisEphemeralCredentials()
        else:
            _store = MemoryEphemeralCredentials()
    return _store


def set_credential_store(store: CredentialStore | None) -> None:
    global _store
    _store = store
