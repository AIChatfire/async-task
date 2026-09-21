"""结果存储（§12.2 result_policy = store / §18.8 数据合规）。

**只服务结果转存**：store 模式下把上游产物转存到对象存储，网关把 ``result_ref``
**现算**成预签名 URL 写进**查询响应**的字段里 —— 网关自身不提供结果端点（见
`docs/IMPLEMENTATION.md` §3.14）。

2026-09-21 裁定（用户指令「转存只走外部 minio、不配置自动不转存、精简架构」）：

* 对象存储是**可选件**：``get_result_store()`` 在未配置时返回 ``None``，转存链路**自动关闭**
  （模板声明 ``store`` 也不转存，降级为直链并在 envelope 声明）——网关仅凭 PG + Redis 即可运行；
* 默认（也是唯一）对接**外部** MinIO/S3（``https://oss.s3ai.cn``）；不再有本地/同机内网那一套；
* 受理路径的 create 请求体 / 响应存档**不经对象存储**（走 Redis，见 ``infra/request_store.py``）。

约定：

* 对象 key = ``{YYYYMMDD}/{tenant}/{task_id}/attempt-{n}.{ext}``（一级是**任务创建日期**，便于按天清理）；
* **不自动建桶**：结果桶由部署侧预建（往往是共享桶）；桶缺失立刻失败，避免把结果写进没人管理的新桶；
* 桶级加密（compose 里用 ``mc encrypt set sse-s3`` 打开）与留存到期清理**尚未接线**；
* 对象存储不可用 → 转存独立重试（独立队列），耗尽只告警，**不伪造失败**。

⚠️ 桶的读权限决定结果的公开性：若桶允许匿名 ``GetObject`` / ``ListBucket``（共享 CDN 桶的常见配置），
转存出去的结果就是**公开可读且可枚举**的，此时预签名 URL 不构成保护。属部署期决定，见 §4.1。

``MemoryResultStore`` 供测试与本地联调使用，接口与 MinIO 实现一致。
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

import anyio.to_thread

from ..config import get_settings


class ObjectNotFound(Exception):
    pass


class ResultStore(Protocol):
    async def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str: ...
    async def put_json(self, key: str, payload: Any) -> str: ...
    async def get_bytes(self, key: str) -> bytes: ...
    async def exists(self, key: str) -> bool: ...
    async def presign_get(self, key: str, ttl_seconds: int) -> str: ...
    async def delete_prefix(self, prefix: str) -> int: ...


@dataclass(slots=True)
class StoredObject:
    key: str
    size: int
    content_type: str


def result_key(
    tenant: str,
    task_id: str,
    *,
    created_at: datetime | None = None,
    attempt: int = 1,
    ext: str = "bin",
) -> str:
    """结果对象 key：``{YYYYMMDD}/{tenant}/{task_id}/attempt-{n}.{ext}``。

    一级前缀是**任务创建日期（UTC）**，与"按天分目录"的桶生命周期 / 清理策略对齐。
    取"创建时间"而不是"写入时刻"：失败重试与跨天重放都会落在同一目录，不产生跨日碎片。
    """
    from datetime import UTC, datetime as _dt

    moment = created_at or _dt.now(UTC)
    if moment.tzinfo is None:  # 兜底：naive 一律按 UTC 解释，别用本机时区偏移
        moment = moment.replace(tzinfo=UTC)
    return f"{moment.astimezone(UTC):%Y%m%d}/{tenant}/{task_id}/attempt-{attempt}.{ext}"


class MemoryResultStore:
    """进程内实现（测试 / 单机联调）。"""

    def __init__(self, base_url: str = "memory://results") -> None:
        self.objects: dict[str, tuple[bytes, str]] = {}
        self.base_url = base_url

    async def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        self.objects[key] = (data, content_type)
        return key

    async def put_json(self, key: str, payload: Any) -> str:
        return await self.put_bytes(
            key, json.dumps(payload, ensure_ascii=False, default=str).encode(), "application/json"
        )

    async def get_bytes(self, key: str) -> bytes:
        if key not in self.objects:
            raise ObjectNotFound(key)
        return self.objects[key][0]

    async def exists(self, key: str) -> bool:
        return key in self.objects

    async def presign_get(self, key: str, ttl_seconds: int) -> str:
        if key not in self.objects:
            raise ObjectNotFound(key)
        return f"{self.base_url}/{key}?ttl={ttl_seconds}"

    async def delete_prefix(self, prefix: str) -> int:
        victims = [k for k in self.objects if k.startswith(prefix)]
        for key in victims:
            self.objects.pop(key, None)
        return len(victims)


class MinioResultStore:
    """MinIO / S3 兼容实现（外部端点）；同步客户端放线程池，避免阻塞事件循环。"""

    def __init__(self) -> None:
        from minio import Minio

        settings = get_settings()
        self.settings = settings
        self.bucket = settings.s3_bucket
        self.client = Minio(
            settings.s3_endpoint.replace("http://", "").replace("https://", ""),
            access_key=settings.s3_access_key,
            secret_key=settings.s3_secret_key,
            secure=settings.s3_secure,
        )
        self._bucket_ready = False

    def _ensure_bucket(self) -> None:
        """确认结果桶可用 —— **不自动建桶**。

        结果桶通常是部署侧预建的**共享桶**（可能挂着 CDN 与既有桶策略），网关静默
        ``make_bucket`` 的风险是：桶名配错时会凭空建出一个新桶、把结果写进一个没人管理的
        位置（既不报警也不可发现）。桶缺失属于部署配置错误，应当立刻失败并暴露。
        """
        if self._bucket_ready:
            return
        if not self.client.bucket_exists(self.bucket):
            raise RuntimeError(f"result bucket not available: {self.bucket}")
        self._bucket_ready = True

    async def put_bytes(self, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
        def _put() -> None:
            self._ensure_bucket()
            self.client.put_object(
                self.bucket, key, io.BytesIO(data), length=len(data), content_type=content_type
            )

        await anyio.to_thread.run_sync(_put)
        return key

    async def put_json(self, key: str, payload: Any) -> str:
        blob = json.dumps(payload, ensure_ascii=False, default=str).encode()
        return await self.put_bytes(key, blob, "application/json")

    async def get_bytes(self, key: str) -> bytes:
        def _get() -> bytes:
            response = self.client.get_object(self.bucket, key)
            try:
                return response.read()
            finally:
                response.close()
                response.release_conn()

        try:
            return await anyio.to_thread.run_sync(_get)
        except Exception as exc:  # noqa: BLE001 - 统一转成领域异常
            raise ObjectNotFound(key) from exc

    async def exists(self, key: str) -> bool:
        def _stat() -> bool:
            try:
                self.client.stat_object(self.bucket, key)
                return True
            except Exception:  # noqa: BLE001
                return False

        return await anyio.to_thread.run_sync(_stat)

    async def presign_get(self, key: str, ttl_seconds: int) -> str:
        from datetime import timedelta

        def _presign() -> str:
            return self.client.presigned_get_object(
                self.bucket, key, expires=timedelta(seconds=ttl_seconds)
            )

        return await anyio.to_thread.run_sync(_presign)

    async def delete_prefix(self, prefix: str) -> int:
        def _purge() -> int:
            count = 0
            for obj in self.client.list_objects(self.bucket, prefix=prefix, recursive=True):
                self.client.remove_object(self.bucket, obj.object_name)
                count += 1
            return count

        return await anyio.to_thread.run_sync(_purge)


_store: ResultStore | None = None


def get_result_store() -> ResultStore | None:
    """结果转存用的对象存储。

    **未配置（``s3_configured`` 为假）时返回 ``None``** —— 调用方据此自动关闭转存
    （不派发、不重试、不告警；查询面降级为直链并在 envelope 声明）。
    测试/嵌入方可用 :func:`set_result_store` 注入替身。
    """
    global _store
    if _store is None:
        if not get_settings().s3_configured:
            return None
        _store = MinioResultStore()
    return _store


def set_result_store(store: ResultStore | None) -> None:
    global _store
    _store = store
