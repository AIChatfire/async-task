"""结果存储（§12.2 result_policy = store / §18.8 数据合规）。

结果**一律**转存对象存储、回引用：

* 桶级加密（compose 里用 ``mc encrypt set sse-s3`` 打开）；
* 留存默认 30 天，租户可配；到期 ``result_ref`` 置 expired、查询返回 410；
* 对象存储不可用 → 转存独立重试（独立队列），耗尽只告警，**不伪造失败**。

``MemoryResultStore`` 供测试与本地联调使用，接口与 MinIO 实现一致。
"""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
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


def result_key(tenant: str, task_id: str, *, attempt: int = 1, ext: str = "bin") -> str:
    return f"results/{tenant}/{task_id}/attempt-{attempt}.{ext}"


def request_key(tenant: str, task_id: str) -> str:
    return f"requests/{tenant}/{task_id}/create.json"


def create_response_key(tenant: str, task_id: str) -> str:
    """受理响应的存档位置：幂等重放必须返回**同一份**上游原生响应。"""
    return f"requests/{tenant}/{task_id}/create_response.json"


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
    """MinIO / S3 兼容实现；同步客户端放线程池，避免阻塞事件循环。"""

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
        if not self._bucket_ready:
            if not self.client.bucket_exists(self.bucket):
                self.client.make_bucket(self.bucket)
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


def get_result_store() -> ResultStore:
    global _store
    if _store is None:
        settings = get_settings()
        _store = MemoryResultStore() if settings.result_store_mode == "memory" else MinioResultStore()
    return _store


def set_result_store(store: ResultStore | None) -> None:
    global _store
    _store = store
