"""受理服务（§4.1 幂等 / §7 用例「任务受理」/ §11 协议面）。

受理要同时满足三件事，顺序不能反：

1. **先落库后入队**：库里先有行，才有真相；
2. **幂等键窗口期内唯一**：显式键优先，否则派生 ``key_hash + 请求体规范化哈希``；
   窗口期内同键同体 → 原样重放（返回同一份上游响应）；同键异体 → 409；
3. **响应体必须让调用方"接得上"**：默认 ``queued``（2026-09-21 裁定，见
   docs/IMPLEMENTATION.md §3.1）——**不等上游**：受理只落库 + 入队，立刻返回 202
   ``{"id", "task_id", "status"}``（``id`` = **网关任务 id**；New API ark 系插件认
   ``body.id``、``generic-async-v1`` 认 ``task_id || id``），上游 create 由 worker
   后台完成（凭证需短生命周期驻留，见 ``infra/credentials``）；调用方随后按该 id 查询
   （查询面：上游 id 优先、网关 task_id 兜底）。``inline`` 保留给"必须同步拿到上游原生
   响应"的场景：受理请求内同步 create，凭证不经手任何持久化介质，响应天然是上游原生响应。
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

from sqlalchemy.exc import IntegrityError

from ..bus.base import Bus, QueueSaturated
from ..db.dao import TaskDAO
from ..db.base import session_scope
from ..db.models import AsyncTask
from ..domain.enums import ErrorCode, Origin, TaskStatus
from ..gateway.container import AuthContext, Container
from ..gateway.idempotency import resolve_key, window_bucket
from ..infra.credentials import credential_ttl_seconds
from ..infra.object_store import create_response_key, request_key
from ..security.callback_auth import new_opaque_token
from ..templates.registry import TemplateVersion
from ..templates.render import render_create
from ..tasks.handlers import HandlerContext, _handle_submit_result, _now
from ..tasks.queues import TRANSFER_QUEUE, poll_queue  # noqa: F401  (re-export 便于测试)

CREATE_RESPONSE_REF = "requests/{tenant}/{task_id}/create_response.json"


@dataclass(slots=True)
class AcceptResult:
    task_id: str
    http_status: int
    body: Any
    headers: dict[str, str] = field(default_factory=dict)
    replayed: bool = False
    envelope: dict[str, Any] | None = None


class AcceptError(Exception):
    def __init__(self, status_code: int, detail: str, *, retry_after: float | None = None) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail
        self.retry_after = retry_after


def _response_ref(tenant: str, task_id: str) -> str:
    return create_response_key(tenant, task_id)


async def _admit(container: Container, bus: Bus, channel: str) -> None:
    """受理速率配额（与上游并发槽**分开计数**）。"""
    decision = await container.accept_limiter.try_acquire(channel)
    if not decision.granted:
        raise AcceptError(429, "accept rate limit exceeded", retry_after=decision.retry_after)


async def _check_capacity(bus: Bus, queue: str) -> None:
    depth = await bus.depth(queue)
    if depth >= container_settings_max_depth(bus):
        raise AcceptError(503, f"queue {queue} saturated", retry_after=2.0)


def container_settings_max_depth(bus: Bus) -> int:
    return getattr(bus, "max_depth", 100_000)


async def accept_create(
    *,
    container: Container,
    bus: Bus,
    auth: AuthContext,
    tv: TemplateVersion,
    body: dict[str, Any],
    explicit_idempotency_key: str | None = None,
    origin: Origin = Origin.USER,
    request_deadline_seconds: int | None = None,
) -> AcceptResult:
    template = tv.resolved
    strategy = container.strategy_for(auth.channel, tv)
    store = container.result_store

    # ---- 背压 ----
    await _admit(container, bus, auth.channel)
    await _check_capacity(bus, "submit:default")

    # ---- 幂等 ----
    idem = resolve_key(
        explicit=explicit_idempotency_key,
        key_hash=auth.key_hash,
        body=body,
        rules=template.normalize,
    )
    window = int(strategy.get("idempotency_window_seconds", 86400))
    bucket = window_bucket(None, window)

    async with session_scope() as s:
        dao = TaskDAO(s)
        existing = await dao.find_by_idempotency(idem.key, bucket)

    if existing is not None:
        return await _replay_or_conflict(container, existing, idem.body_digest)

    # ---- 落库（先落库后入队）----
    task_id = uuid.uuid4().hex[:24]
    deadline_seconds = int(request_deadline_seconds or strategy.get("deadline_seconds", 1800))
    ref = request_key(auth.tenant, task_id)
    await store.put_json(ref, body)
    await store.put_json(
        _response_ref(auth.tenant, task_id),
        {"status": None, "body": None, "pending": True},
    )

    task = AsyncTask(
        task_id=task_id,
        idempotency_key=idem.key,
        idempotency_bucket=bucket,
        idempotency_window_seconds=window,
        tenant=auth.tenant,
        channel=auth.channel,
        task_type=template.alias,
        status=TaskStatus.ACCEPTED.value,
        template_alias=template.alias,
        template_version=tv.version,
        attempts=0,
        max_attempts=int(strategy.get("max_attempts", 3)),
        deadline_at=_now() + timedelta(seconds=deadline_seconds),
        origin=origin.value,
        callback_token=new_opaque_token() if template.capabilities.callback else None,
        create_req_ref=ref,
        create_req_digest=idem.body_digest,
        attributes={"create_response_ref": _response_ref(auth.tenant, task_id)},
    )
    try:
        async with session_scope() as s:
            await TaskDAO(s).insert(task)
    except IntegrityError:
        # 并发抢同一个 (key, bucket)：让输的一方读赢的那行
        async with session_scope() as s:
            winner = await TaskDAO(s).find_by_idempotency(idem.key, bucket)
        if winner is None:  # pragma: no cover - 极端竞态
            raise AcceptError(409, "idempotency conflict", retry_after=1.0) from None
        return await _replay_or_conflict(container, winner, idem.body_digest)

    # 凭证短生命周期驻留（仅当配置允许；inline 模式下受理本身已不需要它）
    credential = getattr(container, "credential_store", None)
    if credential is not None and auth.bearer:
        await credential.put(task_id, auth.bearer, credential_ttl_seconds())

    await _audit_accept(auth, task_id, tv, idem, origin)

    if container.settings.submit_mode == "queued":
        # 不等上游：落库 + 入队即返回 202。``id`` 与 ``task_id`` 同值（网关任务 id）——
        # ark 系插件只认 ``body.id``，generic-async-v1 认 ``task_id || id``，两者都接得上；
        # ``status`` 取对外词表值（queued ∈ 两族插件的宽映射，且与查询面预创建期占位一致，
        # 见 output._PENDING_PLACEHOLDER_STATUS），不暴露内部状态名。
        await bus.enqueue("submit:default", "submit_upstream", {"task_id": task_id})
        return AcceptResult(
            task_id=task_id,
            http_status=202,
            body={"id": task_id, "task_id": task_id, "status": "queued"},
            headers={
                "X-AG-Task-Id": task_id,
                "X-AG-Internal-Status": TaskStatus.ACCEPTED.value,
            },
            replayed=False,
        )

    return await _inline_submit(container, bus, auth, tv, task_id, body)


async def _inline_submit(
    container: Container,
    bus: Bus,
    auth: AuthContext,
    tv: TemplateVersion,
    task_id: str,
    body: dict[str, Any],
) -> AcceptResult:
    """受理请求内同步完成上游 create（响应 = 上游原生响应）。"""
    from ..db.dao import TaskDAO as _DAO

    template = tv.resolved
    ctx = HandlerContext(payload={"task_id": task_id}, bus=bus, container=container)

    async def reload() -> AsyncTask | None:
        async with session_scope() as s:
            return await _DAO(s).get(task_id)

    claimed = await _apply_mark_intent(task_id)
    if not claimed:
        # 已被别的路径接管（例如巡检重投）：交回异步流程
        await bus.enqueue("submit:default", "submit_upstream", {"task_id": task_id})
        return AcceptResult(
            task_id=task_id,
            http_status=202,
            body={"task_id": task_id, "status": TaskStatus.ACCEPTED.value},
        )

    bearer = auth.bearer
    channel_limit, tenant_limit = container.channel_limits(auth.channel)
    decision = await container.limiter.acquire(
        auth.channel, auth.tenant, channel_limit=channel_limit, tenant_limit=tenant_limit
    )
    if not decision.granted:
        await _apply_release_intent(task_id)
        await bus.enqueue("submit:default", "submit_upstream", {"task_id": task_id})
        raise AcceptError(429, "channel concurrency exhausted", retry_after=decision.retry_after or 1.0)

    try:
        callback_url = None
        async with session_scope() as s:
            row = await _DAO(s).get(task_id)
        if row and template.capabilities.callback and row.callback_token:
            callback_url = (
                container.settings.callback_base_url.rstrip("/") + f"/callbacks/{row.callback_token}"
            )
        request = render_create(template, body, callback_url=callback_url)
        client = container.upstream_client(bearer)
        try:
            result = await client.call(request, context="submit", bearer=bearer)
        finally:
            await client.aclose()

        # 复用 worker 的同一套分类逻辑，避免"受理路径与后台路径行为分叉"
        await _handle_submit_result(
            ctx,
            _runtime(ctx),
            await _must_reload(task_id),
            template,
            container.strategy_for(auth.channel, tv),
            result,
        )
    finally:
        await container.limiter.release(auth.channel, auth.tenant)

    task = await _must_reload(task_id)
    status = TaskStatus(task.status)
    native = result.response.json_body if result.response is not None else None
    native_status = result.response.status_code if result.response is not None else 502

    # 把原始 create 响应存起来：幂等重放要返回同一份
    resp_ref = _response_ref(auth.tenant, task_id)
    if task.status == TaskStatus.UPSTREAM_SUBMITTED.value:
        await container.result_store.put_json(
            resp_ref, {"status": native_status, "body": native, "pending": False}
        )
        # 立即派发一次轮询，尽快把快照填成"查询形状"
        await bus.enqueue(poll_queue(template.pool), "poll_upstream", {"task_id": task_id})
        return AcceptResult(
            task_id=task_id,
            http_status=native_status if 200 <= native_status < 300 else 200,
            body=native,
            headers={"X-AG-Task-Id": task_id, "X-AG-Upstream-Id": task.upstream_task_id or ""},
        )

    if status is TaskStatus.SUBMIT_UNKNOWN:
        await container.result_store.put_json(
            resp_ref, {"status": 504, "body": native, "pending": False}
        )
        return AcceptResult(
            task_id=task_id,
            http_status=504,
            body=native
            or {
                "error": {
                    "code": ErrorCode.TIMEOUT.value,
                    "message": "upstream create outcome unknown; safe to retry with the same idempotency key",
                    "task_id": task_id,
                }
            },
            headers={"X-AG-Task-Id": task_id, "X-AG-Internal-Status": status.value},
        )

    # 明确失败：把上游的错误响应原样回给调用方（形状保真 + 让 New API 立刻判失败）
    await container.result_store.put_json(
        resp_ref, {"status": native_status, "body": native, "pending": False}
    )
    return AcceptResult(
        task_id=task_id,
        http_status=native_status,
        body=native
        or {
            "error": {
                "code": task.error_code or ErrorCode.UPSTREAM_TERMINAL.value,
                "message": task.error_message or "upstream create failed",
                "task_id": task_id,
            }
        },
        headers={"X-AG-Task-Id": task_id, "X-AG-Internal-Status": status.value},
    )


async def _replay_or_conflict(container: Container, existing: AsyncTask, digest: str) -> AcceptResult:
    if existing.create_req_digest and digest and existing.create_req_digest != digest:
        raise AcceptError(
            409,
            "idempotency key reused with a different request body within the dedup window",
        )
    ref = (existing.attributes or {}).get("create_response_ref") or _response_ref(
        existing.tenant, existing.task_id
    )
    stored: dict[str, Any] = {}
    try:
        import json

        raw = await container.result_store.get_bytes(ref)
        stored = json.loads(raw.decode())
    except Exception:  # noqa: BLE001 - 响应还没落盘（inline 尚未完成）时退化为 202
        stored = {}
    if not stored or stored.get("pending"):
        # 响应尚未落盘（queued 默认下很常见：受理已返回、worker 还在后台创建）→ 202 + 重放标。
        # 同带 ``id``：ark 系插件只认 ``body.id``，重放分支漏了它会让幂等重试整条失败。
        return AcceptResult(
            task_id=existing.task_id,
            http_status=202,
            body={"id": existing.task_id, "task_id": existing.task_id, "status": existing.status},
            headers={"X-AG-Task-Id": existing.task_id, "X-AG-Idempotent-Replay": "1"},
            replayed=True,
        )
    return AcceptResult(
        task_id=existing.task_id,
        http_status=int(stored.get("status") or 200),
        body=stored.get("body"),
        headers={"X-AG-Task-Id": existing.task_id, "X-AG-Idempotent-Replay": "1"},
        replayed=True,
    )


async def _audit_accept(
    auth: AuthContext, task_id: str, tv: TemplateVersion, idem, origin: Origin
) -> None:
    """受理审计：**只留引用与哈希**，不写请求体（§18.5）。"""
    from ..db.audit import AuditEventType, AuditWriter

    async with session_scope() as s:
        await AuditWriter(s).append(
            AuditEventType.ACCEPTED,
            actor=auth.actor,
            subject_type="task",
            subject_id=task_id,
            refs={
                "alias": tv.resolved.alias,
                "version": tv.version,
                "channel": auth.channel,
                "tenant": auth.tenant,
                "origin": origin.value,
                "derived_key": idem.derived,
                "auth_mode": auth.mode,
            },
            payload={"body_digest": idem.body_digest},
            detail="task accepted",
        )


# ---- 与 dao 的薄绑定（避免在受理路径上重复写 CAS 语句）----
async def _apply_mark_intent(task_id: str) -> bool:
    async with session_scope() as s:
        return await TaskDAO(s).mark_submit_intent(task_id)


async def _apply_release_intent(task_id: str) -> None:
    async with session_scope() as s:
        await TaskDAO(s).release_submit_intent(
            task_id, expected=[TaskStatus.ACCEPTED], next_poll_at=_now(), decrement_attempts=True
        )


async def _must_reload(task_id: str) -> AsyncTask:
    async with session_scope() as s:
        row = await TaskDAO(s).get(task_id)
    if row is None:  # pragma: no cover - 不可能发生
        raise AcceptError(500, "task vanished")
    return row


def _runtime(ctx: HandlerContext):
    from ..tasks.handlers import TaskRuntime

    return TaskRuntime(ctx)


__all__ = [
    "AcceptError",
    "AcceptResult",
    "QueueSaturated",
    "TRANSFER_QUEUE",
    "accept_create",
    "container_settings_max_depth",
    "poll_queue",
]
