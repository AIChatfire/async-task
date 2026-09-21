"""``/async/`` 协议面与回调、结果端点（§11 / §12.2 / §18.3）。

对外承诺三语义：**提交、查询、取消**，外加回调端点与结果端点。

关键实现口径：

* **响应形状**：get 响应就是上游原生形状——走"快照优先 + 同步透传刷新"；create 在默认
  ``queued`` 受理下是网关形状（立刻 202，`id`/`task_id` = 网关任务 id，可按它回查），
  ``inline`` 下受理内同步透传上游原生形状。envelope 只在调用方显式要求
  （``X-AG-Envelope: 1``）时附加，且不改动原生体。
* **未授权与不存在同返 404**：归属校验命中不到就 404，不区分"无权"与"不存在"，
  避免把任务 ID 空间变成可枚举的（§18.5 / 验收 9）。
* **终态冲突仲裁**：``status_source=poll`` 时回调属兜底源，它判出的终态**不立即落库**，
  只触发一次主源复查（§14），防止兜底误判永久锁死正确结果。
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response

from ..db.audit import AuditEventType, AuditWriter
from ..db.base import session_scope
from ..db.dao import TaskDAO
from ..db.models import AsyncTask
from ..domain.enums import ErrorCode, Origin, TaskStatus
from ..domain.state_machine import is_business_terminal
from ..infra.object_store import ObjectNotFound
from ..observability import metrics as M
from ..security.callback_auth import CallbackAuthenticator, body_digest, callback_dedup_key
from ..templates.expression import try_extract
from ..templates.schema import ResultMode, StatusSource
from ..tasks.handlers import HANDLERS, HandlerContext, TaskRuntime, _handle_poll_result, _now
from ..tasks.queues import poll_queue
from .accept import AcceptError, accept_create
from .container import AuthContext, Container, get_container
from .direct import build_direct_version
from .output import build_native_query_response, envelope, response_headers
from .routing import match_route

router = APIRouter()
callbacks = APIRouter()

IDEMPOTENCY_HEADERS = ("idempotency-key", "x-ag-idempotency-key")
ENVELOPE_HEADER = "x-ag-envelope"
ORIGIN_HEADER = "x-ag-origin"


def _wants_envelope(request: Request) -> bool:
    return (request.headers.get(ENVELOPE_HEADER) or "").strip() in ("1", "true", "yes")


def _explicit_idempotency(request: Request) -> str | None:
    for header in IDEMPOTENCY_HEADERS:
        value = request.headers.get(header)
        if value:
            return value.strip()
    return None


def _origin(request: Request) -> Origin:
    raw = (request.headers.get(ORIGIN_HEADER) or "").strip().lower()
    try:
        return Origin(raw) if raw else Origin.USER
    except ValueError:
        raise HTTPException(status_code=400, detail="invalid X-AG-Origin") from None


async def _body(request: Request) -> dict[str, Any]:
    raw = await request.body()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="request body must be valid JSON") from None
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="request body must be a JSON object")
    return parsed


async def _reload(task_id: str) -> AsyncTask | None:
    async with session_scope() as s:
        return await TaskDAO(s).get(task_id)


@router.api_route(
    "/async/{alias}",
    methods=["GET", "POST", "PUT", "DELETE"],
    name="async-root",
)
@router.api_route(
    "/async/{alias}/{rest:path}",
    methods=["GET", "POST", "PUT", "DELETE"],
    name="async-dispatch",
)
async def async_dispatch(alias: str, request: Request, rest: str = "") -> Response:
    container = get_container()
    settings = container.settings
    auth = container.authenticate(request, alias)

    # 模板解析：注册表命中 → 模板路径；未命中 → URL 直配过渡通道（可一键全局关闭）
    direct_used = False
    path = "/" + rest.lstrip("/") if rest else "/"
    try:
        tv = container.template_for_channel(auth.channel, alias)
    except KeyError:
        if not (settings.url_direct_config_enabled and container.url_direct_enabled(auth.channel)):
            raise HTTPException(status_code=404, detail="unknown alias") from None
        tv = build_direct_version(container, auth.channel, alias, path)
        if tv is None:
            raise HTTPException(status_code=404, detail="unknown alias") from None
        direct_used = True

    template = tv.resolved
    match = match_route(template, path, request.method)
    if match is None:
        raise HTTPException(status_code=404, detail="unsupported path for alias")

    if direct_used:
        M.URL_DIRECT_TOTAL.inc({"channel": auth.channel, "alias": alias, "kind": match.kind})

    bus = container.bus
    if match.kind == "create":
        body = await _body(request)
        try:
            accepted = await accept_create(
                container=container,
                bus=bus,
                auth=auth,
                tv=tv,
                body=body,
                explicit_idempotency_key=_explicit_idempotency(request),
                origin=_origin(request),
            )
        except AcceptError as exc:
            M.ACCEPT_REJECTED.inc(
                {"channel": auth.channel, "reason": "quota" if exc.status_code == 429 else "capacity"}
            )
            headers = {"Retry-After": str(max(1, int(exc.retry_after or 1)))} if exc.retry_after else {}
            return JSONResponse(
                status_code=exc.status_code, content={"detail": exc.detail}, headers=headers
            )
        M.ACCEPT_TOTAL.inc({"channel": auth.channel, "alias": alias, "replay": str(accepted.replayed)})
        return JSONResponse(
            status_code=accepted.http_status, content=accepted.body, headers=accepted.headers
        )

    if match.kind == "query":
        return await _handle_query(container, bus, auth, tv, match.upstream_id or "", request)

    return await _handle_cancel(container, bus, auth, tv, match.upstream_id or "", request)


async def _presigned_result_url(container: Container, task: AsyncTask, template) -> str | None:
    """store 模式：把 ``result_ref`` 换成一个可直接取用的预签名 URL。

    按 §12.2 首选**直给对象存储的预签名 URL**，不经过任何网关中转端点 ——
    网关对外**只透传上游原生路径**（提交/查询/取消），不额外发明"结果路径"。
    转存未完成、对象已过期或被删除时返回 ``None``，响应侧据此把结果字段留空。
    """
    if template.result_policy.mode is not ResultMode.STORE or task.result_ref is None:
        return None
    ttl = int(
        template.result_policy.presign_ttl_seconds
        or template.strategy.get("presign_ttl_seconds", container.settings.result_presign_ttl_seconds)
        or container.settings.result_presign_ttl_seconds
    )
    try:
        return await container.result_store.presign_get(task.result_ref, ttl)
    except ObjectNotFound:
        return None


async def _find_task(channel: str, identifier: str) -> AsyncTask | None:
    """按标识找任务：**上游 id 优先、网关 task_id 兜底**。

    兜底是 ``queued``（默认）受理语义的配套：立刻 202 时上游 id 尚不存在，
    调用方（含 New API 两族插件）拿到并回查的就是**网关任务 id**。
    归属（channel/tenant）仍由 ``container.assert_owned`` 收口。
    """
    async with session_scope() as s:
        dao = TaskDAO(s)
        task = await dao.find_by_upstream_id(channel, identifier)
        if task is None:
            by_id = await dao.get(identifier)
            if by_id is not None and by_id.channel == channel:
                task = by_id
        return task


async def _handle_query(
    container: Container,
    bus,
    auth: AuthContext,
    tv,
    upstream_id: str,
    request: Request,
) -> Response:
    template = tv.resolved
    strategy = container.strategy_for(auth.channel, tv)
    min_refresh = float(strategy.get("min_refresh_interval", 3.0))

    task = await _find_task(auth.channel, upstream_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    container.assert_owned(task, auth)

    status = TaskStatus(task.status)
    if not is_business_terminal(status) and auth.bearer:
        stale = (
            task.status_snapshot is None
            or task.status_snapshot_at is None
            or (_now() - task.status_snapshot_at).total_seconds() >= min_refresh
        )
        if stale:
            # 透传刷新：客户端自己带着凭证来了，顺手拿一次最新状态（不落任何凭证）
            try:
                ctx = HandlerContext(payload={"task_id": task.task_id}, bus=bus, container=container)
                rt = TaskRuntime(ctx)
                result = await rt.poll_once(task, template, strategy, bearer=auth.bearer)
                if result.ok:
                    await _handle_poll_result(ctx, rt, task, template, strategy, result)
                    refreshed = await _reload(task.task_id)
                    task = refreshed or task
            except Exception:  # noqa: BLE001 - 上游抖动不能拖垮查询面
                pass

    # store 模式下由 result_ref 直接换出对象存储的预签名 URL（§12.2 首选，不经网关中转）
    result_url = await _presigned_result_url(container, task, template)
    native = build_native_query_response(task, template, result_url=result_url)
    headers = response_headers(
        task, template, refresh_interval=min_refresh, result_available=task.result_ref is not None
    )
    if _wants_envelope(request):
        body = {
            "_envelope": envelope(task, template, result_available=task.result_ref is not None),
            "data": native,
        }
        return JSONResponse(status_code=200, content=body, headers=headers)
    return JSONResponse(status_code=200, content=native, headers=headers)


async def _handle_cancel(
    container: Container,
    bus,
    auth: AuthContext,
    tv,
    upstream_id: str,
    request: Request,
) -> Response:
    template = tv.resolved
    task = await _find_task(auth.channel, upstream_id)
    if task is None:
        raise HTTPException(status_code=404, detail="task not found")
    container.assert_owned(task, auth)

    status = TaskStatus(task.status)
    if is_business_terminal(status):
        return JSONResponse(
            status_code=409,
            content={
                "_envelope": envelope(task, template),
                "detail": f"task already in terminal state {status.value}",
            },
        )

    async with session_scope() as s:
        accepted = await TaskDAO(s).set_cancel_requested(task.task_id)
    if not accepted:
        raise HTTPException(status_code=409, detail="cancel not allowed in current state")

    await bus.enqueue("cancel:default", "cancel_upstream", {"task_id": task.task_id})
    await _audit(
        AuditEventType.CANCELLED,
        subject_id=task.task_id,
        actor=auth.actor,
        refs={"upstream_id": upstream_id, "caps_cancel": template.capabilities.cancel},
        detail="cancel requested"
        + ("" if template.capabilities.cancel else "（上游不支持取消，降级为 cancel_requested 以轮询终态收敛）"),
    )
    latest = await _reload(task.task_id) or task
    return JSONResponse(
        status_code=202,
        content={
            "_envelope": envelope(latest, template),
            "detail": "cancel requested",
            "degraded": not template.capabilities.cancel,
        },
    )


# --------------------------------------------------------------------------------------
# 回调（§18.3）
# --------------------------------------------------------------------------------------
_authenticator: CallbackAuthenticator | None = None


def get_authenticator() -> CallbackAuthenticator:
    global _authenticator
    if _authenticator is None:
        _authenticator = CallbackAuthenticator()
    return _authenticator


def set_authenticator(auth: CallbackAuthenticator | None) -> None:
    global _authenticator
    _authenticator = auth


@callbacks.post("/callbacks/{opaque_token}")
async def receive_callback(opaque_token: str, request: Request) -> Response:
    container = get_container()
    raw = await request.body()
    verify = get_authenticator().verify(dict(request.headers), raw)
    if not verify.ok:
        M.SIGNATURE_FAILURES.inc({"reason": (verify.reason or "unknown").split(":")[0][:32]})
        await _audit(
            AuditEventType.SIGNATURE_FAILED,
            actor="upstream",
            subject_id=opaque_token[:12],
            refs={"kid": verify.kid, "reason": verify.reason, "replay_suspect": verify.replay_suspect},
            detail="回调验签失败：只审计，不进状态机",
        )
        # 对上游统一 202：不给攻击者"签名对不对"的区分信号
        return JSONResponse(status_code=202, content={"accepted": False})

    payload = _safe_json(raw)
    async with session_scope() as s:
        dao = TaskDAO(s)
        task = await dao.find_by_callback_token(opaque_token)
        channel = task.channel if task else (request.headers.get("x-ag-channel") or "unknown")
        upstream_id = None
        if task and task.template_alias:
            tv = container.registry.get(task.template_alias, task.template_version)
            if tv:
                found, value = try_extract(
                    payload if isinstance(payload, dict) else {}, tv.resolved.id_location
                )
                upstream_id = str(value) if found else None
        upstream_id = upstream_id or request.headers.get("x-ag-upstream-id")
        raw_status = None
        if task:
            tv = container.registry.get(task.template_alias, task.template_version)
            if tv:
                found, value = try_extract(
                    payload if isinstance(payload, dict) else {}, tv.resolved.status_field
                )
                raw_status = str(value) if found else None

        digest = body_digest(raw)
        dedup = callback_dedup_key(
            upstream_event_id=request.headers.get("x-ag-event-id"),
            upstream_task_id=upstream_id,
            raw_status=raw_status,
            body_digest=digest,
        )
        fresh = await dao.insert_callback_event(
            channel=channel,
            dedup_key=dedup,
            upstream_task_id=upstream_id,
            raw_status=raw_status,
            kid=verify.kid,
            payload=payload if isinstance(payload, dict) else {"raw": str(payload)[:1000]},
        )
        if not fresh:
            M.CALLBACK_TOTAL.inc({"result": "duplicate"})
            return JSONResponse(status_code=200, content={"accepted": True, "duplicate": True})
        if task is None:
            # orphan 回调：先暂存对账表，由巡检归位（§14）
            if upstream_id:
                await dao.stash_orphan(
                    channel=channel,
                    upstream_task_id=str(upstream_id),
                    raw_status=raw_status,
                    payload=payload if isinstance(payload, dict) else None,
                )
            M.CALLBACK_TOTAL.inc({"result": "orphan"})
            return JSONResponse(status_code=200, content={"accepted": True, "orphan": True})

    await _apply_callback(container, task.task_id, payload, raw_status)
    M.CALLBACK_TOTAL.inc({"result": "applied"})
    return JSONResponse(status_code=200, content={"accepted": True})


def _safe_json(raw: bytes) -> Any:
    try:
        return json.loads(raw or b"{}")
    except json.JSONDecodeError:
        return {"raw": raw.decode("utf-8", errors="replace")[:2000]}


async def _apply_callback(container: Container, task_id: str, payload: Any, raw_status: str | None) -> None:
    task = await _reload(task_id)
    if task is None or is_business_terminal(task.status):
        await _audit(
            AuditEventType.CALLBACK,
            actor="upstream",
            subject_id=task_id,
            refs={"applied": False, "reason": "task terminal or missing", "raw_status": raw_status},
            detail="回调在终态后到达：按单调不翻转丢弃（degraded 在查询面可见）",
        )
        return
    tv = container.registry.get(task.template_alias, task.template_version)
    if tv is None:  # pragma: no cover
        return
    template = tv.resolved
    mapped = template.maps_status(raw_status or "")
    snapshot = payload if isinstance(payload, dict) else {"raw": payload}

    if mapped is None:
        async with session_scope() as s:
            await TaskDAO(s).record_poll_result(
                task_id,
                snapshot=snapshot,
                raw_status=raw_status,
                next_poll_at=task.next_poll_at,
                expected=[task.status],
            )
        return

    # 终态冲突仲裁：poll 为主源时，回调（兜底源）不得直接落终态
    if template.status_source is StatusSource.POLL:
        async with session_scope() as s:
            dao = TaskDAO(s)
            await dao.note_attribute(
                task_id,
                pending_terminal_from_callback={"raw_status": raw_status, "mapped": mapped.value},
            )
            await dao.schedule_next_poll(task_id, _now())
        await _audit(
            AuditEventType.CALLBACK,
            actor="upstream",
            subject_type="task",
            subject_id=task_id,
            refs={"applied": False, "reason": "arbitration_required", "raw_status": raw_status},
            detail="兜底源（回调）判出终态：按 §14 先触发主源复查，不直接落库",
        )
        return

    # 回调为主源：直接按白名单迁移
    async with session_scope() as s:
        dao = TaskDAO(s)
        if mapped is TaskStatus.SUCCEEDED:
            ok = await dao.advance(
                task_id,
                TaskStatus.SUCCEEDED,
                expected=[task.status],
                raw_status=raw_status,
                status_snapshot=snapshot,
                status_snapshot_at=_now(),
                next_poll_at=None,
                finished_at=_now(),
            )
        elif mapped is TaskStatus.TIMEOUT:
            ok = await dao.advance(
                task_id,
                TaskStatus.TIMEOUT,
                expected=[task.status],
                raw_status=raw_status,
                status_snapshot=snapshot,
                error_code=ErrorCode.TIMEOUT.value,
                finished_at=_now(),
                next_poll_at=None,
            )
        else:
            ok = await dao.advance(
                task_id,
                TaskStatus.FAILED,
                expected=[task.status],
                raw_status=raw_status,
                status_snapshot=snapshot,
                status_snapshot_at=_now(),
                error_code=ErrorCode.UPSTREAM_TERMINAL.value,
                finished_at=_now() if mapped is TaskStatus.DEAD else None,
            )
    if ok:
        await _audit(
            AuditEventType.CALLBACK,
            actor="upstream",
            subject_type="task",
            subject_id=task_id,
            refs={"applied": True, "mapped": mapped.value, "raw_status": raw_status},
            detail="回调驱动状态迁移",
        )
        ctx = HandlerContext(payload={"task_id": task_id}, bus=container.bus, container=container)
        await HANDLERS["finalize_task"](ctx)


# --------------------------------------------------------------------------------------
# 结果端点（§18.7）
# --------------------------------------------------------------------------------------
# 结果获取**不走独立端点**：网关对外只透传上游原生路径（提交/查询/取消），
# 结果由**查询路径的响应字段**给出（store 模式 = 对象存储预签名 URL，见 `_presigned_result_url`）。
#
# 原先的 `GET /results/{task_id}` 是这套体系里唯一的"固定端点"，同时踩了三条：
#   1. 违背动态路由原则（routing.py：「不能靠固定端点表路由，要把剩余路径拿去和模板对照」）；
#   2. 用内部主键 task_id 当能力 URL（该值还会经 X-AG-Task-Id 响应头外露 ⇒ 可被枚举取件）；
#   3. 归属校验形同虚设（`x-ag-channel` 带了才校验，不带直接放行 ⇒ 匿名可读），
#      与 §18.x「代理读须带归属校验」不符，也与插件 credentialless 回源相冲突。
# 若将来确需网关代理读（Range 透传/隐藏桶），应做成**带归属校验的独立形态**再加回来。


async def _audit(event_type: str, *, actor: str = "system", **kwargs: Any) -> None:
    async with session_scope() as s:
        await AuditWriter(s).append(event_type, actor=actor, **kwargs)


__all__ = ["callbacks", "router", "get_authenticator", "set_authenticator"]
