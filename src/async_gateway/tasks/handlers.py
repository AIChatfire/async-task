"""任务处理器（§8.1/§8.2 生命周期与异常路径的运行时实现）。

统一原则：

* **worker 只按消息里的 task_id 回库重读状态再决定动作**——消息不携带真相，
  重复投递/丢消息都不会破坏状态机；
* **所有状态写入走 CAS**：抢不到就说明别人先动了，直接返回，绝不"补偿性重写"；
* **不确定场景唯一允许重发创建的入口是 compensate**（§4.1 不变量）；
* 上游 HTTP 调用**不在事务内**（长 IO 不占数据库连接）。

handler 与 queue 的对应：submit_upstream / poll_upstream / finalize_task /
cancel_upstream / compensate_orphan / store_result。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Awaitable, Callable

from ..bus.base import Bus
from ..config import get_settings
from ..db.audit import AuditEventType, AuditWriter
from ..db.dao import TaskDAO
from ..db.base import session_scope
from ..db.models import AsyncTask
from ..domain.enums import ErrorCode, Origin, TaskStatus
from ..domain.errors import backoff_seconds
from ..domain.state_machine import BUSINESS_TERMINAL, is_business_terminal
from ..gateway.container import Container, get_container
from ..infra.credentials import get_credential_store
from ..infra.object_store import get_result_store, request_key, result_key
from ..templates.render import render_cancel, render_create, render_get
from ..templates.schema import ResultMode, ResolvedTemplate
from ..templates.expression import try_extract
from ..upstream.client import UpstreamCallResult, UpstreamClient
from .queues import TRANSFER_QUEUE, poll_queue

MAX_TRANSFER_ATTEMPTS = 5
#: poll 404 连续出现多少次后转确认（多 key 轮换 / 任务空间不匹配的兜底）
POLL_404_CONFIRM_THRESHOLD = 3


def _now() -> datetime:
    return datetime.now(UTC)


async def _load(task_id: str) -> AsyncTask | None:
    async with session_scope() as s:
        return await TaskDAO(s).get(task_id)


async def _apply(method: str, *args: Any, **kwargs: Any) -> Any:
    async with session_scope() as s:
        return await getattr(TaskDAO(s), method)(*args, **kwargs)


async def _audit(event_type: str, **kwargs: Any) -> None:
    async with session_scope() as s:
        await AuditWriter(s).append(event_type, actor=kwargs.pop("actor", "system"), **kwargs)


@dataclass(slots=True)
class HandlerContext:
    payload: dict[str, Any]
    bus: Bus
    container: Container

    @property
    def task_id(self) -> str:
        task_id = self.payload.get("task_id")
        if not task_id:
            raise ValueError("handler payload missing task_id")
        return str(task_id)

    @property
    def credentials(self):
        return get_credential_store()

    @property
    def result_store(self):
        return get_result_store()

    @property
    def settings(self):
        return self.container.settings

    async def enqueue(self, queue: str, name: str, **payload: Any) -> str:
        return await self.bus.enqueue(queue, name, {"task_id": self.task_id, **payload})

    async def enqueue_self_later(self, delay_seconds: float) -> None:
        """把"稍后再做"落到 ``next_poll_at``，由 scheduler 派发（broker 不做延时）。"""
        await _apply("schedule_next_poll", self.task_id, _now() + timedelta(seconds=max(0.0, delay_seconds)))


class TaskRuntime:
    """handler 共用的运行时操作（模板解析、轮询核心、终态收尾）。"""

    def __init__(self, ctx: HandlerContext) -> None:
        self.ctx = ctx
        self.container = ctx.container

    # ---- 模板 ----
    def template(self, task: AsyncTask) -> tuple[ResolvedTemplate, dict[str, Any]]:
        tv = self.container.registry.require(task.template_alias, task.template_version)
        return tv.resolved, self.container.strategy_for(task.channel, tv)

    async def load_create_body(self, task: AsyncTask) -> dict[str, Any]:
        if task.create_req_ref:
            try:
                import json

                raw = await self.ctx.result_store.get_bytes(task.create_req_ref)
                body = json.loads(raw.decode())
                if isinstance(body, dict):
                    return body
            except Exception:  # noqa: BLE001 - 摘要缺失时退回最小体，交给上游报错（不猜内容）
                pass
        return {}

    # ---- 凭证 ----
    async def credential(self, task: AsyncTask) -> str | None:
        return await self.ctx.credentials.get(task.task_id)

    async def drop_credential(self, task: AsyncTask) -> None:
        await self.ctx.credentials.drop(task.task_id)

    # ---- 轮询核心（handler 与查询面共用） ----
    async def poll_once(
        self,
        task: AsyncTask,
        template: ResolvedTemplate,
        strategy: dict[str, Any],
        *,
        bearer: str | None,
        persist: bool = True,
    ) -> UpstreamCallResult:
        client = self.container.upstream_client(bearer)
        try:
            request = render_get(template, task.upstream_task_id or "")
            return await client.call(request, context="poll", bearer=bearer)
        finally:
            await client.aclose()

    async def terminal_sample(self, task: AsyncTask) -> None:
        started = task.started_at or task.created_at
        elapsed = max(0.0, (_now() - started).total_seconds())
        await self.container.polling.record_terminal(task.channel, elapsed)

    async def finalize_success(self, task: AsyncTask, template: ResolvedTemplate) -> None:
        """成功收尾：转存入队 / 凭证清理 / 观测采样。"""
        await self.terminal_sample(task)
        if template.result_policy.mode is ResultMode.STORE:
            await self.ctx.enqueue(TRANSFER_QUEUE, "store_result", attempt=1)
        await self.drop_credential(task)
        await _audit(
            AuditEventType.TERMINAL,
            subject_type="task",
            subject_id=task.task_id,
            refs={"status": TaskStatus.SUCCEEDED.value, "alias": task.template_alias},
        )

    async def finalize_terminal(self, task: AsyncTask, status: TaskStatus) -> None:
        await self.terminal_sample(task)
        await self.drop_credential(task)
        await _audit(
            AuditEventType.TERMINAL,
            subject_type="task",
            subject_id=task.task_id,
            refs={"status": status.value},
        )


# --------------------------------------------------------------------------------------
# 1) submit_upstream
# --------------------------------------------------------------------------------------
async def submit_upstream(ctx: HandlerContext) -> None:
    task = await _load(ctx.task_id)
    if task is None or is_business_terminal(task.status):
        return
    status = TaskStatus(task.status)
    if status is not TaskStatus.ACCEPTED and status is not TaskStatus.FAILED:
        return  # 交给对应 handler，不越权

    rt = TaskRuntime(ctx)
    template, strategy = rt.template(task)
    expected = [status]

    if task.deadline_at and _now() >= task.deadline_at:
        if await _apply("advance", task.task_id, TaskStatus.TIMEOUT, expected=expected,
                        error_code=ErrorCode.TIMEOUT.value, error_message="deadline exceeded before submit"):
            await rt.finalize_terminal(task, TaskStatus.TIMEOUT)
        return

    if task.cancel_requested:
        if await _apply("advance", task.task_id, TaskStatus.CANCELLED, expected=expected):
            await rt.finalize_terminal(task, TaskStatus.CANCELLED)
        return

    bearer = await rt.credential(task)
    if not bearer:
        await _apply(
            "advance", task.task_id, TaskStatus.DEAD, expected=expected,
            error_code=ErrorCode.UPSTREAM_4XX.value,
            error_message="upstream credential unavailable (expired or not retained)",
            retryable=False, finished_at=_now(), next_poll_at=None,
        )
        await rt.finalize_terminal(task, TaskStatus.DEAD)
        return

    channel_limit, tenant_limit = ctx.container.channel_limits(task.channel)
    decision = await ctx.container.limiter.acquire(
        task.channel, task.tenant, channel_limit=channel_limit, tenant_limit=tenant_limit
    )
    if not decision.granted:
        await ctx.enqueue_self_later(decision.retry_after or 1.0)
        return

    try:
        # 提交意图（CAS 抢占 + attempts 预递增）：抢不到说明别的 worker 已在做，或状态已变
        if not await _apply("mark_submit_intent", task.task_id):
            return

        body = await rt.load_create_body(task)
        callback_url = None
        if template.capabilities.callback and task.callback_token:
            callback_url = (
                ctx.settings.callback_base_url.rstrip("/") + f"/callbacks/{task.callback_token}"
            )
        request = render_create(template, body, callback_url=callback_url)

        client = ctx.container.upstream_client(bearer)
        try:
            result = await client.call(request, context="submit", bearer=bearer)
        finally:
            await client.aclose()

        await _handle_submit_result(ctx, rt, task, template, strategy, result)
    finally:
        await ctx.container.limiter.release(task.channel, task.tenant)


async def _handle_submit_result(
    ctx: HandlerContext,
    rt: TaskRuntime,
    task: AsyncTask,
    template: ResolvedTemplate,
    strategy: dict[str, Any],
    result: UpstreamCallResult,
) -> None:
    expected = [TaskStatus.ACCEPTED, TaskStatus.FAILED]
    max_attempts = int(strategy.get("max_attempts", task.max_attempts or 1))

    # ---- SSRF 拒绝：配置/上游响应被篡改，判失败不重试 ----
    if result.outcome == "ssrf_rejected":
        await _apply(
            "advance", task.task_id, TaskStatus.DEAD, expected=expected,
            error_code=ErrorCode.UPSTREAM_TERMINAL.value, error_message=result.safe_error(),
            retryable=False, finished_at=_now(), next_poll_at=None,
        )
        await rt.finalize_terminal(task, TaskStatus.DEAD)
        return

    # ---- 成功：取 id ----
    if result.ok:
        payload = result.response.json_body if result.response else None
        found, upstream_id = try_extract(payload or {}, template.id_location)
        if not found:
            # 2xx 但取不到 id：我们不能断定"没创建"，一律转确认（不猜成功）
            await _apply(
                "enter_unknown", task.task_id,
                error_code=ErrorCode.UPSTREAM_5XX.value,
                error_message=f"create succeeded but id not found at {template.id_location}",
            )
            await ctx.enqueue("compensate:default", "compensate_orphan")
            return
        found_status, raw_status = try_extract(payload or {}, template.status_field)
        interval = await ctx.container.polling.next_interval(
            task.channel, elapsed=0.0, first_poll=True
        )
        submitted_at = _now()
        ok = await _apply(
            "mark_submitted", task.task_id,
            upstream_task_id=str(upstream_id),
            raw_status=str(raw_status) if found_status else None,
            snapshot=None,  # 查询面在首次查询时做一次透传刷新，保证 get 形状正确
            next_poll_at=submitted_at + timedelta(seconds=interval),
        )
        if not ok:
            return
        # attributes 走合并写入：mark_submitted 用的是一条 UPDATE，直接塞 attributes
        # 会把受理时记下的 create_response_ref 覆盖掉（幂等重放就坏了）。
        await _apply("note_attribute", task.task_id, create_accepted_at=submitted_at.isoformat())
        # 回写 create 响应存档：后台提交成功后，幂等重放也必须拿到这份成功响应，
        # 而不是先前那次失败的响应（否则调用方会被旧错误永久挡住）。
        if result.response is not None:
            from ..infra.object_store import create_response_key

            await ctx.result_store.put_json(
                create_response_key(task.tenant, task.task_id),
                {"status": result.response.status_code, "body": payload, "pending": False},
            )
        if task.cancel_requested:
            # submit 落库后强制检查 cancel_requested：杜绝"用户已取消、上游仍跑"
            await ctx.enqueue("cancel:default", "cancel_upstream")
        await ctx.enqueue(poll_queue(template.pool), "poll_upstream")
        return

    classification = result.classification
    assert classification is not None
    code = classification.error_code.value

    # ---- 429：不消耗业务 attempts，独立退避 ----
    if classification.error_class.value == "rate_limited":
        delay = max(
            result.retry_after or 0.0,
            await ctx.container.polling.next_interval(task.channel, elapsed=0.0, first_poll=True),
        )
        await ctx.container.polling.note_rate_limited(task.channel, result.retry_after)
        await _apply(
            "release_submit_intent", task.task_id, expected=expected,
            next_poll_at=_now() + timedelta(seconds=delay),
            decrement_attempts=True, error_code=code,
            error_message=result.safe_error(), retryable=True,
        )
        return

    # ---- 401/403：渠道级故障，烧的不是任务 attempts ----
    if classification.channel_fault:
        await ctx.container.polling.note_rate_limited(task.channel, None)
        await _apply(
            "release_submit_intent", task.task_id, expected=expected,
            next_poll_at=_now() + timedelta(seconds=backoff_seconds(task.attempts, base=5.0, cap=300.0)),
            decrement_attempts=True, error_code=code, error_message=result.safe_error(),
            retryable=True, attributes={"channel_fault": True},
        )
        await _audit(
            AuditEventType.GOVERNANCE,
            subject_type="channel",
            subject_id=task.channel,
            refs={"event": "channel_fault", "task_id": task.task_id, "error_code": code},
            detail="渠道级故障（401/403）：熔断反馈回路 = 网关告警 → New API 渠道禁用该 key → 换 key",
        )
        return

    # ---- 判失败不重试（4xx 参数错误）----
    if classification.error_class.value == "fail_fast":
        await _apply(
            "advance", task.task_id, TaskStatus.DEAD, expected=expected,
            error_code=code, error_message=result.safe_error(), retryable=False,
            finished_at=_now(), next_poll_at=None,
        )
        await rt.finalize_terminal(task, TaskStatus.DEAD)
        return

    # ---- 不确定（响应超时/丢失/5xx/409）：转 submit_unknown，由 compensate 确认 ----
    if classification.is_unknown:
        await _apply(
            "enter_unknown", task.task_id, error_code=code, error_message=result.safe_error(),
        )
        await ctx.enqueue("compensate:default", "compensate_orphan")
        return

    # ---- 可安全重试（连接被拒/DNS 失败）：请求未发出，重试安全 ----
    if task.attempts >= max_attempts:
        await _apply(
            "advance", task.task_id, TaskStatus.DEAD, expected=expected,
            error_code=code, error_message=result.safe_error(), retryable=True,
            finished_at=_now(), next_poll_at=None,
        )
        await rt.finalize_terminal(task, TaskStatus.DEAD)
        return
    await _apply(
        "release_submit_intent", task.task_id, expected=expected,
        next_poll_at=_now() + timedelta(seconds=backoff_seconds(task.attempts)),
        error_code=code, error_message=result.safe_error(), retryable=True,
    )


# --------------------------------------------------------------------------------------
# 2) poll_upstream
# --------------------------------------------------------------------------------------
async def poll_upstream(ctx: HandlerContext) -> None:
    task = await _load(ctx.task_id)
    if task is None or is_business_terminal(task.status):
        return
    status = TaskStatus(task.status)

    if status is TaskStatus.SUBMIT_UNKNOWN:
        # 防御性分流：不确认不许碰创建（§4.1 不变量）
        await ctx.enqueue("compensate:default", "compensate_orphan")
        return
    if status not in (
        TaskStatus.UPSTREAM_SUBMITTED,
        TaskStatus.IN_PROGRESS,
        TaskStatus.POLL_UNRECOGNIZED,
    ):
        # 只有这三种状态归轮询管：failed 归 submit_upstream（任务级重试），
        # accepted 归巡检重投，业务终态直接丢弃。越权处理 = 自己把自己推进非法迁移。
        return

    rt = TaskRuntime(ctx)
    template, strategy = rt.template(task)

    if task.deadline_at and _now() >= task.deadline_at:
        if await _apply(
            "advance", task.task_id, TaskStatus.TIMEOUT,
            expected=[status], error_code=ErrorCode.TIMEOUT.value,
            error_message="deadline exceeded", finished_at=_now(), next_poll_at=None,
        ):
            await rt.finalize_terminal(task, TaskStatus.TIMEOUT)
        return

    paused = await ctx.container.polling.paused_for(task.channel)
    if paused > 0:
        await ctx.enqueue_self_later(paused + 0.5)
        return

    if not task.upstream_task_id:
        # 没有上游 id 却在轮询态：转确认，绝不自作主张重发创建
        await _apply(
            "advance", task.task_id, TaskStatus.SUBMIT_UNKNOWN, expected=[status],
            error_code=ErrorCode.UPSTREAM_TERMINAL.value,
            error_message="poll scheduled without upstream_task_id",
            unknown_since=_now(), next_poll_at=_now(),
        )
        return

    bearer = await rt.credential(task)
    channel_limit, tenant_limit = ctx.container.channel_limits(task.channel)
    decision = await ctx.container.limiter.acquire(
        task.channel, task.tenant, channel_limit=channel_limit, tenant_limit=tenant_limit
    )
    if not decision.granted:
        await ctx.enqueue_self_later(decision.retry_after or 1.0)
        return
    try:
        result = await rt.poll_once(task, template, strategy, bearer=bearer)
        await _handle_poll_result(ctx, rt, task, template, strategy, result)
    finally:
        await ctx.container.limiter.release(task.channel, task.tenant)


async def _handle_poll_result(
    ctx: HandlerContext,
    rt: TaskRuntime,
    task: AsyncTask,
    template: ResolvedTemplate,
    strategy: dict[str, Any],
    result: UpstreamCallResult,
) -> None:
    status = TaskStatus(task.status)
    expected = [status]
    elapsed = max(0.0, (_now() - (task.started_at or task.created_at)).total_seconds())

    async def reschedule(seconds: float | None = None) -> float:
        interval = seconds or await ctx.container.polling.next_interval(task.channel, elapsed=elapsed)
        await _apply("schedule_next_poll", task.task_id, _now() + timedelta(seconds=interval))
        return interval

    # ---- 传输/HTTP 异常 ----
    if not result.ok:
        classification = result.classification
        if result.outcome == "ssrf_rejected":
            await _apply(
                "advance", task.task_id, TaskStatus.DEAD, expected=expected,
                error_code=ErrorCode.UPSTREAM_TERMINAL.value, error_message=result.safe_error(),
                retryable=False, finished_at=_now(), next_poll_at=None,
            )
            await rt.finalize_terminal(task, TaskStatus.DEAD)
            return
        if classification is None:  # pragma: no cover - 理论上不会发生
            await reschedule()
            return

        if classification.error_class.value == "rate_limited":
            await ctx.container.polling.note_rate_limited(task.channel, result.retry_after)
            await _apply(
                "mark_failure", task.task_id, expected=expected,
                error_code=classification.error_code.value, error_message=result.safe_error(),
                retryable=True, next_poll_at=_now() + timedelta(seconds=result.retry_after or 5.0),
            )
            return

        if classification.channel_fault:
            await ctx.container.polling.note_rate_limited(task.channel, None)
            await _audit(
                AuditEventType.GOVERNANCE, subject_type="channel", subject_id=task.channel,
                refs={"event": "channel_fault", "task_id": task.task_id, "phase": "poll"},
                detail="轮询期渠道级故障（401/403）",
            )
            await reschedule(backoff_seconds(task.poll_count + 1, base=5.0, cap=300.0))
            return

        if classification.action == "confirm":
            # 例如 poll 404：多 key 轮换 / 任务空间不匹配。连续出现才转确认，避免误判
            count = int((task.attributes or {}).get("poll_404_count", 0)) + 1
            if result.response is not None and result.response.status_code == 404 and count < POLL_404_CONFIRM_THRESHOLD:
                await _apply("note_attribute", task.task_id, poll_404_count=count)
                await reschedule(backoff_seconds(count, base=5.0))
                return
            await _apply(
                "advance", task.task_id, TaskStatus.SUBMIT_UNKNOWN, expected=expected,
                error_code=classification.error_code.value, error_message=result.safe_error(),
                unknown_since=_now(), next_poll_at=_now(),
            )
            await ctx.enqueue("compensate:default", "compensate_orphan")
            return

        # 5xx / 传输抖动：退避重试，不动状态、不烧 attempts
        await _apply(
            "mark_failure", task.task_id, expected=expected,
            error_code=classification.error_code.value, error_message=result.safe_error(),
            retryable=True, next_poll_at=None,
        )
        await reschedule()
        return

    # ---- 成功响应：判定终态 ----
    body = result.response.json_body if result.response else None
    found_status, raw_status = try_extract(body or {}, template.status_field)
    native = str(raw_status) if found_status else None

    if not found_status or native is None:
        # "提取不到判定"才归 poll_unrecognized（§12.2）：状态字段都读不出来，
        # 我们就无法判断它是进行中还是终态，只能标记不可判定并等模板勘误。
        await _enter_poll_unrecognized(
            ctx, rt, task, template, result, reason="status field not extractable"
        )
        return

    mapped = template.maps_status(native)
    if mapped is None:
        # 取值既不是成功/失败/过期 —— 那就是**上游的进行中状态**（queued/running/...）。
        # 网关不维护全量状态映射表，只关心"是否终态/是否成功/结果在哪"，因此这里
        # 记录快照并按自适应间隔继续轮询，而不是误判为 poll_unrecognized。
        snapshot = _snapshot_with_status(body, template, native)
        interval = await ctx.container.polling.next_interval(task.channel, elapsed=elapsed)
        await _apply(
            "record_poll_result",
            task.task_id,
            snapshot=snapshot,
            raw_status=native,
            next_poll_at=_now() + timedelta(seconds=interval),
            expected=expected,
        )
        if status is not TaskStatus.IN_PROGRESS:
            # 首次确认"上游确实在跑" → 进入 in_progress（受理链路 accepted →
            # upstream_submitted → in_progress 的最后一跳）；
            # poll_unrecognized 也要能借助模板勘误恢复到正常轮询。
            await _apply(
                "advance",
                task.task_id,
                TaskStatus.IN_PROGRESS,
                expected=[status],
                unknown_since=None,
            )
        return

    snapshot = _snapshot_with_status(body, template, native)

    if mapped is TaskStatus.SUCCEEDED:
        # 结果字段回填上游原始值：脱敏后的签名 URL 不可用，转存会永久失败
        _restore_raw_result_field(snapshot, template, result)
        ok = await _apply(
            "advance", task.task_id, TaskStatus.SUCCEEDED, expected=expected,
            raw_status=native, status_snapshot=snapshot, status_snapshot_at=_now(),
            poll_count=task.poll_count + 1, last_polled_at=_now(),
            next_poll_at=None, finished_at=_now(), error_code=None, error_message=None,
        )
        if ok:
            await rt.finalize_success(task, template)
        return

    if mapped is TaskStatus.TIMEOUT:
        if await _apply(
            "advance", task.task_id, TaskStatus.TIMEOUT, expected=expected,
            raw_status=native, status_snapshot=snapshot, error_code=ErrorCode.TIMEOUT.value,
            error_message=f"upstream reported {native}", finished_at=_now(), next_poll_at=None,
        ):
            await rt.finalize_terminal(task, TaskStatus.TIMEOUT)
        return

    # mapped is FAILED：上游报失败 → 按 §14 先落 failed（可重试），耗尽转 dead
    max_attempts = int(strategy.get("max_attempts", task.max_attempts or 1))
    can_retry = task.attempts < max_attempts
    if can_retry:
        if await _apply(
            "advance", task.task_id, TaskStatus.FAILED, expected=expected,
            raw_status=native, status_snapshot=snapshot, status_snapshot_at=_now(),
            error_code=ErrorCode.UPSTREAM_TERMINAL.value,
            error_message=f"upstream reported {native}", retryable=True,
            next_poll_at=_now() + timedelta(seconds=backoff_seconds(task.attempts)),
        ):
            await _audit(
                AuditEventType.GOVERNANCE, subject_type="task", subject_id=task.task_id,
                refs={"event": "task_level_retry", "attempts": task.attempts, "max": max_attempts},
                detail="任务级重试（新业务尝试）：经 submit_upstream 重建并递增 attempts",
            )
        return
    if await _apply(
        "advance", task.task_id, TaskStatus.DEAD, expected=expected,
        raw_status=native, status_snapshot=snapshot, error_code=ErrorCode.UPSTREAM_TERMINAL.value,
        error_message=f"upstream reported {native}; retries exhausted", retryable=True,
        finished_at=_now(), next_poll_at=None,
    ):
        await rt.finalize_terminal(task, TaskStatus.DEAD)


def _snapshot_with_status(body: Any, template: ResolvedTemplate, native: str) -> Any:
    """确保快照里的状态字段就是上游原生取值（透传形状保真的前提）。"""
    from ..gateway.output import PathWriteSkipped, set_path_on_doc

    if not isinstance(body, dict):
        return body
    try:
        set_path_on_doc(body, template.status_field, native)
    except PathWriteSkipped:
        pass
    return body


def _restore_raw_result_field(snapshot: Any, template: ResolvedTemplate, result) -> None:
    """把快照里**结果字段**的取值换回上游原始值（其余字段保持脱敏）。

    上游结果 URL 多带签名查询串，而响应体进网关时已按 §17 做凭证形态脱敏——
    签名长串会被兜底规则抹成 ``***redacted***``，URL 随即不可用。快照又是转存取
    URL 的唯一来源（``store_result`` 读 ``task.status_snapshot``），所以结果字段
    必须用原始值，否则链路表现为 ``succeeded`` + 结果端点 410。

    只回填 ``result_location`` 这一个叶子：错误信息里回显的凭证片段等仍在
    ``json_body`` 上保持脱敏，§17 C2 的防护不受影响。
    """
    raw = result.response.raw_json_body if result.response else None
    if not isinstance(raw, dict) or not isinstance(snapshot, dict) or not template.result_location:
        return
    found, value = try_extract(raw, template.result_location)
    if not found or value is None:
        return
    from ..gateway.output import PathWriteSkipped, set_path_on_doc

    try:
        set_path_on_doc(snapshot, template.result_location, value)
    except PathWriteSkipped:
        pass


async def _enter_poll_unrecognized(
    ctx: HandlerContext,
    rt: TaskRuntime,
    task: AsyncTask,
    template: ResolvedTemplate,
    result: UpstreamCallResult,
    *,
    reason: str,
) -> None:
    """轮询判不出终态：保留 upstream_task_id，**永不触发创建**（§14）。"""
    status = TaskStatus(task.status)
    body = result.response.json_body if result.response else None
    if status is TaskStatus.POLL_UNRECOGNIZED:
        await _apply(
            "record_poll_result", task.task_id, snapshot=body, raw_status=None,
            next_poll_at=_now() + timedelta(seconds=30.0), expected=[status],
        )
        return
    await _apply(
        "advance", task.task_id, TaskStatus.POLL_UNRECOGNIZED, expected=[status],
        status_snapshot=body, status_snapshot_at=_now(), raw_status=None,
        error_code=ErrorCode.UNKNOWN_EXHAUSTED.value, error_message=reason[:500],
        unknown_since=_now(), next_poll_at=_now() + timedelta(seconds=30.0),
    )
    await _audit(
        AuditEventType.GOVERNANCE, subject_type="task", subject_id=task.task_id,
        refs={"event": "poll_unrecognized", "reason": reason, "alias": task.template_alias},
        detail="轮询响应判不出终态；模板勘误后可恢复轮询（允许就地修订）",
    )


# --------------------------------------------------------------------------------------
# 3) compensate_orphan —— 不确定场景唯一允许重发创建的入口
# --------------------------------------------------------------------------------------
async def compensate_orphan(ctx: HandlerContext) -> None:
    task = await _load(ctx.task_id)
    if task is None or is_business_terminal(task.status):
        return
    if TaskStatus(task.status) is not TaskStatus.SUBMIT_UNKNOWN:
        # 可能已被补偿收敛：若仍在活动态但没有上游 id，交回 submit 处理
        return
    rt = TaskRuntime(ctx)
    template, strategy = rt.template(task)
    caps = template.capabilities

    # 取消优先：重发创建/补 ID 前强制检查（§14 v3 收尾-6）
    if task.cancel_requested:
        if await _apply(
            "advance", task.task_id, TaskStatus.DEAD_AWAITING_CONFIRM,
            expected=[TaskStatus.SUBMIT_UNKNOWN],
            error_code=ErrorCode.CANCELLED.value,
            error_message="cancel_requested while task was in submit_unknown; closed for human review",
            next_poll_at=None,
        ):
            await rt.finalize_terminal(task, TaskStatus.DEAD_AWAITING_CONFIRM)
        return

    # 已有 upstream_task_id：任务**一定**已在上游存在 → 禁止任何创建类动作（§18.4
    # 死信重放纪律）。这里只把它送回正常轮询链路，让主源继续收敛。
    if task.upstream_task_id:
        moved = await _apply(
            "relocate_compensate", task.task_id, upstream_task_id=task.upstream_task_id,
            target=TaskStatus.UPSTREAM_SUBMITTED,
            next_poll_at=_now(), note="already created; resume polling (no re-create allowed)",
        )
        if moved:
            await ctx.enqueue(poll_queue(template.pool), "poll_upstream")
        return

    max_lifetime = int(strategy.get("unknown_max_lifetime_seconds", 3600))
    unknown_since = task.unknown_since or task.updated_at
    if (_now() - unknown_since).total_seconds() >= max_lifetime:
        if await _apply(
            "advance", task.task_id, TaskStatus.DEAD_AWAITING_CONFIRM,
            expected=[TaskStatus.SUBMIT_UNKNOWN],
            error_code=ErrorCode.UNKNOWN_EXHAUSTED.value,
            error_message="unknown lifetime exceeded; awaiting human confirmation",
            next_poll_at=None,
        ):
            await rt.finalize_terminal(task, TaskStatus.DEAD_AWAITING_CONFIRM)
        return

    # manual_only：永不自动重发创建 → 人工确认
    if not caps.confirm_auto_allowed:
        if await _apply(
            "advance", task.task_id, TaskStatus.DEAD_AWAITING_CONFIRM,
            expected=[TaskStatus.SUBMIT_UNKNOWN],
            error_code=ErrorCode.UNKNOWN_EXHAUSTED.value,
            error_message="confirm_strategy=manual_only: never auto re-create; awaiting human confirmation",
            next_poll_at=None,
        ):
            await rt.finalize_terminal(task, TaskStatus.DEAD_AWAITING_CONFIRM)
        return

    bearer = await rt.credential(task)
    # ---- 确认：按 confirm_strategy 查询 ----
    confirmed, upstream_id, detail = await _confirm(ctx, rt, task, template, bearer)
    if confirmed and upstream_id:
        await _apply(
            "relocate_compensate", task.task_id, upstream_task_id=str(upstream_id),
            target=TaskStatus.UPSTREAM_SUBMITTED,
            next_poll_at=_now() + timedelta(seconds=1.0), note=detail,
        )
        await ctx.enqueue(poll_queue(template.pool), "poll_upstream")
        return
    if confirmed and not upstream_id:
        # 确认未创建 → 重新发起创建（**唯一**合法入口）。
        # 回到 accepted 并清空提交意图，才能让下一次 submit_upstream 重新抢占 CAS。
        moved = await _apply(
            "advance", task.task_id, TaskStatus.ACCEPTED,
            expected=[TaskStatus.SUBMIT_UNKNOWN],
            submit_started_at=None, unknown_since=None, next_poll_at=_now(),
            error_code=None, error_message=None,
        )
        if moved:
            await ctx.enqueue("submit:default", "submit_upstream", recreated=True)
        return
    # 确认不了：保持 + 告警，按退避重试；超上限由巡检转 dead_awaiting_confirm
    backoff = backoff_seconds(task.confirm_attempts + 1, base=10.0, cap=300.0)
    await _apply(
        "relocate_compensate", task.task_id, upstream_task_id=None,
        target=TaskStatus.SUBMIT_UNKNOWN,
        next_poll_at=_now() + timedelta(seconds=backoff), note=detail,
    )


async def _confirm(
    ctx: HandlerContext,
    rt: TaskRuntime,
    task: AsyncTask,
    template: ResolvedTemplate,
    bearer: str | None,
) -> tuple[bool, str | None, str]:
    """按 confirm_strategy 确认"上游到底创建了没有"。

    返回 ``(是否得出结论, upstream_task_id, 说明)``。``list_and_match`` 的可匹配字段需要
    在 capabilities 里逐上游确认过，否则模板校验阶段就会把它降级掉（§12.2 收尾-13）。
    """
    caps = template.capabilities
    if caps.confirm_strategy.value == "query_by_client_key" and task.create_req_digest:
        client = ctx.container.upstream_client(bearer)
        try:
            # 约定：以 create_req_digest 作为客户端键查询（上游需支持）
            get_path = template.get_path_template.replace("{id}", task.create_req_digest)
            from ..templates.render import RenderedRequest, join_url

            request = RenderedRequest(
                method=template.get_method,
                url=join_url(template.base_url, get_path),
                headers={"Accept": "application/json"},
            )
            result = await client.call(request, context="poll", bearer=bearer)
        finally:
            await client.aclose()
        if result.ok and result.response and isinstance(result.response.json_body, dict):
            found, value = try_extract(result.response.json_body, template.id_location)
            if found:
                return True, str(value), "confirmed created via query_by_client_key"
        if result.response is not None and result.response.status_code in (404, 400):
            return True, None, "confirmed not created via query_by_client_key"
        return False, None, "confirmation inconclusive (query_by_client_key)"

    if caps.confirm_strategy.value == "list_and_match" and caps.list_tasks:
        return False, None, "list_and_match not implemented in this milestone (requires list endpoint mapping)"

    return False, None, "no usable confirmation strategy"


# --------------------------------------------------------------------------------------
# 4) cancel_upstream
# --------------------------------------------------------------------------------------
async def cancel_upstream(ctx: HandlerContext) -> None:
    task = await _load(ctx.task_id)
    if task is None or is_business_terminal(task.status):
        return
    rt = TaskRuntime(ctx)
    template, _strategy = rt.template(task)
    status = TaskStatus(task.status)

    if not template.capabilities.cancel:
        # 上游不支持取消 → 降级为 cancel_requested，以轮询终态收敛（§7 核心用例）
        await _apply("note_attribute", task.task_id, cancel_degraded=True)
        if status is TaskStatus.ACCEPTED:
            await _apply(
                "advance", task.task_id, TaskStatus.CANCELLED, expected=[TaskStatus.ACCEPTED],
                error_code=ErrorCode.CANCELLED.value, finished_at=_now(),
            )
            await rt.finalize_terminal(task, TaskStatus.CANCELLED)
            return
        await ctx.enqueue_self_later(5.0)
        return

    if not task.upstream_task_id:
        if await _apply(
            "advance", task.task_id, TaskStatus.CANCELLED,
            expected=[TaskStatus.ACCEPTED], error_code=ErrorCode.CANCELLED.value,
            finished_at=_now(),
        ):
            await rt.finalize_terminal(task, TaskStatus.CANCELLED)
        return

    bearer = await rt.credential(task)
    client = ctx.container.upstream_client(bearer)
    try:
        request = render_cancel(template, task.upstream_task_id)
        result = await client.call(request, context="cancel", bearer=bearer)
    finally:
        await client.aclose()

    if result.ok or (result.response is not None and result.response.status_code in (404, 409)):
        # 404/409：上游已无此任务或状态冲突——从"取消意图"看都算达成
        if await _apply(
            "advance", task.task_id, TaskStatus.CANCELLED, expected=[status],
            raw_status="cancelled", error_code=ErrorCode.CANCELLED.value,
            finished_at=_now(), next_poll_at=None,
        ):
            await rt.finalize_terminal(task, TaskStatus.CANCELLED)
        return

    await _apply("note_attribute", task.task_id, cancel_degraded=True)
    await ctx.enqueue_self_later(backoff_seconds(task.poll_count + 1, base=5.0))


# --------------------------------------------------------------------------------------
# 5) finalize_task
# --------------------------------------------------------------------------------------
async def finalize_task(ctx: HandlerContext) -> None:
    """终态收尾（幂等）：转存入队、凭证清理、观测采样。"""
    task = await _load(ctx.task_id)
    if task is None:
        return
    if not is_business_terminal(task.status):
        return
    rt = TaskRuntime(ctx)
    template, _ = rt.template(task)
    if TaskStatus(task.status) is TaskStatus.SUCCEEDED and template.result_policy.mode is ResultMode.STORE:
        if task.result_ref is None:
            await ctx.enqueue(TRANSFER_QUEUE, "store_result", attempt=1)
    await rt.drop_credential(task)


# --------------------------------------------------------------------------------------
# 6) store_result —— 独立队列 / 独立 Deployment
# --------------------------------------------------------------------------------------
async def store_result(ctx: HandlerContext) -> None:
    task = await _load(ctx.task_id)
    if task is None or TaskStatus(task.status) is not TaskStatus.SUCCEEDED:
        return
    if task.result_ref is not None:
        return  # 已有结果，重复投递无副作用
    rt = TaskRuntime(ctx)
    template, _ = rt.template(task)
    if template.result_policy.mode is not ResultMode.STORE:
        return

    attempt = int(ctx.payload.get("attempt", 1))
    body = task.status_snapshot
    found, upstream_url = try_extract(body or {}, template.result_location)
    if not found or not isinstance(upstream_url, str):
        await _apply(
            "mark_result_unavailable", task.task_id,
            detail=f"result url not extractable at {template.result_location}",
        )
        await _audit(
            AuditEventType.GOVERNANCE, subject_type="task", subject_id=task.task_id,
            refs={"event": "transfer_failed", "reason": "url_not_extractable"},
            detail="转存永久失败：保持 succeeded + 结果不可用（不伪造失败骗退款）",
        )
        return

    bearer = await rt.credential(task)
    client = ctx.container.upstream_client(bearer)
    try:
        ok, payload = await client.fetch_result(upstream_url, client=None)
    finally:
        await client.aclose()

    if not ok:
        detail = str(payload)
        # 确定性失败不重试：SSRF 拒绝、结果大小超限（只有改配置才可能变）
        retryable = not detail.startswith(("ssrf_rejected", "result too large"))
        if retryable and attempt < MAX_TRANSFER_ATTEMPTS:
            delay = backoff_seconds(attempt, base=5.0, cap=120.0)
            await _apply(
                "schedule_transfer_retry", task.task_id,
                next_poll_at=_now() + timedelta(seconds=delay), attempts=attempt,
            )
            return
        await _apply("mark_result_unavailable", task.task_id, detail=detail)
        await _audit(
            AuditEventType.GOVERNANCE, subject_type="task", subject_id=task.task_id,
            refs={"event": "transfer_failed", "attempt": attempt, "reason": detail[:200]},
            detail="转存永久失败：保持 succeeded + 结果不可用；支持重放转存",
        )
        return

    key = result_key(
        task.tenant, task.task_id, created_at=task.created_at, attempt=1, ext="bin"
    )
    await ctx.result_store.put_bytes(key, payload, "application/octet-stream")  # type: ignore[arg-type]
    ok = await _apply(
        "set_result_ref", task.task_id,
        result_ref=key, summary={"bytes": len(payload)}, degraded=None,  # type: ignore[arg-type]
    )
    if ok:
        await _audit(
            AuditEventType.RESULT_EXPORT, subject_type="task", subject_id=task.task_id,
            refs={"result_ref": key, "bytes": len(payload)},  # type: ignore[arg-type]
            detail="结果已转存对象存储，回引用对外",
        )


HANDLERS: dict[str, Callable[[HandlerContext], Awaitable[None]]] = {
    "submit_upstream": submit_upstream,
    "poll_upstream": poll_upstream,
    "finalize_task": finalize_task,
    "cancel_upstream": cancel_upstream,
    "compensate_orphan": compensate_orphan,
    "store_result": store_result,
}


def build_context(payload: dict[str, Any], bus: Bus, container: Container | None = None) -> HandlerContext:
    return HandlerContext(payload=payload, bus=bus, container=container or get_container())


async def dispatch_message(payload: dict[str, Any], name: str, bus: Bus, container: Container | None = None) -> None:
    handler = HANDLERS.get(name)
    if handler is None:
        raise KeyError(f"unknown task name: {name}")
    await handler(build_context(payload, bus, container))


__all__ = [
    "HANDLERS",
    "HandlerContext",
    "TaskRuntime",
    "build_context",
    "compensate_orphan",
    "cancel_upstream",
    "dispatch_message",
    "finalize_task",
    "poll_upstream",
    "store_result",
    "submit_upstream",
    "BUSINESS_TERMINAL",
    "Origin",
    "UpstreamClient",
    "get_settings",
    "request_key",
    "time",
]
