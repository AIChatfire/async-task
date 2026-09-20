"""条件更新 DAO（§13「所有写路径带前置条件」）。

这里把文档的写路径表逐行实现成 **CAS**（compare-and-swap）语句：任何写操作都必须
声明"期望状态"，条件不满足则返回 ``False``，调用方据此判定"这一枪没打中"。
好处是多个 worker / inspector / 回调同时动手时，**不会**出现两个赢家——这正
保证「终态迁移只允许一次」与「单调不翻转」。

设计细节：

* :func:`_cas` 统一加 ``row_version + 1``，行版本号可用于排查并发；
* ``mark_submit_intent`` 额外要求 ``submit_started_at IS NULL``，即 CAS 抢占，
  防多 worker 并发提交同一任务；
* ``mark_submitted`` 额外要求 ``submit_started_at IS NOT NULL``——意图先行，
  杜绝"没打意图就落提交成功"的时序漏洞。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Iterable, Sequence

from sqlalchemy import Select, and_, func, insert, or_, select, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from ..domain.enums import INTERNAL_TERMINAL, ErrorCode, TaskStatus
from ..domain.state_machine import allowed_targets, assert_transition
from .models import (
    AsyncTask,
    CallbackEvent,
    ChannelPolicyRow,
    OrphanCallback,
    UpstreamTemplateRow,
    utcnow,
)

#: 非终态集合（scheduler 只重排/派发这些）
ACTIVE_STATUSES: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.ACCEPTED,
        TaskStatus.UPSTREAM_SUBMITTED,
        TaskStatus.IN_PROGRESS,
        TaskStatus.SUBMIT_UNKNOWN,
        TaskStatus.POLL_UNRECOGNIZED,
        TaskStatus.FAILED,
    }
)

#: 状态 → 该派发什么任务（scheduler 用；accepted 不在其中，由 inspector 巡检重投）
DISPATCH_BY_STATUS: dict[TaskStatus, str] = {
    TaskStatus.UPSTREAM_SUBMITTED: "poll_upstream",
    TaskStatus.IN_PROGRESS: "poll_upstream",
    TaskStatus.POLL_UNRECOGNIZED: "poll_upstream",
    TaskStatus.SUBMIT_UNKNOWN: "compensate_orphan",
    TaskStatus.FAILED: "submit_upstream",  # 失败重试 = 新业务尝试，经 submit_upstream 重建
}

#: 写路径名 → 允许的前置状态（与 state_machine.WRITE_PATHS 保持一致）
CANCEL_PRECONDITIONS: frozenset[TaskStatus] = frozenset(
    {TaskStatus.ACCEPTED, TaskStatus.UPSTREAM_SUBMITTED, TaskStatus.IN_PROGRESS, TaskStatus.SUBMIT_UNKNOWN}
)


def now_utc() -> datetime:
    return datetime.now(UTC)


def _vals(states: Iterable[TaskStatus | str]) -> list[str]:
    return [s.value if isinstance(s, TaskStatus) else str(s) for s in states]


@dataclass(slots=True)
class ConcurrencyRow:
    channel: str
    tenant: str
    active: int


class TaskDAO:
    """任务相关的全部条件更新入口。"""

    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    # ---------------- 基础 ----------------
    async def get(self, task_id: str) -> AsyncTask | None:
        return await self.s.get(AsyncTask, task_id)

    async def find_by_idempotency(self, key: str, bucket: int) -> AsyncTask | None:
        stmt = select(AsyncTask).where(
            AsyncTask.idempotency_key == key, AsyncTask.idempotency_bucket == bucket
        )
        return (await self.s.execute(stmt)).scalar_one_or_none()

    async def find_by_upstream_id(self, channel: str, upstream_task_id: str) -> AsyncTask | None:
        stmt = select(AsyncTask).where(
            AsyncTask.channel == channel, AsyncTask.upstream_task_id == upstream_task_id
        )
        return (await self.s.execute(stmt)).scalars().first()

    async def find_by_callback_token(self, token: str) -> AsyncTask | None:
        stmt = select(AsyncTask).where(AsyncTask.callback_token == token)
        return (await self.s.execute(stmt)).scalar_one_or_none()

    async def insert(self, task: AsyncTask) -> AsyncTask:
        self.s.add(task)
        await self.s.flush()
        return task

    async def _cas(
        self,
        task_id: str,
        expected: Iterable[TaskStatus | str],
        values: dict[str, Any],
        *extra_where: Any,
    ) -> bool:
        expected_vals = _vals(expected)
        if not expected_vals:
            return False
        stmt = (
            update(AsyncTask)
            .where(
                AsyncTask.task_id == task_id,
                AsyncTask.status.in_(expected_vals),
                *extra_where,
            )
            .values(row_version=AsyncTask.row_version + 1, updated_at=now_utc(), **values)
        )
        result = await self.s.execute(stmt)
        return bool(result.rowcount)

    # ---------------- 写路径（逐条对应 §13 表）----------------
    async def mark_submit_intent(self, task_id: str, *, started_at: datetime | None = None) -> bool:
        """提交意图：status=accepted 且 submit_started_at 为空（CAS 抢占，防多 worker 并发提交）。

        同时 attempts 预递增——口径 = 提交尝试次数，每次 submit_upstream 执行前 +1。
        """
        return await self._cas(
            task_id,
            (TaskStatus.ACCEPTED,),
            {
                "submit_started_at": started_at or now_utc(),
                "attempts": AsyncTask.attempts + 1,
            },
            AsyncTask.submit_started_at.is_(None),
        )

    async def mark_submitted(
        self,
        task_id: str,
        *,
        upstream_task_id: str,
        raw_status: str | None,
        snapshot: dict | None,
        next_poll_at: datetime,
        started_at: datetime | None = None,
    ) -> bool:
        """提交成功落库：status=accepted 且意图已置（意图先行）。

        若此时发现 ``cancel_requested`` 已置位，**仍然**先落库（我们需要 upstream_task_id
        才能取消），调用方紧接着触发 cancel 流程（禁止"用户已取消、上游仍跑"）。
        """
        return await self._cas(
            task_id,
            (TaskStatus.ACCEPTED,),
            {
                "status": TaskStatus.UPSTREAM_SUBMITTED.value,
                "upstream_task_id": upstream_task_id,
                "raw_status": raw_status,
                "status_snapshot": snapshot,
                "status_snapshot_at": now_utc(),
                "started_at": started_at or now_utc(),
                "next_poll_at": next_poll_at,
                "error_code": None,
                "error_message": None,
            },
            AsyncTask.submit_started_at.is_not(None),
        )

    async def advance(
        self,
        task_id: str,
        dst: TaskStatus | str,
        *,
        expected: Iterable[TaskStatus | str] | None = None,
        **values: Any,
    ) -> bool:
        """白名单迁移 + 期望状态匹配。

        ``expected`` 缺省时取状态机里所有"能合法走到 dst"的源状态——这就是
        「白名单迁移」与「条件更新」的合体。

        ``expected`` 显式给出时**仍要逐个校验白名单**：否则"条件更新"会变成绕过白名单的
        后门（只要状态对得上就能跳任何一步）。校验失败直接抛，属编程错误，不静默吞掉。
        """
        dst_status = TaskStatus(dst)
        if expected is None:
            sources: list[TaskStatus | str] = [
                s for s in TaskStatus if dst_status in allowed_targets(s)
            ]
        else:
            sources = list(expected)
            for src in sources:
                assert_transition(src, dst_status)
        payload = dict(values)
        payload["status"] = dst_status.value
        if dst_status in INTERNAL_TERMINAL or dst_status is TaskStatus.SUCCEEDED:
            payload.setdefault("finished_at", now_utc())
        if dst_status is TaskStatus.CANCELLED:
            payload.setdefault("finished_at", now_utc())
        if dst_status is not TaskStatus.SUCCEEDED:
            # 离开成功态不可能，但离开活动态后不再重排轮询
            if dst_status in INTERNAL_TERMINAL or dst_status is TaskStatus.CANCELLED:
                payload.setdefault("next_poll_at", None)
        return await self._cas(task_id, sources, payload)

    async def set_cancel_requested(self, task_id: str) -> bool:
        """status ∈ {accepted, upstream_submitted, in_progress, submit_unknown}。

        submit_unknown 仅置标记不直接迁移，由 compensate 检查后收敛（§14 v3 收尾-6）。
        """
        return await self._cas(
            task_id,
            CANCEL_PRECONDITIONS,
            {"cancel_requested": True, "cancel_requested_at": now_utc()},
        )

    async def cancel_immediately_after_submit(self, task_id: str) -> bool:
        """submit 落库后发现 cancel_requested → 立即触发上游取消。"""
        return await self._cas(
            task_id,
            (TaskStatus.UPSTREAM_SUBMITTED, TaskStatus.IN_PROGRESS),
            {"status": TaskStatus.CANCELLED.value, "finished_at": now_utc(), "next_poll_at": None},
            AsyncTask.cancel_requested.is_(True),
        )

    async def set_result_ref(
        self,
        task_id: str,
        *,
        result_ref: str,
        summary: dict | None = None,
        degraded: list | None = None,
    ) -> bool:
        """store_result：status=succeeded 且 result_ref 为空（防重复转存双写）。"""
        return await self._cas(
            task_id,
            (TaskStatus.SUCCEEDED,),
            {
                "result_ref": result_ref,
                "result_summary": summary,
                "result_degraded": degraded,
            },
            AsyncTask.result_ref.is_(None),
        )

    async def schedule_next_poll(self, task_id: str, next_poll_at: datetime) -> bool:
        """scheduler 写 next_poll_at：仅非终态可重排。"""
        return await self._cas(
            task_id,
            ACTIVE_STATUSES,
            {"next_poll_at": next_poll_at},
        )

    async def record_poll_result(
        self,
        task_id: str,
        *,
        snapshot: dict | None,
        raw_status: str | None,
        next_poll_at: datetime | None,
        expected: Iterable[TaskStatus | str],
    ) -> bool:
        """轮询回写：快照 + 轮询计数（不改状态；状态迁移走 :meth:`advance`）。"""
        values: dict[str, Any] = {
            "status_snapshot": snapshot,
            "status_snapshot_at": now_utc(),
            "raw_status": raw_status,
            "poll_count": AsyncTask.poll_count + 1,
            "last_polled_at": now_utc(),
        }
        if next_poll_at is not None:
            values["next_poll_at"] = next_poll_at
        return await self._cas(task_id, expected, values)

    async def mark_failure(
        self,
        task_id: str,
        *,
        expected: Iterable[TaskStatus | str],
        error_code: str,
        error_message: str | None,
        retryable: bool,
        next_poll_at: datetime | None,
        snapshot: dict | None = None,
    ) -> bool:
        """失败回写。**不改状态**——状态迁移由调用方按 §14 决定：
        可重试 → ``failed``（待重试），耗尽/不可重试 → ``dead``。
        """
        values: dict[str, Any] = {
            "error_code": error_code,
            "error_message": error_message,
            "retryable": retryable,
            "next_poll_at": next_poll_at,
        }
        if snapshot is not None:
            values["status_snapshot"] = snapshot
            values["status_snapshot_at"] = now_utc()
        return await self._cas(task_id, expected, values)

    async def enter_unknown(
        self,
        task_id: str,
        *,
        error_code: str,
        error_message: str | None,
        expected: Iterable[TaskStatus | str] | None = None,
    ) -> bool:
        """进入 ``submit_unknown``（响应超时/丢失/5xx）。"""
        return await self.advance(
            task_id,
            TaskStatus.SUBMIT_UNKNOWN,
            expected=expected or [TaskStatus.ACCEPTED],
            error_code=error_code,
            error_message=error_message,
            unknown_since=now_utc(),
            next_poll_at=now_utc(),
        )

    async def relocate_compensate(
        self,
        task_id: str,
        *,
        upstream_task_id: str | None,
        target: TaskStatus | str,
        raw_status: str | None = None,
        snapshot: dict | None = None,
        next_poll_at: datetime | None = None,
        note: str | None = None,
    ) -> bool:
        """compensate 归位：status=submit_unknown（唯一入口，不允许别处重发创建）。"""
        values: dict[str, Any] = {
            "last_confirmed_at": now_utc(),
            "confirm_attempts": AsyncTask.confirm_attempts + 1,
        }
        if upstream_task_id:
            values["upstream_task_id"] = upstream_task_id
        if raw_status is not None:
            values["raw_status"] = raw_status
        if snapshot is not None:
            values["status_snapshot"] = snapshot
            values["status_snapshot_at"] = now_utc()
        if next_poll_at is not None:
            values["next_poll_at"] = next_poll_at
        if note:
            values["attributes"] = {"compensate_note": note}
        if TaskStatus(target) is TaskStatus.UPSTREAM_SUBMITTED:
            values["unknown_since"] = None
        return await self.advance(
            task_id,
            target,
            expected=[TaskStatus.SUBMIT_UNKNOWN],
            **values,
        )

    # ---------------- 查询（scheduler / inspector）----------------
    async def due_for_dispatch(self, *, limit: int = 500, now: datetime | None = None) -> Sequence[AsyncTask]:
        now = now or now_utc()
        stmt: Select = (
            select(AsyncTask)
            .where(
                AsyncTask.status.in_([s.value for s in DISPATCH_BY_STATUS]),
                AsyncTask.next_poll_at.is_not(None),
                AsyncTask.next_poll_at <= now,
            )
            .order_by(AsyncTask.next_poll_at.asc())
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()

    async def stalled_accepted(
        self,
        *,
        stall_seconds: int,
        limit: int = 200,
        now: datetime | None = None,
    ) -> Sequence[AsyncTask]:
        """accepted 悬挂：超时未推进；调用方按 ``submit_started_at`` 分流。"""
        now = now or now_utc()
        cutoff = now - timedelta(seconds=stall_seconds)
        stmt = (
            select(AsyncTask)
            .where(
                AsyncTask.status == TaskStatus.ACCEPTED.value,
                AsyncTask.updated_at <= cutoff,
            )
            .order_by(AsyncTask.created_at.asc())
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()

    async def unknown_tasks(
        self, *, limit: int = 200, now: datetime | None = None
    ) -> Sequence[AsyncTask]:
        now = now or now_utc()
        stmt = (
            select(AsyncTask)
            .where(
                AsyncTask.status.in_(
                    [TaskStatus.SUBMIT_UNKNOWN.value, TaskStatus.POLL_UNRECOGNIZED.value]
                ),
                AsyncTask.next_poll_at.is_not(None),
                AsyncTask.next_poll_at <= now,
            )
            .order_by(AsyncTask.next_poll_at.asc())
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()

    async def count_unknown_by_channel(self) -> dict[str, int]:
        stmt = (
            select(AsyncTask.channel, func.count())
            .where(
                AsyncTask.status.in_(
                    [TaskStatus.SUBMIT_UNKNOWN.value, TaskStatus.POLL_UNRECOGNIZED.value]
                )
            )
            .group_by(AsyncTask.channel)
        )
        return {row[0]: int(row[1]) for row in (await self.s.execute(stmt)).all()}

    async def over_lifetime_unknown(
        self, *, max_lifetime_seconds: int, limit: int = 200, now: datetime | None = None
    ) -> Sequence[AsyncTask]:
        now = now or now_utc()
        cutoff = now - timedelta(seconds=max_lifetime_seconds)
        stmt = (
            select(AsyncTask)
            .where(
                AsyncTask.status.in_(
                    [TaskStatus.SUBMIT_UNKNOWN.value, TaskStatus.POLL_UNRECOGNIZED.value]
                ),
                AsyncTask.unknown_since.is_not(None),
                AsyncTask.unknown_since <= cutoff,
            )
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()

    async def overdue_deadlines(self, *, limit: int = 200, now: datetime | None = None) -> Sequence[AsyncTask]:
        now = now or now_utc()
        stmt = (
            select(AsyncTask)
            .where(
                AsyncTask.status.in_([s.value for s in ACTIVE_STATUSES]),
                AsyncTask.deadline_at.is_not(None),
                AsyncTask.deadline_at <= now,
            )
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()

    async def active_counts(self) -> list[ConcurrencyRow]:
        """PG 实数（用于 30s 校准 Redis 计数，只纠泄漏方向）。"""
        stmt = (
            select(AsyncTask.channel, AsyncTask.tenant, func.count())
            .where(AsyncTask.status.in_([s.value for s in ACTIVE_STATUSES]))
            .group_by(AsyncTask.channel, AsyncTask.tenant)
        )
        return [
            ConcurrencyRow(channel=row[0], tenant=row[1], active=int(row[2]))
            for row in (await self.s.execute(stmt)).all()
        ]

    async def active_counts_by_channel(self) -> dict[str, int]:
        stmt = (
            select(AsyncTask.channel, func.count())
            .where(AsyncTask.status.in_([s.value for s in ACTIVE_STATUSES]))
            .group_by(AsyncTask.channel)
        )
        return {row[0]: int(row[1]) for row in (await self.s.execute(stmt)).all()}

    async def status_histogram(self, *, since: datetime | None = None) -> dict[str, int]:
        stmt = select(AsyncTask.status, func.count()).group_by(AsyncTask.status)
        rows = (await self.s.execute(stmt)).all()
        return {row[0]: int(row[1]) for row in rows}

    # ---------------- 回调 ----------------
    async def insert_callback_event(
        self,
        *,
        channel: str,
        dedup_key: str,
        upstream_task_id: str | None,
        raw_status: str | None,
        kid: str | None,
        payload: dict | None,
    ) -> bool:
        """返回 False 表示窗口内重复投递（命中即直接 200，不进状态机）。"""
        try:
            async with self.s.begin_nested():
                await self.s.execute(
                    insert(CallbackEvent).values(
                        channel=channel,
                        dedup_key=dedup_key,
                        upstream_task_id=upstream_task_id,
                        raw_status=raw_status,
                        kid=kid,
                        payload=payload,
                        received_at=now_utc(),
                    )
                )
            return True
        except IntegrityError:
            return False

    async def stash_orphan(
        self,
        *,
        channel: str,
        upstream_task_id: str,
        raw_status: str | None,
        payload: dict | None,
    ) -> OrphanCallback:
        row = OrphanCallback(
            channel=channel,
            upstream_task_id=upstream_task_id,
            raw_status=raw_status,
            payload=payload,
        )
        self.s.add(row)
        await self.s.flush()
        return row

    async def unresolved_orphans(self, *, limit: int = 200) -> Sequence[OrphanCallback]:
        stmt = (
            select(OrphanCallback)
            .where(OrphanCallback.resolved.is_(False))
            .order_by(OrphanCallback.received_at.asc())
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()

    # ---------------- 渠道策略 / 模板 ----------------
    async def get_channel_policy(self, channel: str) -> ChannelPolicyRow | None:
        return await self.s.get(ChannelPolicyRow, channel)

    async def list_templates(
        self, *, alias: str | None = None, namespace: str | None = None
    ) -> Sequence[UpstreamTemplateRow]:
        stmt = select(UpstreamTemplateRow)
        conds = []
        if alias:
            conds.append(UpstreamTemplateRow.alias == alias)
        if namespace:
            conds.append(UpstreamTemplateRow.namespace == namespace)
        if conds:
            stmt = stmt.where(and_(*conds))
        stmt = stmt.order_by(UpstreamTemplateRow.alias, UpstreamTemplateRow.version)
        return (await self.s.execute(stmt)).scalars().all()

    async def upsert_template_version(
        self,
        *,
        alias: str,
        namespace: str,
        version: int,
        config: dict,
        config_hash: str,
        patch_ids: list[str],
        enabled: bool = True,
        canary_weight: float = 1.0,
        created_by: str | None = None,
        approved_by: str | None = None,
    ) -> UpstreamTemplateRow:
        stmt = select(UpstreamTemplateRow).where(
            UpstreamTemplateRow.namespace == namespace,
            UpstreamTemplateRow.alias == alias,
            UpstreamTemplateRow.version == version,
        )
        row = (await self.s.execute(stmt)).scalar_one_or_none()
        if row is None:
            row = UpstreamTemplateRow(
                alias=alias,
                namespace=namespace,
                version=version,
                config=config,
                config_hash=config_hash,
                patch_ids=patch_ids,
                enabled=enabled,
                canary_weight=canary_weight,
                created_by=created_by,
                approved_by=approved_by,
            )
            self.s.add(row)
        else:
            row.config = config
            row.config_hash = config_hash
            row.patch_ids = patch_ids
            row.enabled = enabled
            row.canary_weight = canary_weight
            row.approved_by = approved_by or row.approved_by
        await self.s.flush()
        return row

    async def patch_template_in_place(
        self, *, alias: str, namespace: str, version: int, config: dict, config_hash: str
    ) -> bool:
        """就地勘误（仅判定逻辑）：既有版本原地更新，单列审计。"""
        stmt = (
            update(UpstreamTemplateRow)
            .where(
                UpstreamTemplateRow.alias == alias,
                UpstreamTemplateRow.namespace == namespace,
                UpstreamTemplateRow.version == version,
            )
            .values(
                config=config,
                config_hash=config_hash,
                in_place_patches=UpstreamTemplateRow.in_place_patches + 1,
                updated_at=now_utc(),
            )
        )
        return bool((await self.s.execute(stmt)).rowcount)

    # ---------------- 派发租约（scheduler 用）----------------
    async def lease_dispatch(
        self,
        task_id: str,
        *,
        lease_seconds: float,
        expected: Iterable[TaskStatus | str],
        now: datetime | None = None,
    ) -> bool:
        """派发后把 ``next_poll_at`` 向前推一个租约，避免同一任务被反复派发。

        只在 ``next_poll_at`` 仍到期时生效——handler 若已把时间轴推到更远，
        这里自然 CAS 失败（**不会覆盖 handler 的最新调度**）。
        """
        now = now or now_utc()
        return await self._cas(
            task_id,
            expected,
            {"next_poll_at": now + timedelta(seconds=max(1.0, lease_seconds))},
            AsyncTask.next_poll_at.is_not(None),
            AsyncTask.next_poll_at <= now,
        )

    # ---------------- 提交意图的释放/重试 ----------------
    async def release_submit_intent(
        self,
        task_id: str,
        *,
        expected: Iterable[TaskStatus | str],
        next_poll_at: datetime,
        decrement_attempts: bool = False,
        error_code: str | None = None,
        error_message: str | None = None,
        retryable: bool | None = None,
        attributes: dict | None = None,
    ) -> bool:
        """释放本次提交意图（``submit_started_at`` 置空），让下一次 submit_upstream 能重新抢占。

        为什么必须释放：意图是**每次尝试**的一次性抢占标记；不释放的话重试会永远拿不到
        CAS，任务卡死在 accepted。

        ``decrement_attempts`` 用在"这一枪不算数"的场景——429 与 401/403 按 §4.1
        **不消耗业务 attempts**；而连接被拒/DNS 失败这类"请求确实发过（只是没发出去）"
        仍算一次提交尝试，不递减。
        """
        values: dict[str, Any] = {
            "submit_started_at": None,
            "next_poll_at": next_poll_at,
            "error_code": error_code,
            "error_message": error_message,
            "retryable": retryable,
        }
        if decrement_attempts:
            values["attempts"] = func.max(AsyncTask.attempts - 1, 0)
        if attributes:
            values["attributes"] = attributes
        return await self._cas(task_id, expected, values)

    async def note_attribute(self, task_id: str, **attributes: Any) -> bool:
        """合并式写入 attributes（用于 poll_404_count / cancel_degraded 这类标记）。"""
        task = await self.get(task_id)
        if task is None:
            return False
        merged = dict(task.attributes or {})
        merged.update(attributes)
        return await self._cas(task_id, ACTIVE_STATUSES, {"attributes": merged})

    # ---------------- 转存重试（成功态专用通道）----------------
    async def schedule_transfer_retry(
        self, task_id: str, *, next_poll_at: datetime, attempts: int
    ) -> bool:
        """成功但未转存完的任务：用 ``next_poll_at`` 承载转存重试时间（成功态没有轮询）。"""
        return await self._cas(
            task_id,
            (TaskStatus.SUCCEEDED,),
            {
                "next_poll_at": next_poll_at,
                "attributes": {"transfer_attempts": attempts},
            },
            AsyncTask.result_ref.is_(None),
        )

    async def pending_transfers(
        self, *, limit: int = 100, now: datetime | None = None
    ) -> Sequence[AsyncTask]:
        now = now or now_utc()
        stmt = (
            select(AsyncTask)
            .where(
                AsyncTask.status == TaskStatus.SUCCEEDED.value,
                AsyncTask.result_ref.is_(None),
                AsyncTask.next_poll_at.is_not(None),
                AsyncTask.next_poll_at <= now,
            )
            .order_by(AsyncTask.next_poll_at.asc())
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()

    async def mark_result_unavailable(self, task_id: str, *, detail: str) -> bool:
        """转存永久失败：保持 status=succeeded（不伪造失败骗退款），只标注结果不可用。"""
        return await self._cas(
            task_id,
            (TaskStatus.SUCCEEDED,),
            {
                "result_degraded": ["transfer_failed"],
                "error_code": ErrorCode.TRANSFER_FAILED.value,
                "error_message": detail[:500],
                "next_poll_at": None,
            },
            AsyncTask.result_ref.is_(None),
        )

    # ---------------- 孤儿/停滞发现 ----------------
    async def stalled_active(
        self,
        *,
        stall_seconds: int,
        limit: int = 200,
        now: datetime | None = None,
        statuses: Iterable[TaskStatus | str] = (
            TaskStatus.UPSTREAM_SUBMITTED,
            TaskStatus.IN_PROGRESS,
        ),
    ) -> Sequence[AsyncTask]:
        """活动态停滞（心跳缺失）：用于发现"上游已创建但网关侧失联"的疑似孤儿。"""
        now = now or now_utc()
        cutoff = now - timedelta(seconds=stall_seconds)
        stmt = (
            select(AsyncTask)
            .where(
                AsyncTask.status.in_(_vals(statuses)),
                or_(
                    AsyncTask.next_poll_at.is_(None),
                    AsyncTask.next_poll_at <= cutoff,
                ),
                AsyncTask.updated_at <= cutoff,
            )
            .limit(limit)
        )
        return (await self.s.execute(stmt)).scalars().all()
