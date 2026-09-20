"""inspector 进程（§14 巡检面 / §19 与 scheduler 故障域隔离）。

巡检面五件事，**独立 Deployment、单副本**——调度器故障不阻塞收敛，巡检故障不影响派发：

1. **accepted 悬挂**：按"提交意图"分流（无意图 → 条件更新重投；有意图 → 转 submit_unknown
   走 compensate，**禁止未经确认重投**）；
2. **deadline 巡检**：过期 → ``timeout``（内部终态，对外按模板重写成原生失败类取值）；
3. **unknown 有界化**：单渠道上限 + 最大存活时间 → ``dead_awaiting_confirm``，
   覆盖 submit_unknown 与 poll_unrecognized 两态，模板勘误不至也不会无限悬挂；
4. **孤儿对账**：orphan_callback 表 + 疑似孤儿（活动态停滞）转 submit_unknown 让 compensate 归位；
5. **转存重试**：成功但未转存完的任务按其 ``next_poll_at`` 重新入队。

外加**并发计数校准**：用 PG 实数纠 Redis 计数的泄漏方向（只纠"Redis 偏大"，绝不上调）。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time
from datetime import datetime, timedelta, timezone

from ..bus.factory import make_bus
from ..config import get_settings
from ..db.base import dispose_engine, session_scope
from ..db.dao import TaskDAO
from ..domain.enums import ErrorCode, TaskStatus
from ..infra.redis import close_redis
from ..observability.logging import configure_logging
from ..observability.metrics import ACTIVE_TASKS, REGISTRY, TASK_TERMINAL_TOTAL, UNKNOWN_GAUGE
from ..gateway.container import get_container
from ..tasks.queues import TRANSFER_QUEUE, poll_queue

logger = logging.getLogger(__name__)

INSPECTOR_HEARTBEAT = REGISTRY.gauge("ag_inspector_last_tick_timestamp", "inspector 最近一次成功 tick")
INSPECTOR_ACTIONS = REGISTRY.counter("ag_inspector_actions_total", "巡检动作分布")


def _now() -> datetime:
    return datetime.now(timezone.utc)


async def scan_accepted_stalls(bus, *, stall_seconds: int) -> int:
    """accepted 悬挂分流（§14 / M3 故障注入清单第 7 项）。"""
    actions = 0
    async with session_scope() as s:
        dao = TaskDAO(s)
        rows = list(await dao.stalled_accepted(stall_seconds=stall_seconds))
        for task in rows:
            if task.submit_started_at is None:
                # 无提交意图 = 入队失败/消息丢失，上游必未创建 → 重投（安全）
                INSPECTOR_ACTIONS.inc({"action": "accepted_requeue"})
                await bus.enqueue("submit:default", "submit_upstream", {"task_id": task.task_id})
                actions += 1
            else:
                # 有提交意图 = 已调上游未落库（worker 崩溃窗口）→ 转 unknown 走补偿，禁止盲建
                moved = await dao.advance(
                    task.task_id,
                    TaskStatus.SUBMIT_UNKNOWN,
                    expected=[TaskStatus.ACCEPTED],
                    error_code=ErrorCode.TRANSPORT.value,
                    error_message="accepted stalled with submit intent (worker crash window)",
                    unknown_since=_now(),
                    next_poll_at=_now(),
                )
                if moved:
                    INSPECTOR_ACTIONS.inc({"action": "accepted_to_unknown"})
                    await bus.enqueue("compensate:default", "compensate_orphan", {"task_id": task.task_id})
                    actions += 1
    return actions


async def scan_deadlines() -> int:
    actions = 0
    async with session_scope() as s:
        dao = TaskDAO(s)
        rows = list(await dao.overdue_deadlines())
        for task in rows:
            moved = await dao.advance(
                task.task_id,
                TaskStatus.TIMEOUT,
                expected=[task.status],
                error_code=ErrorCode.TIMEOUT.value,
                error_message=f"deadline exceeded at {task.deadline_at}",
                finished_at=_now(),
                next_poll_at=None,
            )
            if moved:
                INSPECTOR_ACTIONS.inc({"action": "deadline_timeout"})
                TASK_TERMINAL_TOTAL.inc({"status": "timeout", "channel": task.channel})
                actions += 1
    return actions


async def bound_unknowns(bus, *, max_per_channel: int, max_lifetime_seconds: int) -> int:
    actions = 0
    async with session_scope() as s:
        dao = TaskDAO(s)
        counts = await dao.count_unknown_by_channel()
        over_lifetime = list(await dao.over_lifetime_unknown(max_lifetime_seconds=max_lifetime_seconds))
        victims: list[str] = []
        for task in over_lifetime:
            victims.append(task.task_id)
        # 单渠道上限：超限的按最老优先转人工确认
        for channel, count in counts.items():
            if count <= max_per_channel:
                continue
            rows = list(await dao.unknown_tasks(limit=count))
            for task in rows:
                if task.channel != channel:
                    continue
                victims.append(task.task_id)
                if len(victims) >= count - max_per_channel + len(over_lifetime):
                    break
        for task_id in dict.fromkeys(victims):
            task = await dao.get(task_id)
            if task is None:
                continue
            moved = await dao.advance(
                task_id,
                TaskStatus.DEAD_AWAITING_CONFIRM,
                expected=[task.status],
                error_code=ErrorCode.UNKNOWN_EXHAUSTED.value,
                error_message="unknown bounded: exceeded channel limit or max lifetime",
                next_poll_at=None,
            )
            if moved:
                INSPECTOR_ACTIONS.inc({"action": "unknown_to_dead_awaiting_confirm"})
                UNKNOWN_GAUGE.add(-1, {"channel": task.channel, "status": task.status})
                actions += 1
    return actions


async def reconcile_orphans(bus, *, stall_seconds: int = 180) -> int:
    """orphan_callback 与疑似孤儿归位（§14）。"""
    actions = 0
    async with session_scope() as s:
        dao = TaskDAO(s)
        orphans = list(await dao.unresolved_orphans())
        for orphan in orphans:
            task = await dao.find_by_upstream_id(orphan.channel, orphan.upstream_task_id)
            if task is not None:
                orphan.resolved = True
                orphan.resolved_task_id = task.task_id
                orphan.resolved_at = _now()
                INSPECTOR_ACTIONS.inc({"action": "orphan_matched"})
                actions += 1
                continue
            # 回调整体在网关侧查无此行 → 记录并留给后续对账（list_tasks 能力缺失时的兜底）
            INSPECTOR_ACTIONS.inc({"action": "orphan_unmatched"})
        # 活动态停滞：上游可能已创建但网关侧失联 → 转 unknown 由 compensate 归位
        for task in await dao.stalled_active(stall_seconds=stall_seconds):
            moved = await dao.advance(
                task.task_id,
                TaskStatus.SUBMIT_UNKNOWN,
                expected=[task.status],
                error_code=ErrorCode.UNKNOWN_EXHAUSTED.value,
                error_message="active task stalled: heartbeat missing",
                unknown_since=_now(),
                next_poll_at=_now(),
            )
            if moved:
                INSPECTOR_ACTIONS.inc({"action": "stalled_to_unknown"})
                await bus.enqueue("compensate:default", "compensate_orphan", {"task_id": task.task_id})
                actions += 1
    return actions


async def dispatch_pending_transfers(bus) -> int:
    actions = 0
    async with session_scope() as s:
        dao = TaskDAO(s)
        rows = list(await dao.pending_transfers())
        for task in rows:
            attempts = int((task.attributes or {}).get("transfer_attempts", 1))
            await bus.enqueue(TRANSFER_QUEUE, "store_result", {"task_id": task.task_id, "attempt": attempts})
            INSPECTOR_ACTIONS.inc({"action": "transfer_redispatch"})
            actions += 1
    return actions


async def calibrate_concurrency() -> int:
    """用 PG 实数校准 Redis 并发计数：**只纠泄漏方向**（Redis 偏大才下调）。"""
    container = get_container()
    limiter = container.limiter
    if not hasattr(limiter, "calibrate_channel"):
        return 0
    fixed = 0
    async with session_scope() as s:
        real = await TaskDAO(s).active_counts_by_channel()
    for channel, count in real.items():
        drift = await limiter.calibrate_channel(channel, count)  # type: ignore[attr-defined]
        if drift:
            fixed += 1
    return fixed


async def publish_gauges() -> None:
    async with session_scope() as s:
        dao = TaskDAO(s)
        by_channel = await dao.active_counts_by_channel()
        unknowns = await dao.count_unknown_by_channel()
    for channel, count in by_channel.items():
        ACTIVE_TASKS.set(count, {"channel": channel})
    for channel, count in unknowns.items():
        UNKNOWN_GAUGE.set(count, {"channel": channel, "status": "all"})


async def run_tick(bus, *, once: bool = False) -> dict[str, int]:
    settings = get_settings()
    result = {
        "accepted_stalls": await scan_accepted_stalls(bus, stall_seconds=settings.accepted_stall_seconds),
        "deadlines": await scan_deadlines(),
        "unknown_bounded": await bound_unknowns(
            bus,
            max_per_channel=settings.unknown_max_per_channel,
            max_lifetime_seconds=settings.unknown_max_lifetime_seconds,
        ),
        "orphans": await reconcile_orphans(bus),
        "transfers": await dispatch_pending_transfers(bus),
        "calibrated_channels": await calibrate_concurrency(),
    }
    await publish_gauges()
    INSPECTOR_HEARTBEAT.set(time.time())
    return result


async def run_inspector(*, max_ticks: int | None = None, tick_seconds: float | None = None) -> int:
    configure_logging()
    settings = get_settings()
    bus = make_bus(settings)
    interval = tick_seconds or settings.inspector_tick_seconds
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover
            pass

    logger.info("inspector started interval=%.1fs", interval)
    ticks = 0
    while not stop.is_set():
        ticks += 1
        try:
            summary = await run_tick(bus)
            if any(summary.values()):
                logger.info("inspector tick %s", summary)
        except Exception as exc:  # noqa: BLE001 - 巡检单 tick 失败不能退出
            logger.exception("inspector tick failed: %s", exc)
        if max_ticks is not None and ticks >= max_ticks:
            break
        await asyncio.sleep(interval)
    logger.info("inspector stopped after %s ticks", ticks)
    await close_redis()
    await dispose_engine()
    return ticks


def main() -> None:
    parser = argparse.ArgumentParser(description="async-gateway inspector（独立单副本）")
    parser.add_argument("--max-ticks", type=int, default=None)
    parser.add_argument("--interval", type=float, default=None)
    args = parser.parse_args()
    asyncio.run(run_inspector(max_ticks=args.max_ticks, tick_seconds=args.interval))


if __name__ == "__main__":  # pragma: no cover
    main()
