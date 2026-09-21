"""scheduler 进程（§15 延迟轮询调度 / §19 单副本 + 就绪探针）。

只有一件事：**把到期的 ``next_poll_at`` 变成一条队列消息**。

* 生产固定**单副本** + 就绪探针（弃"带锁多副本"）；
* 崩溃恢复后按 ``next_poll_at`` **分批放出**（每 tick 至多 ``scheduler_batch_size`` 条，
  且按时间升序），避免积压洪峰一次性砸向上游；
* 派发后写一个**派发租约**（把 ``next_poll_at`` 向前推），防止同一条任务在本 tick
  被反复派发；handler 若已把时间轴推得更远，租约 CAS 自然失败、不覆盖；
* 派发集合除 ``DISPATCH_BY_STATUS`` 外，还含「``accepted`` **且无在途提交意图**」——
  这是 429 / 401/403 释放提交意图后的**准时重投**入口（``next_poll_at`` 由 handler 按
  Retry-After 与退避写好）。旧口径下 accepted 只由 inspector 的 60s 悬挂门槛兜底，
  实测把配置里 3s 的重试意图放大成 60-90s（见 ``docs/IMPLEMENTATION.md`` §3.17）。
  **有**意图的 accepted 仍不派发（可能已调上游未落库，只能走 compensate 确认）。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import time

from ..bus.factory import make_bus
from ..config import get_settings
from ..db.base import dispose_engine, session_scope
from ..db.dao import ACCEPTED_RESUBMIT_HANDLER, DISPATCH_BY_STATUS, TaskDAO
from ..domain.enums import TaskStatus
from ..infra.redis import close_redis
from ..observability.heartbeat import beat
from ..observability.logging import configure_logging
from ..observability.metrics import ACCEPTED_RESUBMIT_TOTAL, QUEUE_DEPTH, REGISTRY, TASK_AGE
from ..tasks.queues import poll_queue

logger = logging.getLogger(__name__)

SCHEDULER_HEARTBEAT = REGISTRY.gauge("ag_scheduler_last_tick_timestamp", "scheduler 最近一次成功 tick")
DISPATCH_TOTAL = REGISTRY.counter("ag_scheduler_dispatch_total", "scheduler 派发数")


def target_queue_and_name(task) -> tuple[str, str] | None:
    status = TaskStatus(task.status)
    name = DISPATCH_BY_STATUS.get(status)
    if name is None and status is TaskStatus.ACCEPTED and task.submit_started_at is None:
        # accepted 的重投入口（受控）：无在途提交意图 ⇒ 上游侧必无本任务的创建请求在途
        # ⇒ 与 inspector 悬挂巡检同判定，但按 next_poll_at 准时发出（不必等 60s 门槛）。
        # 有意图的 accepted 仍返回 None（交给 inspector 分流，禁止未确认重投）。
        name = ACCEPTED_RESUBMIT_HANDLER
    if name is None:
        return None
    if name == "poll_upstream":
        # 池归属来自模板 ``pool`` 字段（渠道增长不再一拆一 Deployment）
        pool = None
        try:
            from ..gateway.container import get_container

            tv = get_container().registry.get(task.template_alias, task.template_version)
            pool = tv.resolved.pool if tv else None
        except Exception:  # noqa: BLE001 - 模板取不到时落默认池
            pool = None
        return poll_queue(pool), name
    if name == "submit_upstream":
        return "submit:default", name
    if name == "compensate_orphan":
        return "compensate:default", name
    return "finalize:default", name


async def dispatch_due(bus, *, batch_size: int | None = None, lease_seconds: float = 30.0) -> int:
    settings = get_settings()
    batch = batch_size or settings.scheduler_batch_size
    dispatched = 0
    async with session_scope() as s:
        dao = TaskDAO(s)
        due = list(await dao.due_for_dispatch(limit=batch))
        for task in due:
            target = target_queue_and_name(task)
            if target is None:
                continue
            queue, name = target
            leased = await dao.lease_dispatch(
                task.task_id, lease_seconds=lease_seconds, expected=[task.status]
            )
            if not leased:
                # 已被 handler 推进过时间轴（或在别处被处理）→ 本轮不派发
                continue
            await bus.enqueue(queue, name, {"task_id": task.task_id})
            DISPATCH_TOTAL.inc({"name": name, "queue": queue})
            if TaskStatus(task.status) is TaskStatus.ACCEPTED:
                ACCEPTED_RESUBMIT_TOTAL.inc({"channel": task.channel})
            dispatched += 1
    return dispatched


async def publish_queue_metrics(bus) -> None:
    from ..tasks.queues import all_queues

    for queue in all_queues():
        depth = await bus.depth(queue)
        QUEUE_DEPTH.set(depth, {"queue": queue})


async def run_scheduler(*, max_ticks: int | None = None, tick_seconds: float | None = None) -> int:
    configure_logging()
    settings = get_settings()
    bus = make_bus(settings)
    interval = tick_seconds or settings.scheduler_tick_seconds
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover
            pass

    logger.info("scheduler started interval=%.2fs batch=%s", interval, settings.scheduler_batch_size)
    ticks = 0
    while not stop.is_set():
        ticks += 1
        try:
            count = await dispatch_due(bus)
            SCHEDULER_HEARTBEAT.set(time.time())
            # 进程级心跳（容器探针判活；见 observability/heartbeat.py）
            beat("scheduler")
            if ticks % 10 == 0:
                await publish_queue_metrics(bus)
            if count:
                logger.debug("dispatched %s tasks", count)
        except Exception as exc:  # noqa: BLE001 - 单 tick 失败不能让调度器退出
            logger.exception("scheduler tick failed: %s", exc)
        if max_ticks is not None and ticks >= max_ticks:
            break
        await asyncio.sleep(interval)

    logger.info("scheduler stopped after %s ticks", ticks)
    await close_redis()
    await dispose_engine()
    return ticks


def main() -> None:
    parser = argparse.ArgumentParser(description="async-gateway scheduler（生产单副本）")
    parser.add_argument("--max-ticks", type=int, default=None)
    parser.add_argument("--interval", type=float, default=None)
    args = parser.parse_args()
    asyncio.run(run_scheduler(max_ticks=args.max_ticks, tick_seconds=args.interval))


if __name__ == "__main__":  # pragma: no cover
    main()
