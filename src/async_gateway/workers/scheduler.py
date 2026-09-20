"""scheduler 进程（§15 延迟轮询调度 / §19 单副本 + 就绪探针）。

只有一件事：**把到期的 ``next_poll_at`` 变成一条队列消息**。

* 生产固定**单副本** + 就绪探针（弃"带锁多副本"）；
* 崩溃恢复后按 ``next_poll_at`` **分批放出**（每 tick 至多 ``scheduler_batch_size`` 条，
  且按时间升序），避免积压洪峰一次性砸向上游；
* 派发后写一个**派发租约**（把 ``next_poll_at`` 向前推），防止同一条任务在本 tick
  被反复派发；handler 若已把时间轴推得更远，租约 CAS 自然失败、不覆盖。
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
from ..db.dao import DISPATCH_BY_STATUS, TaskDAO
from ..domain.enums import TaskStatus
from ..infra.redis import close_redis
from ..observability.logging import configure_logging
from ..observability.metrics import QUEUE_DEPTH, REGISTRY, TASK_AGE
from ..tasks.queues import poll_queue

logger = logging.getLogger(__name__)

SCHEDULER_HEARTBEAT = REGISTRY.gauge("ag_scheduler_last_tick_timestamp", "scheduler 最近一次成功 tick")
DISPATCH_TOTAL = REGISTRY.counter("ag_scheduler_dispatch_total", "scheduler 派发数")


def target_queue_and_name(task) -> tuple[str, str] | None:
    status = TaskStatus(task.status)
    name = DISPATCH_BY_STATUS.get(status)
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
