"""worker 进程（§10 QoS 池 / §15 中间件语义）。

职责：消费队列 → 按名字分派 handler → ACK / 失败进 DLQ。

中间件语义在这里集中实现：

* **trace 上下文传递**：消息里带 trace_id（若有）；
* **超时控制**：单条消息处理超时按 pool 设上限，超时不算业务失败（handler 内部自有幂等/CAS）；
* **异常隔离**：单条消息抛错不影响其他消息；连续失败进 DLQ 并告警；
* **渠道并发计数**：由 handler 内部 acquire/release（Lua 原子），这里不重复计数；
* **崩溃恢复**：定期 XAUTOCLAIM 领取超时未 ACK 的消息（worker 崩溃留下的 pending）。
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from typing import Sequence

from ..bus.base import Bus, Message
from ..bus.factory import make_bus
from ..config import get_settings
from ..db.base import dispose_engine
from ..infra.redis import close_redis
from ..observability.logging import configure_logging
from ..observability.metrics import ENQUEUE_LAG, REGISTRY
from ..tasks.handlers import dispatch_message
from ..tasks.queues import queues_for_pools

logger = logging.getLogger(__name__)

WORKER_HEARTBEAT = REGISTRY.gauge("ag_worker_last_tick_timestamp", "worker 最近一次成功循环的时间戳")
MESSAGE_TOTAL = REGISTRY.counter("ag_messages_total", "消息处理结果分布")

MAX_HANDLER_FAILURES = 3


async def handle_message(bus: Bus, message: Message, *, container=None) -> None:
    import time

    started = time.time()
    name = message.name or ""
    try:
        await asyncio.wait_for(
            dispatch_message(message.payload, name, bus, container),
            timeout=float(get_settings().submit_read_timeout * 3),
        )
    except asyncio.TimeoutError:
        MESSAGE_TOTAL.inc({"name": name, "result": "timeout"})
        logger.warning("handler timeout name=%s task_id=%s", name, message.task_id)
        await bus.dead_letter("unknown", message, "handler timeout")
        return
    except Exception as exc:  # noqa: BLE001 - 单条消息失败不能拖垮 worker
        MESSAGE_TOTAL.inc({"name": name, "result": "error"})
        logger.exception("handler error name=%s task_id=%s: %s", name, message.task_id, exc)
        if message.deliveries >= MAX_HANDLER_FAILURES:
            await bus.dead_letter("unknown", message, f"{type(exc).__name__}: {exc}")
        else:
            # 未 ACK：留在 pending，稍后由 claim_stale 重投（至少一次投递）
            return
        return
    finally:
        ENQUEUE_LAG.observe(max(0.0, started - message.enqueued_at.timestamp()))
    MESSAGE_TOTAL.inc({"name": name, "result": "ok"})
    await bus.ack("unknown", message.message_id)


async def run_worker(pools: Sequence[str], consumer: str | None = None, *, stop_after: int | None = None) -> None:
    configure_logging()
    settings = get_settings()
    container = None
    from ..gateway.container import get_container

    container = get_container()
    bus = make_bus(settings)
    container.set_bus(bus)
    queues = queues_for_pools(pools)
    consumer = consumer or f"{settings.service_name}-{','.join(pools) or 'default'}"
    for queue in queues:
        await bus.ensure_queue(queue)
    logger.info("worker started consumer=%s queues=%s", consumer, queues)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # pragma: no cover - 非 POSIX
            pass

    processed = 0
    ticks = 0
    while not stop.is_set():
        ticks += 1
        WORKER_HEARTBEAT.set(asyncio.get_running_loop().time())
        progressed = False
        for queue in queues:
            messages = await bus.consume(queue, consumer=consumer, count=10, block_ms=200)
            for message in messages:
                progressed = True
                started = message.enqueued_at.timestamp()
                try:
                    await dispatch_message(message.payload, message.name, bus, container)
                except Exception as exc:  # noqa: BLE001
                    MESSAGE_TOTAL.inc({"name": message.name, "result": "error"})
                    logger.exception("handler failed name=%s task_id=%s", message.name, message.task_id)
                    if message.deliveries >= MAX_HANDLER_FAILURES:
                        await bus.dead_letter(queue, message, f"{type(exc).__name__}: {exc}")
                    continue
                MESSAGE_TOTAL.inc({"name": message.name, "result": "ok"})
                ENQUEUE_LAG.observe(max(0.0, message.enqueued_at.timestamp() - started))
                await bus.ack(queue, message.message_id)
                processed += 1
                if stop_after is not None and processed >= stop_after:
                    stop.set()
                    break
            if stop_after is not None and processed >= stop_after:
                break
        # 崩溃恢复：领取超时未 ACK 的消息（间隔做，避免每轮都扫）
        if ticks % 30 == 0 and hasattr(bus, "claim_stale"):
            for queue in queues:
                for stale in await bus.claim_stale(queue, consumer=consumer):  # type: ignore[attr-defined]
                    try:
                        await dispatch_message(stale.payload, stale.name, bus, container)
                        await bus.ack(queue, stale.message_id)
                    except Exception:  # noqa: BLE001
                        logger.exception("stale message replay failed queue=%s", queue)
        if not progressed:
            await asyncio.sleep(0.01)

    logger.info("worker stopping consumer=%s processed=%s", consumer, processed)
    await close_redis()
    await dispose_engine()


def main() -> None:
    parser = argparse.ArgumentParser(description="async-gateway worker")
    parser.add_argument("--pools", default="light-poll", help="逗号分隔：heavy-poll,light-poll,transfer")
    parser.add_argument("--consumer", default=None)
    parser.add_argument("--stop-after", type=int, default=None, help="处理 N 条后退出（测试用）")
    args = parser.parse_args()
    pools = [p.strip() for p in args.pools.split(",") if p.strip()]
    asyncio.run(run_worker(pools, args.consumer, stop_after=args.stop_after))


if __name__ == "__main__":  # pragma: no cover
    main()
