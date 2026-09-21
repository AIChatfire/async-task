"""后台进程启动冒烟：scheduler / inspector / worker 各跑真实一轮。

用法：
    AG_APP_ENV=dev AG_DATABASE_URL=sqlite+aiosqlite:////tmp/ag-smoke.db \
    AG_QUEUE_DRIVER=stream \
    python scripts/smoke_workers.py

（不需要对象存储：转存是可选件，未配置即自动关闭。）
"""

from __future__ import annotations

import asyncio
import os
import sys
import uuid

os.environ.setdefault("AG_SSRF_ALLOW_HOSTS", '["localhost"]')
os.environ.setdefault("AG_LOG_LEVEL", "WARNING")

TIMEOUT = 90


async def main() -> int:
    from datetime import UTC, datetime, timedelta

    from async_gateway.bus.factory import make_bus
    from async_gateway.db.base import dispose_engine, init_schema, session_scope
    from async_gateway.db.models import AsyncTask
    from async_gateway.domain.enums import TaskStatus
    from async_gateway.workers.inspector import run_inspector
    from async_gateway.workers.scheduler import run_scheduler
    from async_gateway.workers.worker import run_worker

    await init_schema()

    task_id = uuid.uuid4().hex[:24]
    async with session_scope() as s:
        s.add(
            AsyncTask(
                task_id=task_id,
                idempotency_key="k-smoke",
                idempotency_bucket=1,
                tenant="t",
                channel="echo",
                task_type="echo",
                status=TaskStatus.IN_PROGRESS.value,
                template_alias="echo",
                template_version=1,
                upstream_task_id="up-smoke",
                attempts=1,
                max_attempts=3,
                next_poll_at=datetime.now(UTC) - timedelta(seconds=5),
            )
        )

    ticks = await run_scheduler(max_ticks=2, tick_seconds=0.01)
    print(f"[scheduler] ticks={ticks}")

    bus = make_bus()
    depth = await bus.depth("poll:light-poll")
    print(f"[scheduler] poll:light-poll depth={depth}")
    assert depth >= 1, "scheduler 应当派发出至少一条 poll 消息"

    ticks = await run_inspector(max_ticks=1, tick_seconds=0.01)
    print(f"[inspector] ticks={ticks}")

    await bus.enqueue("finalize:default", "finalize_task", {"task_id": "does-not-exist"})
    await run_worker(["light-poll"], consumer="smoke", stop_after=1)
    print(f"[worker] consumed and exited; pending={await bus.pending('finalize:default')}")

    await dispose_engine()
    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(asyncio.wait_for(main(), timeout=TIMEOUT)))
