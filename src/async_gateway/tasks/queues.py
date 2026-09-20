"""队列命名与 worker 池划分（§10 / §19）。

* 共享队列（submit / compensate / cancel / finalize）所有 worker 都消费；
* poll 按**模板 ``pool`` 字段**分池：``heavy-poll`` / ``light-poll`` / 灰度隔离池，
  渠道增长时不再"一拆一个 Deployment"；
* 转存走**独立队列 + 独立 Deployment/HPA**，大结果 IO 不与轻量 poll 混部。
"""

from __future__ import annotations

from typing import Iterable

SHARED_QUEUES: tuple[str, ...] = (
    "submit:default",
    "compensate:default",
    "cancel:default",
    "finalize:default",
)
TRANSFER_QUEUE = "transfer:transfer"
DLQ_SUFFIX = "dlq"

#: 模板 pool 取值 → 队列；未知 pool 归入 light-poll（保守：不打爆重池）
KNOWN_POOLS: tuple[str, ...] = ("heavy-poll", "light-poll", "poll")
DEFAULT_POOL = "light-poll"


def pool_of(template_pool: str | None) -> str:
    if not template_pool or template_pool in ("shared", "default"):
        return DEFAULT_POOL
    return template_pool


def poll_queue(template_pool: str | None) -> str:
    return f"poll:{pool_of(template_pool)}"


def queues_for_pools(pools: Iterable[str]) -> list[str]:
    """worker 启动时按 ``--pools`` 展开要消费的队列集合。"""
    wanted = {p.strip() for p in pools if p and p.strip()}
    queues = list(SHARED_QUEUES)
    if "transfer" in wanted:
        return [TRANSFER_QUEUE]
    for pool in wanted:
        queues.append(f"poll:{pool}")
    return queues


def all_queues() -> list[str]:
    return list(SHARED_QUEUES) + [f"poll:{p}" for p in KNOWN_POOLS] + [TRANSFER_QUEUE]
