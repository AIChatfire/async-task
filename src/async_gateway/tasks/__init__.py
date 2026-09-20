"""任务层：队列命名、处理器、scheduler / inspector 使用的巡检逻辑。"""

from .queues import SHARED_QUEUES, TRANSFER_QUEUE, all_queues, poll_queue, pool_of, queues_for_pools

__all__ = [
    "SHARED_QUEUES",
    "TRANSFER_QUEUE",
    "all_queues",
    "poll_queue",
    "pool_of",
    "queues_for_pools",
]
