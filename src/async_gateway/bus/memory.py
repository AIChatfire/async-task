"""进程内 broker（测试 / 无 Redis 本地联调）。

语义对齐 Redis Streams 版：ACK 前消息可重投，DLQ 独立存放，深度受限。
"""

from __future__ import annotations

import asyncio
from typing import Any

from .base import Bus, Message, QueueSaturated


class MemoryBus(Bus):
    def __init__(self, *, max_depth: int = 100_000) -> None:
        self.max_depth = max_depth
        self.queues: dict[str, asyncio.Queue[Message]] = {}
        self.dlq: dict[str, list[tuple[Message, str]]] = {}
        self.acked: list[str] = []
        self._counter = 0

    def _queue(self, queue: str) -> asyncio.Queue[Message]:
        if queue not in self.queues:
            self.queues[queue] = asyncio.Queue()
        return self.queues[queue]

    async def enqueue(self, queue: str, name: str, payload: dict[str, Any]) -> str:
        q = self._queue(queue)
        if q.qsize() >= self.max_depth:
            raise QueueSaturated(queue, q.qsize(), self.max_depth)
        self._counter += 1
        message = Message(name=name, payload=payload, message_id=f"mem-{self._counter}")
        await q.put(message)
        return message.message_id

    async def consume(
        self, queue: str, *, consumer: str = "mem", count: int = 10, block_ms: int = 0
    ) -> list[Message]:
        q = self._queue(queue)
        out: list[Message] = []
        try:
            out.append(await asyncio.wait_for(q.get(), timeout=block_ms / 1000 if block_ms else 0.01))
        except (asyncio.TimeoutError, TimeoutError):
            return out
        while len(out) < count:
            try:
                out.append(q.get_nowait())
            except asyncio.QueueEmpty:
                break
        return out

    async def ack(self, queue: str, message_id: str) -> None:
        self.acked.append(message_id)

    async def dead_letter(self, queue: str, message: Message, reason: str) -> None:
        self.dlq.setdefault(queue, []).append((message, reason))

    async def depth(self, queue: str) -> int:
        return self._queue(queue).qsize()

    async def pending(self, queue: str) -> int:
        return 0

    async def ensure_queue(self, queue: str) -> None:
        self._queue(queue)

    # ---- 测试辅助 ----
    def drain(self, queue: str) -> int:
        q = self._queue(queue)
        count = 0
        while not q.empty():
            q.get_nowait()
            count += 1
        return count
