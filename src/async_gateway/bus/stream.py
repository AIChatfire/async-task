"""Redis Streams 实现（§10 Taskiq 队列 Redis Streams 的落地形态）。

选型说明：文档写的是"Taskiq 队列（Redis Streams）"。本实现直接用 Redis Streams
的 XADD/XREADGROUP 语义，好处是**零框架耦合**：消费组、ACK、pending 重投、DLQ
的行为完全可预测，也便于故障注入测试（M3 清单第 1/5 项）；``taskiq`` 运行时仍是
可选后端（见 ``taskiq_adapter``），二者共用同一套 handler。
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from ..infra.redis import get_redis
from .base import Bus, Message, QueueSaturated


class StreamBus(Bus):
    def __init__(self, *, prefix: str | None = None, group: str | None = None, max_depth: int | None = None) -> None:
        s = get_settings()
        self.prefix = prefix or s.queue_stream_prefix
        self.group = group or s.queue_group
        self.max_depth = max_depth if max_depth is not None else s.queue_max_depth

    def stream(self, queue: str) -> str:
        return f"{self.prefix}{queue}"

    def dlq(self, queue: str) -> str:
        return f"{self.prefix}dlq:{queue}"

    async def ensure_queue(self, queue: str) -> None:
        client = get_redis()
        try:
            await client.xgroup_create(self.stream(queue), self.group, id="0", mkstream=True)
        except Exception as exc:  # noqa: BLE001 - BUSYGROUP 表示已存在
            if "BUSYGROUP" not in str(exc):
                raise

    async def enqueue(self, queue: str, name: str, payload: dict[str, Any]) -> str:
        client = get_redis()
        depth = int(await client.xlen(self.stream(queue)))
        if depth >= self.max_depth:
            raise QueueSaturated(queue, depth, self.max_depth)
        message = Message(name=name, payload=payload)
        await self.ensure_queue(queue)
        message_id = await client.xadd(self.stream(queue), message.encode(), maxlen=self.max_depth * 2)
        return str(message_id)

    async def consume(
        self, queue: str, *, consumer: str, count: int = 10, block_ms: int = 1000
    ) -> list[Message]:
        client = get_redis()
        await self.ensure_queue(queue)
        response = await client.xreadgroup(
            self.group,
            consumer,
            {self.stream(queue): ">"},
            count=count,
            block=block_ms,
        )
        messages: list[Message] = []
        for _stream, entries in response or []:
            for message_id, raw in entries:
                messages.append(Message.decode(raw.get("name", ""), raw, str(message_id)))
        return messages

    async def ack(self, queue: str, message_id: str) -> None:
        await get_redis().xack(self.stream(queue), self.group, message_id)

    async def dead_letter(self, queue: str, message: Message, reason: str) -> None:
        client = get_redis()
        encoded = message.encode()
        encoded["reason"] = reason[:500]
        encoded["failed_at"] = message.enqueued_at.isoformat()
        await client.xadd(self.dlq(queue), encoded, maxlen=10_000)
        await self.ack(queue, message.message_id)

    async def depth(self, queue: str) -> int:
        return int(await get_redis().xlen(self.stream(queue)))

    async def pending(self, queue: str) -> int:
        info = await get_redis().xpending(self.stream(queue), self.group)
        if isinstance(info, dict):  # pragma: no cover - redis-py 版本差异
            return int(info.get("pending", 0))
        return int(info[0]) if info else 0

    async def claim_stale(
        self, queue: str, *, consumer: str, min_idle_ms: int = 60_000, count: int = 20
    ) -> list[Message]:
        """领取超时未 ACK 的消息（worker 崩溃留下的 pending）。"""
        client = get_redis()
        cursor = "0-0"
        claimed: list[Message] = []
        while True:
            _cursor, entries, _deleted = await client.xautoclaim(
                self.stream(queue),
                self.group,
                consumer,
                min_idle_time=min_idle_ms,
                start_id=cursor,
                count=count,
            )
            for message_id, raw in entries:
                if not raw:
                    continue
                claimed.append(Message.decode(raw.get("name", ""), raw, str(message_id)))
            cursor = _cursor
            if cursor in ("0-0", "0") or not entries:
                break
        return claimed
