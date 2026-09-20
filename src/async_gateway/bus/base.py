"""broker 抽象。

**只投递"做什么"，不投递"真相"**：消息体只有 task_id 与少量上下文，worker 领到后
一律回 Postgres 重读任务当前状态再决定动作。这样 Redis 丢消息/重复投递都不会破坏
状态机（§22「Redis 丢任务」的对策之一）。

延迟投递**不放在 broker 里**：延时轮询由 ``async_task.next_poll_at`` + scheduler 承担，
broker 因此可以是最简单的 FIFO，恢复时按 ``next_poll_at`` 分批放出即可（§19）。
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol


class QueueSaturated(RuntimeError):
    """队列深度打满 → 受理侧应回 429/503 + Retry-After。"""

    def __init__(self, queue: str, depth: int, limit: int) -> None:
        super().__init__(f"queue {queue} saturated: {depth}/{limit}")
        self.queue = queue
        self.depth = depth
        self.limit = limit


@dataclass(slots=True)
class Message:
    name: str
    payload: dict[str, Any]
    message_id: str = ""
    enqueued_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    deliveries: int = 1

    @property
    def task_id(self) -> str | None:
        value = self.payload.get("task_id")
        return str(value) if value is not None else None

    def encode(self) -> dict[str, str]:
        return {
            "name": self.name,
            "payload": json.dumps(self.payload, ensure_ascii=False, default=str),
            "enqueued_at": self.enqueued_at.isoformat(),
        }

    @classmethod
    def decode(cls, name: str, raw: dict[str, str], message_id: str) -> Message:
        payload = json.loads(raw.get("payload") or "{}")
        try:
            enqueued = datetime.fromisoformat(raw.get("enqueued_at") or "")
        except ValueError:
            enqueued = datetime.now(UTC)
        return cls(name=name, payload=payload, message_id=message_id, enqueued_at=enqueued)


class Bus(Protocol):
    async def enqueue(self, queue: str, name: str, payload: dict[str, Any]) -> str: ...
    async def consume(
        self, queue: str, *, consumer: str, count: int = 10, block_ms: int = 1000
    ) -> list[Message]: ...
    async def ack(self, queue: str, message_id: str) -> None: ...
    async def dead_letter(self, queue: str, message: Message, reason: str) -> None: ...
    async def depth(self, queue: str) -> int: ...
    async def pending(self, queue: str) -> int: ...
    async def ensure_queue(self, queue: str) -> None: ...
