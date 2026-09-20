"""Taskiq 运行时适配（§10 文档指定的队列框架）。

文档写的是"Taskiq 队列（Redis Streams）"。为了既不违背文档、又不把核心链路绑在
某个框架的 API 版本上，这里的做法是：

* ``StreamBus``（默认）直接用 Redis Streams 语义，行为可预测、便于故障注入；
* 若坚持用 Taskiq 运行时，设 ``AG_QUEUE_DRIVER=taskiq``，投递走 Taskiq broker，
  消费交给 ``taskiq worker async_gateway.bus.taskiq_adapter:broker``——**同一套 handler**
  （``dispatch_message``）被 Taskiq 任务函数调用，两条路径行为一致。
"""

from __future__ import annotations

from typing import Any

from ..config import get_settings
from .base import Bus, Message

try:  # pragma: no cover - 取决于是否安装 taskiq
    from taskiq import TaskiqEvents
    from taskiq_redis import RedisStreamBroker

    _broker = RedisStreamBroker(get_settings().redis_url, queue_name=f"{get_settings().queue_stream_prefix}default")

    @_broker.on_event(TaskiqEvents.WORKER_STARTUP)
    async def _startup(_state: Any) -> None:  # pragma: no cover - 运行时钩子
        await _broker.startup()

    @_broker.task(task_name="async_gateway.dispatch")
    async def dispatch(name: str, payload: dict[str, Any]) -> None:
        """Taskiq 侧的统一入口：按名字分派到同一套 handler。"""
        from ..bus.factory import make_bus
        from ..tasks.handlers import dispatch_message

        await dispatch_message(payload, name, make_bus())

    TASKIQ_AVAILABLE = True
except Exception:  # noqa: BLE001 - 未安装或版本不符时优雅退化
    _broker = None
    dispatch = None  # type: ignore[assignment]
    TASKIQ_AVAILABLE = False


broker = _broker


class TaskiqBus(Bus):
    """把 :class:`Bus` 接口映射到 Taskiq broker（消费交给 Taskiq worker）。"""

    async def enqueue(self, queue: str, name: str, payload: dict[str, Any]) -> str:
        if not TASKIQ_AVAILABLE or dispatch is None:
            raise RuntimeError(
                "taskiq driver selected but taskiq/taskiq-redis is unavailable; "
                "install extras or switch AG_QUEUE_DRIVER=stream"
            )
        labels = {"queue": queue}
        message = await dispatch.kicker().with_labels(**labels).kiq(name, payload)
        return str(getattr(message, "task_id", "") or "")

    async def consume(self, queue: str, **kwargs: Any) -> list[Message]:  # pragma: no cover
        raise NotImplementedError(
            "taskiq driver consumes via `taskiq worker async_gateway.bus.taskiq_adapter:broker`"
        )

    async def ack(self, queue: str, message_id: str) -> None:  # pragma: no cover
        return None

    async def dead_letter(self, queue: str, message: Message, reason: str) -> None:  # pragma: no cover
        return None

    async def depth(self, queue: str) -> int:  # pragma: no cover
        return 0

    async def pending(self, queue: str) -> int:  # pragma: no cover
        return 0

    async def ensure_queue(self, queue: str) -> None:  # pragma: no cover
        return None
