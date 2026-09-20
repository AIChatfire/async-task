"""broker 工厂：按配置挑选实现（Redis Streams / 内存 / Taskiq 运行时）。"""

from __future__ import annotations

from ..config import Settings, get_settings
from .base import Bus
from .memory import MemoryBus
from .stream import StreamBus


def make_bus(settings: Settings | None = None) -> Bus:
    s = settings or get_settings()
    if s.queue_driver == "memory":
        return MemoryBus()
    if s.queue_driver == "taskiq":
        from .taskiq_adapter import TaskiqBus

        return TaskiqBus()
    if s.is_test:
        # 测试环境默认不依赖 Redis；需要真实 Streams 的用例显式构造 StreamBus
        return MemoryBus()
    return StreamBus()


def make_stream_bus(settings: Settings | None = None) -> StreamBus:
    s = settings or get_settings()
    return StreamBus(prefix=s.queue_stream_prefix, group=s.queue_group, max_depth=s.queue_max_depth)
