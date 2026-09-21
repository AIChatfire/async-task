"""后台进程心跳（探针判活的凭据；2026-09-21 补，对应 livetest-ai 报告 F2）。

**为什么需要它**：``worker`` / ``scheduler`` / ``inspector`` 三个进程**不开 HTTP 端口**。
镜像里若写死 ``http://127.0.0.1:8000/healthz``，这四个容器（含 transfer-worker）恒为
unhealthy —— 探的不是自己的进程，运维据此得到的"红"没有信息量，反而是噪音
（报告 ``E2E-ASYNC-TASK-001`` 的 F2）。

**判据 = "循环还在推进"**：每个后台循环每轮 ``beat()`` 一次（touch
``{dir}/{role}.beat``），探针（``scripts/healthcheck.py``）比对 mtime 与
``AG_PROBE_MAX_AGE_SECONDS``。比"进程还在"更严：卡死（比如上游调用永久挂住）的
循环会被判死并触发编排层重启，而"进程还在但什么都没做"不会被误判为健康。

**本模块只依赖标准库**：探针要能在**最小环境**下跑（不 import 配置、不连库、不读
``.env``），所以默认值在这里定义、由 ``Settings`` 与探针共同引用（单一事实来源）。

⚠️ 阈值必须大于**最长单条消息处理时长**，否则忙时的 worker 会被自己的探针判死：
worker 的消息处理上限是 ``AG_SUBMIT_READ_TIMEOUT × 3``（默认 90s），故默认取 120s。
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path

logger = logging.getLogger(__name__)

#: 心跳目录：容器内可写、随容器消失（不产生持久副作用）。
DEFAULT_HEARTBEAT_DIR = "/tmp/ag-heartbeat"
#: 心跳过期阈值（秒）。见模块 docstring 的"必须大于最长单条处理时长"。
DEFAULT_PROBE_MAX_AGE_SECONDS = 120.0

#: 会打心跳的角色（后台循环进程）
LOOP_ROLES: tuple[str, ...] = ("worker", "scheduler", "inspector")

#: 写失败只告警一次（避免每轮循环刷日志），键 = (角色, 原因)
_WARNED: set[tuple[str, str]] = set()


def heartbeat_dir() -> Path:
    """心跳目录。``AG_HEARTBEAT_DIR`` 由部署注入（默认 ``/tmp/ag-heartbeat``）。"""
    return Path(os.environ.get("AG_HEARTBEAT_DIR") or DEFAULT_HEARTBEAT_DIR)


def probe_max_age_seconds() -> float:
    raw = os.environ.get("AG_PROBE_MAX_AGE_SECONDS")
    if raw:
        try:
            return float(raw)
        except ValueError:
            logger.warning("AG_PROBE_MAX_AGE_SECONDS=%r 不是数字，回落默认值", raw)
    return DEFAULT_PROBE_MAX_AGE_SECONDS


def beat_path(role: str) -> Path:
    return heartbeat_dir() / f"{role}.beat"


def beat(role: str) -> bool:
    """打一次心跳。**best-effort**：写不了就告警一次并返回 False，绝不抛出。

    心跳属于可观测性，不能让"磁盘只读/目录不可写"这种环境问题把业务循环带崩；
    但也不能沉默——否则探针报 unhealthy 时无人知道原因（运维会误以为进程死了）。
    """
    path = beat_path(role)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.touch(exist_ok=True)
    except OSError as exc:
        key = (role, str(exc.errno))
        if key not in _WARNED:
            _WARNED.add(key)
            logger.warning(
                "heartbeat write failed role=%s path=%s: %s（探针将按 AG_HEARTBEAT_DIR=%s 找不到心跳而判死；本告警只出一次）",
                role,
                path,
                exc,
                heartbeat_dir(),
            )
        return False
    return True


def heartbeat_age(role: str, *, now: float | None = None) -> float | None:
    """心跳文件年龄（秒）；文件不存在返回 ``None``（= 从未打过心跳）。"""
    try:
        mtime = beat_path(role).stat().st_mtime
    except OSError:
        return None
    return max(0.0, (now if now is not None else time.time()) - mtime)
