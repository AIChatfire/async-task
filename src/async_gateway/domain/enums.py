"""内部状态机取值域与终态集合（§13 / §14）。

对外查询透传上游原生状态；内部状态机仅服务「状态输出、调度与终态收敛」。
两者解耦：透传管"看见"，终态判定管"输出与停止"。
"""

from __future__ import annotations

from enum import Enum


class TaskStatus(str, Enum):
    # —— 活动态 ——
    ACCEPTED = "accepted"
    UPSTREAM_SUBMITTED = "upstream_submitted"
    IN_PROGRESS = "in_progress"
    # 两个 unknown 态互不混淆：能确认已创建 / 不能确认是否创建
    SUBMIT_UNKNOWN = "submit_unknown"
    POLL_UNRECOGNIZED = "poll_unrecognized"
    # 失败待重试（§14：failed 可经 submit_upstream 重建 → 耗尽进 dead）
    FAILED = "failed"
    # —— 业务终态（无出边，单调不翻转）——
    SUCCEEDED = "succeeded"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    DEAD = "dead"
    DEAD_AWAITING_CONFIRM = "dead_awaiting_confirm"


class OutputTerminal(str, Enum):
    """envelope 的 ``terminal`` 三值（仅面向网关自有调用方）。"""

    IN_PROGRESS = "in_progress"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class Origin(str, Enum):
    USER = "user"
    DRY_RUN = "dry_run"
    REPLAY = "replay"


class ErrorClass(str, Enum):
    """§4.1 错误三级分类（+ 两个特殊类）。"""

    RETRY_SAFE = "retry_safe"          # 请求未发出/无副作用 → 直接重试
    FAIL_FAST = "fail_fast"            # 判失败不重试（4xx 参数错误）
    CONFIRM_REQUIRED = "confirm_required"  # 请求可能已到达 → 转 unknown 走确认
    RATE_LIMITED = "rate_limited"      # 429：独立退避计数，不消耗业务 attempts
    CHANNEL_FAULT = "channel_fault"    # 401/403：渠道级故障，熔断+告警，不烧 attempts


class ErrorCode(str, Enum):
    """§13 错误码取值域（retryable 由三级分类推导，不允许模板手配）。"""

    UPSTREAM_4XX = "UPSTREAM_4XX"
    UPSTREAM_5XX = "UPSTREAM_5XX"
    UPSTREAM_TERMINAL = "UPSTREAM_TERMINAL"
    TRANSPORT = "TRANSPORT"
    RATE_LIMITED = "RATE_LIMITED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"
    TRANSFER_FAILED = "TRANSFER_FAILED"
    UNKNOWN_EXHAUSTED = "UNKNOWN_EXHAUSTED"


class IdempotencyOutcome(str, Enum):
    """受理时的幂等判定结果。"""

    NEW = "new"                    # 首次受理
    REPLAY = "replay"              # 窗口期内同键同体 → 返回原任务
    CONFLICT = "conflict"          # 窗口期内同键异体 → 409


#: 网关内部终态：上游原生枚举没有这些取值，对外须按模板映射重写为原生失败类终态（§12.2）
INTERNAL_TERMINAL: frozenset[TaskStatus] = frozenset(
    {TaskStatus.TIMEOUT, TaskStatus.DEAD, TaskStatus.DEAD_AWAITING_CONFIRM}
)

#: 业务终态：进入后永不出边
BUSINESS_TERMINAL: frozenset[TaskStatus] = frozenset(
    {
        TaskStatus.SUCCEEDED,
        TaskStatus.CANCELLED,
        TaskStatus.TIMEOUT,
        TaskStatus.DEAD,
        TaskStatus.DEAD_AWAITING_CONFIRM,
    }
)

#: 失败待定态（可重试；不属于终态，不参与"终态只允许一次"的守卫）
RETRY_PENDING: frozenset[TaskStatus] = frozenset({TaskStatus.FAILED})

#: unknown 两态
UNKNOWN_STATES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.SUBMIT_UNKNOWN, TaskStatus.POLL_UNRECOGNIZED}
)

#: 上游侧镜像态（不进内部状态机，仅供模板比对与展示）
UPSTREAM_MIRROR_STATES: frozenset[str] = frozenset({"queued", "running"})
