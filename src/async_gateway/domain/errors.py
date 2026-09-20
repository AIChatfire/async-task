"""错误三级分类与 retryable 推导（§4.1）。

硬约束：模板**不允许**手配 ``retryable``；它只能由本模块的分类结果推导。

分类要点（易错处已按文档标注）：

* 「连接建立超时」（connect timeout）与「响应超时/丢失」（read timeout）**语义相反**：
  前者请求未发出 → 可安全重试；后者请求可能已到达上游 → 必须转确认。
* 429 不消耗业务 attempts（独立退避计数 + 上限）；其"无副作用"是**上游假设**，
  需在 capabilities ``rate_limit_side_effect_free`` 声明。
* 401/403 是渠道级故障（渠道熔断 + 告警），**不烧任务 attempts**。
* 409 恰是去重抓手 → 转确认。
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, Literal

import httpx

from .enums import ErrorClass, ErrorCode

Context = Literal["submit", "poll", "cancel", "result"]


@dataclass(frozen=True, slots=True)
class Classification:
    error_class: ErrorClass
    error_code: ErrorCode
    retryable: bool
    consumes_attempt: bool
    channel_fault: bool
    action: Literal["retry", "backoff", "fail", "confirm", "channel_break"]

    @property
    def is_unknown(self) -> bool:
        """是否需转 ``submit_unknown``（仅提交上下文有意义）。"""
        return self.action == "confirm"


_RETRY_SAFE_TRANSPORT: Final = Classification(
    error_class=ErrorClass.RETRY_SAFE,
    error_code=ErrorCode.TRANSPORT,
    retryable=True,
    consumes_attempt=True,
    channel_fault=False,
    action="retry",
)
_CONFIRM_TRANSPORT: Final = Classification(
    error_class=ErrorClass.CONFIRM_REQUIRED,
    error_code=ErrorCode.TRANSPORT,
    retryable=False,
    consumes_attempt=True,
    channel_fault=False,
    action="confirm",
)
_CONFIRM_TIMEOUT: Final = Classification(
    error_class=ErrorClass.CONFIRM_REQUIRED,
    error_code=ErrorCode.TIMEOUT,
    retryable=False,
    consumes_attempt=True,
    channel_fault=False,
    action="confirm",
)


def classify_exception(exc: BaseException, context: Context = "submit") -> Classification:
    """传输层异常分类。"""
    # 连接建立超时：请求未发出 → 安全
    if isinstance(exc, httpx.ConnectTimeout):
        return _RETRY_SAFE_TRANSPORT
    # 连接被拒 / DNS 失败：请求未发出 → 安全
    if isinstance(exc, httpx.ConnectError):
        return _RETRY_SAFE_TRANSPORT
    # 响应超时 / 读超时 / 协议错乱：请求可能已到达 → 必须确认
    if isinstance(exc, httpx.TimeoutException):
        if context == "poll":
            return _retry_with(ErrorCode.TIMEOUT, action="backoff")
        return _CONFIRM_TIMEOUT
    if isinstance(exc, (httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError)):
        if context == "poll":
            return _retry_with(ErrorCode.TRANSPORT, action="backoff")
        return _CONFIRM_TRANSPORT
    if isinstance(exc, httpx.HTTPError):
        if context == "poll":
            return _retry_with(ErrorCode.TRANSPORT, action="backoff")
        return _CONFIRM_TRANSPORT
    # 未预期异常按"可能已到达"处理，宁可确认不猜成功
    if context == "poll":
        return _retry_with(ErrorCode.TRANSPORT, action="backoff")
    return _CONFIRM_TRANSPORT


def _retry_with(code: ErrorCode, *, action: str = "retry") -> Classification:
    return Classification(
        error_class=ErrorClass.RETRY_SAFE,
        error_code=code,
        retryable=True,
        consumes_attempt=True,
        channel_fault=False,
        action=action,  # type: ignore[arg-type]
    )


def classify_status(status_code: int, context: Context = "submit") -> Classification:
    """HTTP 状态码分类。"""
    if status_code == 429:
        # 独立退避计数：不消耗业务 attempts
        return Classification(
            error_class=ErrorClass.RATE_LIMITED,
            error_code=ErrorCode.RATE_LIMITED,
            retryable=True,
            consumes_attempt=False,
            channel_fault=False,
            action="backoff",
        )
    if status_code in (401, 403):
        # 渠道级故障：熔断渠道 + 告警；透传模式下网关不持 key，熔断动作=告警+渠道侧联动
        return Classification(
            error_class=ErrorClass.CHANNEL_FAULT,
            error_code=ErrorCode.UPSTREAM_4XX,
            retryable=False,
            consumes_attempt=False,
            channel_fault=True,
            action="channel_break",
        )
    if status_code == 409:
        # 冲突恰是上游侧去重抓手 → 转确认
        return Classification(
            error_class=ErrorClass.CONFIRM_REQUIRED,
            error_code=ErrorCode.UPSTREAM_4XX,
            retryable=False,
            consumes_attempt=False,
            channel_fault=False,
            action="confirm",
        )
    if status_code == 404 and context == "poll":
        # 多 key 轮换/per_key_isolation 下 poll 404 可能只是任务空间不匹配 → 转确认
        return Classification(
            error_class=ErrorClass.CONFIRM_REQUIRED,
            error_code=ErrorCode.UPSTREAM_4XX,
            retryable=False,
            consumes_attempt=False,
            channel_fault=False,
            action="confirm",
        )
    if 400 <= status_code < 500:
        return Classification(
            error_class=ErrorClass.FAIL_FAST,
            error_code=ErrorCode.UPSTREAM_4XX,
            retryable=False,
            consumes_attempt=True,
            channel_fault=False,
            action="fail",
        )
    if 500 <= status_code < 600:
        if context == "poll":
            return Classification(
                error_class=ErrorClass.RETRY_SAFE,
                error_code=ErrorCode.UPSTREAM_5XX,
                retryable=True,
                consumes_attempt=False,
                channel_fault=False,
                action="backoff",
            )
        return Classification(
            error_class=ErrorClass.CONFIRM_REQUIRED,
            error_code=ErrorCode.UPSTREAM_5XX,
            retryable=False,
            consumes_attempt=True,
            channel_fault=False,
            action="confirm",
        )
    # 非预期状态码（1xx/3xx 等）：不让它静默通过
    return Classification(
        error_class=ErrorClass.CONFIRM_REQUIRED,
        error_code=ErrorCode.UPSTREAM_5XX,
        retryable=False,
        consumes_attempt=False,
        channel_fault=False,
        action="confirm",
    )


def extract_retry_after(headers: httpx.Headers | dict[str, str], default: float = 1.0) -> float:
    """从 429 响应解析 Retry-After（respect_retry_after）。"""
    raw = headers.get("retry-after") or headers.get("Retry-After")
    if not raw:
        return default
    try:
        return max(0.0, float(raw))
    except ValueError:
        # HTTP-date 形态用当前时间差近似（不引第三方依赖）
        import email.utils
        import time

        try:
            parsed = email.utils.parsedate_to_datetime(str(raw))
        except (TypeError, ValueError):
            return default
        if parsed is None:
            return default
        return max(0.0, parsed.timestamp() - time.time())


def backoff_seconds(attempt: int, *, base: float = 1.0, cap: float = 60.0, jitter: float = 0.2) -> float:
    """指数退避 + jitter（§15 中间件）。

    ``cap`` 是**硬上限**：jitter 只能在上限之内抖，否则"上限 60s"形同虚设，
    退避曲线会在高 attempt 时冲过闸门。
    """
    import random

    raw = min(cap, base * (2 ** max(0, attempt - 1)))
    jittered = raw * (1.0 - jitter) + raw * jitter * 2 * random.random()
    return float(min(cap, jittered))
