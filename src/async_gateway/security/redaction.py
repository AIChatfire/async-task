"""凭证脱敏（§17 强制脱敏清单 / §18.2）。

三条硬规则：

1. **Authorization 头永不进日志/trace/审计**——``redact_headers`` 是白名单制：
   只放行明确列出的头，其余一律 ``***``。
2. OTel 采集器**显式剔除 header 采集**（白名单制），由 :func:`otel_safe_headers` 提供。
3. 上游错误响应入库/展示前按**凭证值反向扫描**抹除——4xx 回显 token 片段是真实存在的
   泄露路径（§17 C2），:func:`scrub_text` / :func:`scrub_payload` 负责。
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterable, Mapping

#: 永不外泄的头
SENSITIVE_HEADERS: frozenset[str] = frozenset(
    {
        "authorization",
        "proxy-authorization",
        "x-api-key",
        "api-key",
        "apikey",
        "x-goog-api-key",
        "x-auth-token",
        "cookie",
        "set-cookie",
        "x-amz-security-token",
    }
)

#: 允许进日志/trace 的头（白名单制；未列出的一律丢掉）
OTEL_SAFE_HEADERS: frozenset[str] = frozenset(
    {
        "content-type",
        "accept",
        "accept-encoding",
        "user-agent",
        "content-length",
        "retry-after",
        "x-request-id",
        "traceparent",
        "tracestate",
        "x-b3-traceid",
        "x-b3-spanid",
    }
)

#: 疑似凭证的取值形态（兜底扫描，防止凭证被塞进非标准头或响应体）
_SECRET_SHAPES: tuple[re.Pattern[str], ...] = (
    re.compile(r"\b(sk|pk|api|key|token|bearer)[-_][A-Za-z0-9\-_]{12,}\b", re.IGNORECASE),
    re.compile(r"\bBearer\s+[A-Za-z0-9\-._~+/]{16,}=*", re.IGNORECASE),
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    re.compile(r"\b[A-Za-z0-9]{32,}={0,2}\b"),
)

_REDACTED = "***redacted***"


def redact_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """白名单制脱敏：只保留安全头，其余（含凭证头）统一打码。"""
    if not headers:
        return {}
    out: dict[str, str] = {}
    for key, value in headers.items():
        lower = key.lower()
        if lower in SENSITIVE_HEADERS:
            out[key] = _REDACTED
        elif any(shape.search(str(value)) for shape in _SECRET_SHAPES[:2]):
            out[key] = _REDACTED
        else:
            out[key] = str(value)
    return out


def otel_safe_headers(headers: Mapping[str, Any] | None) -> dict[str, str]:
    """给 OTel 用：**只**放行白名单头，其余一个都不采集。"""
    if not headers:
        return {}
    return {k: str(v) for k, v in headers.items() if k.lower() in OTEL_SAFE_HEADERS}


def scrub_text(text: str, secrets: Iterable[str] = ()) -> str:
    """先按已知凭证值反向抹除，再按形态兜底打码。"""
    out = text
    for secret in secrets:
        if secret and len(str(secret)) >= 6:
            out = out.replace(str(secret), _REDACTED)
    for shape in _SECRET_SHAPES:
        out = shape.sub(_REDACTED, out)
    return out


def scrub_payload(payload: Any, secrets: Iterable[str] = ()) -> Any:
    """递归抹除结构化负载里的凭证值（上游响应体入库/展示前调用）。"""
    secret_list = [str(s) for s in secrets if s]
    if isinstance(payload, Mapping):
        return {k: scrub_payload(v, secret_list) for k, v in payload.items()}
    if isinstance(payload, (list, tuple)):
        return [scrub_payload(v, secret_list) for v in payload]
    if isinstance(payload, str):
        return scrub_text(payload, secret_list)
    return payload


def safe_json_dump(payload: Any, secrets: Iterable[str] = (), *, limit: int = 2048) -> str:
    """入库/打点用的摘要：脱敏 + 截断。"""
    try:
        blob = json.dumps(scrub_payload(payload, secrets), ensure_ascii=False, default=str)
    except (TypeError, ValueError):  # pragma: no cover - 兜底
        blob = repr(payload)
    return blob[:limit]
