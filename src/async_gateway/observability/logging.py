"""日志与 trace（§17 强制脱敏清单）。

硬约束：**Authorization 头永不进日志/trace/审计**。这里的做法是白名单制——
``redact_headers`` / ``otel_safe_headers`` 只放行明确列出的头，其余一个都不采集；
结构化的上游错误响应入库前还要按凭证值反向扫描抹除（防 4xx 回显 token 片段）。
"""

from __future__ import annotations

import json
import logging
import sys
from typing import Any

from ..config import get_settings
from ..security.redaction import otel_safe_headers, safe_json_dump, scrub_text

_configured = False
_logfire_configured = False


class RedactingFilter(logging.Filter):
    """兜底：任何日志行里出现凭证形态就抹掉。"""

    def filter(self, record: logging.LogRecord) -> bool:  # noqa: A003
        if isinstance(record.msg, str):
            record.msg = scrub_text(record.msg)
        return True


def configure_logging() -> None:
    global _configured, _logfire_configured
    if _configured:
        return
    settings = get_settings()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    handler.addFilter(RedactingFilter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(settings.log_level.upper())
    _configured = True

    if settings.logfire_token:
        try:  # pragma: no cover - 需要真实 token
            import logfire

            logfire.configure(
                token=settings.logfire_token,
                service_name=settings.service_name,
                send_to_logfire="if-token-present",
            )
            logfire.instrument_httpx()
            _logfire_configured = True
        except Exception as exc:  # noqa: BLE001 - 可观测性不能拖垮主流程
            logging.getLogger(__name__).warning("logfire disabled: %s", exc)


def log_event(logger: logging.Logger, event: str, **fields: Any) -> None:
    """结构化日志：字段一律先脱敏。"""
    safe = {k: (v if k not in ("headers", "response") else _sanitize(v)) for k, v in fields.items()}
    logger.info("%s %s", event, safe_json_dump(safe))


def _sanitize(value: Any) -> Any:
    if isinstance(value, dict):
        return otel_safe_headers(value) if any(k.lower() in ("authorization", "cookie") for k in value) else value
    return value


def upstream_error_for_storage(payload: Any, secrets: list[str]) -> str:
    """上游错误体入库/展示前的统一出口。"""
    return safe_json_dump(payload, secrets, limit=4096)


def trace_headers_for_otel(headers: dict[str, str]) -> dict[str, str]:
    """给 OTel 采集器用的头（白名单制，凭证一个都不进）。"""
    return otel_safe_headers(headers)


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)
