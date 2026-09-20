"""请求渲染（§12.3 预览接口：渲染请求样例）。

预览接口要给出"**实际将发出的 HTTP 请求**"，所以渲染必须与运行时共用同一份实现，
不能各写一套。渲染阶段就做两件安全事：

* 插值净化（``{id}`` 取自上游响应体，属不可信输入）
* 拼接结果必须仍在 ``base_url`` 的同源之下（防绝对 URL 注入把请求带去第三方）
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

from ..security.sanitize import UnsafeInterpolationValue, sanitize_interpolation_value
from .schema import ResolvedTemplate


class RenderError(ValueError):
    """渲染失败（占位符缺失、插值越界、脱离 base_url 同源等）。"""


@dataclass(frozen=True, slots=True)
class RenderedRequest:
    method: str
    url: str
    headers: dict[str, str] = field(default_factory=dict)
    json_body: dict[str, Any] | None = None

    def summary(self, *, redacted_headers: bool = True) -> dict[str, Any]:
        headers = dict(self.headers)
        if redacted_headers:
            for key in list(headers):
                if key.lower() in {"authorization", "x-api-key", "api-key", "cookie"}:
                    headers[key] = "***redacted***"
        return {
            "method": self.method,
            "url": self.url,
            "headers": headers,
            "json_body": self.json_body,
        }


_PLACEHOLDERS = ("{id}",)


def render_path(template: str, values: dict[str, Any]) -> str:
    """替换路径占位符并净化取值。"""
    out = template
    for placeholder in _PLACEHOLDERS:
        if placeholder in out:
            key = placeholder.strip("{}")
            if key not in values:
                raise RenderError(f"路径需要 {placeholder} 但未提供取值：{template}")
            try:
                safe = sanitize_interpolation_value(values[key], field=key)
            except UnsafeInterpolationValue as exc:
                raise RenderError(str(exc)) from exc
            out = out.replace(placeholder, safe)
    if "{" in out or "}" in out:
        raise RenderError(f"路径含未解析占位符：{template}")
    if ".." in out:
        raise RenderError(f"路径含路径穿越：{template}")
    return out


def join_url(base_url: str, path: str) -> str:
    """拼接并校验未脱离 base_url 同源。"""
    base = base_url.rstrip("/") + "/"
    url = urljoin(base, path.lstrip("/"))
    base_parts, url_parts = urlparse(base), urlparse(url)
    if (base_parts.scheme, base_parts.hostname, base_parts.port) != (
        url_parts.scheme,
        url_parts.hostname,
        url_parts.port,
    ):
        raise RenderError(f"渲染结果脱离 base_url 同源：{url}")
    return url


def base_headers(resolved: ResolvedTemplate) -> dict[str, str]:
    headers = {"Content-Type": "application/json", "Accept": "application/json"}
    headers.update(resolved.extra_headers)
    return headers


def render_create(
    resolved: ResolvedTemplate,
    body: dict[str, Any],
    *,
    callback_url: str | None = None,
) -> RenderedRequest:
    payload = dict(body)
    if callback_url and resolved.capabilities.callback:
        payload[resolved.callback_param] = callback_url
    return RenderedRequest(
        method=resolved.create_method,
        url=join_url(resolved.base_url, resolved.create_path),
        headers=base_headers(resolved),
        json_body=payload,
    )


def render_get(resolved: ResolvedTemplate, upstream_task_id: str) -> RenderedRequest:
    path = render_path(resolved.get_path_template, {"id": upstream_task_id})
    return RenderedRequest(
        method=resolved.get_method,
        url=join_url(resolved.base_url, path),
        headers=base_headers(resolved),
    )


def render_cancel(resolved: ResolvedTemplate, upstream_task_id: str) -> RenderedRequest:
    if not resolved.capabilities.cancel:
        raise RenderError(f"{resolved.alias} 的上游不支持取消（capabilities.cancel=false）")
    path = render_path(resolved.cancel_path_template, {"id": upstream_task_id})
    return RenderedRequest(
        method=resolved.cancel_method,
        url=join_url(resolved.base_url, path),
        headers=base_headers(resolved),
    )


def preview_requests(
    resolved: ResolvedTemplate,
    *,
    sample_body: dict[str, Any] | None = None,
    sample_upstream_task_id: str = "task-abc123",
    callback_url: str | None = None,
) -> dict[str, Any]:
    """预览接口用的"渲染请求样例"（§12.3）。"""
    body = sample_body if sample_body is not None else {
        "model": "sample-model",
        "content": [{"type": "text", "text": "sample prompt"}],
    }
    out: dict[str, Any] = {
        "create": render_create(resolved, body, callback_url=callback_url).summary(),
    }
    out["get"] = render_get(resolved, sample_upstream_task_id).summary()
    if resolved.capabilities.cancel:
        out["cancel"] = render_cancel(resolved, sample_upstream_task_id).summary()
    else:
        out["cancel"] = {"supported": False, "degraded": "上游不支持取消，降级为 cancel_requested"}
    return out
