"""上游调用客户端（§11 凭证透传 / §18.1 SSRF / §4.1 错误分级）。

四件必须做对的事：

1. **Authorization 头透传，不落盘**：真实上游 key 由 New API 渠道持有，本模块只在
   内存里把它塞进请求头；日志/trace/审计一律走 :mod:`security.redaction`。
2. **不发重定向**：``follow_redirects=False``；确需跟随要逐跳重验。
3. **请求时刻 SSRF 校验**：包括**结果拉取/转存**——``result_location`` 提取出的 URL
   来自上游响应体，是不可信输入。
4. **resolve-and-pin**：把连接钉在已校验的 IP 上，防 DNS rebinding / TOCTOU。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

import httpx

from ..config import get_settings
from ..domain.errors import Classification, classify_exception, classify_status, extract_retry_after
from ..security.redaction import redact_headers, scrub_payload, scrub_text
from ..security.ssrf import SsrfViolation, ValidatedTarget, avalidate_target
from ..templates.expression import PathNotFound, try_extract
from ..templates.render import RenderedRequest
from ..templates.schema import ResolvedTemplate

Outcome = Literal["ok", "http_error", "transport_error", "ssrf_rejected", "timeout_unknown", "budget_exceeded"]


@dataclass(slots=True)
class UpstreamResponse:
    status_code: int
    headers: dict[str, str]
    json_body: Any | None
    text: str

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 300

    def redacted_headers(self) -> dict[str, str]:
        return redact_headers(self.headers)


@dataclass(slots=True)
class UpstreamCallResult:
    outcome: Outcome
    response: UpstreamResponse | None = None
    classification: Classification | None = None
    error: str | None = None
    retry_after: float | None = None
    target: ValidatedTarget | None = None

    @property
    def ok(self) -> bool:
        return self.outcome == "ok"

    @property
    def needs_confirm(self) -> bool:
        return self.classification is not None and self.classification.is_unknown

    def safe_error(self, secrets: list[str] | None = None) -> str | None:
        if self.error is None:
            return None
        return scrub_text(self.error, secrets or [])

    def safe_body(self, secrets: list[str] | None = None) -> Any | None:
        if self.response is None or self.response.json_body is None:
            return None
        return scrub_payload(self.response.json_body, secrets or [])


def pin_request(request: httpx.Request, pins: dict[str, tuple[str, ...]]) -> httpx.Request:
    """resolve-and-pin 的纯函数部分（便于单测）。

    把 URL 的 host 换成已校验的 IP，保留 ``Host`` 头与 TLS ``sni_hostname``，
    这样证书校验仍然针对原域名，而连接不会再走一次 DNS。
    """
    host = request.url.host
    addresses = pins.get(host)
    if not addresses:
        return request
    ip = addresses[0]
    try:
        request.url = request.url.copy_with(host=ip)
    except Exception:  # pragma: no cover - 非法 IP 字面量兜底
        return request
    default_port = 443 if request.url.scheme == "https" else 80
    port = request.url.port or default_port
    request.headers["Host"] = host if port == default_port else f"{host}:{port}"
    extensions = dict(request.extensions)
    extensions["sni_hostname"] = host
    request.extensions = extensions
    return request


class PinnedTransport(httpx.AsyncHTTPTransport):
    """把已校验 IP 钉进连接层的 transport。"""

    def __init__(self, pins: dict[str, tuple[str, ...]] | None = None, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.pins: dict[str, tuple[str, ...]] = pins or {}

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return await super().handle_async_request(pin_request(request, self.pins))


def _timeout_for(context: str) -> httpx.Timeout:
    s = get_settings()
    if context == "poll":
        return httpx.Timeout(connect=s.submit_connect_timeout, read=s.poll_read_timeout, write=10.0, pool=5.0)
    return httpx.Timeout(connect=s.submit_connect_timeout, read=s.submit_read_timeout, write=15.0, pool=5.0)


class UpstreamClient:
    """按模板渲染好的请求 → 上游。"""

    def __init__(
        self,
        *,
        auth_header: str | None = None,
        pin_dns: bool | None = None,
        allow_hosts: list[str] | None = None,
        client: httpx.AsyncClient | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        max_response_bytes: int = 4 * 1024 * 1024,
    ) -> None:
        s = get_settings()
        self.auth_header = auth_header or s.upstream_auth_header
        self.pin_dns = s.ssrf_pin_dns if pin_dns is None else pin_dns
        self.allow_hosts = allow_hosts if allow_hosts is not None else s.ssrf_allow_hosts
        self.max_response_bytes = max_response_bytes
        self._external_client = client is not None
        self._client = client
        #: 默认 transport（测试注入 mock / 生产注入出口代理）；优先级：call 参数 > 默认 > pin
        self._default_transport = transport
        self.secrets: list[str] = []

    def remember_secret(self, value: str | None) -> None:
        """把数据面凭证值登记进内存，供响应体反向扫描抹除（§17）。"""
        if value and value not in self.secrets:
            self.secrets.append(value)

    async def __aenter__(self) -> UpstreamClient:
        return self

    async def __aexit__(self, *exc: Any) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        if not self._external_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    def _ensure_client(self, transport: httpx.AsyncBaseTransport | None = None) -> httpx.AsyncClient:
        if self._client is None:
            kwargs: dict[str, Any] = {"follow_redirects": False}
            chosen = transport or self._default_transport
            if chosen is not None:
                kwargs["transport"] = chosen
            self._client = httpx.AsyncClient(**kwargs)
        return self._client

    async def call(
        self,
        request: RenderedRequest,
        *,
        context: str = "submit",
        bearer: str | None = None,
        extra_headers: dict[str, str] | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> UpstreamCallResult:
        """发一次出站请求：先 SSRF 校验，再发，再按三级分类归因。"""
        # 1) 请求时刻校验最终 URL
        try:
            target = await avalidate_target(
                request.url,
                allow_hosts=self.allow_hosts,
                resolve=True,
                deny_private=get_settings().ssrf_deny_private,
            )
        except SsrfViolation as exc:
            return UpstreamCallResult(outcome="ssrf_rejected", error=str(exc))

        headers = dict(request.headers)
        headers.update(extra_headers or {})
        if bearer:
            self.remember_secret(bearer)
            headers[self.auth_header] = bearer

        pins: dict[str, tuple[str, ...]] = {}
        if self.pin_dns and target.addresses:
            pins[target.host] = target.addresses
        # transport 选择顺序：显式传入 > 默认（测试替身/出口代理）> resolve-and-pin 钉住
        active_transport = transport or self._default_transport
        if active_transport is None and pins:
            active_transport = PinnedTransport(pins)
        active_client = client or self._client
        if active_client is None:
            active_client = self._ensure_client(active_transport)

        try:
            response = await active_client.request(
                request.method,
                request.url,
                headers=headers,
                json=request.json_body,
                timeout=_timeout_for(context),
            )
        except httpx.HTTPError as exc:
            classification = classify_exception(exc, context)  # type: ignore[arg-type]
            outcome: Outcome = "timeout_unknown" if classification.is_unknown else "transport_error"
            return UpstreamCallResult(
                outcome=outcome,
                classification=classification,
                error=f"{type(exc).__name__}: {exc}",
                target=target,
            )

        raw = response.content[: self.max_response_bytes]
        parsed: Any | None = None
        try:
            parsed = response.json()
        except Exception:  # noqa: BLE001 - 非 JSON 响应合法
            parsed = None
        up = UpstreamResponse(
            status_code=response.status_code,
            headers={k.lower(): v for k, v in response.headers.items()},
            json_body=scrub_payload(parsed, self.secrets) if parsed is not None else None,
            text=scrub_text(raw.decode("utf-8", errors="replace"), self.secrets),
        )
        if up.ok:
            return UpstreamCallResult(outcome="ok", response=up, target=target)

        classification = classify_status(up.status_code, context)  # type: ignore[arg-type]
        return UpstreamCallResult(
            outcome="http_error",
            response=up,
            classification=classification,
            error=f"upstream HTTP {up.status_code}",
            retry_after=extract_retry_after(up.headers) if up.status_code == 429 else None,
            target=target,
        )

    # ---- 结果拉取 / 转存（同样要过 SSRF）----
    async def fetch_result(
        self,
        url: str,
        *,
        bearer: str | None = None,
        max_bytes: int | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> tuple[bool, bytes | str]:
        """拉取结果体。返回 ``(是否成功, bytes 或错误说明)``。"""
        try:
            target = await avalidate_target(url, allow_hosts=self.allow_hosts, resolve=True)
        except SsrfViolation as exc:
            return False, f"ssrf_rejected: {exc}"
        headers: dict[str, str] = {}
        if bearer:
            self.remember_secret(bearer)
            headers[self.auth_header] = bearer
        pins = {target.host: target.addresses} if self.pin_dns and target.addresses else {}
        transport = self._default_transport or (PinnedTransport(pins) if pins else None)
        active_client = client or self._client
        if active_client is None:
            active_client = self._ensure_client(transport)
        limit = max_bytes or self.max_response_bytes
        try:
            response = await active_client.get(
                url,
                headers=headers,
                follow_redirects=False,
                timeout=_timeout_for("result"),
            )
        except httpx.HTTPError as exc:
            return False, f"transport: {type(exc).__name__}: {exc}"
        if not (200 <= response.status_code < 300):
            # 结果拉取只允许直连，3xx 一律视为"需要跟随重定向"→ 交给上层逐跳校验
            return False, f"http {response.status_code}"
        if len(response.content) > limit:
            return False, f"result too large: {len(response.content)}B > {limit}B"
        return True, response.content

    # ---- 模板驱动的字段提取 ----
    @staticmethod
    def extract_id(template: ResolvedTemplate, payload: Any) -> tuple[bool, Any]:
        return try_extract(payload, template.id_location)

    @staticmethod
    def extract_status(template: ResolvedTemplate, payload: Any) -> tuple[bool, Any]:
        return try_extract(payload, template.status_field)

    @staticmethod
    def extract_result(template: ResolvedTemplate, payload: Any) -> tuple[bool, Any]:
        return try_extract(payload, template.result_location)


def probe_health(result: UpstreamCallResult) -> dict[str, Any]:
    """给巡检/看板用的一行摘要（已脱敏）。"""
    return {
        "outcome": result.outcome,
        "status_code": result.response.status_code if result.response else None,
        "error": result.safe_error(),
        "error_class": result.classification.error_class.value if result.classification else None,
    }


__all__ = [
    "PinnedTransport",
    "UpstreamCallResult",
    "UpstreamClient",
    "UpstreamResponse",
    "PathNotFound",
    "pin_request",
    "probe_health",
    "field",
]
