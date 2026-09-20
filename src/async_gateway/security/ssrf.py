"""SSRF 防线：**请求时刻**对最终 URL 做白名单 + 内网/metadata 判定（§18.1）。

要点：

* ``result_location`` 提取出的 URL 来自**上游响应体**，属不可信输入 → 结果拉取/转存
  同样要在**发请求前**校验，不能只在受理时校验一次。
* 显式 ``follow_redirects=False``；确需跟随则逐跳重验（:func:`validate_redirect_chain`）。
* resolve-and-pin：解析出 IP 后把连接**钉死**在这些 IP 上，防 DNS rebinding / TOCTOU；
  由 :class:`~async_gateway.upstream.client.PinnedTransport` 落地。
* 显式白名单（``allow_hosts``）是唯一的"允许私有地址"通道——用于把自家内网上游
  放进白名单，而不是靠关掉校验。
"""

from __future__ import annotations

import asyncio
import ipaddress
import socket
from dataclasses import dataclass
from typing import Iterable, Sequence
from urllib.parse import urlsplit

from ..config import get_settings

#: metadata / 云内网关键地址（永远拒绝，白名单也不放行）
ALWAYS_DENY_IPS: frozenset[str] = frozenset(
    {
        "169.254.169.254",       # AWS/GCP/Azure/阿里云 metadata
        "100.100.100.200",       # 阿里云 metadata
        "169.254.170.2",         # ECS task metadata
        "fd00:ec2::254",         # AWS IPv6 metadata
    }
)

#: 常见内网服务端口：非白名单主机上一律拒绝（纵深防御）
DENY_PORTS: frozenset[int] = frozenset({22, 23, 25, 111, 445, 1433, 3306, 5432, 6379, 9200, 11211, 27017})


class SsrfViolation(ValueError):
    """出站目标被 SSRF 策略拒绝。"""


@dataclass(frozen=True, slots=True)
class ValidatedTarget:
    url: str
    scheme: str
    host: str
    port: int
    addresses: tuple[str, ...]
    allowlisted: bool = False

    @property
    def origin(self) -> str:
        return f"{self.scheme}://{self.host}:{self.port}"


def _parse(url: str) -> tuple[str, str, int]:
    parts = urlsplit(url)
    if parts.scheme.lower() not in ("http", "https"):
        raise SsrfViolation(f"仅允许 http/https，收到 scheme={parts.scheme!r}")
    if parts.username or parts.password:
        raise SsrfViolation("URL 内禁止携带凭证")
    host = (parts.hostname or "").lower()
    if not host:
        raise SsrfViolation("URL 缺少主机名")
    port = parts.port or (443 if parts.scheme.lower() == "https" else 80)
    return parts.scheme.lower(), host, port


def _host_allowlisted(host: str, allow_hosts: Sequence[str]) -> bool:
    """白名单匹配。**metadata 地址永远不因白名单而放行**——否则"把自家内网加白名单"
    就会被当成"把云元数据服务加白名单"，那是最经典的一条提权路径。
    """
    host = host.lower()
    if host in ALWAYS_DENY_IPS:
        return False
    for allowed in allow_hosts:
        allowed = allowed.lower().strip()
        if not allowed:
            continue
        if allowed.startswith("*."):
            if host.endswith(allowed[1:]):
                return True
        elif host == allowed or host.endswith("." + allowed):
            return True
    return False


def is_forbidden_ip(ip: str | ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    """地址是否属于内网/回环/链路本地/保留段。"""
    try:
        addr = ipaddress.ip_address(ip) if isinstance(ip, str) else ip
    except ValueError:
        return True
    if str(addr) in ALWAYS_DENY_IPS:
        return True
    if isinstance(addr, ipaddress.IPv6Address):
        if addr.ipv4_mapped is not None:
            return is_forbidden_ip(addr.ipv4_mapped)
        if addr in ipaddress.ip_network("fc00::/7"):     # ULA
            return True
        if addr in ipaddress.ip_network("fe80::/10"):    # link-local
            return True
    return bool(
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
    )


def resolve_addresses(host: str, port: int) -> tuple[str, ...]:
    """同步 DNS 解析（测试与校验用）。"""
    try:
        infos = socket.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise SsrfViolation(f"DNS 解析失败：{host}（{exc}）") from exc
    return tuple(sorted({info[4][0] for info in infos}))


async def resolve_addresses_async(host: str, port: int) -> tuple[str, ...]:
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(host, port, proto=socket.IPPROTO_TCP)
    except OSError as exc:
        raise SsrfViolation(f"DNS 解析失败：{host}（{exc}）") from exc
    return tuple(sorted({info[4][0] for info in infos}))


def validate_target(
    url: str,
    *,
    allow_hosts: Iterable[str] | None = None,
    allow_schemes: Sequence[str] | None = None,
    deny_private: bool = True,
    resolve: bool = True,
) -> ValidatedTarget:
    """同步校验出站目标（受理前预校验、测试、巡检均可用）。"""
    settings = get_settings()
    allow_hosts = list(allow_hosts if allow_hosts is not None else settings.ssrf_allow_hosts)
    allow_schemes = list(allow_schemes if allow_schemes is not None else settings.ssrf_allow_schemes)

    scheme, host, port = _parse(url)
    if scheme not in [s.lower() for s in allow_schemes]:
        raise SsrfViolation(f"scheme {scheme!r} 不在白名单 {allow_schemes}")

    allowlisted = _host_allowlisted(host, allow_hosts)
    if not allowlisted and deny_private and port in DENY_PORTS:
        raise SsrfViolation(f"端口 {port} 属内网服务端口，非白名单主机不允许访问")

    if not resolve:
        return ValidatedTarget(url, scheme, host, port, (), allowlisted)

    addresses = resolve_addresses(host, port)
    if not addresses:
        raise SsrfViolation(f"DNS 未返回地址：{host}")
    if not allowlisted:
        bad = [a for a in addresses if is_forbidden_ip(a)]
        if bad:
            raise SsrfViolation(f"目标解析到内网/保留地址，已拒绝：{host} -> {bad}")
    return ValidatedTarget(url, scheme, host, port, addresses, allowlisted)


async def avalidate_target(
    url: str,
    *,
    allow_hosts: Iterable[str] | None = None,
    allow_schemes: Sequence[str] | None = None,
    deny_private: bool = True,
    resolve: bool = True,
) -> ValidatedTarget:
    """异步校验出站目标（运行路径使用，避免阻塞事件循环）。"""
    settings = get_settings()
    allow_hosts = list(allow_hosts if allow_hosts is not None else settings.ssrf_allow_hosts)
    allow_schemes = list(allow_schemes if allow_schemes is not None else settings.ssrf_allow_schemes)

    scheme, host, port = _parse(url)
    if scheme not in [s.lower() for s in allow_schemes]:
        raise SsrfViolation(f"scheme {scheme!r} 不在白名单 {allow_schemes}")
    allowlisted = _host_allowlisted(host, allow_hosts)
    if not allowlisted and deny_private and port in DENY_PORTS:
        raise SsrfViolation(f"端口 {port} 属内网服务端口，非白名单主机不允许访问")
    if not resolve:
        return ValidatedTarget(url, scheme, host, port, (), allowlisted)
    addresses = await resolve_addresses_async(host, port)
    if not addresses:
        raise SsrfViolation(f"DNS 未返回地址：{host}")
    if not allowlisted:
        bad = [a for a in addresses if is_forbidden_ip(a)]
        if bad:
            raise SsrfViolation(f"目标解析到内网/保留地址，已拒绝：{host} -> {bad}")
    return ValidatedTarget(url, scheme, host, port, addresses, allowlisted)


def validate_redirect_chain(
    hops: Sequence[str],
    *,
    allow_hosts: Iterable[str] | None = None,
) -> tuple[ValidatedTarget, ...]:
    """逐跳重验（确需跟随重定向时使用）。"""
    return tuple(validate_target(hop, allow_hosts=allow_hosts) for hop in hops)
