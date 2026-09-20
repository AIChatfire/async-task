"""应用容器与策略解析（§11 认证 / §12.2 策略组 / §4.3 限额）。

认证模型：**上游凭证随 Authorization 头透传**（数据面不存 key、不落盘）。
所以这里对请求只做三件事：取到凭证原值、算出 ``key_hash``（派生幂等键要用）、
确定 channel/tenant 归属；**不解析、不改写、不持久化**凭证。

策略组优先级：**渠道策略 > 模板 strategy > 全局默认**。
灰度：渠道维度按比例绑模板新版本，权重归零即回滚。
"""

from __future__ import annotations

import hmac
import random
from dataclasses import dataclass, field
from typing import Any, Mapping

from fastapi import HTTPException, Request

from ..config import Settings, get_settings
from ..infra.concurrency import (
    ConcurrencyLimiter,
    MemoryAcceptRateLimiter,
    MemoryConcurrencyLimiter,
    RedisAcceptRateLimiter,
    RedisConcurrencyLimiter,
)
from ..infra.object_store import ResultStore, get_result_store
from ..infra.polling import MemoryPollingController, PollingController, RedisPollingController
from ..templates.registry import TemplateRegistry, TemplateVersion
from ..upstream.client import UpstreamClient

CHANNEL_HEADER = "x-ag-channel"
TENANT_HEADER = "x-ag-tenant"
UPSTREAM_KEY_HEADER = "x-ag-upstream-key"

#: 策略组默认可被渠道覆盖的键
OVERRIDABLE_KEYS: frozenset[str] = frozenset(get_settings().model_dump().keys()) | frozenset(
    {
        "channel_concurrency",
        "tenant_concurrency",
        "accept_rate_per_second",
        "accept_burst",
        "max_attempts",
        "deadline_seconds",
        "idempotency_window_seconds",
        "min_refresh_interval",
        "retention_days",
        "presign_ttl_seconds",
        "unknown_max_per_channel",
        "unknown_max_lifetime_seconds",
        "submit_confirm_window_seconds",
    }
)


@dataclass(slots=True)
class AuthContext:
    channel: str
    tenant: str
    bearer: str | None
    key_hash: str
    actor: str
    mode: str
    authenticated: bool = True

    @property
    def scope(self) -> tuple[str, str]:
        return self.channel, self.tenant


def select_version(tv_list: list[TemplateVersion], canary: Mapping[str, Any] | None, alias: str) -> TemplateVersion | None:
    """渠道维度按比例绑版本；权重归零 = 回滚到无该版本。"""
    candidates = [tv for tv in tv_list if tv.enabled]
    if not candidates:
        return None
    binding = (canary or {}).get(alias) or {}
    weighted: list[tuple[TemplateVersion, float]] = []
    for tv in candidates:
        raw_weight = binding.get(str(tv.version))
        weight = 1.0 if raw_weight is None and not binding else float(raw_weight or 0.0)
        if weight > 0:
            weighted.append((tv, weight))
    if not weighted:
        return candidates[-1]
    total = sum(w for _, w in weighted)
    if total <= 0:
        return candidates[-1]
    pick = random.uniform(0, total)
    acc = 0.0
    for tv, weight in weighted:
        acc += weight
        if pick <= acc:
            return tv
    return weighted[-1][0]


class Container:
    """进程内依赖容器（不含 DB 会话；会话按请求开）。"""

    def __init__(
        self,
        *,
        settings: Settings | None = None,
        registry: TemplateRegistry | None = None,
        limiter: ConcurrencyLimiter | None = None,
        accept_limiter: Any | None = None,
        polling: PollingController | None = None,
        result_store: ResultStore | None = None,
        channel_policies: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.registry = registry or TemplateRegistry()
        self._redis_ready = not self.settings.is_test and bool(self.settings.redis_url)
        self.limiter = limiter or (
            RedisConcurrencyLimiter() if self._redis_ready else MemoryConcurrencyLimiter()
        )
        self.accept_limiter = accept_limiter or (
            RedisAcceptRateLimiter(self.settings.accept_rate_per_second, self.settings.accept_burst)
            if self._redis_ready
            else MemoryAcceptRateLimiter(self.settings.accept_rate_per_second, self.settings.accept_burst)
        )
        self.polling = polling or (
            RedisPollingController() if self._redis_ready else MemoryPollingController()
        )
        self.result_store = result_store or get_result_store()
        #: 渠道策略缓存（生产由 DB 装载；此处支持静态注入，便于测试与单机联调）
        self.channel_policies: dict[str, dict[str, Any]] = channel_policies or {}
        self._bus: Any | None = None
        #: 上游 transport 工厂（测试注入 mock；生产注入固定出口代理）
        self.transport_factory: Any | None = None

    # ---- 模板 ----
    def template_for_channel(self, channel: str, alias: str) -> TemplateVersion:
        policy = self.channel_policies.get(channel) or {}
        versions = [tv for tv in self.registry.all_versions() if tv.alias == alias]
        tv = select_version(versions, policy.get("canary"), alias)
        if tv is None:
            raise KeyError(f"unknown template alias: {alias}")
        return tv

    def template_for(self, alias: str, version: int | None = None) -> TemplateVersion:
        return self.registry.require(alias, version)

    # ---- 策略组 ----
    def strategy_for(self, channel: str, tv: TemplateVersion) -> dict[str, Any]:
        merged: dict[str, Any] = dict(tv.resolved.strategy)
        policy = self.channel_policies.get(channel) or {}
        merged.update({k: v for k, v in (policy.get("policy") or {}).items() if k in OVERRIDABLE_KEYS})
        return merged

    def channel_limits(self, channel: str) -> tuple[int, int]:
        policy = self.channel_policies.get(channel) or {}
        overrides = policy.get("policy") or {}
        channel_limit = int(overrides.get("channel_concurrency", self.settings.channel_concurrency))
        tenant_limit = int(overrides.get("tenant_concurrency", self.settings.tenant_concurrency))
        return channel_limit, tenant_limit

    def url_direct_enabled(self, channel: str) -> bool:
        policy = self.channel_policies.get(channel) or {}
        return bool(policy.get("url_direct_enabled", self.settings.url_direct_config_enabled))

    # ---- 上游 ----
    def upstream_client(self, bearer: str | None = None, *, pin_dns: bool | None = None) -> UpstreamClient:
        """每次调用一个短生命周期客户端：凭证只活在这一次请求里，不落任何地方。

        ``transport_factory`` 供测试注入 mock transport，或生产注入固定出口代理——
        出口白名单在代理层再执行一次（平台层兜底，§18.7）。
        """
        client = UpstreamClient(
            auth_header=self.settings.upstream_auth_header,
            pin_dns=pin_dns,
            transport=self.transport_factory() if self.transport_factory else None,
        )
        client.remember_secret(bearer)
        return client

    # ---- 凭证（数据面：短生命周期存放，见 infra/credentials）----
    @property
    def credential_store(self):
        from ..infra.credentials import get_credential_store

        return get_credential_store()

    # ---- 队列 ----
    @property
    def bus(self):
        if self._bus is None:
            from ..bus.factory import make_bus

            self._bus = make_bus(self.settings)
        return self._bus

    def set_bus(self, bus) -> None:
        self._bus = bus

    # ---- 认证 ----
    def authenticate(self, request: Request, alias: str) -> AuthContext:
        headers = request.headers
        channel = (headers.get(CHANNEL_HEADER) or alias).strip() or alias
        tenant = (headers.get(TENANT_HEADER) or "default").strip() or "default"
        raw_auth = headers.get("authorization") or ""

        if self.settings.auth_mode == "admin":
            token = raw_auth[7:].strip() if raw_auth.lower().startswith("bearer ") else raw_auth.strip()
            if not token or not hmac.compare_digest(token, self.settings.admin_token):
                raise HTTPException(status_code=401, detail="admin credential required")
            upstream_key = headers.get(UPSTREAM_KEY_HEADER)
            from ..gateway.idempotency import key_hash_of

            return AuthContext(
                channel=channel,
                tenant=tenant,
                bearer=upstream_key,
                key_hash=key_hash_of(upstream_key or channel),
                actor="admin",
                mode="admin",
            )

        # passthrough：调用方须持有效上游凭证；网关只做"有没有"的检查，不做校验（上游自己会判）
        if not raw_auth.strip():
            raise HTTPException(status_code=401, detail="missing upstream credential")
        from ..gateway.idempotency import key_hash_of

        return AuthContext(
            channel=channel,
            tenant=tenant,
            bearer=raw_auth.strip(),
            key_hash=key_hash_of(raw_auth.strip()),
            actor=f"key:{key_hash_of(raw_auth.strip())[:8]}",
            mode="passthrough",
        )

    def assert_owned(self, task, auth: AuthContext) -> None:
        """归属校验（可选纵深，默认开启）：未命中统一 404，不区分"不存在"与"无权"。"""
        if not self.settings.ownership_check:
            return
        if task is None or task.channel != auth.channel or task.tenant != auth.tenant:
            raise HTTPException(status_code=404, detail="task not found")


_container: Container | None = None


def get_container() -> Container:
    global _container
    if _container is None:
        _container = Container()
    return _container


def configure_container(container: Container | None) -> None:
    global _container
    _container = container


def container_dependency() -> Container:  # pragma: no cover - FastAPI 依赖
    return get_container()


__all__ = [
    "AuthContext",
    "CHANNEL_HEADER",
    "Container",
    "TENANT_HEADER",
    "UPSTREAM_KEY_HEADER",
    "configure_container",
    "get_container",
    "select_version",
    "field",
]
