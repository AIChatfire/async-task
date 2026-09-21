"""配置与策略组默认值（§12.2 策略组 / §4.3 配额 / §15 轮询 / §18 安全）。

所有变量以 ``AG_`` 前缀注入，嵌套结构用双下划线。策略组的优先级：
渠道覆盖(row) > 模板显式 > 本文件默认（见 :func:`load_strategy_defaults`）。
"""

from __future__ import annotations

import json
from functools import lru_cache
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_prefix="AG_",
        env_nested_delimiter="__",
        extra="ignore",
        case_sensitive=False,
    )

    # ---- 基础 ----
    app_env: Literal["dev", "test", "prod"] = "dev"
    log_level: str = "INFO"
    logfire_token: str | None = None
    service_name: str = "async-gateway"

    # ---- 真相库 ----
    database_url: str = "postgresql+asyncpg://async_gateway:async_gateway@localhost:5432/async_gateway"
    db_echo: bool = False
    db_pool_size: int = 10
    db_max_overflow: int = 20

    # ---- broker / 缓存 ----
    redis_url: str = "redis://localhost:6379/0"
    queue_driver: Literal["stream", "taskiq", "memory"] = "stream"
    queue_stream_prefix: str = "ag:q:"
    queue_group: str = "ag-workers"
    queue_max_depth: int = 100_000

    # ---- 对象存储 ----
    # 默认**对接外部对象存储**（生产口径）。本地联调由 docker-compose 的
    # `${AG_S3_*:-...}` 兜底覆盖成本地 MinIO（见 docker-compose.yml）。
    # 凭据**故意不给默认值**：写 `minioadmin` 这类假默认，只会在配置遗漏时静默连上错误的对象存储；
    # 留空则第一次使用就明确失败 —— 与"不自动建桶"同一取向（部署错误要立刻暴露）。
    s3_endpoint: str = "https://oss.s3ai.cn"
    s3_access_key: str = ""
    s3_secret_key: str = ""
    s3_bucket: str = "cdn"
    s3_secure: bool = True
    result_store_mode: Literal["object", "memory"] = "object"

    # ---- 策略组默认值 ----
    idempotency_window_seconds: int = 86_400
    max_attempts: int = 3
    task_deadline_seconds: int = 1800
    result_retention_days: int = 30
    result_presign_ttl_seconds: int = 900
    transfer_max_inline_bytes: int = 1_048_576
    #: 结果**文件**单次拉取上限（转存用）。
    #:
    #: 与"上游 API 响应体上限"（``UpstreamClient.max_response_bytes``，4 MiB）是**两件事**：
    #: 前者是 JSON 状态响应，后者是产物本体。实测火山 3D 产物用 obj + 高细分即 41 MB，
    #: 视频产物量级更高——沿用 4 MiB 会让转存在大小检查处确定性失败
    #: （表现为 ``succeeded`` + ``result_degraded=['transfer_failed']`` + 结果端点 410）。
    result_max_bytes: int = 268_435_456
    #: 结果策略模式的**全局默认**（模板未显式声明 ``result_policy.mode`` 时生效）。
    #:
    #: 默认 ``passthrough``（不转存、直接给上游直链）的理由：转存链路依赖对象存储
    #: （MinIO/S3）与"存储地址对调用方可达"这两件在本环境**尚未验证**的事，且上游产物
    #: 常见几十 MB；先直链可以让链路一次跑通，待对象存储验完再按模板/渠道逐个切回 ``store``。
    #: 代价：结果链接依赖上游有效期（火山 TOS 预签名 24h），且上游直链会暴露给调用方
    #: （envelope ``degraded[]`` 会声明该依赖）。
    result_mode_default: Literal["store", "passthrough", "redirect"] = "passthrough"
    submit_confirm_window_seconds: int = 120

    # 背压：受理速率配额 与 上游并发槽 分离计数（§4.3）
    accept_rate_per_second: float = 20.0
    accept_burst: int = 40
    channel_concurrency: int = 16
    tenant_concurrency: int = 8

    # 自适应轮询（§15）
    poll_base_interval: float = 3.0
    poll_min_interval: float = 3.0
    poll_max_interval: float = 60.0
    poll_initial_interval: float = 5.0
    poll_hot_start_samples: int = 30
    poll_histogram_window_days: int = 7
    poll_recompute_seconds: int = 300
    poll_hard_timeout_seconds: float = 10.0

    # 查询面（§12.2）
    min_refresh_interval: float = 3.0
    query_429_circuit_ratio: float = 0.05
    query_429_circuit_window_seconds: int = 60

    # unknown 有界化（§4.4 / §14）
    unknown_max_per_channel: int = 200
    unknown_max_lifetime_seconds: int = 3600

    # 受理侧超时（提交上游）
    submit_connect_timeout: float = 5.0
    submit_read_timeout: float = 30.0
    poll_read_timeout: float = 10.0

    # ---- 认证（§11 / §18.2）----
    auth_mode: Literal["passthrough", "admin"] = "passthrough"
    ownership_check: bool = True
    upstream_auth_header: str = "Authorization"
    # 受理模式（2026-09-21 反转裁定，见 docs/IMPLEMENTATION.md §3.1）：
    #   queued —— **不等上游**：受理只落库 + 入队，立刻返回 202 `{"id","task_id","status"}`，
    #             上游 create 由 worker 后台完成（需要短生命周期凭证驻留，见 infra/credentials）。
    #             响应里的 id 是**网关任务 id**（提交时上游 id 还不存在），New API 两族插件
    #             （ark 系认 body.id / generic-async-v1 认 task_id||id）都能解析并按它查询。
    #   inline —— 受理请求内同步完成 create：返回上游原生形状（上游 id 原样透出）；
    #             上游受理慢会拖慢受理，仅"必须同步拿上游原生响应"的场景显式配置。
    submit_mode: Literal["inline", "queued"] = "queued"
    # 数据面凭证的短生命周期存放后端：redis_ephemeral | memory | none
    credential_channel: Literal["redis_ephemeral", "memory", "none"] = "redis_ephemeral"

    # ---- SSRF（§18.1）----
    ssrf_allow_hosts: list[str] = Field(default_factory=list)
    ssrf_allow_schemes: list[str] = Field(default_factory=lambda: ["https", "http"])
    ssrf_pin_dns: bool = True
    ssrf_deny_private: bool = True
    url_direct_config_enabled: bool = True
    url_direct_allowed_prefixes: list[str] = Field(default_factory=list)

    # ---- 回调（§18.3）----
    callback_base_url: str = "http://localhost:8000"
    callback_hmac_keys: dict[str, str] = Field(default_factory=lambda: {"k1": "dev-only-secret"})
    callback_active_kid: str = "k1"
    callback_tolerance_seconds: int = 300
    callback_rate_per_minute_per_channel: int = 6000

    # ---- 治理面（§18.4）----
    admin_token: str = "dev-admin-token"
    admin_require_approval: bool = True
    replay_rate_per_minute: int = 100

    # ---- 灰度（§17）----
    canary_min_samples: int = 100
    canary_error_sigma: float = 2.0
    canary_window_seconds: int = 300

    # ---- 巡检周期 ----
    scheduler_tick_seconds: float = 1.0
    scheduler_batch_size: int = 500
    inspector_tick_seconds: float = 15.0
    accepted_stall_seconds: int = 60
    concurrency_calibrate_seconds: int = 30

    @field_validator("ssrf_allow_hosts", "ssrf_allow_schemes", "url_direct_allowed_prefixes", mode="before")
    @classmethod
    def _parse_list(cls, v: object) -> object:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return []
            if v.startswith("["):
                return json.loads(v)
            return [item.strip() for item in v.split(",") if item.strip()]
        return v

    @field_validator("callback_hmac_keys", mode="before")
    @classmethod
    def _parse_keys(cls, v: object) -> object:
        if isinstance(v, str):
            v = v.strip()
            if not v:
                return {}
            if v.startswith("{"):
                return json.loads(v)
            out: dict[str, str] = {}
            for pair in v.split(","):
                if "=" in pair:
                    kid, secret = pair.split("=", 1)
                    out[kid.strip()] = secret.strip()
            return out
        return v

    # ---- 派生 ----
    @property
    def is_sqlite(self) -> bool:
        return self.database_url.startswith("sqlite")

    @property
    def is_test(self) -> bool:
        """**只有** ``app_env=test`` 才算测试环境。

        这里刻意不把 ``dev`` 也算进来：``dev`` 是 docker-compose 的环境
        （Redis/Postgres/MinIO 都在），若把它当测试环境，broker 会静默退化成**进程内**
        内存队列——网关受理后投的消息 worker 永远收不到，而且不报任何错。
        """
        return self.app_env == "test"

    @property
    def uses_ephemeral_infra(self) -> bool:
        """是否使用进程内的替身实现（内存 broker / 内存结果存储 / 内存凭证）。"""
        return self.is_test or not self.redis_url


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def reset_settings_cache() -> None:
    """测试用：清掉配置缓存。"""
    get_settings.cache_clear()


def load_strategy_defaults() -> dict[str, object]:
    """策略组默认值（模板 ``strategy`` / 渠道覆盖可逐项覆盖）。"""
    s = get_settings()
    return {
        "idempotency_window_seconds": s.idempotency_window_seconds,
        "max_attempts": s.max_attempts,
        "deadline_seconds": s.task_deadline_seconds,
        "min_refresh_interval": s.min_refresh_interval,
        "retention_days": s.result_retention_days,
        "presign_ttl_seconds": s.result_presign_ttl_seconds,
        "unknown_max_per_channel": s.unknown_max_per_channel,
        "unknown_max_lifetime_seconds": s.unknown_max_lifetime_seconds,
        "submit_confirm_window_seconds": s.submit_confirm_window_seconds,
    }


@lru_cache(maxsize=1)
def get_strategy_defaults_cached() -> dict[str, object]:
    return load_strategy_defaults()
