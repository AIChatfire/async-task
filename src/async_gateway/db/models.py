"""数据模型（§13）。

三条不可让步的约束：

1. **大字段一律引用**：``result_ref`` 指向对象存储（转存产物，未配置对象存储时恒为空）；
   ``create_req_ref`` 指向 **Redis**（``infra/request_store`` 的短生命周期请求数据）——
   都不把请求体/结果体塞进行里。
2. **``audit_event`` 独立表 + append-only**，与任务表分离；看板聚合走只读副本。
3. **所有写路径带前置条件**——条件更新由 :mod:`async_gateway.db.dao` 统一实现，
   模型层用 ``status`` + 版本号 ``row_version`` 支撑 CAS。
"""

from __future__ import annotations

from datetime import UTC, datetime

from sqlalchemy import (
    JSON,
    Boolean,
    DateTime,
    Float,
    Index,
    Integer,
    String,
    Text,
    TypeDecorator,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import TIMESTAMP as PG_TIMESTAMP
from sqlalchemy.orm import Mapped, mapped_column

from ..domain.enums import Origin, TaskStatus
from .base import Base

JSONType = JSON().with_variant(JSONB(), "postgresql")


class UTCDateTime(TypeDecorator):
    """UTC 时间戳：写入统一为 aware UTC，读出时若为 naive 则补回 UTC。

    为什么必须有这一层：SQLite 不支持带时区的时间戳，读回来是 naive，于是任何
    "是否到期/是否停滞/存活多久" 的判断都会直接 ``TypeError: can't subtract
    offset-naive and offset-aware``；Postgres 返回的又是 aware。把**读语义**在这一层
    拉齐，业务代码只需要写一种时间比较，也避免把后端差异泄漏到领域逻辑里。
    """

    impl = DateTime
    cache_ok = True

    def load_dialect_impl(self, dialect):  # noqa: ANN001, ANN201
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_TIMESTAMP(timezone=True))
        return dialect.type_descriptor(DateTime())

    def process_bind_param(self, value, dialect):  # noqa: ANN001, ANN201
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)

    def process_result_value(self, value, dialect):  # noqa: ANN001, ANN201
        if value is None:
            return None
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)


def utcnow() -> datetime:
    return datetime.now(UTC)


class AsyncTask(Base):
    """异步任务：网关核心实体，状态机驱动。"""

    __tablename__ = "async_task"

    task_id: Mapped[str] = mapped_column(String(64), primary_key=True)

    # 幂等：显式键优先，否则派生 = key_hash + 请求体规范化哈希；重试派生 {key}#attempt{n}
    # window_bucket = floor(now / 窗口秒数) → (idempotency_key, window_bucket) 唯一
    # 即"窗口期内唯一"，窗口外同名键视为新请求。
    idempotency_key: Mapped[str] = mapped_column(String(160), nullable=False)
    idempotency_bucket: Mapped[int] = mapped_column(Integer, nullable=False)
    idempotency_window_seconds: Mapped[int] = mapped_column(Integer, nullable=False, default=86_400)

    tenant: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    task_type: Mapped[str] = mapped_column(String(64), nullable=False, default="default")

    status: Mapped[str] = mapped_column(String(32), nullable=False, default=TaskStatus.ACCEPTED.value)
    row_version: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # 受理时写入，随行归档（§13 P0-8）
    template_alias: Mapped[str] = mapped_column(String(64), nullable=False)
    template_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    upstream_task_id: Mapped[str | None] = mapped_column(String(191), nullable=True)

    # attempts = 提交尝试次数（每次 submit_upstream 执行前随提交意图预递增，恰好 +1）
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    # 提交意图标记：worker 调上游**之前**条件更新写入，用于区分"入队失败"与"已调上游未落库"
    submit_started_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)

    deadline_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    origin: Mapped[str] = mapped_column(String(16), nullable=False, default=Origin.USER.value)
    replay_of: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # 回调端点 token（不可预测，每任务一个；唯一索引支撑反查）
    callback_token: Mapped[str | None] = mapped_column(String(96), nullable=True, unique=True)

    # create 请求摘要：list_and_match 确认用（请求体存 Redis，行内只放 digest + 引用）
    create_req_ref: Mapped[str | None] = mapped_column(String(255), nullable=True)
    create_req_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # 结果：store 模式转存回引用（§12.2 result_policy；未配置对象存储 ⇒ 恒为空）
    result_ref: Mapped[str | None] = mapped_column(String(512), nullable=True)
    result_summary: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    result_degraded: Mapped[list | None] = mapped_column(JSONType, nullable=True)

    # 对外状态快照：透传上游原生响应（形状保真），仅用于查询面快照优先
    status_snapshot: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    status_snapshot_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    raw_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    final_output_status: Mapped[str | None] = mapped_column(String(64), nullable=True)

    error_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    retryable: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    cancel_requested: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    cancel_requested_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)

    # unknown 有界化依据
    unknown_since: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    confirm_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_confirmed_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)

    poll_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    next_poll_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    last_polled_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow, onupdate=utcnow
    )
    started_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    finished_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)
    trace_id: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # 因转存/回调/补偿产生的附加标记
    attributes: Mapped[dict | None] = mapped_column(JSONType, nullable=True)

    __table_args__ = (
        UniqueConstraint("idempotency_key", "idempotency_bucket", name="uq_task_idem_window"),
        Index("ix_task_tenant_channel_status", "tenant", "channel", "status"),
        # scheduler 的主查询：非终态 + next_poll_at 到期
        Index("ix_task_status_next_poll", "status", "next_poll_at"),
        # 回调反查 / orphan 对账（单列索引）
        Index("ix_task_upstream_task_id", "upstream_task_id"),
        Index("ix_task_created_at", "created_at"),
        Index("ix_task_origin", "origin"),
    )

    @property
    def is_terminal(self) -> bool:
        return TaskStatus(self.status) in (
            TaskStatus.SUCCEEDED,
            TaskStatus.CANCELLED,
            TaskStatus.TIMEOUT,
            TaskStatus.DEAD,
            TaskStatus.DEAD_AWAITING_CONFIRM,
        )


class CallbackEvent(Base):
    """回调去重表（§13 A7）：命中直接 200，不进状态机。"""

    __tablename__ = "callback_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    dedup_key: Mapped[str] = mapped_column(String(191), nullable=False, unique=True)
    upstream_task_id: Mapped[str | None] = mapped_column(String(191), nullable=True)
    raw_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    kid: Mapped[str | None] = mapped_column(String(32), nullable=True)
    payload: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow
    )

    __table_args__ = (Index("ix_cb_task", "channel", "upstream_task_id"),)


class OrphanCallback(Base):
    """网关侧查无此行的回调先暂存，由巡检对账归位（§14）。"""

    __tablename__ = "orphan_callback"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    upstream_task_id: Mapped[str] = mapped_column(String(191), nullable=False)
    raw_status: Mapped[str | None] = mapped_column(String(64), nullable=True)
    payload: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    received_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow
    )
    resolved: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    resolved_task_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(UTCDateTime(), nullable=True)

    __table_args__ = (
        Index("ix_orphan_lookup", "channel", "upstream_task_id", "resolved"),
    )


class UpstreamTemplateRow(Base):
    """模板版本化存储（§12.3/§13）：alias + 命名空间归属唯一 + version。"""

    __tablename__ = "upstream_template"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    alias: Mapped[str] = mapped_column(String(64), nullable=False)
    namespace: Mapped[str] = mapped_column(String(64), nullable=False)
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    config: Mapped[dict] = mapped_column(JSONType, nullable=False)
    config_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    patch_ids: Mapped[list | None] = mapped_column(JSONType, nullable=True)
    canary_weight: Mapped[float] = mapped_column(Float, nullable=False, default=1.0)
    # 就地勘误（仅判定逻辑）会改动既有版本，单列审计
    in_place_patches: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    created_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    approved_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow, onupdate=utcnow
    )

    __table_args__ = (
        UniqueConstraint("namespace", "alias", "version", name="uq_template_ns_alias_version"),
    )


class ChannelPolicyRow(Base):
    """渠道策略覆盖（§12.2 策略组 / §4.3 限额 / URL 直配开关 / 灰度绑定）。"""

    __tablename__ = "channel_policy"

    channel: Mapped[str] = mapped_column(String(64), primary_key=True)
    namespace: Mapped[str] = mapped_column(String(64), nullable=False, default="default")
    policy: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    # {alias: {version: weight}}；权重归零即回滚
    canary: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    url_direct_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow
    )
    updated_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow, onupdate=utcnow
    )


class AuditEvent(Base):
    """审计事件：append-only / WORM；**只存引用与哈希**，不含个人负载与凭证值（§18.5）。

    链式锚定：``prev_hash`` + ``hash``，配合定期导出 WORM 桶完成防篡改。
    """

    __tablename__ = "audit_event"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    event_type: Mapped[str] = mapped_column(String(48), nullable=False)
    actor: Mapped[str] = mapped_column(String(128), nullable=False)
    actor_role: Mapped[str | None] = mapped_column(String(32), nullable=True)
    subject_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    subject_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payload_digest: Mapped[str | None] = mapped_column(String(64), nullable=True)
    refs: Mapped[dict | None] = mapped_column(JSONType, nullable=True)
    detail: Mapped[str | None] = mapped_column(Text, nullable=True)
    prev_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    hash: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        UTCDateTime(), nullable=False, default=utcnow
    )

    __table_args__ = (
        Index("ix_audit_event_type_created", "event_type", "created_at"),
        Index("ix_audit_subject", "subject_type", "subject_id"),
    )


class MetricsRollup(Base):
    """分渠道健康度与灰度门控的小时级预聚合（§17；看板走这里，不扫任务表）。"""

    __tablename__ = "metrics_rollup"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    channel: Mapped[str] = mapped_column(String(64), nullable=False)
    template_alias: Mapped[str | None] = mapped_column(String(64), nullable=True)
    template_version: Mapped[int | None] = mapped_column(Integer, nullable=True)
    bucket: Mapped[datetime] = mapped_column(UTCDateTime(), nullable=False)
    requests: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    successes: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    failures: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    unknowns: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    rate_limited: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    p95_latency_ms: Mapped[float | None] = mapped_column(Float, nullable=True)

    __table_args__ = (
        UniqueConstraint("channel", "template_alias", "template_version", "bucket", name="uq_rollup"),
    )
