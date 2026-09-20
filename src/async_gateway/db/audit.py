"""审计：append-only + 哈希链锚定（§18.5）。

两条纪律：

* **只存引用/哈希**：审计记录里只有 task_id、payload digest、凭证引用名这类不可逆引用，
  不含个人负载与凭证值——这样 WORM 留存与"删除权"不冲突（删除权作用于任务数据）。
* **链式哈希**：每条记录带 ``prev_hash``，定期把链尾哈希导出 WORM 桶即可证明未被篡改；
  DB 层另需撤销治理账号对审计表的 UPDATE/DELETE 权限（见 alembic 迁移里的注释）。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from .models import AuditEvent


class AuditEventType:
    """§9 审计事件清单（P2-2 补全）。"""

    ACCEPTED = "task.accepted"
    CANCELLED = "task.cancelled"
    CALLBACK = "task.callback"
    TERMINAL = "task.terminal"
    TEMPLATE_CHANGED = "template.changed"
    TEMPLATE_PATCHED = "template.patched_in_place"
    TEMPLATE_CANARY = "template.canary_changed"
    CREDENTIAL_REF_CHANGED = "credential_ref.changed"
    SIGNATURE_FAILED = "callback.signature_failed"
    RESULT_READ = "result.read"
    RESULT_EXPORT = "result.export"
    DRY_RUN = "task.dry_run"
    REPLAY = "task.replay"
    GOVERNANCE = "governance.operation"


def _digest(payload: Any) -> str:
    blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, default=str)
    return hashlib.sha256(blob.encode()).hexdigest()


@dataclass(slots=True)
class AuditRecord:
    event_type: str
    actor: str
    actor_role: str | None
    subject_type: str | None
    subject_id: str | None
    payload_digest: str | None
    refs: dict[str, Any] | None
    detail: str | None
    prev_hash: str | None
    hash: str
    created_at: datetime


def _canonical_timestamp(value: datetime) -> str:
    """把时间戳规范成**跨后端可复算**的字符串。

    链式哈希要能被事后复算，就不能依赖时区往返：SQLite 读回来是 naive（UTC 墙钟），
    Postgres 读回来是 aware UTC。统一成"去掉 tz 的 UTC 墙钟 + 固定格式"即可两边一致。
    """
    if value.tzinfo is not None:
        value = value.astimezone(UTC).replace(tzinfo=None)
    return value.strftime("%Y-%m-%dT%H:%M:%S.%f")


def compute_hash(
    *,
    event_type: str,
    actor: str,
    subject_id: str | None,
    payload_digest: str | None,
    prev_hash: str | None,
    created_at: datetime,
) -> str:
    material = "|".join(
        [
            prev_hash or "genesis",
            event_type,
            actor,
            subject_id or "-",
            payload_digest or "-",
            _canonical_timestamp(created_at),
        ]
    )
    return hashlib.sha256(material.encode()).hexdigest()


class AuditWriter:
    """把审计事件追加进链。写入方永远不提供 UPDATE/DELETE 路径。"""

    def __init__(self, session: AsyncSession) -> None:
        self.s = session

    async def append(
        self,
        event_type: str,
        *,
        actor: str,
        actor_role: str | None = None,
        subject_type: str | None = None,
        subject_id: str | None = None,
        payload: Any | None = None,
        refs: dict[str, Any] | None = None,
        detail: str | None = None,
    ) -> AuditEvent:
        prev = (
            await self.s.execute(select(AuditEvent).order_by(AuditEvent.id.desc()).limit(1))
        ).scalar_one_or_none()
        created_at = datetime.now(UTC)
        payload_digest = _digest(payload) if payload is not None else None
        chain_hash = compute_hash(
            event_type=event_type,
            actor=actor,
            subject_id=subject_id,
            payload_digest=payload_digest,
            prev_hash=prev.hash if prev else None,
            created_at=created_at,
        )
        row = AuditEvent(
            event_type=event_type,
            actor=actor,
            actor_role=actor_role,
            subject_type=subject_type,
            subject_id=subject_id,
            payload_digest=payload_digest,
            refs=refs,
            detail=detail,
            prev_hash=prev.hash if prev else None,
            hash=chain_hash,
            created_at=created_at,
        )
        self.s.add(row)
        await self.s.flush()
        return row

    async def verify_chain(self, *, limit: int = 1000) -> tuple[bool, str | None]:
        rows = (
            await self.s.execute(select(AuditEvent).order_by(AuditEvent.id.asc()).limit(limit))
        ).scalars().all()
        prev_hash: str | None = None
        for row in rows:
            expected = compute_hash(
                event_type=row.event_type,
                actor=row.actor,
                subject_id=row.subject_id,
                payload_digest=row.payload_digest,
                prev_hash=prev_hash,
                created_at=row.created_at,
            )
            if row.prev_hash != prev_hash:
                return False, f"audit chain broken at id={row.id} (prev_hash mismatch)"
            if row.hash != expected:
                return False, f"audit chain broken at id={row.id} (hash mismatch)"
            prev_hash = row.hash
        return True, None

    @staticmethod
    def tail_hash(rows: list[AuditEvent]) -> str | None:
        return rows[-1].hash if rows else None
