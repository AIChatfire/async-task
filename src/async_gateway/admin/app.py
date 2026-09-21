"""Task-admin 治理面（§18.4 / §9 / M5）。

治理面的红线是"**不绕过安全**"：

* 所有模板变更走**同一个校验器**（:mod:`async_gateway.templates.validator`），没有特权旁路；
* **职责分离**：提交人与审批人不得同一人；"内网可达"只作网络纵深，不作授权依据；
* **高危操作二级管控**（死信/unknown 重放、灰度变更、凭证引用变更、模板高危字段变更）：
  双人复核 + 执行前展示影响面（任务数、预计上游调用数）；
* **重放第一步强制确认查询**，且已有 ``upstream_task_id`` 的任务**禁止重放创建类动作**；
* 全部操作写 append-only 审计，审计只存引用/哈希。

身份来源：生产由企业 SSO 中间件（OIDC）注入 ``X-AG-Actor`` 与角色；本地/联调用
``AG_ADMIN_TOKEN`` 网关 + 同名头模拟。**审批票据当前是进程内实现**（见 docs/IMPLEMENTATION.md
"待确认决策"），单副本内网服务可接受，重启即失效属安全的一侧。
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Literal

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from ..config import get_settings
from ..db.audit import AuditEventType, AuditWriter, _digest
from ..db.base import session_scope
from ..db.dao import TaskDAO
from ..db.models import AsyncTask
from ..domain.enums import ErrorCode, Origin, TaskStatus
from ..domain.state_machine import is_business_terminal
from ..gateway.container import get_container
from ..gateway.idempotency import attempt_of, retry_key
from ..infra.credentials import credential_ttl_seconds
from ..infra.request_store import body_ttl_seconds, request_key
from ..observability.logging import configure_logging
from ..observability.metrics import GOVERNANCE_TOTAL, REGISTRY, render_metrics
from ..security.callback_auth import new_opaque_token
from ..security.redaction import scrub_payload
from ..templates.registry import ApplyMode, TemplateRegistry
from ..templates.render import preview_requests
from ..templates.schema import TemplateDraft
from ..templates.validator import validate
from ..tasks.queues import poll_queue

logger = logging.getLogger(__name__)

Role = Literal["template-author", "approver", "operator", "auditor"]
ALL_ROLES: tuple[str, ...] = ("template-author", "approver", "operator", "auditor")

#: 高危操作 → 需要的角色
HIGH_RISK_ACTIONS: dict[str, tuple[str, ...]] = {
    "template.publish": ("template-author", "approver"),
    "template.patch": ("template-author", "approver"),
    "template.canary": ("operator", "approver"),
    "task.replay": ("operator", "approver"),
    "credential_ref.change": ("operator", "approver"),
}

READONLY_REPLICA_NOTE = "看板/审计走只读副本（生产由连接串指向 replica；此处标明语义）"
ADMIN_APPROVAL_GUARD = REGISTRY.gauge("ag_admin_pending_approvals", "待审批票据数")
REPLAY_TOTAL = REGISTRY.counter("ag_admin_replay_total", "重放结果分布")


# ----------------------------------------------------------------------------- 身份
@dataclass(slots=True)
class Actor:
    name: str
    roles: tuple[str, ...]

    def has(self, *roles: str) -> bool:
        return any(r in self.roles for r in roles)

    @property
    def approver(self) -> bool:
        return self.has("approver")


def _parse_actor(request: Request) -> Actor:
    settings = get_settings()
    token = request.headers.get("x-ag-admin-token")
    authorization = request.headers.get("authorization", "")
    bearer = authorization[7:].strip() if authorization.lower().startswith("bearer ") else authorization
    if not (token == settings.admin_token or bearer == settings.admin_token):
        raise HTTPException(status_code=401, detail="admin credential required")
    name = (request.headers.get("x-ag-actor") or "").strip()
    if not name:
        raise HTTPException(status_code=401, detail="X-AG-Actor required (SSO subject)")
    roles_raw = (request.headers.get("x-ag-roles") or "").strip()
    roles = tuple(r.strip() for r in roles_raw.split(",") if r.strip())
    unknown = [r for r in roles if r not in ALL_ROLES]
    if unknown:
        raise HTTPException(status_code=403, detail=f"unknown roles: {unknown}")
    if not roles:
        raise HTTPException(status_code=403, detail="actor has no roles")
    return Actor(name=name, roles=roles)


def require(*roles: str):
    def _dep(request: Request) -> Actor:
        actor = _parse_actor(request)
        if not actor.has(*roles):
            raise HTTPException(status_code=403, detail=f"requires one of {roles}")
        return actor

    return _dep


# ----------------------------------------------------------------------------- 审批门票
@dataclass(slots=True)
class ApprovalTicket:
    ticket_id: str
    action: str
    subject: str
    payload_digest: str
    submitted_by: str
    impact: dict[str, Any] = field(default_factory=dict)
    approved_by: str | None = None
    approved_at: datetime | None = None
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    expires_at: datetime = field(default_factory=lambda: datetime.now(UTC) + timedelta(hours=4))

    @property
    def approved(self) -> bool:
        return self.approved_by is not None and self.approved_at is not None

    @property
    def expired(self) -> bool:
        return datetime.now(UTC) > self.expires_at


class ApprovalStore:
    """进程内审批票据（单副本内网服务；生产应落库并保留审计链）。"""

    def __init__(self) -> None:
        self._tickets: dict[str, ApprovalTicket] = {}

    def create(self, *, action: str, subject: str, payload_digest: str, actor: str, impact: dict) -> ApprovalTicket:
        ticket = ApprovalTicket(
            ticket_id=uuid.uuid4().hex[:16],
            action=action,
            subject=subject,
            payload_digest=payload_digest,
            submitted_by=actor,
            impact=impact,
        )
        self._tickets[ticket.ticket_id] = ticket
        ADMIN_APPROVAL_GUARD.set(len([t for t in self._tickets.values() if not t.approved]))
        return ticket

    def approve(self, ticket_id: str, approver: Actor) -> ApprovalTicket:
        ticket = self._tickets.get(ticket_id)
        if ticket is None:
            raise HTTPException(status_code=404, detail="unknown approval ticket")
        if ticket.expired:
            raise HTTPException(status_code=410, detail="approval ticket expired")
        if ticket.submitted_by == approver.name:
            raise HTTPException(status_code=403, detail="职责分离：提交人不得自审")
        if not approver.approver:
            raise HTTPException(status_code=403, detail="requires approver role")
        ticket.approved_by = approver.name
        ticket.approved_at = datetime.now(UTC)
        return ticket

    def consume(self, ticket_id: str | None, *, action: str, subject: str) -> ApprovalTicket:
        """消费审批票据。**票据缺失一律拒绝**——绝不能让"没审批"变成静默放行。"""
        if not ticket_id:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "该操作属高危操作，必须携带 X-AG-Approval（先建票据并由他人复核）",
                    "action": action,
                    "subject": subject,
                },
            )
        ticket = self._tickets.get(ticket_id)
        if ticket is None:
            raise HTTPException(status_code=404, detail="unknown approval ticket")
        if not ticket.approved:
            raise HTTPException(status_code=409, detail="approval ticket not approved")
        if ticket.expired:
            raise HTTPException(status_code=410, detail="approval ticket expired")
        if ticket.action != action or ticket.subject != subject:
            raise HTTPException(status_code=409, detail="approval ticket does not match operation")
        if ticket.submitted_by == ticket.approved_by:  # pragma: no cover - approve() 已拦
            raise HTTPException(status_code=403, detail="职责分离：不得自审自批")
        return ticket

    def pending(self) -> list[ApprovalTicket]:
        return [t for t in self._tickets.values() if not t.approved and not t.expired]


APPROVALS = ApprovalStore()


# ----------------------------------------------------------------------------- 治理审计
async def audit(event_type: str, *, actor: Actor, subject_type: str, subject_id: str, **kwargs: Any) -> None:
    async with session_scope() as s:
        await AuditWriter(s).append(
            event_type,
            actor=actor.name,
            actor_role=",".join(actor.roles),
            subject_type=subject_type,
            subject_id=subject_id,
            **kwargs,
        )


def _registry() -> TemplateRegistry:
    return get_container().registry


router = APIRouter(prefix="/admin")


# ----------------------------------------------------------------------------- 模板
@router.get("/templates")
async def list_templates(actor: Actor = Depends(require(*ALL_ROLES))) -> dict[str, Any]:
    registry = _registry()
    return {
        "note": READONLY_REPLICA_NOTE,
        "templates": [
            {
                "alias": tv.alias,
                "namespace": tv.namespace,
                "version": tv.version,
                "enabled": tv.enabled,
                "canary_weight": tv.canary_weight,
                "config_hash": tv.config_hash,
                "version_hash": tv.resolved.version_hash,
                "patch_ids": tv.patch_ids,
                "missing_capabilities": [
                    k for k, v in tv.resolved.capabilities.model_dump().items() if v is False
                ],
            }
            for tv in registry.all_versions()
        ],
    }


@router.get("/templates/{alias}")
async def get_template(
    alias: str,
    version: int | None = Query(default=None),
    actor: Actor = Depends(require(*ALL_ROLES)),
) -> dict[str, Any]:
    registry = _registry()
    tv = registry.get(alias, version)
    if tv is None:
        raise HTTPException(status_code=404, detail="unknown alias")
    return {
        "alias": tv.alias,
        "namespace": tv.namespace,
        "version": tv.version,
        "resolved": tv.resolved.model_dump(mode="json"),
        "provenance": tv.resolved.provenance,
        "patch_ids": tv.patch_ids,
        "raw": tv.raw,
    }


@dataclass(slots=True)
class PreviewRequest:
    """预览请求体：草稿 + 可选样例响应（用于验证终态判定取值确实提取得到）。"""

    raw: dict[str, Any]
    sample_status_response: dict[str, Any] | None = None
    sample_create_response: dict[str, Any] | None = None


@router.post("/templates/preview")
async def preview_template(
    request: PreviewRequest,
    actor: Actor = Depends(require(*ALL_ROLES)),
) -> dict[str, Any]:
    """校验 + 展开 + **渲染请求样例** + 版本 diff（§12.3）。"""
    report = validate(
        request.raw,
        sample_status_response=request.sample_status_response,
        sample_create_response=request.sample_create_response,
    )
    result: dict[str, Any] = {"validation": report.as_dict()}
    if report.resolved is not None:
        result["preview"] = preview_requests(report.resolved)
        result["resolved"] = report.resolved.model_dump(mode="json")
        result["provenance"] = report.resolved.provenance
        existing = _registry().get(report.resolved.alias)
        if existing is not None:
            from ..templates.registry import classify_change

            plan = classify_change(existing.resolved, report.resolved)
            result["change"] = plan.as_dict()
        else:
            result["change"] = {"changed_fields": ["<new-alias>"], "apply_mode": "versioned",
                                "requires_dual_review": True, "forced_review_fields": ["<new-alias>"],
                                "is_noop": False}
    return result


@router.post("/templates/{alias}/patch")
async def patch_template(
    alias: str,
    raw: dict[str, Any],
    request: Request,
    actor: Actor = Depends(require("template-author", "approver")),
) -> dict[str, Any]:
    """就地勘误：**仅**终态判定/提取表达式类字段，且必须经审批票据。"""
    registry = _registry()
    try:
        plan = registry.plan_change(raw)
    except Exception as exc:  # noqa: BLE001 - schema 错误按 400 回
        raise HTTPException(status_code=400, detail=str(exc)) from None
    if plan.apply_mode is not ApplyMode.IN_PLACE:
        raise HTTPException(
            status_code=409,
            detail={"message": "这些字段不允许就地修订，请走版本化灰度", "change": plan.as_dict()},
        )
    ticket_id = request.headers.get("x-ag-approval")
    APPROVALS.consume(ticket_id, action="template.patch", subject=alias)
    tv, report, _ = registry.patch_in_place(raw)
    if tv is None:
        raise HTTPException(status_code=400, detail=report.as_dict())
    await _persist_template(tv, actor, approved_by=ticket_id)
    await audit(
        AuditEventType.TEMPLATE_PATCHED,
        actor=actor,
        subject_type="template",
        subject_id=f"{alias}@{tv.version}",
        refs={"apply_mode": plan.apply_mode.value, "approval_ticket": ticket_id},
        payload=raw,
        detail="就地勘误（仅判定逻辑，不改请求形状）",
    )
    GOVERNANCE_TOTAL.inc({"action": "template.patch"})
    return {"alias": alias, "version": tv.version, "version_hash": tv.resolved.version_hash, "change": plan.as_dict()}


@router.post("/templates")
async def publish_template(
    raw: dict[str, Any],
    request: Request,
    actor: Actor = Depends(require("template-author", "approver")),
) -> dict[str, Any]:
    """注册/发布新版本。策略与能力变更**强制**审批票据 + 双人复核。"""
    registry = _registry()
    report = validate(raw)
    if not report.ok or report.resolved is None:
        raise HTTPException(status_code=400, detail=report.as_dict())
    plan = registry.plan_change(raw)
    ticket_id = request.headers.get("x-ag-approval")
    if plan.requires_dual_review:
        APPROVALS.consume(ticket_id, action="template.publish", subject=report.resolved.alias)
    elif not ticket_id:
        # 纯勘误走快速通道，但仍记录
        pass
    tv, report = registry.register(raw)
    if tv is None:
        raise HTTPException(status_code=400, detail=report.as_dict())
    await _persist_template(tv, actor, approved_by=ticket_id)
    await audit(
        AuditEventType.TEMPLATE_CHANGED,
        actor=actor,
        subject_type="template",
        subject_id=f"{tv.alias}@{tv.version}",
        refs={"change": plan.as_dict(), "approval_ticket": ticket_id, "patch_ids": tv.patch_ids},
        payload=raw,
        detail="模板版本发布",
    )
    GOVERNANCE_TOTAL.inc({"action": "template.publish"})
    return {
        "alias": tv.alias,
        "version": tv.version,
        "version_hash": tv.resolved.version_hash,
        "change": plan.as_dict(),
        "assumptions": report.assumptions,
    }


@router.post("/templates/{alias}/canary")
async def set_canary(
    alias: str,
    body: dict[str, Any],
    request: Request,
    actor: Actor = Depends(require("operator", "approver")),
) -> dict[str, Any]:
    """渠道维度按比例绑新版本；**权重归零即回滚**（影响面在执行前展示）。"""
    version = int(body.get("version", 0))
    weight = float(body.get("weight", 0.0))
    channel = str(body.get("channel", ""))
    if not channel:
        raise HTTPException(status_code=400, detail="channel required")
    container = get_container()
    policy = container.channel_policies.setdefault(channel, {})
    canary = policy.setdefault("canary", {})
    canary.setdefault(alias, {})[str(version)] = max(0.0, min(1.0, weight))
    ticket_id = request.headers.get("x-ag-approval")
    APPROVALS.consume(ticket_id, action="template.canary", subject=alias)
    impact = await _canary_impact(channel, alias)
    await audit(
        AuditEventType.TEMPLATE_CANARY,
        actor=actor,
        subject_type="template",
        subject_id=f"{alias}@{version}",
        refs={"channel": channel, "weight": weight, "impact": impact, "approval_ticket": ticket_id},
        detail="灰度权重变更（权重 0 = 回滚）",
    )
    GOVERNANCE_TOTAL.inc({"action": "template.canary"})
    return {"channel": channel, "alias": alias, "version": version, "weight": weight, "impact": impact}


async def _canary_impact(channel: str, alias: str) -> dict[str, Any]:
    """执行前影响面：该渠道当前活动任务数（预计会被新版本影响的规模）。"""
    async with session_scope() as s:
        counts = await TaskDAO(s).active_counts_by_channel()
    return {"channel": channel, "active_tasks": counts.get(channel, 0), "alias": alias}


async def _persist_template(tv, actor: Actor, *, approved_by: str | None) -> None:
    async with session_scope() as s:
        await TaskDAO(s).upsert_template_version(
            alias=tv.alias,
            namespace=tv.namespace,
            version=tv.version,
            config=tv.raw,
            config_hash=tv.config_hash,
            patch_ids=tv.patch_ids,
            enabled=tv.enabled,
            canary_weight=tv.canary_weight,
            created_by=actor.name,
            approved_by=approved_by,
        )


# ----------------------------------------------------------------------------- 审批
@router.post("/approvals")
async def create_approval(
    body: dict[str, Any],
    actor: Actor = Depends(require(*ALL_ROLES)),
) -> dict[str, Any]:
    action = str(body.get("action", ""))
    subject = str(body.get("subject", ""))
    if action not in HIGH_RISK_ACTIONS:
        raise HTTPException(status_code=400, detail=f"unknown action: {action}")
    required = HIGH_RISK_ACTIONS[action]
    if not actor.has(*required):
        raise HTTPException(status_code=403, detail=f"requires one of {required}")
    digest = _digest(body.get("payload", {}))
    ticket = APPROVALS.create(
        action=action,
        subject=subject,
        payload_digest=digest,
        actor=actor.name,
        impact=body.get("impact", {}),
    )
    await audit(
        AuditEventType.GOVERNANCE,
        actor=actor,
        subject_type="approval",
        subject_id=ticket.ticket_id,
        refs={"action": action, "subject": subject, "impact": ticket.impact},
        detail="高危操作待审批（需他人复核）",
    )
    return {
        "ticket_id": ticket.ticket_id,
        "action": action,
        "subject": subject,
        "submitted_by": actor.name,
        "expires_at": ticket.expires_at.isoformat(),
        "next": "由另一位具备 approver 角色的成员调用 POST /admin/approvals/{ticket_id}/approve",
    }


@router.post("/approvals/{ticket_id}/approve")
async def approve(
    ticket_id: str,
    actor: Actor = Depends(require("approver")),
) -> dict[str, Any]:
    ticket = APPROVALS.approve(ticket_id, actor)
    await audit(
        AuditEventType.GOVERNANCE,
        actor=actor,
        subject_type="approval",
        subject_id=ticket.ticket_id,
        refs={"action": ticket.action, "subject": ticket.subject, "submitted_by": ticket.submitted_by},
        detail="高危操作复核通过",
    )
    GOVERNANCE_TOTAL.inc({"action": "approval.approve"})
    return {"ticket_id": ticket.ticket_id, "approved_by": actor.name, "status": "approved"}


@router.get("/approvals")
async def list_approvals(actor: Actor = Depends(require(*ALL_ROLES))) -> dict[str, Any]:
    return {
        "pending": [
            {
                "ticket_id": t.ticket_id,
                "action": t.action,
                "subject": t.subject,
                "submitted_by": t.submitted_by,
                "impact": t.impact,
                "created_at": t.created_at.isoformat(),
            }
            for t in APPROVALS.pending()
        ]
    }


# ----------------------------------------------------------------------------- 任务治理
@router.get("/tasks")
async def list_tasks(
    status: str | None = Query(default=None),
    channel: str | None = Query(default=None),
    limit: int = Query(default=50, le=500),
    actor: Actor = Depends(require(*ALL_ROLES)),
) -> dict[str, Any]:
    from sqlalchemy import select

    stmt = select(AsyncTask).order_by(AsyncTask.created_at.desc()).limit(limit)
    if status:
        stmt = stmt.where(AsyncTask.status == status)
    if channel:
        stmt = stmt.where(AsyncTask.channel == channel)
    async with session_scope() as s:
        rows = (await s.execute(stmt)).scalars().all()
    return {
        "note": READONLY_REPLICA_NOTE,
        "tasks": [
            {
                "task_id": r.task_id,
                "alias": r.template_alias,
                "template_version": r.template_version,
                "channel": r.channel,
                "tenant": r.tenant,
                "status": r.status,
                "attempts": r.attempts,
                "max_attempts": r.max_attempts,
                "origin": r.origin,
                "replay_of": r.replay_of,
                "error_code": r.error_code,
                "upstream_task_id": r.upstream_task_id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "deadline_at": r.deadline_at.isoformat() if r.deadline_at else None,
                "next_poll_at": r.next_poll_at.isoformat() if r.next_poll_at else None,
            }
            for r in rows
        ],
    }


@router.post("/tasks/{task_id}/replay")
async def replay_task(
    task_id: str,
    request: Request,
    actor: Actor = Depends(require("operator", "approver")),
) -> dict[str, Any]:
    """死信/unknown 重放（§14 / §18.4）。

    三条硬约束：
    1. **第一步强制执行确认查询**；manual_only 上游必须由人工在请求里显式确认；
    2. 已有 ``upstream_task_id`` 的任务**禁止**重放创建类动作；
    3. 批量重放设上限与限速；双人复核 + 影响面展示。
    """
    container = get_container()
    parent = None
    async with session_scope() as s:
        parent = await TaskDAO(s).get(task_id)
    if parent is None:
        raise HTTPException(status_code=404, detail="task not found")
    if not is_business_terminal(parent.status) and TaskStatus(parent.status) is not TaskStatus.FAILED:
        raise HTTPException(status_code=409, detail="only terminal/retryable tasks can be replayed")
    if parent.origin == Origin.DRY_RUN.value:
        raise HTTPException(status_code=409, detail="dry-run tasks are not replayable")

    APPROVALS.consume(request.headers.get("x-ag-approval"), action="task.replay", subject=task_id)
    _enforce_replay_rate(actor.name)

    if parent.upstream_task_id:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "已有 upstream_task_id：禁止重放创建类动作（只能继续轮询/人工关闭）",
                "upstream_task_id": parent.upstream_task_id,
            },
        )

    tv = container.registry.get(parent.template_alias, parent.template_version)
    if tv is None:
        raise HTTPException(status_code=409, detail="template version unavailable")
    if not tv.resolved.capabilities.confirm_auto_allowed:
        human = (request.headers.get("x-ag-human-confirmed") or "").lower() in ("1", "true", "yes")
        if not human:
            raise HTTPException(
                status_code=409,
                detail={
                    "message": "该上游 confirm_strategy=manual_only：重放前必须人工确认上游未创建"
                    "（请以 X-AG-Human-Confirmed: 1 提交，并附确认依据）",
                    "confirm_strategy": tv.resolved.capabilities.confirm_strategy.value,
                },
            )

    # 影响面：派生键与预计上游调用数
    attempt = attempt_of(parent.idempotency_key) + 1
    child_key = retry_key(parent.idempotency_key, attempt)
    impact = {"upstream_calls": 1, "derived_key": child_key, "parent_status": parent.status}

    # 复制 create 请求体（重放必须是同一份请求）
    body: dict[str, Any] = {}
    ref = parent.create_req_ref
    if ref:
        try:
            raw = await container.request_store.get_bytes(ref)
            parsed = json.loads(raw.decode())
            if isinstance(parsed, dict):
                body = parsed
        except Exception:  # noqa: BLE001
            body = {}

    # 重放会触发一次**真实**的上游创建 → 必须有可用凭证。
    # 数据面凭证按 §18.2 不落盘，所以这里要求随审批一起提供，并且只停留在
    # 短生命周期存放里（TTL 与任务 deadline 同量级，任务终态即删）。
    credential_value = (request.headers.get("x-ag-credential") or "").strip()
    if not credential_value:
        raise HTTPException(
            status_code=409,
            detail={
                "message": "重放会触发一次真实的上游创建，需要随请求提供数据面凭证（X-AG-Credential）。"
                "网关不持久化任何数据面凭证。",
                "reason": "no data-plane credential available for replay",
                "alternative": "若只是想让任务继续收敛，请改用继续轮询或 POST /admin/tasks/{id}/close",
            },
        )

    child_id = uuid.uuid4().hex[:24]
    window = parent.idempotency_window_seconds
    from ..gateway.idempotency import window_bucket

    child_ref = request_key(parent.tenant, child_id)
    await container.request_store.put_json(child_ref, body, body_ttl_seconds())
    child = AsyncTask(
        task_id=child_id,
        idempotency_key=child_key,
        idempotency_bucket=window_bucket(None, window),
        idempotency_window_seconds=window,
        tenant=parent.tenant,
        channel=parent.channel,
        task_type=parent.task_type,
        status=TaskStatus.ACCEPTED.value,
        template_alias=parent.template_alias,
        template_version=parent.template_version,
        max_attempts=parent.max_attempts,
        deadline_at=datetime.now(UTC) + timedelta(seconds=int(tv.resolved.strategy.get("deadline_seconds", 1800))),
        origin=Origin.REPLAY.value,
        replay_of=parent.task_id,
        callback_token=new_opaque_token() if tv.resolved.capabilities.callback else None,
        create_req_ref=child_ref,
        create_req_digest=parent.create_req_digest,
        attributes={"replay_reason": request.headers.get("x-ag-replay-reason", "")[:200]},
    )
    async with session_scope() as s:
        await TaskDAO(s).insert(child)
    await container.credential_store.put(child_id, credential_value, credential_ttl_seconds())
    await container.bus.enqueue("submit:default", "submit_upstream", {"task_id": child_id})

    REPLAY_TOTAL.inc({"result": "accepted"})
    await audit(
        AuditEventType.REPLAY,
        actor=actor,
        subject_type="task",
        subject_id=child_id,
        refs={"parent": parent.task_id, "impact": impact, "approval_ticket": request.headers.get("x-ag-approval")},
        detail="死信/unknown 重放：新任务行（origin=replay），原任务保持不变",
    )
    return {"child_task_id": child_id, "replay_of": parent.task_id, "impact": impact}


_REPLAY_TIMES: list[float] = []


def _enforce_replay_rate(actor: str) -> None:
    """批量重放限速（进程内滑动窗口；单副本治理面够用）。"""
    limit = get_settings().replay_rate_per_minute
    now = time.time()
    global _REPLAY_TIMES
    _REPLAY_TIMES = [t for t in _REPLAY_TIMES if now - t < 60]
    if len(_REPLAY_TIMES) >= limit:
        raise HTTPException(status_code=429, detail=f"replay rate limit {limit}/min exceeded")
    _REPLAY_TIMES.append(now)


@router.post("/tasks/{task_id}/close")
async def close_task(
    task_id: str,
    request: Request,
    body: dict[str, Any] | None = None,
    actor: Actor = Depends(require("operator", "approver")),
) -> dict[str, Any]:
    """人工关闭（§14「人工确认后重放或关闭」的另一半）。"""
    APPROVALS.consume(request.headers.get("x-ag-approval"), action="task.replay", subject=task_id)
    async with session_scope() as s:
        dao = TaskDAO(s)
        task = await dao.get(task_id)
        if task is None:
            raise HTTPException(status_code=404, detail="task not found")
        moved = await dao.advance(
            task_id,
            TaskStatus.DEAD,
            expected=[task.status],
            error_code=ErrorCode.UNKNOWN_EXHAUSTED.value,
            error_message=(body or {}).get("reason", "closed by human")[:500],
            finished_at=datetime.now(UTC),
            next_poll_at=None,
        )
    if not moved:
        raise HTTPException(status_code=409, detail="illegal transition from current state")
    await audit(
        AuditEventType.GOVERNANCE,
        actor=actor,
        subject_type="task",
        subject_id=task_id,
        refs={"operation": "close", "reason": (body or {}).get("reason")},
        detail="人工关闭",
    )
    GOVERNANCE_TOTAL.inc({"action": "task.close"})
    return {"task_id": task_id, "status": TaskStatus.DEAD.value}


@router.post("/dry-run")
async def dry_run(
    body: dict[str, Any],
    actor: Actor = Depends(require("operator", "template-author", "approver")),
) -> dict[str, Any]:
    """dry-run 实测（§5 第 5 步 / §12.4）。

    ``origin=dry_run`` 隔离：计费与配额统计显式排除；确认后自动 cancel → 清理 → 归档；
    上游不支持 cancel 时打标记等自然终态。返回详情**自动脱敏**。
    """
    alias = str(body.get("alias") or "")
    payload = body.get("body") or {}
    credential_value = str(body.get("credential") or "")
    if not alias:
        raise HTTPException(status_code=400, detail="alias required")
    container = get_container()
    tv = container.registry.get(alias)
    if tv is None:
        raise HTTPException(status_code=404, detail="unknown alias")

    from ..gateway.accept import AcceptError, accept_create
    from ..gateway.container import AuthContext
    from ..gateway.idempotency import key_hash_of

    auth = AuthContext(
        channel=str(body.get("channel") or "dry-run"),
        tenant=str(body.get("tenant") or "dry-run"),
        bearer=credential_value or None,
        key_hash=key_hash_of(credential_value or "dry-run"),
        actor=actor.name,
        mode="admin",
    )
    try:
        accepted = await accept_create(
            container=container,
            bus=container.bus,
            auth=auth,
            tv=tv,
            body=payload,
            origin=Origin.DRY_RUN,
        )
    except AcceptError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from None

    cleanup = await _cleanup_dry_run(container, accepted.task_id, tv)
    await audit(
        AuditEventType.DRY_RUN,
        actor=actor,
        subject_type="task",
        subject_id=accepted.task_id,
        refs={"alias": alias, "http_status": accepted.http_status, "cleanup": cleanup},
        detail="dry-run 实测（origin=dry_run，计费与配额统计显式排除）",
    )
    GOVERNANCE_TOTAL.inc({"action": "dry-run"})
    return {
        "task_id": accepted.task_id,
        "http_status": accepted.http_status,
        "response": scrub_payload(accepted.body, [credential_value] if credential_value else []),
        "cleanup": cleanup,
    }


async def _cleanup_dry_run(container, task_id: str, tv) -> dict[str, Any]:
    """dry-run 收尾：能取消就取消，不能取消就打标记等自然终态。"""
    from ..security.redaction import scrub_text

    async with session_scope() as s:
        task = await TaskDAO(s).get(task_id)
    if task is None:
        return {"cleaned": False, "reason": "task vanished"}
    if not tv.resolved.capabilities.cancel:
        async with session_scope() as s:
            await TaskDAO(s).note_attribute(task_id, dry_run_marked=True)
        return {"cleaned": False, "reason": "upstream cannot cancel; marked to converge naturally"}
    if not task.upstream_task_id:
        return {"cleaned": False, "reason": "no upstream task created"}
    client = container.upstream_client(None)  # 凭证从短生命周期存放里取，不从别处来
    try:
        from ..templates.render import render_cancel

        request = render_cancel(tv.resolved, task.upstream_task_id)
        bearer = await container.credential_store.get(task_id)
        result = await client.call(request, context="cancel", bearer=bearer or None)
    except Exception as exc:  # noqa: BLE001
        return {"cleaned": False, "reason": scrub_text(str(exc))[:200]}
    finally:
        await client.aclose()
    await container.credential_store.drop(task_id)
    return {"cleaned": result.ok, "upstream_status": result.response.status_code if result.response else None}


# ----------------------------------------------------------------------------- 凭证引用 / 审计 / 观测
@router.post("/credential-refs")
async def register_credential_ref(
    body: dict[str, Any],
    request: Request,
    actor: Actor = Depends(require("operator", "approver")),
) -> dict[str, Any]:
    """控制面凭证引用登记（§18.2）：**只登记引用名**，密钥本体存 vault，网关不落盘。"""
    name = str(body.get("name") or "").strip()
    if not name:
        raise HTTPException(status_code=400, detail="name required")
    if body.get("secret"):
        raise HTTPException(
            status_code=400,
            detail="不要在治理面提交密钥明文：密钥应写入 vault，网关只登记引用名",
        )
    APPROVALS.consume(request.headers.get("x-ag-approval"), action="credential_ref.change", subject=name)
    await audit(
        AuditEventType.CREDENTIAL_REF_CHANGED,
        actor=actor,
        subject_type="credential_ref",
        subject_id=name,
        refs={"purpose": "dry_run", "provider": body.get("provider")},
        detail="登记控制面凭证引用（数据面仍为 header 透传，不使用本引用）",
    )
    GOVERNANCE_TOTAL.inc({"action": "credential_ref.register"})
    return {"name": name, "registered": True}


@router.get("/audit")
async def read_audit(
    limit: int = Query(default=100, le=1000),
    event_type: str | None = Query(default=None),
    actor: Actor = Depends(require("auditor", "approver")),
) -> dict[str, Any]:
    """审计只读（auditor 专属）。只返回引用/哈希。"""
    from sqlalchemy import select

    from ..db.models import AuditEvent

    stmt = select(AuditEvent).order_by(AuditEvent.id.desc()).limit(limit)
    if event_type:
        stmt = stmt.where(AuditEvent.event_type == event_type)
    async with session_scope() as s:
        rows = (await s.execute(stmt)).scalars().all()
    return {
        "note": "append-only / WORM；记录只含引用与哈希，不含个人负载与凭证值",
        "events": [
            {
                "id": r.id,
                "event_type": r.event_type,
                "actor": r.actor,
                "actor_role": r.actor_role,
                "subject": f"{r.subject_type}:{r.subject_id}",
                "payload_digest": r.payload_digest,
                "refs": r.refs,
                "detail": r.detail,
                "hash": r.hash,
                "prev_hash": r.prev_hash,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            }
            for r in rows
        ],
    }


@router.get("/audit/verify")
async def verify_audit(actor: Actor = Depends(require("auditor", "approver"))) -> dict[str, Any]:
    async with session_scope() as s:
        ok, reason = await AuditWriter(s).verify_chain(limit=10_000)
    return {"ok": ok, "reason": reason}


@router.get("/metrics", include_in_schema=False)
async def metrics(actor: Actor = Depends(require(*ALL_ROLES))) -> PlainTextResponse:
    return PlainTextResponse(render_metrics(), media_type="text/plain; version=0.0.4")


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


def create_admin_app() -> FastAPI:
    configure_logging()
    app = FastAPI(
        title="Async Gateway Task-admin",
        version="0.1.0",
        description="治理面：模板/灰度/重放/dry-run/审计。仅内网可达，不接生产流量。",
        docs_url="/docs",
        redoc_url=None,
    )
    app.include_router(router)
    return app


app = create_admin_app()


__all__ = ["ApprovalStore", "Actor", "app", "create_admin_app", "router"]
