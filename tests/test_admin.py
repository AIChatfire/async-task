"""治理面：RBAC、职责分离、双人复核、模板校验复用、重放纪律、dry-run、审计。"""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

import httpx
import pytest
import pytest_asyncio

from async_gateway.db.base import session_scope
from async_gateway.db.models import AsyncTask
from async_gateway.domain.enums import Origin, TaskStatus
from async_gateway.infra.request_store import request_key

ADMIN_TOKEN = "test-admin"

AUTHOR = {"X-AG-Admin-Token": ADMIN_TOKEN, "X-AG-Actor": "alice", "X-AG-Roles": "template-author"}
APPROVER = {"X-AG-Admin-Token": ADMIN_TOKEN, "X-AG-Actor": "bob", "X-AG-Roles": "approver"}
OPERATOR = {"X-AG-Admin-Token": ADMIN_TOKEN, "X-AG-Actor": "carol", "X-AG-Roles": "operator"}
AUDITOR = {"X-AG-Admin-Token": ADMIN_TOKEN, "X-AG-Actor": "dave", "X-AG-Roles": "auditor"}
NOBODY = {"X-AG-Admin-Token": ADMIN_TOKEN, "X-AG-Actor": "eve", "X-AG-Roles": "operator,x"}

MINIMAL = {
    "alias": "volc-seedance",
    "base_url": "https://ark.cn-beijing.volces.com",
    "create_path": "/api/v3/contents/generations/tasks",
    "result_location": "$.content.video_url",
}


@pytest_asyncio.fixture
async def admin(container, db, result_store, credentials) -> httpx.AsyncClient:
    import importlib

    module = importlib.import_module("async_gateway.admin.app")
    module.APPROVALS._tickets.clear()
    app = module.create_admin_app()
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://admin.test") as c:
        yield c


async def _insert_parent(task_id: str | None = None, **over) -> str:
    task_id = task_id or uuid.uuid4().hex[:24]
    defaults = dict(
        task_id=task_id,
        idempotency_key=f"k-{uuid.uuid4().hex[:10]}",
        idempotency_bucket=1,
        tenant="default",
        channel="echo",
        task_type="echo",
        status=TaskStatus.DEAD.value,
        template_alias="echo",
        template_version=1,
        attempts=1,
        max_attempts=3,
        origin=Origin.USER.value,
        create_req_ref=request_key("default", task_id),
    )
    defaults.update(over)
    async with session_scope() as s:
        s.add(AsyncTask(**defaults))
    return task_id


# ------------------------------------------------------------------ 认证与授权
async def test_admin_requires_token(admin):
    assert (await admin.get("/admin/templates")).status_code == 401
    bad = await admin.get("/admin/templates", headers={"X-AG-Admin-Token": "nope", "X-AG-Actor": "x", "X-AG-Roles": "auditor"})
    assert bad.status_code == 401


async def test_admin_requires_actor_identity(admin):
    response = await admin.get("/admin/templates", headers={"X-AG-Admin-Token": ADMIN_TOKEN})
    assert response.status_code == 401  # SSO subject 缺失


async def test_unknown_role_rejected(admin):
    response = await admin.get("/admin/templates", headers=NOBODY)
    assert response.status_code == 403


async def test_role_gating_on_audit_read(admin):
    assert (await admin.get("/admin/audit", headers=OPERATOR)).status_code == 403
    assert (await admin.get("/admin/audit", headers=AUDITOR)).status_code == 200


# ------------------------------------------------------------------ 模板
async def test_template_list_shows_missing_capabilities(admin):
    response = await admin.get("/admin/templates", headers=AUDITOR)
    assert response.status_code == 200
    seedance = next(t for t in response.json()["templates"] if t["alias"] == "volc-seedance")
    assert "cancel" in seedance["missing_capabilities"]
    assert seedance["patch_ids"] == ["volc-ark-seedance"]


async def test_template_preview_runs_the_same_validator_and_renders_requests(admin):
    response = await admin.post("/admin/templates/preview", json={"raw": MINIMAL}, headers=AUTHOR)
    assert response.status_code == 200
    payload = response.json()
    assert payload["validation"]["ok"] is True
    assert payload["preview"]["get"]["url"].endswith("/contents/generations/tasks/task-abc123")
    assert "Authorization" not in payload["preview"]["create"]["headers"]
    assert payload["provenance"]["capabilities.upstream_idempotent"] == "patched"


async def test_template_preview_rejects_bad_expression(admin):
    broken = {**MINIMAL, "id_location": "$.a[*].b"}
    payload = (await admin.post("/admin/templates/preview", json={"raw": broken}, headers=AUTHOR)).json()
    assert payload["validation"]["ok"] is False
    assert any("表达式沙箱拒绝" in e["message"] for e in payload["validation"]["errors"])


async def test_template_preview_warns_on_unrecognized_sample_status(admin):
    payload = (
        await admin.post(
            "/admin/templates/preview",
            json={"raw": MINIMAL, "sample_status_response": {"status": "brand-new-state"}},
            headers=AUTHOR,
        )
    ).json()
    assert payload["validation"]["ok"] is True
    assert payload["validation"]["warnings"]


async def test_publishing_strategy_change_requires_approval_ticket(admin):
    new_version = {**MINIMAL, "pool": "heavy-poll"}
    blocked = await admin.post("/admin/templates", json=new_version, headers=AUTHOR)
    # 高危变更没有审批票据 → 直接拒绝（绝不能"没审批=放行"）
    assert blocked.status_code == 409
    assert "X-AG-Approval" in json.dumps(blocked.json(), ensure_ascii=False)


async def test_dual_approval_happy_path_and_separation_of_duties(admin):
    new_version = {**MINIMAL, "pool": "heavy-poll"}
    ticket = (
        await admin.post(
            "/admin/approvals",
            json={"action": "template.publish", "subject": "volc-seedance", "payload": new_version},
            headers=AUTHOR,
        )
    ).json()
    # 提交人自审 → 拒绝
    self_approve = await admin.post(f"/admin/approvals/{ticket['ticket_id']}/approve", headers=AUTHOR)
    assert self_approve.status_code == 403
    # 他人复核 → 通过
    approved = await admin.post(f"/admin/approvals/{ticket['ticket_id']}/approve", headers=APPROVER)
    assert approved.status_code == 200
    published = await admin.post(
        "/admin/templates",
        json=new_version,
        headers={**AUTHOR, "X-AG-Approval": ticket["ticket_id"]},
    )
    assert published.status_code == 200
    assert published.json()["change"]["apply_mode"] == "versioned"
    assert published.json()["version"] == 2


async def test_in_place_patch_requires_errata_and_approval(admin):
    # 就地勘误必须**基于既有版本**改（改动集只含判定逻辑），否则混入策略字段会被判 versioned
    existing = (await admin.get("/admin/templates/volc-seedance", headers=AUTHOR)).json()["raw"]
    ticket = (
        await admin.post(
            "/admin/approvals",
            json={"action": "template.patch", "subject": "volc-seedance"},
            headers=AUTHOR,
        )
    ).json()
    await admin.post(f"/admin/approvals/{ticket['ticket_id']}/approve", headers=APPROVER)

    errata = {**existing, "terminal": {"success": ["succeeded"], "failure": ["failed", "error"]}}
    ok = await admin.post(
        "/admin/templates/volc-seedance/patch",
        json=errata,
        headers={**AUTHOR, "X-AG-Approval": ticket["ticket_id"]},
    )
    assert ok.status_code == 200, ok.text
    assert ok.json()["change"]["apply_mode"] == "in_place"

    # 策略字段不允许就地修订
    illegal = await admin.post(
        "/admin/templates/volc-seedance/patch",
        json={**existing, "pool": "heavy-poll"},
        headers={**AUTHOR, "X-AG-Approval": ticket["ticket_id"]},
    )
    assert illegal.status_code == 409


async def test_canary_weight_zero_is_rollback(admin, container):
    ticket = (
        await admin.post(
            "/admin/approvals",
            json={"action": "template.canary", "subject": "volc-seedance"},
            headers=OPERATOR,
        )
    ).json()
    await admin.post(f"/admin/approvals/{ticket['ticket_id']}/approve", headers=APPROVER)
    response = await admin.post(
        "/admin/templates/volc-seedance/canary",
        json={"channel": "volc-ch", "version": 2, "weight": 0.0},
        headers={**OPERATOR, "X-AG-Approval": ticket["ticket_id"]},
    )
    assert response.status_code == 200
    assert response.json()["weight"] == 0.0
    assert "impact" in response.json()
    assert container.channel_policies["volc-ch"]["canary"]["volc-seedance"]["2"] == 0.0


# ------------------------------------------------------------------ 重放纪律
async def _replay_ticket(admin_client, task_id: str) -> str:
    ticket = (
        await admin_client.post(
            "/admin/approvals",
            json={"action": "task.replay", "subject": task_id},
            headers=OPERATOR,
        )
    ).json()
    await admin_client.post(f"/admin/approvals/{ticket['ticket_id']}/approve", headers=APPROVER)
    return ticket["ticket_id"]


async def test_replay_requires_approval_ticket(admin):
    task_id = await _insert_parent()
    response = await admin.post(f"/admin/tasks/{task_id}/replay", headers=OPERATOR)
    assert response.status_code == 409  # 没有 X-AG-Approval → 拒绝，而不是静默放行
    assert "X-AG-Approval" in json.dumps(response.json(), ensure_ascii=False)


async def test_replay_refuses_when_upstream_task_exists(admin):
    task_id = await _insert_parent(upstream_task_id="up-1")
    ticket = await _replay_ticket(admin, task_id)
    response = await admin.post(
        f"/admin/tasks/{task_id}/replay",
        headers={**OPERATOR, "X-AG-Approval": ticket, "X-AG-Credential": "Bearer sk-x"},
    )
    assert response.status_code == 409
    assert "禁止重放创建类动作" in json.dumps(response.json(), ensure_ascii=False)


async def test_replay_requires_data_plane_credential(admin):
    task_id = await _insert_parent(upstream_task_id=None)
    ticket = await _replay_ticket(admin, task_id)
    response = await admin.post(
        f"/admin/tasks/{task_id}/replay", headers={**OPERATOR, "X-AG-Approval": ticket}
    )
    assert response.status_code == 409
    assert "X-AG-Credential" in json.dumps(response.json(), ensure_ascii=False)


async def test_replay_creates_child_task_and_keeps_parent(admin, container, request_store):
    task_id = await _insert_parent(upstream_task_id=None, idempotency_key="auto:key1|deadbeef")
    await request_store.put_json(request_key("default", task_id), {"prompt": "replay me"}, 3600)
    ticket = await _replay_ticket(admin, task_id)
    response = await admin.post(
        f"/admin/tasks/{task_id}/replay",
        headers={**OPERATOR, "X-AG-Approval": ticket, "X-AG-Credential": "Bearer sk-replay"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["replay_of"] == task_id
    assert body["impact"]["derived_key"].endswith("#attempt2")

    child_id = body["child_task_id"]
    async with session_scope() as s:
        child = await s.get(AsyncTask, child_id)
        parent = await s.get(AsyncTask, task_id)
    assert child is not None and child.origin == Origin.REPLAY.value
    assert child.replay_of == task_id
    assert child.status == TaskStatus.ACCEPTED.value
    assert parent.status == TaskStatus.DEAD.value  # 原任务不变，单调性保住
    assert await container.bus.depth("submit:default") == 1
    assert await container.credential_store.get(child_id) == "Bearer sk-replay"


async def test_replay_rate_limit(admin, container, request_store, monkeypatch):
    from async_gateway.admin.app import _REPLAY_TIMES
    from async_gateway.config import get_settings

    monkeypatch.setattr(get_settings(), "replay_rate_per_minute", 2)
    _REPLAY_TIMES.clear()

    codes = []
    for _ in range(4):
        tid = await _insert_parent(upstream_task_id=None)
        await request_store.put_json(request_key("default", tid), {"prompt": "x"}, 3600)
        ticket = await _replay_ticket(admin, tid)
        response = await admin.post(
            f"/admin/tasks/{tid}/replay",
            headers={**OPERATOR, "X-AG-Approval": ticket, "X-AG-Credential": "Bearer sk-x"},
        )
        codes.append(response.status_code)
    assert codes[:2] == [200, 200]
    assert codes[2] == 429  # 批量重放限速（§18.4）


async def test_close_task_is_audited(admin):
    task_id = await _insert_parent(status=TaskStatus.DEAD_AWAITING_CONFIRM.value)
    ticket = await _replay_ticket(admin, task_id)
    response = await admin.post(
        f"/admin/tasks/{task_id}/close",
        json={"reason": "confirmed not created upstream"},
        headers={**OPERATOR, "X-AG-Approval": ticket},
    )
    assert response.status_code == 200
    assert response.json()["status"] == TaskStatus.DEAD.value
    audit = (await admin.get("/admin/audit?event_type=governance.operation", headers=AUDITOR)).json()
    assert any(e["refs"] and e["refs"].get("operation") == "close" for e in audit["events"])


# ------------------------------------------------------------------ dry-run
async def test_dry_run_isolated_and_cleaned_up(admin, container, fake_upstream, result_store):
    response = await admin.post(
        "/admin/dry-run",
        json={"alias": "echo", "body": {"prompt": "probe"}, "credential": "Bearer sk-dry"},
        headers=OPERATOR,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["http_status"] == 200
    assert payload["cleanup"]["cleaned"] is True  # 上游支持取消 → 自动清理
    assert fake_upstream.cancel_calls == 1

    async with session_scope() as s:
        task = await s.get(AsyncTask, payload["task_id"])
    assert task is not None and task.origin == Origin.DRY_RUN.value
    assert fake_upstream.last_auth == "Bearer sk-dry"


async def test_dry_run_marks_when_upstream_cannot_cancel(admin, fake_upstream, result_store):
    response = await admin.post(
        "/admin/dry-run",
        json={"alias": "echo-manual", "body": {"prompt": "probe"}, "credential": "Bearer sk-dry"},
        headers=OPERATOR,
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["cleanup"]["cleaned"] is False
    assert "cannot cancel" in payload["cleanup"]["reason"]


# ------------------------------------------------------------------ 凭证引用与审计
async def test_credential_ref_never_accepts_plaintext_secret(admin):
    response = await admin.post(
        "/admin/credential-refs",
        json={"name": "dry-run-ark", "secret": "sk-plaintext"},
        headers=OPERATOR,
    )
    assert response.status_code == 400
    assert "vault" in json.dumps(response.json(), ensure_ascii=False)


async def test_audit_chain_verifies_and_only_stores_references(admin):
    await admin.post("/admin/dry-run", json={"alias": "echo", "body": {"prompt": "p"}}, headers=OPERATOR)
    verify = (await admin.get("/admin/audit/verify", headers=AUDITOR)).json()
    assert verify["ok"] is True, verify
    events = (await admin.get("/admin/audit", headers=AUDITOR)).json()["events"]
    assert events
    blob = json.dumps(events, ensure_ascii=False)
    assert "sk-dry" not in blob


async def test_admin_metrics_endpoint(admin):
    response = await admin.get("/admin/metrics", headers=OPERATOR)
    assert response.status_code == 200
    assert "ag_governance_total" in response.text or "ag_admin" in response.text


# ------------------------------------------------------------------ 根路径探针（F3）
async def test_admin_root_probes_exist_and_report_dependencies(admin, monkeypatch):
    """治理面根路径必须有探针 —— 否则照搬数据面探针路径会把服务判成不健康。

    对应 livetest-ai 报告 `E2E-ASYNC-TASK-001` 的 F3：``/healthz`` / ``/livez`` /
    ``/readyz`` 全 404，部署侧只能把探针指向 ``/openapi.json``（与健康无关的路径）。
    """
    import importlib

    module = importlib.import_module("async_gateway.admin.app")

    # 探针不要求任何身份：不带 X-AG-Admin-Token 也必须是 200
    live = await admin.get("/healthz")
    assert live.status_code == 200, live.text
    assert live.json() == {"status": "ok"}
    assert (await admin.get("/livez")).status_code == 200
    # 历史路径保留（老部署的探针指向 /admin/healthz）
    assert (await admin.get("/admin/healthz")).status_code == 200

    monkeypatch.setattr(module, "redis_ping", lambda: _async_true())
    ready = await admin.get("/readyz")
    assert ready.status_code == 200, ready.text
    assert set(ready.json()) == {"db", "cache"}
    assert ready.json()["db"] is True


async def test_admin_readyz_is_503_when_database_is_down(admin, monkeypatch):
    """就绪判据必须真的能判死：PG 不可达时 200 会让"就绪"变成永远为真。"""
    import importlib

    module = importlib.import_module("async_gateway.admin.app")
    monkeypatch.setattr(module, "db_healthy", lambda: _async_false())
    monkeypatch.setattr(module, "redis_ping", lambda: _async_true())
    ready = await admin.get("/readyz")
    assert ready.status_code == 503
    assert ready.json() == {"db": False, "cache": True}


async def _async_true() -> bool:
    return True


async def _async_false() -> bool:
    return False


# ------------------------------------------------------------------ 渠道自适应状态（§3.19）
async def _pollute(container, channel: str = "echo", times: int = 6) -> float:
    for _ in range(times):
        await container.polling.note_rate_limited(channel, 3.0)
    return float(await container.polling.current_multiplier(channel))


async def test_channel_pacing_view_reports_baseline_and_needs_auth(admin, container):
    """未带身份不得读（401）；基线态乘子=1，两个间隔相等（还没有限流惩罚）。"""
    assert (await admin.get("/admin/channels/echo/pacing")).status_code == 401
    response = await admin.get("/admin/channels/echo/pacing", headers=OPERATOR)
    assert response.status_code == 200, response.text
    payload = response.json()
    polling = container.polling
    assert payload["multiplier"] == 1.0
    assert payload["paused_for"] == 0.0
    # 乘子上限由 poll_max / poll_base 推出（不写死数字：测试基座把轮询间隔钉在 0.01s 级）
    assert payload["max_multiplier"] == pytest.approx(polling.maximum / polling.base)
    assert payload["next_poll_interval"] == payload["resubmit_base_interval"]
    assert payload["samples"] == 0 and payload["warm"] is False


async def test_channel_pacing_view_exposes_multiplier_apart_from_resubmit_base(admin, container):
    """F5 语义必须**看得见**：轮询被乘子放慢，重投基准不受影响（§3.18 的两条曲线）。"""
    await _pollute(container)
    payload = (await admin.get("/admin/channels/echo/pacing", headers=OPERATOR)).json()
    polling = container.polling
    assert payload["multiplier"] > 1.0
    assert payload["paused_for"] > 0
    assert payload["last_rate_limited_at"] is not None
    assert payload["next_poll_interval"] > payload["resubmit_base_interval"], payload
    # 重投基准 = **不含乘子**的冷启动首轮间隔（§3.18 的核心口径）
    assert payload["resubmit_base_interval"] == pytest.approx(polling.clamp(polling.initial))
    assert payload["next_poll_interval"] == pytest.approx(
        polling.clamp(payload["resubmit_base_interval"] * payload["multiplier"])
    )


async def test_channel_pacing_reset_clears_penalty_and_is_audited(admin, container):
    """复位 = 运维杠杆：不必等静默期、也不必手工去 Redis 删键（旧口径的 workaround）。"""
    await _pollute(container)
    assert float(await container.polling.current_multiplier("echo")) > 1.0

    # 读角色不能复位（写操作要 operator/approver）
    assert (
        await admin.post("/admin/channels/echo/pacing/reset", headers=AUDITOR)
    ).status_code == 403
    # 未知角色连读都进不来
    assert (await admin.get("/admin/channels/echo/pacing", headers=NOBODY)).status_code == 403

    response = await admin.post("/admin/channels/echo/pacing/reset", headers=OPERATOR)
    assert response.status_code == 200, response.text
    payload = response.json()
    assert payload["before"]["multiplier"] > 1.0
    assert payload["after"]["multiplier"] == 1.0
    assert payload["after"]["paused_for"] == 0.0
    assert payload["after"]["last_rate_limited_at"] is None
    assert payload["include_samples"] is False

    # 复位后：乘子回 1.0、轮询与重投基准重新相等
    assert float(await container.polling.current_multiplier("echo")) == 1.0
    view = (await admin.get("/admin/channels/echo/pacing", headers=OPERATOR)).json()
    assert view["multiplier"] == 1.0
    assert view["next_poll_interval"] == view["resubmit_base_interval"]

    # append-only 审计留痕（谁在什么时候把乘子从多少清到多少）
    events = (await admin.get("/admin/audit", headers=AUDITOR)).json()["events"]
    reset_events = [e for e in events if (e.get("refs") or {}).get("operation") == "pacing.reset"]
    assert reset_events, events[:3]
    assert reset_events[0]["subject"] == "channel:echo"
    assert reset_events[0]["actor"] == "carol"  # OPERATOR 的身份

    # 幂等：再复位一次不报错
    assert (await admin.post("/admin/channels/echo/pacing/reset", headers=OPERATOR)).status_code == 200


async def test_channel_pacing_reset_keeps_samples_by_default(admin, container):
    """默认**不清样本**：限流惩罚与时长学习是两件事，复位前者不该牺牲后者。"""
    await container.polling.record_terminal("echo", 12.0)
    await container.polling.record_terminal("echo", 12.0)
    before = (await admin.get("/admin/channels/echo/pacing", headers=OPERATOR)).json()
    assert before["samples"] == 2

    kept = (await admin.post("/admin/channels/echo/pacing/reset", headers=OPERATOR)).json()
    assert kept["after"]["samples"] == 2, "默认不得清样本"

    cleared = (
        await admin.post(
            "/admin/channels/echo/pacing/reset",
            json={"include_samples": True},
            headers=OPERATOR,
        )
    ).json()
    assert cleared["after"]["samples"] == 0, "显式 include_samples 才清学习样本"


async def test_channel_pacing_rejects_unsafe_channel_names(admin):
    """渠道名要进 Redis 键 ⇒ 必须按白名单收口（治理面不能变成"任意键的读写把手"）。"""
    for bad in ("Echo", "up;flushall", "a/../../x", "{tag}", "", "a" * 80):
        response = await admin.get(f"/admin/channels/{bad}/pacing", headers=OPERATOR)
        assert response.status_code in (400, 404), f"{bad!r} 竟被接受：{response.status_code}"
