"""后台链路：崩溃窗口分流、补偿收敛、回调去重与仲裁、巡检有界化、调度租约、审计链。"""

from __future__ import annotations

import json
import time
import uuid
from datetime import UTC, datetime, timedelta

from sqlalchemy import update

from async_gateway.db.audit import AuditEventType, AuditWriter
from async_gateway.db.base import session_scope
from async_gateway.db.dao import TaskDAO
from async_gateway.db.models import AsyncTask
from async_gateway.domain.enums import Origin, TaskStatus
from async_gateway.security.callback_auth import new_opaque_token, sign
from async_gateway.tasks.handlers import dispatch_message
from async_gateway.tasks.queues import TRANSFER_QUEUE
from async_gateway.workers import inspector as inspector_mod
from async_gateway.workers.scheduler import dispatch_due

AUTH = {"Authorization": "Bearer sk-test-key-123456"}


def _now() -> datetime:
    return datetime.now(UTC)


async def make_task(**over) -> str:
    task_id = over.pop("task_id", uuid.uuid4().hex[:24])
    defaults = dict(
        task_id=task_id,
        idempotency_key=over.pop("idempotency_key", f"k-{uuid.uuid4().hex[:10]}"),
        idempotency_bucket=1,
        tenant="default",
        channel="echo",
        task_type="echo",
        status=TaskStatus.ACCEPTED.value,
        template_alias="echo",
        template_version=1,
        attempts=0,
        max_attempts=3,
        origin=Origin.USER.value,
        created_at=_now(),
        updated_at=_now(),
    )
    defaults.update(over)
    async with session_scope() as s:
        s.add(AsyncTask(**defaults))
    return task_id


async def get_task(task_id: str) -> AsyncTask:
    async with session_scope() as s:
        row = await TaskDAO(s).get(task_id)
    assert row is not None
    return row


async def drain(bus, queue: str, container, limit: int = 20) -> int:
    messages = await bus.consume(queue, consumer="test", count=limit, block_ms=0)
    for message in messages:
        await dispatch_message(message.payload, message.name, bus, container)
    return len(messages)


# ---------------------------------------------------------------- 崩溃窗口两子项
async def test_accepted_stall_without_submit_intent_is_requeued(container, fake_upstream, db):
    """无提交意图 = 入队失败/消息丢失，上游必未创建 → 条件更新重投（安全）。"""
    task_id = await make_task(status=TaskStatus.ACCEPTED.value, submit_started_at=None)
    moved = await inspector_mod.scan_accepted_stalls(container.bus, stall_seconds=0)
    assert moved == 1
    task = await get_task(task_id)
    assert task.status == TaskStatus.ACCEPTED.value
    assert await container.bus.depth("submit:default") == 1


async def test_accepted_stall_with_submit_intent_goes_unknown_not_recreate(container, fake_upstream, db):
    """有提交意图 = 已调上游未落库 → 转 submit_unknown 走补偿，**禁止直接重投**。"""
    task_id = await make_task(
        status=TaskStatus.ACCEPTED.value, submit_started_at=_now(), attempts=1
    )
    await inspector_mod.scan_accepted_stalls(container.bus, stall_seconds=0)
    task = await get_task(task_id)
    assert task.status == TaskStatus.SUBMIT_UNKNOWN.value
    assert await container.bus.depth("submit:default") == 0  # 没有盲建
    assert await container.bus.depth("compensate:default") == 1


# ---------------------------------------------------------------- 补偿唯一入口
async def test_compensate_manual_only_awaits_human(client, container, fake_upstream, db):
    created = await client.post("/async/echo-manual/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    await _force_unknown(task_id)
    await drain(container.bus, "compensate:default", container)
    task = await get_task(task_id)
    assert task.status == TaskStatus.DEAD_AWAITING_CONFIRM.value
    assert task.error_code == "UNKNOWN_EXHAUSTED"
    assert fake_upstream.create_calls == 1  # 永不自动重发创建


async def test_compensate_confirmed_not_created_resubmits(client, container, fake_upstream, db):
    """query_by_client_key + 上游 404 → 确认未创建 → 唯一允许的重发创建路径。"""
    created = await client.post(
        "/async/echo/v1/tasks",
        json={"prompt": "x"},
        headers={**AUTH, "X-AG-Idempotency-Key": "comp-1"},
    )
    task_id = created.headers["x-ag-task-id"]
    await _force_unknown(task_id)
    before = fake_upstream.create_calls
    await drain(container.bus, "compensate:default", container)
    task = await get_task(task_id)
    assert task.status == TaskStatus.ACCEPTED.value
    assert task.submit_started_at is None
    assert await container.bus.depth("submit:default") >= 1
    assert fake_upstream.create_calls == before


async def test_compensate_respects_cancel_requested(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    await _force_unknown(task_id)
    async with session_scope() as s:
        await s.execute(
            update(AsyncTask)
            .where(AsyncTask.task_id == task_id)
            .values(cancel_requested=True, cancel_requested_at=_now())
        )
    await drain(container.bus, "compensate:default", container)
    task = await get_task(task_id)
    # 置位则转人工关闭，绝不重发创建
    assert task.status == TaskStatus.DEAD_AWAITING_CONFIRM.value
    assert "cancel_requested" in (task.error_message or "")


async def test_compensate_with_existing_upstream_id_resumes_polling_without_recreating(
    client, container, fake_upstream, db
):
    """已有 upstream_task_id = 任务一定已在上游存在 → 禁止任何创建类动作（§18.4）。"""
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    await _force_unknown(task_id, clear_upstream_id=False)
    before = fake_upstream.create_calls

    await drain(container.bus, "compensate:default", container)
    task = await get_task(task_id)
    assert task.status == TaskStatus.UPSTREAM_SUBMITTED.value
    assert task.upstream_task_id == upstream_id
    assert fake_upstream.create_calls == before  # 没有重发创建
    assert await container.bus.depth("poll:light-poll") >= 1


async def test_compensate_unknown_lifetime_exceeded(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    await _force_unknown(task_id, unknown_since=_now() - timedelta(hours=2))
    await drain(container.bus, "compensate:default", container)
    task = await get_task(task_id)
    assert task.status == TaskStatus.DEAD_AWAITING_CONFIRM.value


async def _force_unknown(
    task_id: str, *, unknown_since: datetime | None = None, clear_upstream_id: bool = True
) -> None:
    """把任务推到 submit_unknown（模拟"提交响应丢失、不知道上游是否已创建"）。

    ``clear_upstream_id=True`` 是**逼真的**模拟：submit_unknown 的语义就是"我们没有上游 id"。
    若已有 upstream_task_id，那就不是 unknown 而是"已创建、只是失联"，走的是另一条路
    （见 test_compensate_with_existing_id_resumes_polling_without_recreating）。
    """
    current = await get_task(task_id)
    async with session_scope() as s:
        await TaskDAO(s).enter_unknown(
            task_id,
            error_code="UPSTREAM_5XX",
            error_message="forced for test",
            expected=[TaskStatus(current.status)],
        )
        values: dict = {}
        if unknown_since is not None:
            values["unknown_since"] = unknown_since
        if clear_upstream_id:
            values["upstream_task_id"] = None
        if values:
            await s.execute(update(AsyncTask).where(AsyncTask.task_id == task_id).values(**values))
    moved = await get_task(task_id)
    assert moved.status == TaskStatus.SUBMIT_UNKNOWN.value, moved.status
    # 真实链路里是 worker 在进入 unknown 时投递补偿任务，这里补上同一步
    from async_gateway.gateway.container import get_container

    container = get_container()
    await container.bus.enqueue("compensate:default", "compensate_orphan", {"task_id": task_id})


# ---------------------------------------------------------------- 轮询
async def test_poll_success_enqueues_transfer(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    fake_upstream.set_status(upstream_id, "succeeded")

    await drain(container.bus, "poll:light-poll", container)
    task = await get_task(task_id)
    assert task.status == TaskStatus.SUCCEEDED.value
    assert task.next_poll_at is None
    assert await container.bus.depth(TRANSFER_QUEUE) == 1


async def test_poll_unrecognized_status_value(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    # 先清掉受理时投递的那条轮询消息，避免它把剧本吃掉
    await drain(container.bus, "poll:light-poll", container)
    await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)  # 进入 in_progress 快照

    fake_upstream.poll_script.append("unrecognized")
    await container.bus.enqueue("poll:light-poll", "poll_upstream", {"task_id": task_id})
    await drain(container.bus, "poll:light-poll", container)

    task = await get_task(task_id)
    assert task.status == TaskStatus.POLL_UNRECOGNIZED.value
    assert task.upstream_task_id == upstream_id  # 保留 id，永不触发创建
    assert fake_upstream.create_calls == 1


async def test_poll_404_escalates_only_after_threshold(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]

    fake_upstream.poll_script.extend(["not_found", "not_found", "not_found"])
    for _ in range(3):
        await container.bus.enqueue("poll:light-poll", "poll_upstream", {"task_id": task_id})
        await drain(container.bus, "poll:light-poll", container)

    task = await get_task(task_id)
    assert task.status == TaskStatus.SUBMIT_UNKNOWN.value
    assert await container.bus.depth("compensate:default") >= 1


async def test_upstream_reported_failure_schedules_task_level_retry(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)

    fake_upstream.set_status(upstream_id, "failed")
    await container.bus.enqueue("poll:light-poll", "poll_upstream", {"task_id": task_id})
    await drain(container.bus, "poll:light-poll", container)

    task = await get_task(task_id)
    assert task.status == TaskStatus.FAILED.value  # 待重试，不是终态
    assert task.error_code == "UPSTREAM_TERMINAL"


async def test_attempts_exhausted_goes_to_dead(client, container, fake_upstream, db):
    task_id = await make_task(
        status=TaskStatus.IN_PROGRESS.value,
        upstream_task_id="up-1",
        attempts=3,
        max_attempts=3,
        status_snapshot={"id": "up-1", "status": "running", "output": {"url": "http://localhost:9099/files/up-1.bin"}},
        status_snapshot_at=_now(),
        next_poll_at=_now(),
    )
    fake_upstream.tasks["up-1"] = {"id": "up-1", "status": "failed", "output": {"url": "x"}}
    await container.bus.enqueue("poll:light-poll", "poll_upstream", {"task_id": task_id})
    await drain(container.bus, "poll:light-poll", container)

    task = await get_task(task_id)
    assert task.status == TaskStatus.DEAD.value
    assert task.retryable is True


# ---------------------------------------------------------------- 转存
async def test_store_result_success_sets_result_ref(client, container, fake_upstream, db, result_store):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    fake_upstream.set_status(upstream_id, "succeeded")
    await drain(container.bus, "poll:light-poll", container)
    await drain(container.bus, TRANSFER_QUEUE, container)

    task = await get_task(task_id)
    assert task.result_ref is not None
    assert await result_store.get_bytes(task.result_ref) == fake_upstream.result_bytes

    # 重复投递无副作用（result_ref 已存在）
    await container.bus.enqueue(TRANSFER_QUEUE, "store_result", {"task_id": task_id, "attempt": 1})
    await drain(container.bus, TRANSFER_QUEUE, container)
    assert (await get_task(task_id)).result_ref == task.result_ref


async def test_store_result_retries_then_marks_unavailable(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    fake_upstream.set_status(upstream_id, "succeeded")
    await drain(container.bus, "poll:light-poll", container)
    fake_upstream.result_status = 503

    # 前 4 次尝试失败 → 只安排转存重试（由巡检按 next_poll_at 重新派发），保持 succeeded
    for attempt in range(1, 5):
        await container.bus.enqueue(TRANSFER_QUEUE, "store_result", {"task_id": task_id, "attempt": attempt})
        await drain(container.bus, TRANSFER_QUEUE, container)
    task = await get_task(task_id)
    assert task.result_ref is None
    assert task.next_poll_at is not None
    assert task.result_degraded is None

    # 第 5 次（= MAX_TRANSFER_ATTEMPTS）仍失败 → 标记结果不可用，但状态保持 succeeded
    await container.bus.enqueue(TRANSFER_QUEUE, "store_result", {"task_id": task_id, "attempt": 5})
    await drain(container.bus, TRANSFER_QUEUE, container)
    task = await get_task(task_id)
    assert task.result_degraded == ["transfer_failed"]
    assert task.status == TaskStatus.SUCCEEDED.value  # 不伪造失败


# ---------------------------------------------------------------- 回调
async def test_callback_signature_failure_only_audits(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    task = await get_task(task_id)
    raw = json.dumps({"id": task.upstream_task_id, "status": "succeeded"}).encode()
    response = await client.post(
        f"/callbacks/{task.callback_token}",
        content=raw,
        headers={"x-ag-kid": "k1", "x-ag-timestamp": str(int(time.time())), "x-ag-signature": "bad"},
    )
    assert response.status_code == 202
    assert response.json()["accepted"] is False
    assert (await get_task(task_id)).status == TaskStatus.UPSTREAM_SUBMITTED.value


async def test_callback_arbitration_when_poll_is_primary(client, container, fake_upstream, db):
    """poll 是主源时，回调判出的终态不直接落库，只触发主源复查（§14 终态冲突仲裁）。"""
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    task = await get_task(task_id)
    raw = json.dumps({"id": task.upstream_task_id, "status": "succeeded"}).encode()
    ts = int(time.time())
    response = await client.post(
        f"/callbacks/{task.callback_token}",
        content=raw,
        headers={
            "x-ag-kid": "k1",
            "x-ag-timestamp": str(ts),
            "x-ag-signature": sign("k1", "secret-one", ts, raw),
        },
    )
    assert response.status_code == 200
    refreshed = await get_task(task_id)
    assert refreshed.status == TaskStatus.UPSTREAM_SUBMITTED.value  # 没有翻转
    assert (refreshed.attributes or {}).get("pending_terminal_from_callback") is not None


async def test_callback_applies_when_callback_is_primary(client, container, fake_upstream, db):
    created = await client.post("/async/echo-cb/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    task = await get_task(task_id)
    raw = json.dumps({"task_id": task.upstream_task_id, "status": "succeeded"}).encode()
    ts = int(time.time())
    response = await client.post(
        f"/callbacks/{task.callback_token}",
        content=raw,
        headers={
            "x-ag-kid": "k1",
            "x-ag-timestamp": str(ts),
            "x-ag-signature": sign("k1", "secret-one", ts, raw),
        },
    )
    assert response.status_code == 200
    applied = await get_task(task_id)
    assert applied.status == TaskStatus.SUCCEEDED.value


async def test_callback_duplicate_is_idempotent(client, container, fake_upstream, db):
    created = await client.post("/async/echo-cb/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task = await get_task(created.headers["x-ag-task-id"])
    raw = json.dumps({"task_id": task.upstream_task_id, "status": "succeeded"}).encode()
    ts = int(time.time())
    headers = {
        "x-ag-kid": "k1",
        "x-ag-timestamp": str(ts),
        "x-ag-signature": sign("k1", "secret-one", ts, raw),
    }
    first = await client.post(f"/callbacks/{task.callback_token}", content=raw, headers=headers)
    second = await client.post(f"/callbacks/{task.callback_token}", content=raw, headers=headers)
    assert first.status_code == 200
    assert second.json().get("duplicate") is True


async def test_unknown_callback_token_is_stashed_as_orphan(client, container, fake_upstream, db):
    raw = json.dumps({"task_id": "up-orphan", "status": "succeeded"}).encode()
    ts = int(time.time())
    response = await client.post(
        f"/callbacks/{new_opaque_token()}",
        content=raw,
        headers={
            "x-ag-kid": "k1",
            "x-ag-timestamp": str(ts),
            "x-ag-signature": sign("k1", "secret-one", ts, raw),
        },
    )
    assert response.status_code == 200
    assert response.json().get("orphan") is True


# ---------------------------------------------------------------- 巡检与调度
async def test_inspector_bounds_unknown_by_lifetime(container, fake_upstream, db):
    task_id = await make_task(
        status=TaskStatus.SUBMIT_UNKNOWN.value,
        unknown_since=_now() - timedelta(hours=3),
        next_poll_at=_now() - timedelta(seconds=1),
    )
    moved = await inspector_mod.bound_unknowns(
        container.bus, max_per_channel=10_000, max_lifetime_seconds=60
    )
    assert moved == 1
    task = await get_task(task_id)
    assert task.status == TaskStatus.DEAD_AWAITING_CONFIRM.value
    assert task.next_poll_at is None


async def test_inspector_bounds_unknown_by_channel_limit(container, fake_upstream, db):
    for _ in range(3):
        await make_task(
            status=TaskStatus.SUBMIT_UNKNOWN.value,
            unknown_since=_now(),
            next_poll_at=_now() - timedelta(seconds=1),
        )
    moved = await inspector_mod.bound_unknowns(
        container.bus, max_per_channel=1, max_lifetime_seconds=10_000
    )
    assert moved >= 2


async def test_inspector_flags_stalled_active_task_as_orphan(container, fake_upstream, db):
    task_id = await make_task(
        status=TaskStatus.IN_PROGRESS.value,
        upstream_task_id="up-lost",
        next_poll_at=_now() - timedelta(seconds=600),
        updated_at=_now() - timedelta(seconds=600),
    )
    await inspector_mod.reconcile_orphans(container.bus, stall_seconds=60)
    task = await get_task(task_id)
    assert task.status == TaskStatus.SUBMIT_UNKNOWN.value
    assert await container.bus.depth("compensate:default") == 1


async def test_scheduler_dispatches_due_tasks_with_lease(container, fake_upstream, db):
    task_id = await make_task(
        status=TaskStatus.IN_PROGRESS.value,
        upstream_task_id="up-sched",
        next_poll_at=_now() - timedelta(seconds=1),
    )
    dispatched = await dispatch_due(container.bus, batch_size=10, lease_seconds=60)
    assert dispatched == 1
    assert await container.bus.depth("poll:light-poll") == 1
    # 派发后时间轴被推后 → 同一 tick 不会重复派发
    assert await dispatch_due(container.bus, batch_size=10, lease_seconds=60) == 0
    task = await get_task(task_id)
    assert task.next_poll_at > _now()


# ---------------------------------------------------------------- 审计
async def test_audit_chain_detects_tampering(client, container, fake_upstream, db):
    from async_gateway.db.models import AuditEvent

    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    upstream_id = created.headers["x-ag-upstream-id"]
    await client.delete(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)

    async with session_scope() as s:
        writer = AuditWriter(s)
        ok, reason = await writer.verify_chain()
        assert ok, reason
        # 篡改一条审计记录 → 链必须报错
        await s.execute(update(AuditEvent).values(actor="attacker"))

    async with session_scope() as s:
        ok, reason = await AuditWriter(s).verify_chain()
        assert not ok and "broken" in (reason or "")


async def test_audit_stores_only_references(client, container, fake_upstream, db):
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "secret-prompt"}, headers=AUTH)
    assert created.status_code == 200
    from sqlalchemy import select

    from async_gateway.db.models import AuditEvent

    async with session_scope() as s:
        rows = (await s.execute(select(AuditEvent))).scalars().all()
    assert rows
    blob = json.dumps([{"refs": r.refs, "detail": r.detail} for r in rows], ensure_ascii=False)
    assert "secret-prompt" not in blob  # 审计只存引用/哈希，不存个人负载
    assert any(r.event_type == AuditEventType.TERMINAL for r in rows) or any(
        r.event_type == AuditEventType.ACCEPTED for r in rows
    )

async def test_transfer_auto_disabled_when_store_unconfigured(
    client, container, fake_upstream, db, monkeypatch
):
    """未配置对象存储 ⇒ 转存自动关闭：不派发 transfer、结果保留上游直链、envelope 声明降级。"""
    from async_gateway.infra.object_store import MemoryResultStore, set_result_store

    monkeypatch.setattr(container, "result_store", None, raising=False)
    set_result_store(None)
    try:
        created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
        task_id = created.headers["x-ag-task-id"]
        upstream_id = created.headers["x-ag-upstream-id"]
        fake_upstream.set_status(upstream_id, "succeeded")
        await drain(container.bus, "poll:light-poll", container)

        task = await get_task(task_id)
        assert task.status == TaskStatus.SUCCEEDED.value
        assert task.result_ref is None  # 没有转存
        assert await container.bus.depth(TRANSFER_QUEUE) == 0  # 也没有派发 transfer

        queried = await client.get(
            f"/async/echo/v1/tasks/{task_id}", headers={**AUTH, "x-ag-envelope": "1"}
        )
        body = queried.json()
        codes = [n["code"] for n in body["_envelope"]["degraded"]]
        assert "transfer_disabled" in codes
        assert "result_pending" not in codes  # 没转存就不该谎报"转存中"
        # 结果仍是上游直链（没被清空）——信封把原生体放在 data 下
        assert body["data"]["output"]["url"].startswith(fake_upstream.base_url)
    finally:
        set_result_store(MemoryResultStore())
