"""F5 守卫：上游限流退避 × 任务 deadline 预算的联动（livetest-ai 报告 E2E-ASYNC-TASK-001）。

**报告原文（0.0.4 轮新发现，medium）**：

> 渠道被限流到 AIMD 乘子上限（20×）时，重投延迟与轮询间隔都被拉到 60s；在模板 deadline=120s
> 下，「60s 重投 + 创建 + 60s 首次轮询」链条必然超期 ⇒ 任务被 timeout **杀掉**（而非重试成功）。
> 根因：AIMD 乘子上限与模板 deadline 无联动校验；429 的重投 delay 直接取 next_interval（含乘子）
> —— 两套机制各自合理、组合必死。

三条根因各配一条守卫，外加两条"顺手修掉的既有缺陷"（同一条代码路径上）：

1. 重投退避**不再**继承轮询 AIMD 乘子（``test_rate_limited_resubmit_delay_ignores_poll_multiplier``）；
2. 上游造成的等待**从任务预算里剔除**且有界
   （``test_upstream_wait_extends_deadline`` / ``..._is_capped``）；
3. AIMD 乘子**会恢复**（此前 ``note_clean_period`` 只在单测里出现 ⇒ 一次风暴后乘子保留到
   Redis 键 TTL 7 天，所有轮询按 60s 跑）：``test_aimd_recovers_after_quiet_period``、
   ``test_successful_submit_decays_aimd``；
4. 渠道级故障释放意图时**不再整体覆盖 attributes**（旧代码会把受理期落下的
   ``create_response_ref`` 一起抹掉，幂等重放就取不到占位响应）：
   ``test_channel_fault_release_preserves_accepted_attributes``；
5. 启动期**警示**模板 deadline 未显著大于轮询上限：``test_validator_warns_...`` 等。
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml

from async_gateway.db.base import session_scope
from async_gateway.db.dao import TaskDAO
from async_gateway.db.models import AsyncTask
from async_gateway.domain.enums import TaskStatus
from async_gateway.infra.polling import MemoryPollingController
from async_gateway.templates.registry import BUILTIN_DIR, TemplateRegistry
from async_gateway.templates.validator import validate

AUTH = {"Authorization": "Bearer sk-test-key-123456"}
CREATE_PATH = "/async/echo/v1/tasks"
CHANNEL = "echo"

MINIMAL = {
    "alias": "volc-seedance",
    "base_url": "https://ark.cn-beijing.volces.com",
    "create_path": "/api/v3/contents/generations/tasks",
    "result_location": "$.content.video_url",
}


def _now() -> datetime:
    return datetime.now(UTC)


async def _task(task_id: str) -> AsyncTask:
    async with session_scope() as s:
        row = await TaskDAO(s).get(task_id)
    assert row is not None
    return row


async def _storm(container, *, channel: str = CHANNEL) -> float:
    """把渠道乘子顶到上限（模拟持续 429 风暴），返回当前乘子。"""
    for _ in range(20):
        await container.polling.note_rate_limited(channel, None)
    return float(container.polling.multiplier.get(channel, 1.0))


# ---------------------------------------------------------------- 1) 重投退避与乘子解耦
async def test_rate_limited_resubmit_delay_ignores_poll_multiplier(
    client, container, fake_upstream, db
):
    """轮询间隔被限流退避推到 60s 级时，**重投**必须仍是秒级。

    旧口径：``delay = max(retry_after, next_interval(...))`` —— 乘子一乘进来就是 60s，
    任务在创建成功前毫无进展 ⇒ 被 deadline 收尾成 timeout（F5 现场）。
    """
    multiplier = await _storm(container)
    poll_interval = await container.polling.next_interval(CHANNEL, elapsed=0.0, first_poll=True)
    assert multiplier > 100, f"测试前提：乘子应被顶高，实得 {multiplier}"
    assert poll_interval >= 30.0, f"测试前提：轮询面确实被拉到 60s 级，实得 {poll_interval}"

    fake_upstream.queue_create("rate_limited")
    response = await client.post(CREATE_PATH, json={"prompt": "storm"}, headers=AUTH)
    assert response.status_code == 429
    task = await _task(response.headers["x-ag-task-id"])

    delay = (task.next_poll_at - _now()).total_seconds()
    assert task.status == TaskStatus.ACCEPTED.value
    assert task.attempts == 0, "429 不消耗业务 attempts"
    assert delay <= 2.0, f"重投退避应≈Retry-After(1s)，实得 {delay}s —— 被乘子放大了"
    assert delay < poll_interval / 10, "重投退避不得随轮询乘子一起放大"
    # 乘子仍然生效（轮询面照旧退避），只是不再乘到重投上
    assert container.polling.multiplier[CHANNEL] > 100


async def test_storm_then_resubmit_converges_without_waiting_sixty_seconds(
    client, container, fake_upstream, db
):
    """风暴态下重投到第二跳成功的**全链**：把退避时刻拨到当前即可完成（无需真等 60s）。"""
    import uuid

    await _storm(container)
    fake_upstream.queue_create("rate_limited")
    created = await client.post(
        CREATE_PATH, json={"prompt": "epic-1"}, headers={**AUTH, "X-AG-Idempotency-Key": str(uuid.uuid4())}
    )
    assert created.status_code == 429
    task_id = created.headers["x-ag-task-id"]

    # 退避到期（等价于"等了一个 Retry-After 周期"，不真睡）→ 重投
    from datetime import timedelta

    from async_gateway.tasks.handlers import dispatch_message
    from async_gateway.workers.scheduler import dispatch_due

    async with session_scope() as s:
        assert await TaskDAO(s).schedule_next_poll(task_id, _now() - timedelta(seconds=1))
    assert await dispatch_due(container.bus, batch_size=10, lease_seconds=60) == 1
    messages = await container.bus.consume("submit:default", consumer="test", count=10, block_ms=0)
    for message in messages:
        await dispatch_message(message.payload, message.name, container.bus, container)

    task = await _task(task_id)
    assert task.status in (TaskStatus.UPSTREAM_SUBMITTED.value, TaskStatus.IN_PROGRESS.value)
    assert task.upstream_task_id, "第二跳必须成功创建（而不是被 timeout 收尾）"


# ---------------------------------------------------------------- 2) 等待不计入任务预算
async def test_upstream_wait_extends_deadline(client, container, fake_upstream, db):
    """429 的等待期从任务预算里顺延：deadline 必须大于模板名义预算。"""
    fake_upstream.queue_create("rate_limited")
    response = await client.post(CREATE_PATH, json={"prompt": "extend"}, headers=AUTH)
    assert response.status_code == 429
    task = await _task(response.headers["x-ag-task-id"])

    nominal = _effective_deadline_seconds(container)
    budget = (task.deadline_at - task.created_at).total_seconds()
    extended = float((task.attributes or {}).get("deadline_extended_seconds", 0.0) or 0.0)
    assert extended >= 1.0, "窗口内应至少顺延出上游要求的 Retry-After（1s）"
    assert budget > nominal, f"deadline 应被顺延：预算 {budget:.1f}s vs 名义 {nominal:.0f}s"


async def test_upstream_wait_extension_is_capped(client, container, fake_upstream, db, monkeypatch):
    """顺延**必须有界**：429 不消耗 attempts，无限顺延会让限流渠道里的任务永不收敛。"""
    monkeypatch.setattr(container.settings, "deadline_extension_max_seconds", 3.0)
    fake_upstream.queue_create("rate_limited")
    fake_upstream.retry_after_header = "600"  # 上游要求等 10 分钟
    response = await client.post(CREATE_PATH, json={"prompt": "cap"}, headers=AUTH)
    assert response.status_code == 429
    task = await _task(response.headers["x-ag-task-id"])

    extended = float((task.attributes or {}).get("deadline_extended_seconds", 0.0) or 0.0)
    assert extended == pytest.approx(3.0), f"累计顺延须被 AG_DEADLINE_EXTENSION_MAX_SECONDS 截断，实得 {extended}"
    # 退避本身仍尊重上游的 Retry-After（不裁剪）——被裁的只是"从任务预算里剔除的部分"
    delay = (task.next_poll_at - _now()).total_seconds()
    assert delay > 500.0, f"上游显式 Retry-After=600 应被尊重，实得 {delay}s"


async def test_deadline_extension_dao_primitive(container, db):
    """DAO 层原语：release 带顺延 ⇒ deadline_at 前移；不带 ⇒ 不动。"""
    from async_gateway.domain.enums import Origin

    task_id = "t" + "0" * 23
    async with session_scope() as s:
        s.add(
            AsyncTask(
                task_id=task_id,
                idempotency_key="k-extend",
                idempotency_bucket=1,
                tenant="default",
                channel=CHANNEL,
                task_type="echo",
                status=TaskStatus.ACCEPTED.value,
                template_alias="echo",
                template_version=1,
                attempts=1,
                max_attempts=3,
                origin=Origin.USER.value,
                deadline_at=_now() + timedelta(seconds=100),
                submit_started_at=_now(),
            )
        )
    async with session_scope() as s:
        dao = TaskDAO(s)
        before = await dao.get(task_id)
        assert before is not None
        original_deadline = before.deadline_at  # 会话关闭后属性会刷新 ⇒ 先取标量快照
        assert await dao.release_submit_intent(
            task_id,
            expected=[TaskStatus.ACCEPTED],
            next_poll_at=_now(),
            new_deadline_at=original_deadline + timedelta(seconds=7.5),
            decrement_attempts=True,
        )
    after = await _task(task_id)
    assert after.deadline_at - original_deadline == timedelta(seconds=7.5)
    assert after.submit_started_at is None


# ---------------------------------------------------------------- 3) AIMD 会恢复
async def test_aimd_recovers_after_quiet_period():
    """静默期未满 ⇒ 不回落；满了 ⇒ 回落一步（下限 1.0）。"""
    ctrl = MemoryPollingController(base=3.0, minimum=3.0, maximum=60.0, initial=5.0)
    await ctrl.note_rate_limited("ch", None)  # 2×
    assert await ctrl.decay_if_clean("ch", quiet_seconds=3600) is None, "静默期未满不得回落"
    assert ctrl.multiplier["ch"] == 2.0
    new = await ctrl.decay_if_clean("ch", quiet_seconds=0)
    assert new is not None and new == pytest.approx(1.8)


async def test_successful_submit_decays_aimd(client, container, fake_upstream, db, monkeypatch):
    """成功调用 = 干净周期：接到恢复路径上（旧代码里 note_clean_period 从未被调用）。"""
    monkeypatch.setattr(container.settings, "aimd_quiet_seconds", 0.0)
    for _ in range(3):
        await container.polling.note_rate_limited(CHANNEL, None)
    before = float(container.polling.multiplier[CHANNEL])
    assert before > 1.0

    response = await client.post(CREATE_PATH, json={"prompt": "healthy"}, headers=AUTH)
    assert response.status_code == 200
    after = float(container.polling.multiplier[CHANNEL])
    assert after < before, f"成功后乘子应回落：{before} → {after}"


# ---------------------------------------------------------------- 4) 渠道故障不覆盖受理期属性
async def test_channel_fault_release_preserves_accepted_attributes(client, container, fake_upstream, db):
    """401/403 释放意图时走**合并写入**：受理期的 create_response_ref 不得被抹掉。"""
    fake_upstream.queue_create("unauthorized")
    response = await client.post(CREATE_PATH, json={"prompt": "cf"}, headers=AUTH)
    assert response.status_code == 403
    task = await _task(response.headers["x-ag-task-id"])
    attrs = task.attributes or {}
    assert attrs.get("channel_fault") is True
    assert attrs.get("create_response_ref"), "受理期的响应存档引用被整体覆盖了（幂等重放会坏）"


# ---------------------------------------------------------------- 5) 启动期警示
def test_validator_warns_when_deadline_too_close_to_poll_max():
    """deadline ≤ 2× poll_max_interval ⇒ 告警（不阻断），并给出改法。"""
    report = validate({**MINIMAL, "strategy": {"deadline_seconds": 120}}, poll_max_interval=60.0)
    assert report.ok, report.as_dict()
    messages = [i.message for i in report.warnings]
    assert any("未显著大于轮询上限" in m for m in messages), messages
    assert any("§3.18" in m for m in messages), "告警要指到文档，便于承接方判断"


def test_validator_accepts_deadline_far_above_poll_max():
    report = validate({**MINIMAL, "strategy": {"deadline_seconds": 600}}, poll_max_interval=60.0)
    assert not any("未显著大于轮询上限" in i.message for i in report.warnings)


def test_builtin_echo_template_ships_safe_deadline():
    """内置 echo 模板不得再带着"必然超期"的组合出厂（F5 现场用的就是它）。"""
    raw = yaml.safe_load((BUILTIN_DIR / "echo.yaml").read_text(encoding="utf-8"))
    report = validate(raw, poll_max_interval=60.0)
    assert not any("未显著大于轮询上限" in i.message for i in report.warnings), report.as_dict()


def test_registry_logs_validation_warnings_at_load(tmp_path, caplog):
    """启动期（文件加载）必须把这类告警落到日志 —— 否则没人会看见。"""
    (tmp_path / "risky.yaml").write_text(
        yaml.safe_dump(
            {
                **MINIMAL,
                "alias": "risky-template",
                "strategy": {"deadline_seconds": 60},
            },
            allow_unicode=True,
        ),
        encoding="utf-8",
    )
    with caplog.at_level(logging.WARNING, logger="async_gateway.templates.registry"):
        TemplateRegistry(extra_dirs=[tmp_path])
    assert any("未显著大于轮询上限" in r.getMessage() for r in caplog.records), [
        r.getMessage() for r in caplog.records
    ]


def _effective_deadline_seconds(container, alias: str = "echo") -> float:
    """**生效模板**的名义任务预算（fixtures 会覆盖 builtin 同名 alias，别读错文件）。"""
    tv = container.registry.get(alias)
    assert tv is not None, f"模板 {alias} 未注册"
    return float(tv.resolved.strategy.get("deadline_seconds", 0))
