"""受理默认模式（``queued``：不等上游、立刻 202）的协议面用例。

2026-09-21 裁定：默认 ``AG_SUBMIT_MODE=queued`` —— 受理只落库 + 入队即返回 202，
上游 create 由 worker 后台完成；调用方（含 New API 两族插件）用响应里的
``id`` / ``task_id``（= 网关任务 id）回查任务。

会话级测试基座把 ``AG_SUBMIT_MODE`` 钉在 inline（见 conftest）——本模块把**默认值**
与 **queued 全链路**单独钉住：默认值断言 + 202 体形状 + 不等上游 + 网关 id 查询兜底 +
后台创建/轮询/转存收敛 + 取消兜底。
"""

from __future__ import annotations

import pytest

from async_gateway.config import Settings
from async_gateway.db.base import session_scope
from async_gateway.db.dao import TaskDAO
from async_gateway.domain.enums import TaskStatus
from async_gateway.tasks.handlers import dispatch_message
from async_gateway.tasks.queues import TRANSFER_QUEUE

AUTH = {"Authorization": "Bearer sk-test-key-123456"}
CREATE_PATH = "/async/echo/v1/tasks"


def test_submit_mode_default_is_queued():
    """代码默认 = queued（不等上游、立刻 202）；inline 需显式配置。"""
    assert Settings.model_fields["submit_mode"].default == "queued"


@pytest.fixture
def queued(container, monkeypatch):
    monkeypatch.setattr(container.settings, "submit_mode", "queued")


async def _create(client, body=None, headers=None):
    return await client.post(
        CREATE_PATH,
        json=body if body is not None else {"prompt": "hello"},
        headers={**AUTH, **(headers or {})},
    )


async def _task(task_id: str):
    async with session_scope() as s:
        return await TaskDAO(s).get(task_id)


async def _drain(bus, queue: str, container, limit: int = 20) -> list:
    messages = await bus.consume(queue, consumer="test", count=limit, block_ms=0)
    for message in messages:
        await dispatch_message(message.payload, message.name, bus, container)
    return messages


async def test_queued_accept_returns_202_without_contacting_upstream(
    queued, client, container, fake_upstream, db
):
    response = await _create(client)
    assert response.status_code == 202
    task_id = response.headers["x-ag-task-id"]
    # id 与 task_id 同值：ark 系插件只认 body.id，generic-async-v1 认 task_id||id
    assert response.json() == {"id": task_id, "task_id": task_id, "status": "queued"}
    # 不等上游：受理返回时上游一次都没被调用（这就是"立刻 202"的全部意义）
    assert fake_upstream.create_calls == 0
    assert fake_upstream.get_calls == 0
    assert fake_upstream.request_log == []
    assert await container.bus.depth("submit:default") == 1
    task = await _task(task_id)
    assert task.status == TaskStatus.ACCEPTED.value


async def test_queued_accept_replay_carries_id(queued, client, fake_upstream, db):
    first = await _create(client)
    replay = await _create(client)
    assert replay.status_code == 202
    assert replay.headers.get("x-ag-idempotent-replay") == "1"
    assert replay.json()["id"] == first.headers["x-ag-task-id"]
    assert replay.json()["task_id"] == first.headers["x-ag-task-id"]
    # 重放不改事实：上游仍未被动过
    assert fake_upstream.create_calls == 0


async def test_query_by_gateway_task_id_before_submit(queued, client, fake_upstream, db):
    created = await _create(client)
    task_id = created.headers["x-ag-task-id"]
    queried = await client.get(f"/async/echo/v1/tasks/{task_id}", headers=AUTH)
    assert queried.status_code == 200
    # 预创建期合成非终态占位（否则 New API 侧宽映射读不到状态会判 UNKNOWN）
    assert queried.json()["status"] == "queued"
    assert queried.headers["x-ag-snapshot"] == "cached"


async def test_background_submit_then_query_converges(
    queued, client, container, fake_upstream, db
):
    created = await _create(client)
    task_id = created.headers["x-ag-task-id"]

    # 后台完成上游创建（与 worker 相同的 handler）
    await _drain(container.bus, "submit:default", container)
    assert fake_upstream.create_calls == 1
    task = await _task(task_id)
    assert task.status == TaskStatus.UPSTREAM_SUBMITTED.value
    upstream_id = task.upstream_task_id
    assert upstream_id

    # 上游成功 → 轮询收敛 → 转存收尾
    fake_upstream.set_status(upstream_id, "succeeded")
    await _drain(container.bus, "poll:light-poll", container)
    await _drain(container.bus, TRANSFER_QUEUE, container)

    task = await _task(task_id)
    assert task.status == TaskStatus.SUCCEEDED.value
    assert task.result_ref is not None

    # 网关 task_id 与上游 id 都能查到（查询面双解析）
    by_task = await client.get(f"/async/echo/v1/tasks/{task_id}", headers=AUTH)
    assert by_task.status_code == 200
    assert by_task.json()["status"] == "succeeded"
    by_upstream = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert by_upstream.status_code == 200
    assert by_upstream.json()["status"] == "succeeded"


async def test_cancel_by_gateway_task_id(queued, client, container, fake_upstream, db):
    created = await _create(client)
    task_id = created.headers["x-ag-task-id"]
    response = await client.delete(f"/async/echo/v1/tasks/{task_id}", headers=AUTH)
    assert response.status_code == 202
    await _drain(container.bus, "cancel:default", container)
    task = await _task(task_id)
    assert task.status == TaskStatus.CANCELLED.value
    # 未创建即取消：上游从头到尾没被创建过
    assert fake_upstream.create_calls == 0


def _counter_value(counter, labels: dict[str, str]) -> float:
    return counter.samples.get(tuple(sorted(labels.items())), 0.0)


async def test_query_before_submit_skips_passthrough_refresh(queued, client, fake_upstream, db):
    """预创建期查询**显式跳过**透传刷新，且对上游零请求（livetest-ai 报告 §3.5）。

    旧行为：任务还没有上游 id 时照样构造一次透传 GET —— 空 id 被净化层拒绝
    （"插值变量 id 为空"），请求根本没发出，异常又被查询面的 ``except Exception: pass``
    吞掉：既不产生价值，也不留任何痕迹（报告称"净化层拒绝 + 吞异常兜底"）。

    守两点：
    * 命中 ``skipped_no_upstream_id`` 计数（说明走的是"显式跳过"这条新路径）；
    * 期间 ``error`` 样本零增长（说明**没有**再去做那次注定失败的构造）。
    """
    from async_gateway.observability import metrics as M

    skipped_before = _counter_value(M.QUERY_REFRESH_TOTAL, {"result": "skipped_no_upstream_id"})
    error_before = _counter_value(M.QUERY_REFRESH_TOTAL, {"result": "error"})

    created = await _create(client)
    task_id = created.headers["x-ag-task-id"]
    for _ in range(4):  # 报告 T3c：预创建期连续 4 次查询
        response = await client.get(f"/async/echo/v1/tasks/{task_id}", headers=AUTH)
        assert response.status_code == 200
        assert response.json()["status"] == "queued"  # 占位状态不受影响
        assert response.headers["x-ag-snapshot"] == "cached"

    skipped_after = _counter_value(M.QUERY_REFRESH_TOTAL, {"result": "skipped_no_upstream_id"})
    error_after = _counter_value(M.QUERY_REFRESH_TOTAL, {"result": "error"})
    assert skipped_after - skipped_before == 4, "预创建期查询应逐次命中「显式跳过」"
    assert error_after == error_before, "不得再产生「构造空 id 请求 → 吞异常」的失败样本"
    assert fake_upstream.request_log == [], "预创建期对上游必须零请求"


async def test_query_refresh_failure_is_counted_not_swallowed(client, container, fake_upstream, db):
    """刷新不成功不再静默：必须在指标里留痕（可诊断性）。

    触发手段与真实故障同源：已提交的任务，上游对查询返回 5xx（``poll_script=http_500``）。
    查询仍必须 200（刷新是尽力而为、失败不许拖垮查询面），但"没刷到"这件事必须可观测——
    旧实现只在异常分支 ``pass``，成功与否都不留痕。
    """
    from async_gateway.observability import metrics as M

    created = await _create(client, {"prompt": "refresh-fail"})
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    assert upstream_id

    before = _counter_value(M.QUERY_REFRESH_TOTAL, {"result": "upstream_not_ok"})
    fake_upstream.poll_script = ["http_500", "http_500", "http_500"]
    response = await client.get(f"/async/echo/v1/tasks/{task_id}", headers=AUTH)
    assert response.status_code == 200  # 上游抖动不能把查询面拖死
    after = _counter_value(M.QUERY_REFRESH_TOTAL, {"result": "upstream_not_ok"})
    assert after > before, "刷新未成功必须进指标（此前是静默的）"
