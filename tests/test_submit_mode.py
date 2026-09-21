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
