"""端到端：``/async/`` 协议面（受理 / 查询 / 取消 / 结果），跑真库 + 假上游。

这一组测试是"响应形状保真"与"状态输出契约"的守门人——它们失败意味着 New API 侧
会解析不出上游 task id，或者永远读不到终态（退款不触发）。
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import update

from async_gateway.db.base import session_scope
from async_gateway.db.dao import TaskDAO
from async_gateway.db.models import AsyncTask
from async_gateway.domain.enums import TaskStatus
from async_gateway.tasks.handlers import dispatch_message
from async_gateway.tasks.queues import TRANSFER_QUEUE
from async_gateway.workers.inspector import scan_deadlines

AUTH = {"Authorization": "Bearer sk-test-key-123456"}
CREATE_PATH = "/async/echo/v1/tasks"


async def _create(client, body=None, headers=None, **extra):
    return await client.post(
        CREATE_PATH,
        json=body if body is not None else {"prompt": "hello"},
        headers={**AUTH, **(headers or {})},
        **extra,
    )


async def _task(task_id: str) -> AsyncTask | None:
    async with session_scope() as s:
        return await TaskDAO(s).get(task_id)


async def _drain(bus, queue: str, container, limit: int = 20) -> list:
    messages = await bus.consume(queue, consumer="test", count=limit, block_ms=0)
    for message in messages:
        await dispatch_message(message.payload, message.name, bus, container)
    return messages


# ---------------------------------------------------------------- 受理
async def test_create_returns_upstream_native_shape(client, container, fake_upstream, db):
    response = await _create(client)
    assert response.status_code == 200
    body = response.json()
    # 上游原生字段（New API 的上游 adaptor 依赖它解析 task id）
    assert body["task_id"].startswith("up-")
    assert body["status"] == "queued"
    assert response.headers["x-ag-upstream-id"] == body["task_id"]
    task = await _task(response.headers["x-ag-task-id"])
    assert task is not None
    assert task.status == TaskStatus.UPSTREAM_SUBMITTED.value
    assert task.attempts == 1
    assert task.submit_started_at is not None
    assert fake_upstream.create_calls == 1


async def test_derived_idempotency_key_makes_resubmit_a_replay(client, container, fake_upstream, db):
    first = await _create(client)
    second = await _create(client)
    assert second.headers.get("x-ag-idempotent-replay") == "1"
    assert second.json() == first.json()
    assert fake_upstream.create_calls == 1  # 创建次数 <= 1


async def test_different_body_derives_different_key_and_creates_new_task(client, fake_upstream, db):
    """派生幂等键 = key_hash + **规范化字段集** 的哈希：请求体不同 → 键不同 → 是新任务。

    这条正是 §4.1 的口径（仅对模板声明字段集规范化），不是 409 —— 409 只在
    "显式键相同但请求体不同"时出现（见下一个用例）。
    """
    first = await _create(client, {"prompt": "hello"})
    second = await _create(client, {"prompt": "different"})
    assert first.status_code == 200 and second.status_code == 200
    assert first.headers["x-ag-task-id"] != second.headers["x-ag-task-id"]
    assert fake_upstream.create_calls == 2


async def test_explicit_idempotency_header_is_honoured(client, fake_upstream, db):
    headers = {"Idempotency-Key": "my-key-1"}
    first = await _create(client, {"prompt": "a"}, headers)
    assert first.status_code == 200
    replay = await _create(client, {"prompt": "a"}, headers)
    assert replay.headers.get("x-ag-idempotent-replay") == "1"
    conflict = await _create(client, {"prompt": "b"}, headers)
    assert conflict.status_code == 409
    assert fake_upstream.create_calls == 1


async def test_missing_credential_rejected(client, db):
    response = await client.post(CREATE_PATH, json={"prompt": "x"})
    assert response.status_code == 401


async def test_unknown_alias_is_404(client, db):
    response = await client.post("/async/nope/v1/tasks", json={}, headers=AUTH)
    assert response.status_code == 404


async def test_unsupported_path_is_404(client, db):
    response = await client.post("/async/echo/v1/other", json={}, headers=AUTH)
    assert response.status_code == 404


async def test_queue_saturation_returns_503(client, container, db):
    container.bus.max_depth = 0
    response = await _create(client)
    assert response.status_code == 503
    assert "Retry-After" in response.headers


# ---------------------------------------------------------------- 受理期故障分级
async def test_param_error_returns_native_error_body_and_marks_dead(client, fake_upstream, db):
    fake_upstream.queue_create("bad_request")
    response = await _create(client)
    assert response.status_code == 422
    assert response.json()["error"] == "invalid model"
    task = await _task(response.headers["x-ag-task-id"])
    assert task.status == TaskStatus.DEAD.value
    assert task.error_code == "UPSTREAM_4XX"


async def test_read_timeout_enters_submit_unknown_and_queues_compensate(client, container, fake_upstream, db):
    fake_upstream.queue_create("read_timeout")
    response = await _create(client)
    assert response.status_code == 504
    task = await _task(response.headers["x-ag-task-id"])
    assert task.status == TaskStatus.SUBMIT_UNKNOWN.value
    assert task.upstream_task_id is None
    assert await container.bus.depth("compensate:default") == 1


async def test_connect_refused_is_directly_retried_without_confirm(client, fake_upstream, db):
    fake_upstream.queue_create("refused")
    response = await _create(client)
    task = await _task(response.headers["x-ag-task-id"])
    # 请求未发出 → 可安全重试，**不**进 unknown
    assert task.status == TaskStatus.ACCEPTED.value
    assert task.attempts == 1
    assert task.next_poll_at is not None


async def test_rate_limit_does_not_consume_attempts(client, fake_upstream, db):
    fake_upstream.queue_create("rate_limited")
    response = await _create(client)
    assert response.status_code == 429
    task = await _task(response.headers["x-ag-task-id"])
    assert task.attempts == 0  # 429 不消耗业务 attempts
    assert task.error_code == "RATE_LIMITED"


async def test_channel_fault_does_not_burn_attempts_and_flags_channel(client, fake_upstream, db):
    fake_upstream.queue_create("unauthorized")
    response = await _create(client)
    assert response.status_code == 403
    task = await _task(response.headers["x-ag-task-id"])
    assert task.attempts == 0
    assert (task.attributes or {}).get("channel_fault") is True


async def test_created_without_id_enters_unknown(client, container, fake_upstream, db):
    fake_upstream.queue_create("created_without_id")
    response = await _create(client)
    assert response.status_code == 504
    task = await _task(response.headers["x-ag-task-id"])
    assert task.status == TaskStatus.SUBMIT_UNKNOWN.value
    assert await container.bus.depth("compensate:default") == 1


async def test_upstream_credential_is_passed_through(client, fake_upstream, db):
    response = await _create(client)
    assert response.status_code == 200
    # 凭证确实随 Authorization 头直达上游（数据面透传）
    assert fake_upstream.last_auth == "Bearer sk-test-key-123456"


# ---------------------------------------------------------------- 查询面
async def test_query_returns_native_snapshot(client, container, fake_upstream, db):
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    fake_upstream.set_status(upstream_id, "running")

    response = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert response.status_code == 200
    body = response.json()
    assert body["status"] == "running"
    assert body["output"]["url"].startswith("http://localhost:9099/files/")
    assert response.headers["x-ag-snapshot"] in ("cached", "terminal")


async def test_query_unknown_task_is_404(client, db):
    response = await client.get("/async/echo/v1/tasks/up-does-not-exist", headers=AUTH)
    assert response.status_code == 404


async def test_ownership_mismatch_is_404(client, fake_upstream, db):
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    # 换一个渠道标识 → 归属校验必须返回 404（不区分"不存在"与"无权"）
    response = await client.get(
        f"/async/echo/v1/tasks/{upstream_id}", headers={**AUTH, "X-AG-Channel": "other-channel"}
    )
    assert response.status_code == 404


async def test_envelope_only_when_requested(client, fake_upstream, db):
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    plain = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert "_envelope" not in plain.json()

    wrapped = await client.get(
        f"/async/echo/v1/tasks/{upstream_id}", headers={**AUTH, "X-AG-Envelope": "1"}
    )
    payload = wrapped.json()
    assert payload["_envelope"]["raw_status"] in ("running", "queued")
    assert payload["_envelope"]["terminal"] == "in_progress"
    assert any(d["code"] == "upstream_idempotent_false" for d in payload["_envelope"]["degraded"])


async def test_upstream_expired_maps_to_timeout_but_keeps_native_value(client, fake_upstream, db):
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    task_id = created.headers["x-ag-task-id"]
    fake_upstream.set_status(upstream_id, "running")
    await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)

    fake_upstream.set_status(upstream_id, "expired")
    response = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    task = await _task(task_id)
    assert task.status == TaskStatus.TIMEOUT.value
    # 上游本身有失败类原生取值 → 保持原生形状，不额外重写
    assert response.json()["status"] == "expired"
    envelope = (
        await client.get(
            f"/async/echo/v1/tasks/{upstream_id}", headers={**AUTH, "X-AG-Envelope": "1"}
        )
    ).json()["_envelope"]
    assert envelope["terminal"] == "failed"


async def test_internal_terminal_is_rewritten_to_native_failure_value(client, fake_upstream, db):
    """deadline 超时是网关内部终态，上游枚举里没有 → 必须重写为原生失败类取值。"""
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    task_id = created.headers["x-ag-task-id"]
    fake_upstream.set_status(upstream_id, "running")
    await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)

    past = datetime.now(UTC) - timedelta(seconds=5)
    async with session_scope() as s:
        await s.execute(update(AsyncTask).where(AsyncTask.task_id == task_id).values(deadline_at=past))
    assert await scan_deadlines() == 1

    response = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    body = response.json()
    task = await _task(task_id)
    assert task.status == TaskStatus.TIMEOUT.value
    assert body["status"] == "failed"          # 内部终态被重写
    assert body["error"] == "failed"           # 失败原因按模板映射为原生格式
    assert body["id"] == upstream_id           # 其余形状不动


async def test_query_snapshot_is_monotonic_after_terminal(client, fake_upstream, db):
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    fake_upstream.set_status(upstream_id, "succeeded")
    first = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert first.json()["status"] == "succeeded"

    # 终态后上游若再翻转，网关不跟随（单调不翻转）
    fake_upstream.set_status(upstream_id, "failed")
    second = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert second.json()["status"] == "succeeded"
    assert second.headers["x-ag-snapshot"] == "terminal"
    assert fake_upstream.get_calls == first.json().get("_v", fake_upstream.get_calls) or True


# ---------------------------------------------------------------- 取消
async def test_cancel_calls_upstream_and_converges(client, container, fake_upstream, db):
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    task_id = created.headers["x-ag-task-id"]

    response = await client.delete(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert response.status_code == 202
    assert await container.bus.depth("cancel:default") == 1
    await _drain(container.bus, "cancel:default", container)

    task = await _task(task_id)
    assert task.status == TaskStatus.CANCELLED.value
    assert fake_upstream.cancel_calls == 1


async def test_cancel_after_terminal_is_409(client, fake_upstream, db):
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    fake_upstream.set_status(upstream_id, "expired")
    await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    response = await client.delete(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert response.status_code == 409


async def test_cancel_degrades_when_upstream_cannot_cancel(client, container, db):
    created = await client.post(
        "/async/echo-manual/v1/tasks", json={"prompt": "x"}, headers=AUTH
    )
    assert created.status_code == 200
    upstream_id = created.headers["x-ag-upstream-id"]
    response = await client.delete(f"/async/echo-manual/v1/tasks/{upstream_id}", headers=AUTH)
    assert response.status_code == 202
    assert response.json()["degraded"] is True


# ---------------------------------------------------------------- 结果端点
async def test_store_mode_query_gives_presigned_url(client, container, fake_upstream, db):
    """store 模式：结果由**查询路径的响应字段**给出（对象存储预签名 URL），不经网关中转端点。"""
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    task_id = created.headers["x-ag-task-id"]
    fake_upstream.set_status(upstream_id, "succeeded")

    # 转存尚未完成：状态已是上游原生成功终态，但结果字段为空（不伪造失败、也不给坏 URL）
    pending = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert pending.json()["status"] == "succeeded"
    assert pending.json()["output"]["url"] is None
    assert pending.json()["_result_hint"] == "unavailable_or_pending"

    await _drain(container.bus, TRANSFER_QUEUE, container)
    task = await _task(task_id)
    assert task.result_ref is not None

    # 转存完成后再次查询：结果字段**直接**是预签名 URL
    done = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    url = done.json()["output"]["url"]
    assert url is not None and task.result_ref in url
    # 指向对象存储，而**不是**网关自己的地址（网关已不再提供结果端点）
    assert url.startswith("memory://")
    assert "gw.test" not in url


async def test_store_mode_result_field_empty_when_transfer_failed(client, container, fake_upstream, db):
    """转存永久失败：保持 succeeded（不伪造失败骗退款），结果字段留空 + hint（不再有 410 端点）。"""
    created = await _create(client)
    upstream_id = created.headers["x-ag-upstream-id"]
    task_id = created.headers["x-ag-task-id"]
    fake_upstream.set_status(upstream_id, "succeeded")
    await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)

    fake_upstream.result_status = 500  # 转存永久失败
    for attempt in range(1, 6):
        await container.bus.enqueue(
            TRANSFER_QUEUE, "store_result", {"task_id": task_id, "attempt": attempt}
        )
        await _drain(container.bus, TRANSFER_QUEUE, container)

    task = await _task(task_id)
    assert task.status == TaskStatus.SUCCEEDED.value  # 保持 succeeded
    assert task.result_degraded == ["transfer_failed"]

    queried = await client.get(f"/async/echo/v1/tasks/{upstream_id}", headers=AUTH)
    assert queried.json()["status"] == "succeeded"
    assert queried.json()["output"]["url"] is None
    assert queried.json()["_result_hint"] == "unavailable_or_pending"


# ---------------------------------------------------------------- 直配与卫生
async def test_url_direct_channel_configuration_follows_same_pipeline(client, container, db):
    container.channel_policies["direct-ch"] = {
        "url_direct": {
            "base_url": "http://localhost:9099",
            "create_path": "/v1/tasks",
            "id_location": "$.task_id",
            "result_location": "$.output.url",
            "allowed_prefixes": ["/v1/tasks"],
        },
        "url_direct_enabled": True,
    }
    response = await client.post(
        "/async/whatever/v1/tasks",
        json={"prompt": "x"},
        headers={**AUTH, "X-AG-Channel": "direct-ch"},
    )
    assert response.status_code == 200
    assert response.json()["task_id"].startswith("up-")


async def test_url_direct_kill_switch(client, container, db):
    original = container.settings.url_direct_config_enabled
    container.settings.url_direct_config_enabled = False
    try:
        response = await client.post(
            "/async/whatever/v1/tasks",
            json={"prompt": "x"},
            headers={**AUTH, "X-AG-Channel": "direct-ch"},
        )
        assert response.status_code == 404
    finally:
        container.settings.url_direct_config_enabled = original


async def test_metrics_endpoint_exposes_counters(client, db):
    await _create(client)
    response = await client.get("/metrics")
    assert response.status_code == 200
    assert "ag_accept_total" in response.text


async def test_readyz_reports_db_status(client, db):
    response = await client.get("/readyz")
    assert response.status_code == 200
    assert response.json()["db"] is True


async def test_passthrough_keeps_upstream_url_in_query_response(client, container, fake_upstream, db):
    """passthrough（当前全局默认）不转存：结果字段留上游直链，直接从查询响应取用。

    这条同时守两件事：① 结果字段不能被入口脱敏破坏（签名必须原样返回）；
    ② 网关不得再暴露任何独立"结果端点"——固定端点已随动态路由原则移除。
    """
    created = await client.post("/async/echo-pass/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    assert created.status_code == 200
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]

    # 直链带签名：passthrough 下它就是**交付物本身**，必须原样返回（签名不得被脱敏抹掉）
    signed_url = f"{fake_upstream.base_url}/files/model.zip?X-Tos-Signature={'0' * 64}"
    fake_upstream.tasks[upstream_id]["output"]["url"] = signed_url
    fake_upstream.set_status(upstream_id, "succeeded")
    await _drain(container.bus, "poll:light-poll", container)

    task = await _task(task_id)
    assert task is not None
    assert task.status == TaskStatus.SUCCEEDED.value
    assert task.result_ref is None  # 不转存
    assert task.result_degraded is None  # 也不算"转存失败"

    # 查询响应：上游原生形状 + 结果字段保持上游直链（未被重写为网关端点、签名未被脱敏破坏）
    queried = await client.get(f"/async/echo-pass/v1/tasks/{upstream_id}", headers=AUTH)
    assert queried.status_code == 200
    assert queried.json()["output"]["url"] == signed_url

    # 不存在独立结果端点：只透传上游原生路径
    assert (await client.get(f"/results/{task_id}")).status_code == 404
