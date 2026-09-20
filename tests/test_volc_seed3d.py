"""火山方舟图生 3D（doubao-seed3d）接入面 + 真机抓到的一个结果转存缺陷的回归。

这个上游与 Seedance 同基址、同 create_path，检验四件最容易出错的事：

1. **路径口径**：New API 任务插件（volcengine-ark-3d）在 JS 里硬编码
   ``ctx.baseUrl + "/api/v3/contents/generations/tasks"``，而网关按
   ``/async/{alias}`` 之后剩余路径与 ``create_path`` 逐字比对归位 ——
   两边口径必须逐字对齐，否则表现为"插件发出的请求在网关上 404"。
2. **结果字段**：直连方舟时产物在 ``content.file_url``（**不是** New API 插件 README
   所记的 ``content.url``——那是其 BFF 归一化后的形状）。字段写错表现为
   "任务 succeeded 但取不到结果"。
3. **带签名的结果 URL 必须能转存**：方舟结果 URL 是 TOS 预签名，签名长串会命中
   脱敏兜底规则。若快照存的是脱敏值，``store_result`` 拿到的 URL 不可用 →
   转存永久失败 → 结果端点 410。**这是 2026-09-20 真机抓到的真实缺陷**，
   下方两个用例是它的回归保护。
4. **原生形状 + 内部终态重写**：面向 New API 的响应必须保真，且网关内部终态要
   重写成上游原生失败类取值，否则退款链路不触发。

除最后两个转存用例外都是纯函数/纯解析断言；转存用例走内存 broker + MockTransport 假上游，
不打真实网络、不消耗上游额度。
"""

from __future__ import annotations

import copy
from datetime import UTC, datetime

from async_gateway.db.base import session_scope
from async_gateway.db.dao import TaskDAO
from async_gateway.db.models import AsyncTask
from async_gateway.domain.enums import Origin, TaskStatus
from async_gateway.gateway.output import build_native_query_response
from async_gateway.gateway.routing import match_route
from async_gateway.security.redaction import scrub_payload
from async_gateway.tasks.handlers import _restore_raw_result_field, dispatch_message
from async_gateway.tasks.queues import TRANSFER_QUEUE
from async_gateway.templates.derive import effective_failure_status, resolve
from async_gateway.templates.registry import TemplateRegistry
from async_gateway.templates.schema import (
    ConfirmStrategy,
    FieldSource,
    TemplateDraft,
    TemplateTier,
    template_tier,
)
from async_gateway.upstream.client import UpstreamCallResult, UpstreamClient, UpstreamResponse

AUTH = {"Authorization": "Bearer sk-test-key-123456"}

SEED3D = {
    "alias": "volc-seed3d",
    "base_url": "https://ark.cn-beijing.volces.com",
    "create_path": "/api/v3/contents/generations/tasks",
    "result_location": "$.content.file_url",
}

#: New API 插件 volcengine-ark-3d 中 buildSubmitRequest/buildQueryRequest 的硬编码后缀
#: （plugins/tasks/volcengine-ark-3d/1.0.1/plugin.js:140,174-177）
PLUGIN_PATH_SUFFIX = "/api/v3/contents/generations/tasks"

#: 方舟 3D 查询响应（成功）——2026-09-20 真机实测形状（签名已缩略）
ARK_QUERY_SUCCEEDED = {
    "id": "cgt-20260920125622-cxlkg",
    "model": "doubao-seed3d-2-0-260328",
    "status": "succeeded",
    "content": {
        "file_url": (
            "https://ark-content-generation-cn-beijing.tos-cn-beijing.volces.com/"
            "doubao-seed3d-2-0/model.zip?X-Tos-Algorithm=TOS4-HMAC-SHA256"
            # 注意：凭据段**不要**写成"格式合规的假 AK ID"（真实方舟 AK 是固定前缀 + 32 位），
            # GitHub Push Protection 会按 VolcEngine Access Key ID 模式拦截整个 push。
            # 这里只需要一个占位串；真正触发"签名被抹"的是下面那段 64 位签名。
            "&X-Tos-Credential=redaction-check-placeholder%2F20260920%2Fcn-beijing%2Ftos%2Frequest"
            "&X-Tos-Date=20260920T050058Z&X-Tos-Expires=86400"
            "&X-Tos-Signature=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
            "&X-Tos-SignedHeaders=host"
        )
    },
    # 实测：seed3d 返回 completion_tokens=30000（插件 extractUsage 自报 1，未采信上游值）
    "usage": {"completion_tokens": 30000, "total_tokens": 30000},
    "created_at": 1789880183,
    "updated_at": 1789880458,
    "subdivisionlevel": "high",
    "fileformat": "obj",
    "service_tier": "default",
    "execution_expires_after": 172800,
    "draft": False,
    "priority": 0,
}
ARK_RESULT_URL = ARK_QUERY_SUCCEEDED["content"]["file_url"]


def _resolved():
    return resolve(TemplateDraft.model_validate(SEED3D))


def _call_result(raw: dict) -> UpstreamCallResult:
    """模拟一次上游调用结果（json_body 脱敏 / raw_json_body 原始）。"""
    return UpstreamCallResult(
        outcome="ok",
        response=UpstreamResponse(
            status_code=200,
            headers={},
            json_body=scrub_payload(raw),
            text="",
            raw_json_body=raw,
        ),
    )


def _task(**over) -> AsyncTask:
    now = datetime.now(UTC)
    defaults = dict(
        task_id="t-seed3d-1",
        idempotency_key="k-seed3d-1",
        idempotency_bucket=1,
        tenant="default",
        channel="volc",
        task_type="image_to_3d",
        status=TaskStatus.SUCCEEDED.value,
        template_alias="volc-seed3d",
        template_version=1,
        attempts=1,
        max_attempts=1,
        origin=Origin.USER.value,
        raw_status="succeeded",
        status_snapshot=copy.deepcopy(ARK_QUERY_SUCCEEDED),
        created_at=now,
        updated_at=now,
    )
    defaults.update(over)
    return AsyncTask(**defaults)


async def _get_task(task_id: str) -> AsyncTask:
    async with session_scope() as session:
        row = await TaskDAO(session).get(task_id)
    assert row is not None
    return row


async def _drain(bus, queue: str, container, limit: int = 20) -> int:
    messages = await bus.consume(queue, consumer="test-seed3d", count=limit, block_ms=0)
    for message in messages:
        await dispatch_message(message.payload, message.name, bus, container)
    return len(messages)


# ---- 1. 极简配置面与平台补丁 ----
def test_seed3d_is_minimal_tier_and_hits_ark_patch():
    draft = TemplateDraft.model_validate(SEED3D)
    assert {k for k in SEED3D} == {"alias", "base_url", "create_path", "result_location"}
    assert template_tier(draft) is TemplateTier.MINIMAL

    resolved = _resolved()
    # 补丁按 **host** 命中（不依赖 alias），故同域名的第二个上游白拿方言修正
    assert "volc-ark-seedance" in resolved.provenance["_patches"]
    assert resolved.provenance["capabilities.upstream_idempotent"] == FieldSource.PATCHED.value
    assert resolved.capabilities.confirm_strategy is ConfirmStrategy.MANUAL_ONLY
    assert resolved.capabilities.upstream_idempotent is False
    assert resolved.namespace == "volc"
    assert resolved.status_field == "$.status"
    assert effective_failure_status(resolved) == "failed"

    # get/cancel 路径由 create_path 推导，与插件 query 路由同形
    assert resolved.get_path_template == f"{PLUGIN_PATH_SUFFIX}/{{id}}"
    assert resolved.cancel_path_template == f"{PLUGIN_PATH_SUFFIX}/{{id}}"


def test_builtin_file_is_registered_by_registry():
    registry = TemplateRegistry()
    assert "volc-seed3d" in registry.aliases()
    tv = registry.require("volc-seed3d")
    assert tv.resolved.base_url == "https://ark.cn-beijing.volces.com"
    assert tv.resolved.result_location == "$.content.file_url"


# ---- 2. 与 New API 任务插件的拼接口径逐字对齐 ----
def test_newapi_plugin_concatenation_lands_on_create_route():
    """渠道 base_url 指向网关时，插件拼出的路径必须被归位为 create。"""
    resolved = _resolved()
    gateway_prefix = "/async/volc-seed3d"  # 渠道 base_url 写在网关上
    rest = gateway_prefix + PLUGIN_PATH_SUFFIX
    rest = rest[len(gateway_prefix) :]  # 网关只把 alias 之后的部分作为 rest
    assert rest == resolved.create_path
    match = match_route(resolved, rest, "POST")
    assert match is not None and match.kind == "create"


def test_newapi_plugin_query_path_lands_on_query_route():
    resolved = _resolved()
    rest = f"{PLUGIN_PATH_SUFFIX}/cgt-20260920125622-cxlkg"
    match = match_route(resolved, rest, "GET")
    assert match is not None and match.kind == "query"
    assert match.upstream_id == "cgt-20260920125622-cxlkg"

    match_del = match_route(resolved, rest, "DELETE")
    assert match_del is not None and match_del.kind == "cancel"


def test_real_upstream_url_is_still_correct():
    """口径收敛到"根域名 + 全路径"后，直连上游的 URL 不能变样。"""
    resolved = _resolved()
    assert f"{resolved.base_url}{resolved.create_path}" == (
        "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks"
    )


# ---- 3. 结果字段与状态映射 ----
def test_result_and_status_extraction_on_real_shape():
    resolved = _resolved()
    ok_result, url = UpstreamClient.extract_result(resolved, ARK_QUERY_SUCCEEDED)
    assert ok_result and url == ARK_RESULT_URL

    ok_id, task_id = UpstreamClient.extract_id(resolved, ARK_QUERY_SUCCEEDED)
    assert ok_id and task_id == "cgt-20260920125622-cxlkg"

    ok_status, raw = UpstreamClient.extract_status(resolved, ARK_QUERY_SUCCEEDED)
    assert ok_status and raw == "succeeded"
    assert resolved.maps_status("succeeded") is TaskStatus.SUCCEEDED
    # 进行中取值不在终态表里 → None（按快照继续轮询），不是 poll_unrecognized
    assert resolved.maps_status("running") is None
    assert resolved.maps_status("queued") is None


def test_result_location_errata_is_in_place_but_still_dual_reviewed():
    """字段写错时的两条轴是正交的：**不换版本**（存量任务即时恢复），但仍**须双人评审**。"""
    from async_gateway.templates.registry import ApplyMode, TemplateRegistry as R

    registry = R()
    tv = registry.require("volc-seed3d")
    wrong = dict(tv.raw)
    wrong["result_location"] = "$.content.video_url"
    updated, report, plan = registry.patch_in_place(wrong)
    assert report.ok
    assert plan.changed_fields == ["result_location"]
    assert plan.apply_mode is ApplyMode.IN_PLACE  # 纯提取表达式 → 就地生效
    assert plan.requires_dual_review is True  # 但 result_location 在强制评审清单里
    assert updated.resolved.result_location == "$.content.video_url"


# ---- 4. 带签名结果 URL 的转存（真机缺陷回归） ----
def test_signed_result_url_is_restored_from_raw_for_transfer():
    """快照里的结果字段必须保留**上游原始** URL，而不是入口脱敏后的残值。"""
    resolved = _resolved()
    scrubbed = scrub_payload(copy.deepcopy(ARK_QUERY_SUCCEEDED))
    signed = scrubbed["content"]["file_url"]
    # 触发条件：脱敏兜底规则把签名长串抹掉了 —— 此时 URL 已不可用
    assert "***redacted***" in signed
    assert signed != ARK_RESULT_URL

    _restore_raw_result_field(scrubbed, resolved, _call_result(copy.deepcopy(ARK_QUERY_SUCCEEDED)))
    assert scrubbed["content"]["file_url"] == ARK_RESULT_URL
    assert "***redacted***" not in scrubbed["content"]["file_url"]
    # 只回填结果字段：其它字段仍走脱敏路径（此处本无敏感值，断言形状未被动过）
    assert scrubbed["usage"] == ARK_QUERY_SUCCEEDED["usage"]


async def test_store_result_succeeds_with_signed_result_url(
    client, container, fake_upstream, db, result_store
):
    """端到端：上游结果 URL 带签名查询串时，转存必须成功（否则真机表现为 410）。

    假上游**校验签名**（`result_expected_signature`）：签名被脱敏抹掉即 403 ——
    这正是真机上"快照存了脱敏 URL 导致转存永久失败"的可复现形态。
    """
    signature = "0" * 64
    signed_url = (
        f"{fake_upstream.base_url}/files/model.zip"
        f"?X-Tos-Signature={signature}&X-Tos-Expires=86400"
    )
    fake_upstream.result_expected_signature = signature
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]

    fake_upstream.tasks[upstream_id]["output"]["url"] = signed_url
    fake_upstream.set_status(upstream_id, "succeeded")
    await _drain(container.bus, "poll:light-poll", container)
    await _drain(container.bus, TRANSFER_QUEUE, container)

    task = await _get_task(task_id)
    assert task.status == TaskStatus.SUCCEEDED.value
    assert task.result_ref is not None, "带签名 URL 的结果必须能转存"
    assert task.result_degraded is None
    assert fake_upstream.result_fetches >= 1
    assert await result_store.get_bytes(task.result_ref) == fake_upstream.result_bytes


# ---- 5. 结果文件大小上限（真机 41 MB 触发的缺陷回归） ----
async def test_result_fetch_limit_is_decoupled_from_api_response_limit(fake_upstream):
    """结果**文件**上限与上游 API 响应上限是两件事（实测 3D 产物 41 MB）。"""
    payload = b"x" * 8192
    fake_upstream.result_bytes = payload
    client = UpstreamClient(
        transport=fake_upstream.transport(),
        pin_dns=False,
        allow_hosts=["localhost"],
        max_response_bytes=1024,  # API 响应上限：故意极小，不应影响结果拉取
        max_result_bytes=64 * 1024,
    )
    try:
        ok, data = await client.fetch_result(f"{fake_upstream.base_url}/files/model.zip")
        assert ok and data == payload
        # 显式收紧到小于产物时仍按上限拒绝
        ok2, detail = await client.fetch_result(
            f"{fake_upstream.base_url}/files/model.zip", max_bytes=1024
        )
        assert not ok2 and "result too large" in detail
    finally:
        await client.aclose()


async def test_oversized_result_is_marked_unavailable_without_retry(
    client, container, fake_upstream, db, monkeypatch, settings
):
    """大小超限是**确定性**失败：直接标记结果不可用，不耗尽重试次数。"""
    monkeypatch.setattr(settings, "result_max_bytes", 1024)
    fake_upstream.result_bytes = b"y" * 4096
    created = await client.post("/async/echo/v1/tasks", json={"prompt": "x"}, headers=AUTH)
    task_id = created.headers["x-ag-task-id"]
    upstream_id = created.headers["x-ag-upstream-id"]
    fake_upstream.set_status(upstream_id, "succeeded")

    await _drain(container.bus, "poll:light-poll", container)
    await _drain(container.bus, TRANSFER_QUEUE, container)

    task = await _get_task(task_id)
    assert task.result_ref is None
    assert task.result_degraded == ["transfer_failed"]  # 一次即判定，不再重试
    assert task.status == TaskStatus.SUCCEEDED.value  # 不伪造失败骗退款
    assert task.next_poll_at is None


# ---- 6. 面向 New API 的原生形状保真与结果字段直给 ----
def test_succeeded_response_keeps_native_shape_and_gives_presigned_url():
    """store 模式：结果字段**直接**是对象存储的预签名 URL（不经网关中转端点）。"""
    resolved = _resolved()
    presigned = "https://minio.internal/async-gateway/obj?X-Amz-Signature=deadbeef"
    body = build_native_query_response(_task(), resolved, result_url=presigned)
    # 形状保真：上游字段原样保留
    assert body["id"] == "cgt-20260920125622-cxlkg"
    assert body["model"] == "doubao-seed3d-2-0-260328"
    assert body["usage"] == {"completion_tokens": 30000, "total_tokens": 30000}
    assert body["subdivisionlevel"] == "high"
    assert body["status"] == "succeeded"
    # 结果字段就地换成预签名 URL（同一个叶子），上游 URL 不再出现
    assert body["content"]["file_url"] == presigned
    assert ARK_RESULT_URL not in str(body)
    # 结果可用时不带"不可用"提示（hint 只在转存未完成/失败时出现）
    assert "_result_hint" not in body


def test_gateway_internal_terminal_is_rewritten_to_native_failure_value():
    """网关内部 timeout 必须对外报上游原生失败类取值，否则 New API 退款不触发。"""
    resolved = _resolved()
    body = build_native_query_response(
        _task(status=TaskStatus.TIMEOUT.value, raw_status="running"),
        resolved,
        result_url=None,
    )
    assert body["status"] == "failed"
    assert body["error"] == "failed"


def test_transfer_failed_reports_native_success_with_empty_result_field():
    """转存永久失败：状态仍报上游原生成功终态（不伪造失败骗退款），结果字段留空 + hint。"""
    resolved = _resolved()
    body = build_native_query_response(
        _task(result_degraded=["transfer_failed"]),
        resolved,
        result_url=None,
    )
    assert body["status"] == "succeeded"
    assert body["content"]["file_url"] is None
    assert body["_result_hint"] == "unavailable_or_pending"
