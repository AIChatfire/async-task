"""模板层：三级配置面、约定推导、平台补丁、校验器、沙箱、渲染、版本/变更分类。"""

from __future__ import annotations

import pytest

from async_gateway.templates.derive import (
    effective_failure_status,
    effective_review_ok,
    load_builtin_patches,
    resolve,
)
from async_gateway.templates.expression import ExpressionError, compile_expression, extract, try_extract
from async_gateway.templates.registry import ApplyMode, ChangePlan, TemplateRegistry, classify_change
from async_gateway.templates.render import RenderError, preview_requests, render_cancel, render_get
from async_gateway.templates.schema import (
    Capabilities,
    ConfirmStrategy,
    FieldSource,
    ResultMode,
    TemplateDraft,
    TemplateTier,
    TerminalMapping,
    template_tier,
)
from async_gateway.templates.validator import validate

#: 方舟系上游的最小配置。路径口径刻意是「站点根 + /api/v3 全路径」：
#: New API 的任务插件硬编码 `/api/v3/contents/generations/tasks` 后缀，
#: 而网关按 create_path 逐字比对归位，两边必须一致
#: （见 test_ark_templates_align_with_newapi_plugin_path）。
MINIMAL = {
    "alias": "volc-seedance",
    "base_url": "https://ark.cn-beijing.volces.com",
    "create_path": "/api/v3/contents/generations/tasks",
    "result_location": "$.content.video_url",
}


# ---- 三级配置面 ----
def test_tier_detection():
    assert template_tier(TemplateDraft.model_validate(MINIMAL)) is TemplateTier.MINIMAL
    diff = {**MINIMAL, "id_location": "$.data.id"}
    assert template_tier(TemplateDraft.model_validate(diff)) is TemplateTier.DIFF
    full = {**MINIMAL, "extra_headers": {"X-Trace": "1"}}
    assert template_tier(TemplateDraft.model_validate(full)) is TemplateTier.FULL


def test_minimal_template_is_exactly_four_lines():
    from async_gateway.templates.registry import BUILTIN_DIR

    assert {k for k in MINIMAL} == {"alias", "base_url", "create_path", "result_location"}
    assert BUILTIN_DIR.is_dir()


# ---- 约定推导 ----
def test_derivation_defaults_and_provenance():
    resolved = resolve(TemplateDraft.model_validate(MINIMAL))
    assert resolved.get_path_template == "/api/v3/contents/generations/tasks/{id}"
    assert resolved.cancel_path_template == "/api/v3/contents/generations/tasks/{id}"
    assert resolved.id_location == "$.id"
    assert resolved.status_field == "$.status"
    assert resolved.status_source.value == "poll"
    assert resolved.result_policy.mode.value == "store"
    assert resolved.callback_param == "callback_url"
    assert resolved.pool == "shared"
    assert resolved.namespace == "volc"
    assert resolved.provenance["get_path_template"] == FieldSource.DERIVED.value
    assert resolved.provenance["id_location"] == FieldSource.DEFAULT.value


def test_platform_patch_applies_to_seedance():
    resolved = resolve(TemplateDraft.model_validate(MINIMAL))
    assert resolved.capabilities.upstream_idempotent is False
    assert resolved.capabilities.confirm_strategy is ConfirmStrategy.MANUAL_ONLY
    assert resolved.provenance["capabilities.upstream_idempotent"] == FieldSource.PATCHED.value
    assert "volc-ark-seedance" in resolved.provenance["_patches"]
    assert effective_failure_status(resolved) == "failed"
    assumptions = " ".join(effective_review_ok(resolved))
    assert "manual_only" in assumptions


def test_explicit_declaration_wins_over_patch():
    raw = {**MINIMAL, "capabilities": {"upstream_idempotent": True, "confirm_strategy": "query_by_client_key"}}
    resolved = resolve(TemplateDraft.model_validate(raw))
    assert resolved.capabilities.upstream_idempotent is True
    assert resolved.capabilities.confirm_strategy is ConfirmStrategy.QUERY_BY_CLIENT_KEY
    # 补丁仍被记录命中（便于审计发现"绕过补丁"的意图）
    assert "volc-ark-seedance" in resolved.provenance["_patches"]


def test_builtin_patches_load():
    patches = load_builtin_patches()
    assert any(p.patch_id == "volc-ark-seedance" for p in patches)


# ---- 校验器 ----
def test_validator_rejects_bad_expression():
    raw = {**MINIMAL, "id_location": "$.a[*].b"}
    report = validate(raw)
    assert not report.ok
    assert any("表达式沙箱拒绝" in i.message for i in report.errors)


def test_validator_rejects_missing_id_placeholder():
    raw = {**MINIMAL, "get_path_template": "/contents/tasks"}
    report = validate(raw)
    assert not report.ok
    assert any("{id}" in i.message for i in report.errors)


def test_validator_rejects_credentials_in_template():
    raw = {**MINIMAL, "extra_headers": {"Authorization": "Bearer leak"}}
    report = validate(raw)
    assert not report.ok
    assert any("凭证类头" in i.message for i in report.errors)


def test_validator_requires_normalize_fields_for_list_and_match():
    raw = {**MINIMAL, "capabilities": {"confirm_strategy": "list_and_match"}}
    report = validate(raw)
    assert not report.ok
    assert any("list_and_match" in i.message for i in report.errors)


def test_validator_rejects_passthrough_without_permanent_url():
    raw = {**MINIMAL, "result_policy": {"mode": "passthrough"}}
    report = validate(raw)
    assert not report.ok


def test_validator_marks_unrecognized_sample_status_as_warning():
    raw = {**MINIMAL, "terminal": {"success": ["succeeded"], "failure": ["failed"]}}
    report = validate(raw, sample_status_response={"status": "some-new-state"})
    assert report.ok, report.as_dict()
    assert any(i.severity == "warning" for i in report.warnings)


def test_validator_reports_assumptions_for_manual_only():
    report = validate(MINIMAL)
    assert report.ok
    assert any("manual_only" in a for a in report.assumptions)


# ---- 表达式沙箱 ----
@pytest.mark.parametrize("spec", ["$.a[*]", "$..a", "$.a[?(@.b)]", "a.b", "$.a[(1)]", "$.a|", "$.a@"])
def test_sandbox_rejects_forbidden_constructs(spec):
    with pytest.raises(ExpressionError):
        compile_expression(spec)


def test_expression_extraction_shapes():
    doc = {"a": {"b": [1, {"c": "x"}]}, "content-type": "json"}
    assert extract(doc, '$.a.b[1]["c"]') == "x"
    assert extract(doc, "$['content-type']") == "json"
    assert try_extract(doc, "$.nope.deep") == (False, None)


def test_expression_result_size_cap():
    from async_gateway.templates.expression import EvalBudgetExceeded, extract as ex

    with pytest.raises(EvalBudgetExceeded):
        ex({"big": "x" * 1000}, "$.big", max_result_bytes=10)


# ---- 结果策略模式的全局默认 ----
def test_result_mode_default_comes_from_settings(settings, monkeypatch):
    """未声明 ``result_policy.mode`` ⇒ 取全局默认；作者显式声明 ⇒ 优先于全局默认。

    这条支撑"默认先不转存、验完对象存储再按模板逐个切回 store"的操作方式：
    必须能区分"真的没写"与"写了 store"，否则切不回去。
    """
    resolved = resolve(TemplateDraft.model_validate(MINIMAL))
    assert resolved.result_policy.mode is ResultMode.STORE  # 测试基座把默认设成 store
    assert resolved.provenance["result_policy.mode"] == FieldSource.DEFAULT.value

    monkeypatch.setattr(settings, "result_mode_default", "passthrough")
    flipped = resolve(TemplateDraft.model_validate(MINIMAL))
    assert flipped.result_policy.mode is ResultMode.PASSTHROUGH

    # 显式声明不被全局默认覆盖
    explicit = resolve(TemplateDraft.model_validate({**MINIMAL, "result_policy": {"mode": "store"}}))
    assert explicit.result_policy.mode is ResultMode.STORE
    assert explicit.provenance["result_policy.mode"] == FieldSource.EXPLICIT.value


# ---- 渲染 ----
def test_render_preview_produces_real_requests():
    resolved = resolve(TemplateDraft.model_validate(MINIMAL))
    preview = preview_requests(resolved, sample_upstream_task_id="t-1")
    assert preview["get"]["url"] == (
        "https://ark.cn-beijing.volces.com/api/v3/contents/generations/tasks/t-1"
    )
    # 凭证不进模板：渲染出的头里根本没有 Authorization
    assert "Authorization" not in preview["create"]["headers"]
    assert preview["create"]["method"] == "POST"
    assert preview["cancel"]["supported"] is False


def test_render_sanitizes_interpolation():
    resolved = resolve(TemplateDraft.model_validate(MINIMAL))
    with pytest.raises(RenderError):
        render_get(resolved, "../../etc/passwd")
    with pytest.raises(RenderError):
        render_get(resolved, "http://evil.example/x")


def test_render_rejects_cancel_when_unsupported():
    resolved = resolve(TemplateDraft.model_validate(MINIMAL))
    assert resolved.capabilities.cancel is False
    with pytest.raises(RenderError):
        render_cancel(resolved, "t-1")


def test_render_blocks_origin_escape():
    from async_gateway.templates.render import join_url

    with pytest.raises(RenderError):
        join_url("https://api.example.com/v1", "https://evil.example.com/x")


# ---- 版本与变更分类 ----
def test_change_classification_ephemeral_vs_versioned():
    base = resolve(TemplateDraft.model_validate(MINIMAL))
    errata = resolve(
        TemplateDraft.model_validate(
            {**MINIMAL, "terminal": {"success": ["succeeded"], "failure": ["failed", "error"]}}
        )
    )
    plan: ChangePlan = classify_change(base, errata)
    assert plan.apply_mode is ApplyMode.IN_PLACE
    assert plan.requires_dual_review is False

    strategy_change = resolve(TemplateDraft.model_validate({**MINIMAL, "pool": "heavy-poll"}))
    plan2 = classify_change(base, strategy_change)
    assert plan2.apply_mode is ApplyMode.VERSIONED
    assert plan2.requires_dual_review is True

    noop = classify_change(base, base)
    assert noop.is_noop


def test_registry_loads_builtin_and_extra_dirs(registry: TemplateRegistry):
    assert "volc-seedance" in registry.aliases()
    assert "echo" in registry.aliases()
    # 测试夹具覆盖了内置 echo 的 base_url
    assert registry.require("echo").resolved.base_url == "http://localhost:9099"


#: New API 任务插件里**硬编码**的上游路径后缀（方舟原生同构形态）。
#: 来源：new-api-plugins 仓库 `plugins/tasks/volcengine-ark-3d/<ver>/plugin.js`：
#:   `url: ctx.baseUrl + "/api/v3/contents/generations/tasks"`
#: 插件只能改渠道 base_url、改不了这段后缀，所以网关侧的 create_path 必须与它一致，
#: 才能做到「渠道 base_url 指向网关 → 零改插件复用」。
NEWAPI_PLUGIN_PATH_SUFFIX = "/api/v3/contents/generations/tasks"


@pytest.mark.parametrize("alias", ["volc-seedance", "volc-seed3d"])
def test_ark_templates_align_with_newapi_plugin_path(registry: TemplateRegistry, alias: str):
    """方舟系模板必须与插件硬编码后缀逐字对齐，否则经 New API 的请求在网关上 404。"""
    from async_gateway.gateway.routing import match_route

    resolved = registry.require(alias).resolved
    assert resolved.create_path == NEWAPI_PLUGIN_PATH_SUFFIX
    # 路径切分（/api/v3 放 base_url 还是 create_path）不得改变直连上游的 URL
    assert f"{resolved.base_url}{resolved.create_path}" == (
        f"https://ark.cn-beijing.volces.com{NEWAPI_PLUGIN_PATH_SUFFIX}"
    )
    # 插件的 query / cancel 路由（后缀 + /{id}）能归位
    matched = match_route(resolved, f"{NEWAPI_PLUGIN_PATH_SUFFIX}/cgt-1", "GET")
    assert matched is not None and matched.kind == "query"
    matched_del = match_route(resolved, f"{NEWAPI_PLUGIN_PATH_SUFFIX}/cgt-1", "DELETE")
    assert matched_del is not None and matched_del.kind == "cancel"


def test_registry_in_place_patch_only_for_errata(registry: TemplateRegistry):
    tv = registry.require("echo")
    patched = dict(tv.raw)
    patched["terminal"] = {"success": ["succeeded"], "failure": ["failed", "error"]}
    updated, report, plan = registry.patch_in_place(patched)
    assert report.ok and plan.apply_mode is ApplyMode.IN_PLACE
    assert "error" in updated.resolved.terminal.failure

    illegal = dict(tv.raw)
    illegal["pool"] = "heavy-poll"
    with pytest.raises(ValueError):
        registry.patch_in_place(illegal)


def test_registry_canary_weight_zero_rolls_back(registry: TemplateRegistry):
    tv = registry.require("echo")
    registry.set_canary_weight("echo", tv.version, 0.0)
    assert registry.get("echo").canary_weight >= 0.0
    registered, _ = registry.register({**tv.raw, "result_location": "$.other.url"})
    assert registered.version == tv.version + 1


def test_capabilities_missing_fields_default_false():
    caps = Capabilities()
    assert caps.cancel is False
    assert caps.list_tasks is False
    assert caps.confirm_strategy is ConfirmStrategy.MANUAL_ONLY
    assert caps.confirm_auto_allowed is False
    assert TerminalMapping().success == ["succeeded"]
