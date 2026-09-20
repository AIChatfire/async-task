"""约定推导规则与平台差异补丁（§12.2 / §12.5）。

推导表（能推导的绝不手写）::

    get_path_template    = create_path + "/{id}"
    id_location          = $.id
    cancel_path_template = create_path + "/{id}"   (DELETE)
    status_field         = $.status
    terminal             = success:[succeeded] failure:[failed] expired:[expired]
    status_source        = poll
    result_policy        = store
    callback_param       = callback_url
    pool                 = shared
    namespace            = alias 的连字符前缀（缺省 default）
    strategy             = 全局策略组默认值

**平台差异补丁**是平台内置的逐上游能力/方言修正（如 Seedance 附加
``upstream_idempotent: false`` + ``confirm_strategy: manual_only``），补丁命中后
字段来源标记为 ``patched``，模板作者无需重复声明、也无法绕过。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import yaml

from ..config import get_settings, load_strategy_defaults
from .schema import (
    Capabilities,
    ConfirmStrategy,
    FieldSource,
    NormalizationRules,
    ResolvedTemplate,
    ResultMode,
    ResultPolicy,
    StatusSource,
    StrategyOverride,
    TemplateDraft,
    TerminalMapping,
)

PATCH_DIR = Path(__file__).parent / "builtin" / "patches"

#: 策略键（用于 provenance 的 ``strategy.*`` 展开）
STRATEGY_KEYS: tuple[str, ...] = tuple(load_strategy_defaults().keys())


@dataclass(frozen=True, slots=True)
class PlatformPatch:
    """平台内置的逐上游差异补丁。"""

    patch_id: str
    reason: str
    fields: dict[str, Any] = field(default_factory=dict)
    match_hosts: tuple[str, ...] = ()
    match_aliases: tuple[str, ...] = ()

    def matches(self, *, alias: str, base_url: str) -> bool:
        host = (urlparse(base_url).hostname or "").lower()
        if alias in self.match_aliases:
            return True
        return bool(host) and any(host == h or host.endswith("." + h) for h in self.match_hosts)


def load_builtin_patches(patch_dir: Path | None = None) -> list[PlatformPatch]:
    directory = patch_dir or PATCH_DIR
    patches: list[PlatformPatch] = []
    if not directory.is_dir():
        return patches
    for path in sorted(directory.glob("*.yaml")):
        raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        for item in raw.get("patches", []):
            patches.append(
                PlatformPatch(
                    patch_id=item["id"],
                    reason=item.get("reason", ""),
                    fields=item.get("fields", {}) or {},
                    match_hosts=tuple(item.get("match_hosts", []) or []),
                    match_aliases=tuple(item.get("match_aliases", []) or []),
                )
            )
    return patches


def _set_path(target: dict[str, Any], dotted: str, value: Any) -> None:
    parts = dotted.split(".")
    node = target
    for part in parts[:-1]:
        node = node.setdefault(part, {})
    node[parts[-1]] = value


def _get_path(target: dict[str, Any], dotted: str) -> Any:
    node: Any = target
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def apply_platform_patches(
    payload: dict[str, Any],
    provenance: dict[str, str],
    *,
    alias: str,
    base_url: str,
    patches: list[PlatformPatch] | None = None,
) -> tuple[dict[str, Any], dict[str, str], list[str]]:
    """就地套用平台补丁；返回 (payload, provenance, 命中的补丁 id)。

    ``provenance`` 是**扁平**的点号键表（``capabilities.upstream_idempotent`` → ``explicit``），
    便于预览接口逐字段展示来源；``payload`` 仍是嵌套结构。
    """
    patches = patches if patches is not None else load_builtin_patches()
    applied: list[str] = []
    for patch in patches:
        if not patch.matches(alias=alias, base_url=base_url):
            continue
        for dotted, value in patch.fields.items():
            if provenance.get(dotted) == FieldSource.EXPLICIT.value:
                # 作者显式声明优先，但仍记录命中，便于审计发现"绕过补丁"的意图
                applied.append(patch.patch_id)
                continue
            _set_path(payload, dotted, value)
            provenance[dotted] = FieldSource.PATCHED.value
        applied.append(patch.patch_id)
    return payload, provenance, sorted(set(applied))


def resolve(
    draft: TemplateDraft,
    *,
    patches: list[PlatformPatch] | None = None,
) -> ResolvedTemplate:
    """草稿 → 展开后完整形态（带 provenance）。"""
    src = FieldSource.EXPLICIT.value
    dfl = FieldSource.DEFAULT.value
    drv = FieldSource.DERIVED.value

    prov: dict[str, str] = {}

    def note(field_name: str, value: Any, source: str) -> Any:
        prov[field_name] = source
        return value

    create_path = draft.create_path
    get_path = note(
        "get_path_template",
        draft.get_path_template or f"{create_path}/{{id}}",
        src if draft.get_path_template else drv,
    )
    cancel_path = note(
        "cancel_path_template",
        draft.cancel_path_template or f"{create_path}/{{id}}",
        src if draft.cancel_path_template else drv,
    )
    id_location = note("id_location", draft.id_location or "$.id", src if draft.id_location else dfl)
    status_field = note(
        "status_field", draft.status_field or "$.status", src if draft.status_field else dfl
    )
    terminal = draft.terminal or TerminalMapping()
    note("terminal", None, src if draft.terminal else dfl)
    capabilities = draft.capabilities or Capabilities()
    note("capabilities", None, src if draft.capabilities else dfl)
    result_policy = draft.result_policy or ResultPolicy()
    note("result_policy", None, src if draft.result_policy else dfl)
    if "mode" not in (draft.result_policy.model_fields_set if draft.result_policy else frozenset()):
        # 模板未显式声明结果策略模式 → 取**全局默认**（``AG_RESULT_MODE_DEFAULT``）。
        # 用 model_fields_set 区分"真的没写"与"写了 store"：后者必须原样保留（作者显式优先），
        # 否则"先默认不转存、验证完再逐模板切回 store"这条路会走不通。
        result_policy = result_policy.model_copy(
            update={"mode": ResultMode(get_settings().result_mode_default)}
        )
        prov["result_policy.mode"] = dfl
    pool = note("pool", draft.pool or "shared", src if draft.pool else dfl)
    status_source = note(
        "status_source",
        draft.status_source or StatusSource.POLL,
        src if draft.status_source else dfl,
    )
    callback_param = note(
        "callback_param", draft.callback_param or "callback_url", src if draft.callback_param else dfl
    )
    alias = draft.alias
    default_ns = alias.rsplit("-", 1)[0] if "-" in alias else "default"
    namespace = note(
        "namespace",
        draft.namespace or default_ns,
        src if draft.namespace else drv,
    )
    normalize = note(
        "normalize", draft.normalize or NormalizationRules(), src if draft.normalize else dfl
    )

    # 容器类字段：用 model_fields_set 记录作者**显式声明**的叶子。否则 pydantic 的默认值
    # 会冒充"显式"，平台补丁的"作者显式优先"判定就失效了。
    for container_name, model in (
        ("capabilities", draft.capabilities),
        ("terminal", draft.terminal),
        ("result_policy", draft.result_policy),
        ("normalize", draft.normalize),
    ):
        if model is None:
            continue
        for leaf in model.model_fields_set:
            prov[f"{container_name}.{leaf}"] = src

    # 策略组：全局默认 ← 模板覆盖
    strategy: dict[str, Any] = dict(load_strategy_defaults())
    prov["strategy"] = dfl
    override: StrategyOverride | None = draft.strategy
    if override is not None:
        for key, value in override.model_dump(exclude_none=True).items():
            strategy[key] = value
            prov[f"strategy.{key}"] = src

    payload: dict[str, Any] = {
        "alias": alias,
        "namespace": namespace,
        "base_url": draft.base_url,
        "create_path": create_path,
        "create_method": draft.create_method,
        "get_path_template": get_path,
        "get_method": draft.get_method,
        "cancel_path_template": cancel_path,
        "cancel_method": draft.cancel_method,
        "id_location": id_location,
        "status_field": status_field,
        "result_location": draft.result_location,
        "terminal": terminal.model_dump(mode="json"),
        "capabilities": capabilities.model_dump(mode="json"),
        "result_policy": result_policy.model_dump(mode="json"),
        "pool": pool,
        "status_source": status_source.value,
        "callback_param": callback_param,
        "normalize": normalize.model_dump(mode="json"),
        "strategy": strategy,
        "extra_headers": draft.extra_headers,
        "request_body_template": draft.request_body_template,
        "notes": draft.notes,
        "_provenance": prov,
    }
    prov["base_url"] = src
    prov["result_location"] = src
    prov["create_path"] = src
    prov["alias"] = src
    prov["create_method"] = src if draft.create_method != "POST" else dfl

    payload, prov, applied = apply_platform_patches(
        payload, prov, alias=alias, base_url=draft.base_url, patches=patches
    )
    payload.pop("_provenance", None)
    payload["provenance"] = copy.deepcopy(prov)
    resolved = ResolvedTemplate.model_validate(payload)
    resolved.provenance["_patches"] = ",".join(applied)
    return resolved


def resolved_to_raw_dict(resolved: ResolvedTemplate) -> dict[str, Any]:
    """转回草稿形态（用于版本 diff：去掉 provenance 与补丁痕迹）。"""
    return resolved.model_dump(mode="json", exclude={"provenance"})


def effective_failure_status(resolved: ResolvedTemplate) -> str:
    """内部终态对外重写用的"上游原生失败类终态取值"（§12.2 硬规则）。"""
    return resolved.terminal.internal_terminal_rewrite or (resolved.terminal.failure or ["failed"])[0]


def effective_failure_error_code(resolved: ResolvedTemplate) -> str:
    return resolved.terminal.internal_terminal_error_code or effective_failure_status(resolved)


def effective_review_ok(resolved: ResolvedTemplate) -> list[str]:
    """返回该模板残留的"能力假设"清单（dry-run 前必须人工确认的项）。"""
    caps = resolved.capabilities
    assumptions: list[str] = []
    if caps.confirm_strategy is ConfirmStrategy.MANUAL_ONLY:
        assumptions.append("submit_unknown 永不自动重发创建，转人工确认（confirm_strategy=manual_only）")
    if not caps.upstream_idempotent:
        assumptions.append("上游创建非幂等 → 创建去重责任在网关")
    if caps.rate_limit_side_effect_free:
        assumptions.append("429 视为无副作用（上游假设）")
    if caps.per_key_isolation:
        assumptions.append("上游按 key 隔离任务空间 → 要求渠道单 key 或任务级 key 粘性")
    if resolved.result_policy.mode.value == "passthrough":
        assumptions.append("依赖上游直链有效期（须在 envelope degraded[] 声明）")
    return assumptions
