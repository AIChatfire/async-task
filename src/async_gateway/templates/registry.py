"""模板注册表：加载 / 版本 / 灰度 / 变更分类（§12.3）。

变更分类有两个正交的轴：

* **生效方式**（``apply_mode``）：纯"终态判定 / 提取表达式"类勘误 → ``in_place``
  （就地修订既有版本，存量 ``poll_unrecognized`` 任务不换版本即恢复轮询）；
  其余（策略/能力/请求形状）→ ``versioned``（渠道维度按比例绑新版本）。
* **审批强度**（``requires_dual_review``）：策略/能力/URL/表达式/callback 参数变更
  强制双人评审；纯字段勘误走快速通道。
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Iterable, Literal

import yaml

from .derive import PlatformPatch, load_builtin_patches, resolve
from .schema import ResolvedTemplate, TemplateDraft
from .validator import ValidationReport, validate

logger = logging.getLogger(__name__)

BUILTIN_DIR = Path(__file__).parent / "builtin"

#: 仅改"判定逻辑/提取表达式"、不改请求形状 → 允许就地修订
EPHEMERAL_FIELDS: frozenset[str] = frozenset(
    {
        "status_field",
        "terminal",
        "terminal.success",
        "terminal.failure",
        "terminal.expired",
        "terminal.internal_terminal_rewrite",
        "terminal.internal_terminal_error_code",
        "terminal.error_field",
        "id_location",
        "result_location",
        "notes",
    }
)

#: 触发强制双人评审的字段前缀/全名
FORCED_REVIEW_FIELDS: frozenset[str] = frozenset(
    {
        "base_url",
        "create_path",
        "create_method",
        "get_method",
        "cancel_method",
        "get_path_template",
        "cancel_path_template",
        "callback_param",
        "extra_headers",
        "request_body_template",
        "id_location",
        "result_location",
        "status_field",
        "status_source",
        "pool",
        "terminal",
        "normalize",
        "namespace",
    }
)
FORCED_REVIEW_PREFIXES: tuple[str, ...] = ("capabilities", "result_policy", "strategy")


class ApplyMode(str, Enum):
    IN_PLACE = "in_place"
    VERSIONED = "versioned"


@dataclass(slots=True)
class ChangePlan:
    changed_fields: list[str]
    apply_mode: ApplyMode
    requires_dual_review: bool
    forced_review_fields: list[str]
    is_noop: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "changed_fields": self.changed_fields,
            "apply_mode": self.apply_mode.value,
            "requires_dual_review": self.requires_dual_review,
            "forced_review_fields": self.forced_review_fields,
            "is_noop": self.is_noop,
        }


def _flatten(payload: dict[str, Any], prefix: str = "") -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in payload.items():
        dotted = f"{prefix}{key}"
        if isinstance(value, dict):
            out[dotted] = value
            out.update(_flatten(value, f"{dotted}."))
        else:
            out[dotted] = value
    return out


def classify_change(old: ResolvedTemplate, new: ResolvedTemplate) -> ChangePlan:
    """两次模板形态的差异 → 生效方式与审批强度。"""
    a = _flatten(old.model_dump(mode="json", exclude={"provenance"}))
    b = _flatten(new.model_dump(mode="json", exclude={"provenance"}))
    changed = sorted(k for k in set(a) | set(b) if a.get(k) != b.get(k))
    # 容器级键（capabilities/terminal/...）本身变化不算字段变化，保留叶子
    changed = [k for k in changed if not any(other.startswith(k + ".") for other in changed)]
    if not changed:
        return ChangePlan([], ApplyMode.IN_PLACE, False, [], is_noop=True)

    ephemeral_only = all(f in EPHEMERAL_FIELDS for f in changed)
    forced = [
        f
        for f in changed
        if f in FORCED_REVIEW_FIELDS or any(f.startswith(p) for p in FORCED_REVIEW_PREFIXES)
    ]
    return ChangePlan(
        changed_fields=changed,
        apply_mode=ApplyMode.IN_PLACE if ephemeral_only else ApplyMode.VERSIONED,
        requires_dual_review=bool(forced),
        forced_review_fields=forced,
    )


@dataclass(slots=True)
class TemplateVersion:
    alias: str
    namespace: str
    version: int
    enabled: bool
    resolved: ResolvedTemplate
    raw: dict[str, Any]
    patch_ids: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    #: 灰度权重（0 = 回滚）
    canary_weight: float = 1.0

    @property
    def config_hash(self) -> str:
        blob = json.dumps(self.raw, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    @property
    def ref(self) -> str:
        return f"{self.alias}@{self.version}"


class TemplateRegistry:
    """进程内模板注册表（文件 + 运行时注册 + 就地勘误）。"""

    def __init__(
        self,
        builtin_dir: Path | None = None,
        extra_dirs: Iterable[Path] = (),
        patches: list[PlatformPatch] | None = None,
        *,
        load_builtin: bool = True,
    ) -> None:
        self._dirs: list[Path] = []
        if load_builtin:
            self._dirs.append(builtin_dir or BUILTIN_DIR)
        self._dirs.extend(Path(d) for d in extra_dirs)
        self._patches = patches if patches is not None else load_builtin_patches()
        self._versions: dict[str, list[TemplateVersion]] = {}
        if load_builtin:
            self.reload_files()

    # ---- 加载 ----
    def reload_files(self) -> None:
        for directory in self._dirs:
            if not directory.is_dir():
                continue
            for path in sorted(directory.glob("*.yaml")):
                raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
                self.register(raw, version=1, enabled=True, validate_first=False)

    def register(
        self,
        raw: dict[str, Any],
        *,
        version: int | None = None,
        enabled: bool = True,
        validate_first: bool = True,
    ) -> tuple[TemplateVersion, ValidationReport]:
        report = validate(raw)
        if validate_first and not report.ok:
            return None, report  # type: ignore[return-value]
        # 校验**告警**（不改 ok 的那些）在此落日志：模板从文件加载时（启动期）最容易漏看，
        # 而它们往往正是"两套机制各自合理、组合必死"的那类问题（例：deadline 与轮询上限，F5）
        for issue in report.warnings:
            logger.warning(
                "模板校验告警 alias=%s field=%s: %s", raw.get("alias") or "?", issue.field, issue.message
            )
        resolved = report.resolved or resolve(TemplateDraft.model_validate(raw), patches=self._patches)
        existing = self._versions.get(resolved.alias, [])
        next_version = version or (max((v.version for v in existing), default=0) + 1)
        tv = TemplateVersion(
            alias=resolved.alias,
            namespace=resolved.namespace,
            version=next_version,
            enabled=enabled,
            resolved=resolved,
            raw=raw,
            patch_ids=[p for p in resolved.provenance.get("_patches", "").split(",") if p],
        )
        self._versions.setdefault(resolved.alias, [])
        self._versions[resolved.alias] = [v for v in self._versions[resolved.alias] if v.version != next_version]
        self._versions[resolved.alias].append(tv)
        self._versions[resolved.alias].sort(key=lambda v: v.version)
        return tv, report

    # ---- 查询 ----
    def get(self, alias: str, version: int | None = None) -> TemplateVersion | None:
        versions = self._versions.get(alias) or []
        if version is None:
            for tv in reversed(versions):
                if tv.enabled and tv.canary_weight > 0:
                    return tv
            return versions[-1] if versions else None
        for tv in versions:
            if tv.version == version:
                return tv
        return None

    def require(self, alias: str, version: int | None = None) -> TemplateVersion:
        tv = self.get(alias, version)
        if tv is None:
            raise KeyError(f"unknown template alias: {alias}")
        return tv

    def aliases(self) -> list[str]:
        return sorted(self._versions)

    def all_versions(self) -> list[TemplateVersion]:
        return [tv for alias in self.aliases() for tv in self._versions[alias]]

    def is_namespace_owned(self, alias: str, namespace: str) -> bool:
        """alias 命名空间归属（§18.5：未授权与不存在同返 404）。"""
        tv = self.get(alias)
        return bool(tv) and tv.namespace == namespace

    # ---- 变更 ----
    def plan_change(self, raw: dict[str, Any]) -> ChangePlan:
        draft = TemplateDraft.model_validate(raw)
        new = resolve(draft, patches=self._patches)
        old_tv = self.get(new.alias)
        if old_tv is None:
            return ChangePlan(["<new-alias>"], ApplyMode.VERSIONED, True, ["<new-alias>"])
        return classify_change(old_tv.resolved, new)

    def patch_in_place(self, raw: dict[str, Any]) -> tuple[TemplateVersion, ValidationReport, ChangePlan]:
        """就地修订既有版本（仅允许纯勘误）。"""
        plan = self.plan_change(raw)
        report = validate(raw)
        if not report.ok:
            return None, report, plan  # type: ignore[return-value]
        if plan.apply_mode is not ApplyMode.IN_PLACE:
            raise ValueError(
                f"这些字段不允许就地修订，请走版本化灰度：{plan.changed_fields}"
            )
        resolved = report.resolved
        assert resolved is not None
        current = self.get(resolved.alias)
        assert current is not None
        current.resolved = resolved
        current.raw = raw
        current.updated_at = datetime.now(UTC)
        return current, report, plan

    def set_canary_weight(self, alias: str, version: int, weight: float) -> TemplateVersion:
        tv = self.require(alias, version)
        tv.canary_weight = max(0.0, min(1.0, weight))
        return tv


_Literal = Literal  # 显式保留 typing.Literal 导入（供类型注解扩展）
