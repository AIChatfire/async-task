"""模板三级配置面与全字段 schema（§12.1 / §12.2）。

三级配置面不是三套模型，而是**同一份草稿的三种填充度**：

* 极简：``alias / base_url / create_path / result_location``（4 行）
* 差异：只写偏离约定的字段（``id_location`` / ``get_path_template`` / 终态取值 / capabilities / pool）
* 完整：不规则上游展开七组全字段

:func:`template_tier` 依填充度判定当前处于哪一级；:class:`ResolvedTemplate` 是"展开后
完整形态"，携带 ``provenance``（每字段来源：显式/推导/默认/平台补丁），供预览接口展示。
"""

from __future__ import annotations

import hashlib
import json
from enum import Enum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from ..domain.enums import TaskStatus


class ConfirmStrategy(str, Enum):
    """submit_unknown 的确认方式（§12.2 capabilities）。"""

    QUERY_BY_CLIENT_KEY = "query_by_client_key"
    LIST_AND_MATCH = "list_and_match"
    MANUAL_ONLY = "manual_only"


class StatusSource(str, Enum):
    POLL = "poll"
    CALLBACK = "callback"


class ResultMode(str, Enum):
    STORE = "store"
    PASSTHROUGH = "passthrough"
    REDIRECT = "redirect"


class TemplateTier(str, Enum):
    MINIMAL = "minimal"
    DIFF = "diff"
    FULL = "full"


class FieldSource(str, Enum):
    EXPLICIT = "explicit"
    DERIVED = "derived"
    DEFAULT = "default"
    PATCHED = "patched"      # 平台差异补丁


class Capabilities(BaseModel):
    """上游能力集：**全字段缺省 false**，逐上游确认后声明。"""

    model_config = ConfigDict(extra="forbid")

    upstream_idempotent: bool = False
    confirm_strategy: ConfirmStrategy = ConfirmStrategy.MANUAL_ONLY
    cancel: bool = False
    callback: bool = False
    list_tasks: bool = False
    max_payload_bytes: int = 1_048_576
    permanent_result_url: bool = False
    rate_limit_side_effect_free: bool = False
    per_key_isolation: bool = True

    @property
    def confirm_auto_allowed(self) -> bool:
        """是否允许自动重发创建（manual_only 永不自动重发创建，§12.2）。"""
        return self.confirm_strategy is not ConfirmStrategy.MANUAL_ONLY


class TerminalMapping(BaseModel):
    model_config = ConfigDict(extra="forbid")

    success: list[str] = Field(default_factory=lambda: ["succeeded"])
    failure: list[str] = Field(default_factory=lambda: ["failed"])
    #: 上游 expired 统一映射进网关 timeout（§12.2）
    expired: list[str] = Field(default_factory=lambda: ["expired"])
    #: 网关内部终态对外重写为哪个"上游原生失败类终态取值"（§12.2 硬规则）
    internal_terminal_rewrite: str | None = None
    #: 内部终态的 error_code 原生格式映射
    internal_terminal_error_code: str | None = None
    #: 失败原因写进上游原生形状的哪个字段（表达式）；为空则只重写状态字段
    error_field: str | None = None

    @field_validator("success", "failure", "expired", mode="before")
    @classmethod
    def _as_list(cls, v: Any) -> Any:
        if isinstance(v, str):
            return [v]
        return v


class ResultPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: ResultMode = ResultMode.STORE
    presign_ttl_seconds: int | None = None
    retention_days: int | None = None
    #: 转存永久失败时结果端点返回的语义（§4.2：410）
    unavailable_gone: bool = True

    @property
    def requires_transfer(self) -> bool:
        return self.mode is ResultMode.STORE


class NormalizationRules(BaseModel):
    """派生幂等键的规范化字段集（§4.1：**仅**对模板声明字段集规范化）。"""

    model_config = ConfigDict(extra="forbid")

    fields: list[str] = Field(default_factory=list)
    case_insensitive_fields: list[str] = Field(default_factory=list)
    trim_strings: bool = True


class StrategyOverride(BaseModel):
    """策略组覆盖（§12.2 策略组：全局默认；渠道侧可按渠道覆盖）。"""

    model_config = ConfigDict(extra="forbid")

    idempotency_window_seconds: int | None = None
    max_attempts: int | None = None
    deadline_seconds: int | None = None
    min_refresh_interval: float | None = None
    retention_days: int | None = None
    presign_ttl_seconds: int | None = None
    unknown_max_per_channel: int | None = None
    unknown_max_lifetime_seconds: int | None = None
    submit_confirm_window_seconds: int | None = None


class TemplateDraft(BaseModel):
    """三级配置面草稿。除极简 4 项外全部可选。"""

    model_config = ConfigDict(extra="forbid")

    # 极简模式 4 行
    alias: str
    base_url: str
    create_path: str
    result_location: str

    # 差异模式
    id_location: str | None = None
    get_path_template: str | None = None
    cancel_path_template: str | None = None
    status_field: str | None = None
    terminal: TerminalMapping | None = None
    capabilities: Capabilities | None = None
    result_policy: ResultPolicy | None = None
    pool: str | None = None
    status_source: StatusSource | None = None
    callback_param: str | None = None

    # 完整模式
    namespace: str | None = None
    create_method: Literal["POST", "PUT"] = "POST"
    get_method: Literal["GET"] = "GET"
    cancel_method: Literal["DELETE", "POST"] = "DELETE"
    extra_headers: dict[str, str] = Field(default_factory=dict)
    request_body_template: dict[str, Any] | None = None
    normalize: NormalizationRules | None = None
    strategy: StrategyOverride | None = None
    notes: str | None = None

    @field_validator("alias")
    @classmethod
    def _alias_shape(cls, v: str) -> str:
        v = v.strip()
        if not v or len(v) > 64:
            raise ValueError("alias 必填且长度 <= 64")
        if not all(c.isalnum() or c in "-_" for c in v):
            raise ValueError("alias 仅允许字母/数字/连字符/下划线")
        return v

    @field_validator("base_url")
    @classmethod
    def _base_url_shape(cls, v: str) -> str:
        v = v.strip().rstrip("/")
        if not v.startswith(("http://", "https://")):
            raise ValueError("base_url 必须以 http:// 或 https:// 开头")
        if "@" in v.split("//", 1)[1].split("/", 1)[0]:
            raise ValueError("base_url 禁止内嵌凭证（凭证一律随 Authorization 头透传）")
        return v

    @field_validator("create_path", "get_path_template", "cancel_path_template")
    @classmethod
    def _path_shape(cls, v: str | None) -> str | None:
        if v is None:
            return None
        v = v.strip()
        if not v.startswith("/"):
            raise ValueError("path 必须以 / 开头")
        return v


class ResolvedTemplate(BaseModel):
    """展开后的完整形态（模板预览接口返回的形态）。"""

    model_config = ConfigDict(extra="forbid")

    alias: str
    namespace: str
    base_url: str
    create_path: str
    create_method: str
    get_path_template: str
    get_method: str
    cancel_path_template: str
    cancel_method: str
    id_location: str
    status_field: str
    result_location: str
    terminal: TerminalMapping
    capabilities: Capabilities
    result_policy: ResultPolicy
    pool: str
    status_source: StatusSource
    callback_param: str
    normalize: NormalizationRules
    strategy: dict[str, Any]
    extra_headers: dict[str, str]
    request_body_template: dict[str, Any] | None = None
    notes: str | None = None

    #: 每字段来源：explicit / derived / default / patched
    provenance: dict[str, str] = Field(default_factory=dict)

    @property
    def version_hash(self) -> str:
        payload = self.model_dump(mode="json", exclude={"provenance"})
        blob = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(blob.encode()).hexdigest()[:16]

    def is_managed(self, field: str) -> bool:
        return self.provenance.get(field, FieldSource.DEFAULT.value) == FieldSource.EXPLICIT.value

    def supports_cancel(self) -> bool:
        return self.capabilities.cancel

    def maps_status(self, upstream_status: str) -> TaskStatus | None:
        """上游原生状态 → 内部终态判定（取不到返回 None → poll_unrecognized）。"""
        s = str(upstream_status)
        if s in self.terminal.success:
            return TaskStatus.SUCCEEDED
        if s in self.terminal.expired:
            # 上游 expired 统一映射进网关 timeout
            return TaskStatus.TIMEOUT
        if s in self.terminal.failure:
            return TaskStatus.FAILED
        return None


def template_tier(draft: TemplateDraft) -> TemplateTier:
    """按填充度判定当前处于哪一级配置面。"""
    diff_fields = (
        "id_location",
        "get_path_template",
        "cancel_path_template",
        "terminal",
        "capabilities",
        "result_policy",
        "pool",
        "status_source",
        "callback_param",
        "status_field",
    )
    full_fields = (
        "namespace",
        "extra_headers",
        "request_body_template",
        "normalize",
        "strategy",
        "notes",
    )
    if any(getattr(draft, f) not in (None, {}, []) for f in full_fields) or draft.create_method != "POST":
        return TemplateTier.FULL
    if any(getattr(draft, f) is not None for f in diff_fields):
        return TemplateTier.DIFF
    return TemplateTier.MINIMAL


def load_draft(raw: dict[str, Any]) -> TemplateDraft:
    return TemplateDraft.model_validate(raw)
