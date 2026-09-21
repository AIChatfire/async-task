"""注册校验器（§12.3）。

校验三件事：

1. **schema**：字段形状/类型/别名合法性（pydantic 定位到字段）。
2. **推导完整性**：推导不出来的必填项报错并定位到字段。
3. **表达式沙箱校验**：JSONPath 严格子集，禁 filter/脚本，附步数/大小/超时上限；
   提供样例响应时进一步验证"终态判定取值确实提取得到"，提取不到只标 warning，
   对应响应归 ``poll_unrecognized``（§12.3）。

同一套校验器被 Task-admin 与 config 仓评审复用，**无特权旁路**（§18.4）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from pydantic import ValidationError

from ..config import get_settings
from ..security.sanitize import is_safe_interpolation_value
from .derive import effective_failure_status, resolve
from .expression import ExpressionError, compile_expression, try_extract
from .schema import (
    ConfirmStrategy,
    ResultMode,
    ResolvedTemplate,
    StatusSource,
    TemplateDraft,
    TemplateTier,
    template_tier,
)

Severity = Literal["error", "warning"]


@dataclass(frozen=True, slots=True)
class Issue:
    field: str
    message: str
    severity: Severity = "error"

    def as_dict(self) -> dict[str, str]:
        return {"field": self.field, "message": self.message, "severity": self.severity}


@dataclass(slots=True)
class ValidationReport:
    tier: TemplateTier | None = None
    resolved: ResolvedTemplate | None = None
    issues: list[Issue] = field(default_factory=list)
    assumptions: list[str] = field(default_factory=list)

    @property
    def errors(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == "error"]

    @property
    def warnings(self) -> list[Issue]:
        return [i for i in self.issues if i.severity == "warning"]

    @property
    def ok(self) -> bool:
        return not self.errors and self.resolved is not None

    def add(self, field_name: str, message: str, severity: Severity = "error") -> None:
        self.issues.append(Issue(field_name, message, severity))

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "tier": self.tier.value if self.tier else None,
            "version_hash": self.resolved.version_hash if self.resolved else None,
            "errors": [i.as_dict() for i in self.errors],
            "warnings": [i.as_dict() for i in self.warnings],
            "assumptions": self.assumptions,
        }


def validate(
    raw: dict[str, Any],
    *,
    sample_status_response: dict[str, Any] | None = None,
    sample_create_response: dict[str, Any] | None = None,
    poll_max_interval: float | None = None,
) -> ValidationReport:
    report = ValidationReport()
    try:
        draft = TemplateDraft.model_validate(raw)
    except ValidationError as exc:
        for err in exc.errors():
            loc = ".".join(str(p) for p in err["loc"]) or "<root>"
            report.add(loc, err["msg"])
        return report

    report.tier = template_tier(draft)
    try:
        resolved = resolve(draft)
    except ValidationError as exc:
        for err in exc.errors():
            loc = ".".join(str(p) for p in err["loc"]) or "<root>"
            report.add(loc, err["msg"])
        return report
    report.resolved = resolved

    _check_expressions(resolved, report)
    _check_paths(resolved, report)
    _check_terminal(resolved, report)
    _check_capabilities(resolved, report)
    _check_strategy(resolved, draft, report, poll_max_interval=poll_max_interval)
    _check_credentials(resolved, report)

    if report.ok:
        from .derive import effective_review_ok

        report.assumptions = effective_review_ok(resolved)

    if sample_create_response is not None and report.ok:
        ok, value = try_extract(sample_create_response, resolved.id_location)
        if not ok:
            report.add(
                "id_location",
                f"样例创建响应中取不到 id（{resolved.id_location}）——上游若另有包裹层需显式覆盖",
            )
        elif not is_safe_interpolation_value(value):
            report.add("id_location", f"样例 id 取值 {value!r} 不能安全用于 URL 插值")

    if sample_status_response is not None and report.ok:
        ok, value = try_extract(sample_status_response, resolved.status_field)
        if not ok:
            report.add(
                "status_field",
                f"样例查询响应中取不到状态（{resolved.status_field}）→ 该响应会归 poll_unrecognized",
                "warning",
            )
        elif resolved.maps_status(str(value)) is None:
            report.add(
                "terminal",
                f"样例状态取值 {value!r} 未落进 success/failure/expired 任一集合 → "
                "该响应会归 poll_unrecognized",
                "warning",
            )
    return report


def _check_expressions(resolved: ResolvedTemplate, report: ValidationReport) -> None:
    for field_name, spec in (
        ("id_location", resolved.id_location),
        ("status_field", resolved.status_field),
        ("result_location", resolved.result_location),
    ):
        try:
            compile_expression(spec)
        except ExpressionError as exc:
            report.add(field_name, f"表达式沙箱拒绝：{exc}")


def _check_paths(resolved: ResolvedTemplate, report: ValidationReport) -> None:
    if "{id}" not in resolved.get_path_template:
        report.add("get_path_template", "查询路径缺少 {id} 占位符，无法定位单个任务")
    if resolved.capabilities.cancel:
        if "{id}" not in resolved.cancel_path_template:
            report.add("cancel_path_template", "capabilities.cancel=true 时取消路径必须含 {id}")
    if not resolved.base_url:
        report.add("base_url", "base_url 不能为空")
    if resolved.status_source is StatusSource.CALLBACK and not resolved.capabilities.callback:
        report.add("status_source", "status_source=callback 但 capabilities.callback=false")
    if resolved.status_source is StatusSource.CALLBACK and "{id}" not in resolved.callback_param:
        pass  # callback_param 是参数名，不要求占位符


def _check_terminal(resolved: ResolvedTemplate, report: ValidationReport) -> None:
    if not resolved.terminal.success:
        report.add("terminal.success", "必须声明至少一个成功终态取值")
    if not resolved.terminal.failure:
        report.add(
            "terminal.failure",
            "必须声明至少一个失败终态取值——内部终态需要重写为上游原生失败类取值（§12.2 硬规则）",
        )
    overlap = set(resolved.terminal.success) & set(resolved.terminal.failure)
    if overlap:
        report.add("terminal", f"成功与失败终态取值重叠：{sorted(overlap)}")
    rewrite = effective_failure_status(resolved)
    if rewrite not in set(resolved.terminal.failure) | set(resolved.terminal.expired):
        report.add(
            "terminal.internal_terminal_rewrite",
            f"内部终态重写取值 {rewrite!r} 不在 failure/expired 集合内 → 上游原生形状不自洽",
            "warning",
        )


def _check_capabilities(resolved: ResolvedTemplate, report: ValidationReport) -> None:
    caps = resolved.capabilities
    if caps.confirm_strategy is ConfirmStrategy.LIST_AND_MATCH and not resolved.normalize.fields:
        report.add(
            "normalize.fields",
            "confirm_strategy=list_and_match 需要请求体摘要匹配字段集（normalize.fields 不能为空）",
        )
    if caps.per_key_isolation and caps.upstream_idempotent:
        report.add(
            "capabilities",
            "per_key_isolation=true 且 upstream_idempotent=true：多 key 轮换会让派生幂等键 key_hash 漂移，"
            "上游幂等也救不回跨 key 重复创建，请复核",
            "warning",
        )
    if resolved.result_policy.mode is ResultMode.PASSTHROUGH and not caps.permanent_result_url:
        report.add(
            "result_policy.mode",
            "passthrough 依赖上游直链永久有效，但 capabilities.permanent_result_url=false",
        )
    if resolved.result_policy.mode is ResultMode.REDIRECT:
        report.add("result_policy.mode", "redirect 模式第一版未实现（§12.2 只实现 store/passthrough）")
    if resolved.capabilities.callback and resolved.callback_param.strip() == "":
        report.add("callback_param", "capabilities.callback=true 时 callback_param 不能为空")
    if resolved.normalize.fields and resolved.request_body_template is not None:
        missing = [f for f in resolved.normalize.fields if f not in resolved.request_body_template]
        if missing:
            report.add(
                "normalize.fields",
                f"规范化字段未出现在 request_body_template 中：{missing}",
                "warning",
            )


def _check_strategy(
    resolved: ResolvedTemplate,
    draft: TemplateDraft,
    report: ValidationReport,
    *,
    poll_max_interval: float | None = None,
) -> None:
    strategy = resolved.strategy
    if int(strategy.get("max_attempts", 1)) < 1:
        report.add("strategy.max_attempts", "max_attempts 必须 >= 1")
    if float(strategy.get("deadline_seconds", 1)) <= 0:
        report.add("strategy.deadline_seconds", "deadline_seconds 必须 > 0")
    deadline_warning = risky_deadline_warning(
        float(strategy.get("deadline_seconds", 0) or 0), poll_max_interval
    )
    if deadline_warning:
        # 与轮询上限的**联动**校验（F5）：单看 deadline 或单看 poll_max 都合理，
        # 组合起来却是"任务必然超期"。这里只警示、不阻断（判断依据见 §3.18）。
        report.add("strategy.deadline_seconds", deadline_warning, "warning")
    if float(strategy.get("min_refresh_interval", 0)) < 0:
        report.add(
            "strategy.min_refresh_interval",
            "min_refresh_interval 不得为负（0 表示每次查询都做透传刷新）",
        )
    if int(strategy.get("idempotency_window_seconds", 1)) <= 0:
        report.add("strategy.idempotency_window_seconds", "幂等窗口必须 > 0")
    if draft.strategy is not None:
        for key, value in draft.strategy.model_dump(exclude_none=True).items():
            if value is None:  # pragma: no cover - exclude_none 已过滤
                continue
            if key not in resolved.strategy:  # pragma: no cover - schema 已约束
                report.add(f"strategy.{key}", "未知策略键")


def risky_deadline_warning(
    deadline_seconds: float, poll_max_interval: float | None = None
) -> str | None:
    """模板 deadline 与轮询上限的联动检查（F5）：不清楚就返回 ``None``。

    判据：``deadline_seconds <= 2 × poll_max_interval`` ⇒ 警示。理由：限流退避把该渠道的
    轮询间隔推到上限时，一次轮询就要等满 ``poll_max_interval``；若任务预算只有一两倍，
    "重投 + 创建 + 首次轮询"链条必然超期，任务被 ``timeout`` 杀掉而不是重试成功
    （livetest-ai 报告 E2E-ASYNC-TASK-001 的 F5）。

    改法：把 ``deadline_seconds`` 提到 ``poll_max_interval`` 的 2 倍以上（生产默认
    ``AG_TASK_DEADLINE_SECONDS=1800`` 对 ``AG_POLL_MAX_INTERVAL=60`` 是 30 倍，安全）。
    """
    if poll_max_interval is None:
        poll_max_interval = float(get_settings().poll_max_interval)
    if deadline_seconds <= 0 or poll_max_interval <= 0:
        return None
    if deadline_seconds <= 2 * poll_max_interval:
        return (
            f"deadline_seconds={deadline_seconds:g} 未显著大于轮询上限 "
            f"poll_max_interval={poll_max_interval:g}（应 > 2 倍）：限流退避把轮询推到上限时，"
            "重投 + 首轮询可能直接超出任务预算 ⇒ 任务被 timeout 收尾而不是重试成功"
            "（详见 docs/IMPLEMENTATION.md §3.18）"
        )
    return None


def _check_credentials(resolved: ResolvedTemplate, report: ValidationReport) -> None:
    """凭证不进模板（§12.1/§18.2）。"""
    for key in resolved.extra_headers:
        if key.lower() in {"authorization", "x-api-key", "api-key", "x-goog-api-key"}:
            report.add(
                "extra_headers",
                f"{key} 属凭证类头，必须由渠道持有并随 Authorization 头透传，不允许写进模板",
            )
