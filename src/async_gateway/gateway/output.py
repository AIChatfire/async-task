"""状态输出契约（§4.2 / §12.2）。

网关不参与计费；它对计费的全部责任收敛为**状态输出契约**：

1. **真实**：输出状态反映上游真实状态，不伪造成功、不伪造失败。
2. **单调收敛不翻转**：终态只允许一次迁移（条件更新保证），输出侧只读。
3. **重复查询一致**：终态后固定读 ``result_ref``，不再打上游。
4. **内部终态有原生输出路径**：``timeout/dead/dead_awaiting_confirm`` 是网关内部终态，
   上游原生枚举没有 → 响应**保持上游原生形状**，但状态字段按模板映射重写为
   **上游原生失败类终态取值**，失败原因字段按模板映射 error_code 为原生格式。
   否则快照停在最后非终态，New API 永远读不到终态、退款不触发。

另外两条容易被忽略的硬规则：

* **响应形状保真（查询面）**：get 响应保持上游原生形状（envelope 只给网关自有调用方）——
  所以这里做的是"在原生形状上就地重写字段"，而不是包一层。create 响应在默认 ``queued``
  受理下是网关形状 + 可解析 id（见 docs/IMPLEMENTATION.md §3.1），``inline`` 下仍是原生形状。
* **转存永久失败的上游成功任务报 succeeded + 结果不可用**：状态字段 = 上游原生成功终态，
  结果字段重写为网关结果端点 URL（该 URL 返回 410 语义）——New API 按成功正常结算，
  用户取结果时得到明确不可用信号；平台承担上游成本，不伪造失败骗退款。
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any

from ..db.models import AsyncTask
from ..domain.enums import OutputTerminal, TaskStatus
from ..domain.state_machine import is_internal_terminal, output_terminal
from ..infra.object_store import get_result_store
from ..templates.derive import effective_failure_error_code, effective_failure_status
from ..templates.expression import compile_expression
from ..templates.schema import ResultMode, ResolvedTemplate

# 结果获取：**不经网关中转**（§12.2 首选）。store 模式下由 `result_ref` 直接换出对象存储的
# 预签名 URL 写进响应体；因此这里既没有"结果端点"常量，也没有可枚举的结果路径。
# 若将来确需网关代理读（Range 透传 / 隐藏桶），那必须是**带归属校验**的独立形态（§18.x），
# 而不是现在这条匿名、用内部主键做路径的中转端点。


class PathWriteSkipped(Exception):
    """快照形状不支持就地重写（例如快照不是对象）。"""


def set_path_on_doc(doc: dict[str, Any], spec: str, value: Any) -> None:
    """把取值写到表达式指明的路径上（就地重写原生形状用）。

    只支持在既有对象上写**键**；路径含下标或中间层不存在时按"新建 dict"处理，
    无法新建（父节点是标量/数组元素）则抛 :class:`PathWriteSkipped`。
    """
    path = compile_expression(spec)
    if not path.steps:
        raise PathWriteSkipped("空路径")
    node: Any = doc
    for step in path.steps[:-1]:
        if step.kind != "key":
            raise PathWriteSkipped(f"路径含下标，无法安全重写：{spec}")
        child = node.get(step.value)
        if not isinstance(child, dict):
            child = {}
            node[step.value] = child
        node = child
    last = path.steps[-1]
    if last.kind != "key":
        raise PathWriteSkipped(f"路径末段是下标，无法安全重写：{spec}")
    node[last.value] = value


@dataclass(frozen=True, slots=True)
class DegradedNotice:
    code: str
    detail: str

    def as_dict(self) -> dict[str, str]:
        return {"code": self.code, "detail": self.detail}


def degraded_notices(task: AsyncTask, template: ResolvedTemplate, *, result_available: bool) -> list[DegradedNotice]:
    """``degraded[]``：显式声明能力降级，不静默承诺（§4.2 / 验收 7）。"""
    notices: list[DegradedNotice] = []
    attrs = task.attributes or {}
    if attrs.get("cancel_degraded"):
        notices.append(
            DegradedNotice("cancel_requested", "上游不支持取消，已降级为 cancel_requested 标记")
        )
    if template.result_policy.mode is ResultMode.PASSTHROUGH:
        notices.append(
            DegradedNotice("passthrough_result", "结果直链依赖上游有效期，未转存")
        )
    if template.result_policy.mode is ResultMode.STORE and get_result_store() is None:
        notices.append(
            DegradedNotice("transfer_disabled", "未配置对象存储，已自动关闭结果转存（结果直链透传）")
        )
    if task.result_degraded:
        notices.append(DegradedNotice("result_unavailable", "结果转存永久失败，结果端点返回 410"))
    if TaskStatus(task.status) is TaskStatus.POLL_UNRECOGNIZED:
        notices.append(
            DegradedNotice("poll_unrecognized", "轮询响应判不出终态，已按不可判定处理并继续重试")
        )
    if is_internal_terminal(task.status):
        notices.append(
            DegradedNotice(
                "gateway_internal_terminal",
                f"{task.status} 为网关内部终态，已按模板映射重写为上游原生失败类取值",
            )
        )
    if not template.capabilities.upstream_idempotent:
        notices.append(
            DegradedNotice("upstream_idempotent_false", "上游创建非幂等，创建去重由网关承担")
        )
    if template.status_source.value == "callback" and not template.capabilities.callback:
        notices.append(DegradedNotice("callback_unavailable", "上游无可靠回调，已降级为轮询"))
    if (
        not result_available
        and TaskStatus(task.status) is TaskStatus.SUCCEEDED
        and template.result_policy.mode is ResultMode.STORE
        and get_result_store() is not None  # 转存被自动关闭时不谎报"转存中"（见 transfer_disabled）
    ):
        notices.append(DegradedNotice("result_pending", "结果转存进行中，稍后重试结果端点"))
    return notices


def envelope(task: AsyncTask, template: ResolvedTemplate, *, result_available: bool = False) -> dict[str, Any]:
    """envelope：**仅面向网关自有调用方**（New API 侧不用）。"""
    return {
        "task_id": task.task_id,
        "alias": task.template_alias,
        "template_version": task.template_version,
        "raw_status": task.raw_status,
        "terminal": output_terminal(task.status).value,
        "internal_status": task.status,
        "attempts": task.attempts,
        "error_code": task.error_code,
        "next_poll_at": task.next_poll_at.isoformat() if task.next_poll_at else None,
        "deadline_at": task.deadline_at.isoformat() if task.deadline_at else None,
        "degraded": [n.as_dict() for n in degraded_notices(task, template, result_available=result_available)],
    }


def native_snapshot(task: AsyncTask) -> Any:
    if task.status_snapshot is not None:
        return copy.deepcopy(task.status_snapshot)
    return {}


#: 预创建期/首次轮询前的**非终态占位取值**（对外词表）。
#:
#: ``queued``（默认）受理下，任务可能在还没有任何上游状态时就被查询（立刻 202 之后、
#: worker 完成 create 之前）。没有这个占位，响应会是空体 ``{}``，New API 侧宽映射读不到
#: 状态即判 UNKNOWN（generic-async-v1 会直接判失败）。``queued`` 在两族插件词表里都归属
#: "进行中"，是最安全的占位。
_PENDING_PLACEHOLDER_STATUS = "queued"


def _effective_status_value(task: AsyncTask, template: ResolvedTemplate) -> tuple[str | None, bool]:
    """返回 ``(要写入的原生状态值, 是否需要重写)``。

    只有"网关内部终态 / 取消"才**必须**重写；成功态尽量沿用上游原生取值。
    """
    status = TaskStatus(task.status)
    native = task.raw_status
    success_values = set(template.terminal.success)
    failure_values = set(template.terminal.failure) | set(template.terminal.expired)

    if status is TaskStatus.SUCCEEDED:
        if native in success_values:
            return native, False
        return (template.terminal.success or ["succeeded"])[0], True
    if status is TaskStatus.CANCELLED:
        if native in failure_values:
            return native, False
        # 取消必须让计费侧读到失败类终态，否则会被当成在途任务
        return effective_failure_status(template), True
    if is_internal_terminal(status):
        if native in failure_values and status is TaskStatus.TIMEOUT and native in template.terminal.expired:
            return native, False
        return effective_failure_status(template), True
    if native is None:
        # 非终态但还没有任何上游侧状态（queued 默认下的预创建窗口 / 首次轮询前）：
        # 合成占位取值，别让调用方读到"没有状态"（详见常量注释）。
        return _PENDING_PLACEHOLDER_STATUS, True
    return native, False


def build_native_query_response(
    task: AsyncTask,
    template: ResolvedTemplate,
    *,
    result_url: str | None,
) -> Any:
    """构造面向 New API 的查询响应：**上游原生形状** + 必要字段重写。

    ``result_url`` 由调用方**预先算好**（store 模式 = 对象存储的预签名 URL）：
    转存尚未完成、对象已过期或被删除时传 ``None``，此时结果字段置空并带上
    `_result_hint` —— "结果不可用"由**字段本身**表达，不再依赖某个网关端点返回 410。
    """
    snapshot = native_snapshot(task)
    if not isinstance(snapshot, dict):
        # 快照不是对象（例如上游返回数组）→ 形状保真优先，不做重写
        return snapshot

    status_value, needs_rewrite = _effective_status_value(task, template)
    if status_value is not None:
        # 两件事都可能要求我们写状态字段：
        #   1) 内部终态必须重写为上游原生失败类取值（§12.2 硬规则）；
        #   2) 快照里压根没有状态字段（例如 create 之后还没轮询、或首次查询把空快照
        #      直接回给调用方）—— 一个没有 status 的原生形状对 New API 毫无用处。
        present = False
        try:
            from ..templates.expression import try_extract as _try

            present, _ = _try(snapshot, template.status_field)
        except Exception:  # noqa: BLE001 - 形状异常时按"不存在"处理
            present = False
        if needs_rewrite or not present:
            try:
                set_path_on_doc(snapshot, template.status_field, status_value)
            except PathWriteSkipped:
                pass

    status = TaskStatus(task.status)
    is_success = status is TaskStatus.SUCCEEDED
    if is_success:
        if template.result_policy.mode is ResultMode.STORE and get_result_store() is not None:
            # store：结果字段 = 对象存储的预签名 URL（直给，不经网关中转）
            try:
                set_path_on_doc(snapshot, template.result_location, result_url)
            except PathWriteSkipped:
                pass
            if result_url is None:
                # 转存未完成或永久失败：**不伪造失败** —— 状态保持上游原生成功终态，
                # 结果字段留空；envelope 的 degraded[] 已声明 result_pending/unavailable
                snapshot.setdefault("_result_hint", "unavailable_or_pending")
        # passthrough：保留上游直链（degraded[] 已声明有效期依赖），不动结果字段
    elif is_internal_terminal(status) or status is TaskStatus.CANCELLED:
        error_field = template.terminal.error_field
        if error_field:
            try:
                set_path_on_doc(snapshot, error_field, effective_failure_error_code(template))
            except PathWriteSkipped:
                pass
        if template.result_policy.mode is ResultMode.STORE:
            # 终态无产物：结果字段清空（原先指向网关中转端点，该端点已取消）
            try:
                set_path_on_doc(snapshot, template.result_location, None)
            except PathWriteSkipped:
                pass
    return snapshot


def body_field_name(template: ResolvedTemplate) -> str | None:
    """结果字段名（用于判断客户端能否读到结果）。"""
    steps = compile_expression(template.result_location).steps
    if steps and steps[-1].kind == "key":
        return str(steps[-1].value)
    return None


def response_headers(
    task: AsyncTask,
    template: ResolvedTemplate,
    *,
    refresh_interval: float,
    result_available: bool,
) -> dict[str, str]:
    """查询面行为头：快照优先 → 引导客户端降频 + 终态不刷新。"""
    headers: dict[str, str] = {
        "X-AG-Task-Id": task.task_id,
        "X-AG-Internal-Status": task.status,
        "X-AG-Envelope": "1",
    }
    status = TaskStatus(task.status)
    if status in (TaskStatus.SUCCEEDED, TaskStatus.POLL_UNRECOGNIZED) or is_internal_terminal(status):
        # 终态后读 result_ref，不再打上游
        headers["X-AG-Snapshot"] = "terminal"
        return headers
    headers["X-AG-Snapshot"] = "cached"
    headers["Retry-After"] = str(max(1, int(refresh_interval)))
    return headers


def create_response(task: AsyncTask, template: ResolvedTemplate, native_body: Any) -> Any:
    """受理响应：原样透传上游 create 响应（New API 依赖它解析上游 task id）。"""
    return native_body


def output_terminal_value(status: str | TaskStatus) -> OutputTerminal:
    return output_terminal(status)
