"""状态机（§14）：迁移白名单显式化 + 终态单调不翻转。

设计口径（与文档对照）：

* 迁移白名单严格照抄 §14「迁移白名单显式」小节，未列入的迁移一律拒绝。
* ``failed`` 不是终态——§14 明确 ``in_progress → failed（可重试 → submit_upstream 重建
  attempts+1 → 耗尽 dead）``；因此它是"失败待重试"的调度态，可出边到 ``upstream_submitted``
  或 ``dead``。它也不在 §12.2 的"内部终态对外重写"集合里，所以对外不会以终态形态泄露。
* 真正的终态（``BUSINESS_TERMINAL``）无出边，写入必须带期望状态条件更新，保证
  「终态迁移只允许一次」。
* 死信/unknown 的重放走**新任务行**（``origin=replay``），因此不需要终态出边，单调性不被破坏。
"""

from __future__ import annotations

from .enums import BUSINESS_TERMINAL, INTERNAL_TERMINAL, OutputTerminal, TaskStatus

A, US, IP = TaskStatus.ACCEPTED, TaskStatus.UPSTREAM_SUBMITTED, TaskStatus.IN_PROGRESS
SU, PU, FL = TaskStatus.SUBMIT_UNKNOWN, TaskStatus.POLL_UNRECOGNIZED, TaskStatus.FAILED
OK, CN, TO = TaskStatus.SUCCEEDED, TaskStatus.CANCELLED, TaskStatus.TIMEOUT
DEAD, DAC = TaskStatus.DEAD, TaskStatus.DEAD_AWAITING_CONFIRM

#: §14「迁移白名单显式」原表 + 三处**落地补全**（已在 docs/IMPLEMENTATION.md 记录）：
#:   * ``accepted → {dead, timeout}``：提交阶段**确定性**失败（4xx 参数错误 / SSRF 拒绝 /
#:     凭证已失效）与提交阶段超期，原表没有出口，会让任务卡死在 accepted；
#:   * ``submit_unknown → accepted``：compensate「确认未创建 → 重新发起创建」需要一个
#:     可重新抢占提交意图的落点；
#:   * ``poll_unrecognized → {succeeded, failed}``：轮询恢复后（模板勘误生效）直接读到终态。
ALLOWED_TRANSITIONS: dict[TaskStatus, frozenset[TaskStatus]] = {
    A: frozenset({US, CN, SU, DEAD, DAC, TO}),
    # upstream_submitted → 终态 合法：快速任务回调先于轮询到达
    US: frozenset({IP, OK, FL, CN, TO, DEAD, DAC, SU, PU}),
    IP: frozenset({OK, FL, CN, TO, DEAD, DAC, PU, SU}),
    SU: frozenset({US, DAC, A}),
    PU: frozenset({IP, DAC, TO, OK, FL}),
    # 失败重试 = 新业务尝试（派生键 {key}#attempt{n}），耗尽的归宿是 dead；
    # 重试这一枪若"响应超时/丢失"同样要转 submit_unknown 走确认（不能因为重试就豁免）
    FL: frozenset({US, SU, DEAD, DAC, CN}),
    OK: frozenset(),
    CN: frozenset(),
    TO: frozenset(),
    DEAD: frozenset(),
    # dead_awaiting_confirm → dead：**人工确认后的"关闭"动作**（§14「人工确认后重放或关闭」）。
    # 这是 failure → failure 的收敛，不构成"成功/失败翻转"，因此与"单调不翻转"不冲突。
    DAC: frozenset({DEAD}),
}

#: §13「所有写路径带前置条件」表的机器可读副本；测试会断言 dao 覆盖每一行
WRITE_PATHS: dict[str, tuple[frozenset[TaskStatus], str]] = {
    "cancel_requested": (
        frozenset({A, US, IP, SU}),
        "status ∈ {accepted, upstream_submitted, in_progress, submit_unknown}；submit_unknown 仅置标记不迁移",
    ),
    "submit_intent": (frozenset({A}), "status = accepted 且 submit_started_at 为空（CAS 抢占）"),
    "submit_ok": (frozenset({A}), "status = accepted 且 submit_started_at 已置（意图先行）"),
    "cancel_after_submit": (
        frozenset({US, IP}),
        "submit 落库后发现 cancel_requested 置位 → 立即触发上游取消",
    ),
    "advance": (frozenset(ALLOWED_TRANSITIONS) - BUSINESS_TERMINAL, "白名单迁移 + 期望状态匹配"),
    "store_result": (frozenset({OK}), "status = succeeded 且 result_ref 为空"),
    "next_poll_at": (
        frozenset({A, US, IP, SU, PU, FL}),
        "非终态才可重排（终态不重排，保证不翻转）",
    ),
    "compensate_relocate": (frozenset({SU}), "status = submit_unknown"),
    "replay_child": (frozenset({DEAD, DAC}), "仅从业务终态派生新任务行（origin=replay）"),
}


class TransitionError(ValueError):
    """非法状态迁移。"""

    def __init__(self, src: TaskStatus | str, dst: TaskStatus | str) -> None:
        super().__init__(f"illegal transition: {src} -> {dst}")
        self.src = TaskStatus(src) if not isinstance(src, TaskStatus) else src
        self.dst = TaskStatus(dst) if not isinstance(dst, TaskStatus) else dst


def can_transition(src: TaskStatus | str, dst: TaskStatus | str) -> bool:
    src = TaskStatus(src)
    dst = TaskStatus(dst)
    return dst in ALLOWED_TRANSITIONS.get(src, frozenset())


def assert_transition(src: TaskStatus | str, dst: TaskStatus | str) -> None:
    if not can_transition(src, dst):
        raise TransitionError(src, dst)


def is_business_terminal(status: TaskStatus | str) -> bool:
    return TaskStatus(status) in BUSINESS_TERMINAL


def is_internal_terminal(status: TaskStatus | str) -> bool:
    """内部终态：对外须按下游模板映射重写为上游原生失败类终态（§12.2）。"""
    return TaskStatus(status) in INTERNAL_TERMINAL


def is_terminal_for_output(status: TaskStatus | str) -> bool:
    """对外可安全输出的"停止轮询"判定：业务终态。"""
    return is_business_terminal(status)


def output_terminal(status: TaskStatus | str) -> OutputTerminal:
    """内部状态 → envelope ``terminal`` 三值。"""
    status = TaskStatus(status)
    if status is TaskStatus.SUCCEEDED:
        return OutputTerminal.SUCCEEDED
    if status in BUSINESS_TERMINAL or status is TaskStatus.FAILED:
        return OutputTerminal.FAILED
    return OutputTerminal.IN_PROGRESS


def allowed_targets(status: TaskStatus | str) -> frozenset[TaskStatus]:
    return ALLOWED_TRANSITIONS.get(TaskStatus(status), frozenset())
