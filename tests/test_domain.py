"""领域层：状态机白名单、终态集合、错误三级分类。"""

from __future__ import annotations

import httpx
import pytest

from async_gateway.domain.enums import (
    BUSINESS_TERMINAL,
    INTERNAL_TERMINAL,
    RETRY_PENDING,
    UNKNOWN_STATES,
    ErrorClass,
    ErrorCode,
    OutputTerminal,
    TaskStatus,
)
from async_gateway.domain.errors import (
    backoff_seconds,
    classify_exception,
    classify_status,
    extract_retry_after,
)
from async_gateway.domain.state_machine import (
    WRITE_PATHS,
    TransitionError,
    allowed_targets,
    assert_transition,
    can_transition,
    is_internal_terminal,
    output_terminal,
)

S = TaskStatus


def test_whitelist_matches_document():
    assert can_transition(S.ACCEPTED, S.UPSTREAM_SUBMITTED)
    assert can_transition(S.ACCEPTED, S.CANCELLED)
    assert can_transition(S.ACCEPTED, S.SUBMIT_UNKNOWN)
    assert can_transition(S.UPSTREAM_SUBMITTED, S.SUCCEEDED)  # 快速任务回调直达
    assert can_transition(S.SUBMIT_UNKNOWN, S.UPSTREAM_SUBMITTED)
    assert can_transition(S.SUBMIT_UNKNOWN, S.DEAD_AWAITING_CONFIRM)
    assert can_transition(S.POLL_UNRECOGNIZED, S.IN_PROGRESS)
    assert can_transition(S.POLL_UNRECOGNIZED, S.DEAD_AWAITING_CONFIRM)


def test_terminal_states_have_no_flipping_edges():
    """业务终态不得有"翻转"出边；唯一例外是 dead_awaiting_confirm → dead（人工关闭）。

    DAC→DEAD 是 failure → failure 的收敛（§14「人工确认后重放或关闭」），
    不构成成功/失败翻转，所以不违反"单调不翻转"。
    """
    allowed_closure = {S.DEAD_AWAITING_CONFIRM: {S.DEAD}}
    for status in BUSINESS_TERMINAL:
        expected = allowed_closure.get(status, set())
        assert allowed_targets(status) == frozenset(expected), status


def test_illegal_transitions_rejected():
    assert not can_transition(S.SUCCEEDED, S.FAILED)
    assert not can_transition(S.CANCELLED, S.IN_PROGRESS)
    assert not can_transition(S.TIMEOUT, S.SUCCEEDED)
    assert not can_transition(S.IN_PROGRESS, S.ACCEPTED)
    with pytest.raises(TransitionError):
        assert_transition(S.IN_PROGRESS, S.ACCEPTED)


def test_failed_is_retry_pending_not_terminal():
    """§14：failed 可经 submit_upstream 重建 → 因此它**不是**终态。"""
    assert S.FAILED in RETRY_PENDING
    assert S.FAILED not in BUSINESS_TERMINAL
    assert can_transition(S.FAILED, S.UPSTREAM_SUBMITTED)
    assert can_transition(S.FAILED, S.DEAD)


def test_internal_terminal_set_matches_doc():
    """§12.2 只把 timeout/dead/dead_awaiting_confirm 视为"需要原生重写的内部终态"。"""
    assert {s.value for s in INTERNAL_TERMINAL} == {"timeout", "dead", "dead_awaiting_confirm"}
    assert not is_internal_terminal(S.FAILED)
    assert not is_internal_terminal(S.CANCELLED)


def test_unknown_states_are_two():
    assert {s.value for s in UNKNOWN_STATES} == {"submit_unknown", "poll_unrecognized"}


def test_output_terminal_mapping():
    assert output_terminal(S.SUCCEEDED) is OutputTerminal.SUCCEEDED
    assert output_terminal(S.IN_PROGRESS) is OutputTerminal.IN_PROGRESS
    assert output_terminal(S.SUBMIT_UNKNOWN) is OutputTerminal.IN_PROGRESS
    assert output_terminal(S.TIMEOUT) is OutputTerminal.FAILED
    assert output_terminal(S.DEAD_AWAITING_CONFIRM) is OutputTerminal.FAILED
    assert output_terminal(S.FAILED) is OutputTerminal.FAILED


def test_write_path_table_covers_documented_rows():
    expected = {
        "cancel_requested",
        "submit_intent",
        "submit_ok",
        "advance",
        "store_result",
        "next_poll_at",
        "compensate_relocate",
    }
    assert expected.issubset(set(WRITE_PATHS))
    assert S.SUBMIT_UNKNOWN in WRITE_PATHS["cancel_requested"][0]


# ---- 错误分级：连接超时 vs 响应超时是最容易搞反的一处 ----
def test_connect_timeout_is_retry_safe():
    exc = httpx.ConnectTimeout("boom")
    c = classify_exception(exc, "submit")
    assert c.error_class is ErrorClass.RETRY_SAFE
    assert c.retryable and c.action == "retry"


def test_connect_refused_and_dns_are_retry_safe():
    c = classify_exception(httpx.ConnectError("connection refused"), "submit")
    assert c.error_class is ErrorClass.RETRY_SAFE


def test_read_timeout_requires_confirmation():
    c = classify_exception(httpx.ReadTimeout("boom"), "submit")
    assert c.error_class is ErrorClass.CONFIRM_REQUIRED
    assert c.is_unknown and not c.retryable


def test_read_timeout_in_poll_just_backs_off():
    c = classify_exception(httpx.ReadTimeout("boom"), "poll")
    assert c.retryable and c.action == "backoff"


def test_status_classification_table():
    assert classify_status(429, "submit").error_class is ErrorClass.RATE_LIMITED
    assert classify_status(429, "submit").consumes_attempt is False
    assert classify_status(401, "submit").channel_fault is True
    assert classify_status(403, "submit").consumes_attempt is False
    assert classify_status(409, "submit").action == "confirm"
    assert classify_status(422, "submit").error_class is ErrorClass.FAIL_FAST
    assert classify_status(500, "submit").action == "confirm"
    assert classify_status(503, "poll").action == "backoff"
    assert classify_status(404, "poll").action == "confirm"


def test_error_code_domain_is_closed():
    assert {c.value for c in ErrorCode} == {
        "UPSTREAM_4XX",
        "UPSTREAM_5XX",
        "UPSTREAM_TERMINAL",
        "TRANSPORT",
        "RATE_LIMITED",
        "TIMEOUT",
        "CANCELLED",
        "TRANSFER_FAILED",
        "UNKNOWN_EXHAUSTED",
    }


def test_extract_retry_after():
    assert extract_retry_after({"retry-after": "3"}) == 3.0
    assert extract_retry_after({}) == 1.0


def test_backoff_is_bounded_and_jittered():
    for attempt in range(1, 8):
        value = backoff_seconds(attempt, base=1.0, cap=60.0)
        assert 0 < value <= 60.0
