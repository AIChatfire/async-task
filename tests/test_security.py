"""安全层：SSRF、插值净化、凭证脱敏、回调验签、resolve-and-pin。"""

from __future__ import annotations

import time

import httpx
import pytest

from async_gateway.security.callback_auth import (
    CallbackAuthenticator,
    body_digest,
    callback_dedup_key,
    new_opaque_token,
    sign,
)
from async_gateway.security.redaction import (
    otel_safe_headers,
    redact_headers,
    safe_json_dump,
    scrub_payload,
    scrub_text,
)
from async_gateway.security.sanitize import (
    UnsafeInterpolationValue,
    is_safe_interpolation_value,
    sanitize_interpolation_value,
)
from async_gateway.security.ssrf import (
    SsrfViolation,
    is_forbidden_ip,
    validate_redirect_chain,
    validate_target,
)
from async_gateway.upstream.client import pin_request


# ---- SSRF ----
@pytest.mark.parametrize(
    "ip",
    [
        "127.0.0.1",
        "10.1.2.3",
        "192.168.1.1",
        "172.16.0.9",
        "169.254.169.254",
        "100.100.100.200",
        "::1",
        "fc00::1",
        "fe80::1",
        "0.0.0.0",
        "::ffff:127.0.0.1",
    ],
)
def test_forbidden_ip_ranges(ip: str):
    assert is_forbidden_ip(ip) is True


@pytest.mark.parametrize("ip", ["8.8.8.8", "1.1.1.1", "2606:4700:4700::1111"])
def test_public_ips_allowed(ip: str):
    assert is_forbidden_ip(ip) is False


def test_reject_non_http_scheme():
    with pytest.raises(SsrfViolation):
        validate_target("file:///etc/passwd", resolve=False)
    with pytest.raises(SsrfViolation):
        validate_target("gopher://evil/x", resolve=False)


def test_reject_embedded_credentials():
    with pytest.raises(SsrfViolation):
        validate_target("https://user:pass@example.com/x", resolve=False)


def test_internal_ports_blocked_for_unlisted_hosts():
    with pytest.raises(SsrfViolation):
        validate_target("http://example.com:6379/", resolve=False)


def test_allowlist_bypasses_private_check_but_not_metadata():
    """白名单是"放行自家内网上游"的唯一通道，但**不能**放行 metadata 地址。"""
    target = validate_target("http://localhost:9099/v1/tasks", allow_hosts=["localhost"])
    assert target.allowlisted is True
    assert target.addresses

    with pytest.raises(SsrfViolation):
        validate_target("http://169.254.169.254/latest/meta-data", allow_hosts=["169.254.169.254"])


def test_runtime_validation_catches_unlisted_private_targets():
    # 显式清空白名单：内网地址必须被拒（allow_hosts 是唯一放行通道）
    with pytest.raises(SsrfViolation):
        validate_target("http://127.0.0.1:9099/x", allow_hosts=[])
    with pytest.raises(SsrfViolation):
        validate_target("http://10.0.0.5:9099/x", allow_hosts=[])


def test_redirect_chain_revalidates_each_hop():
    with pytest.raises(SsrfViolation):
        validate_redirect_chain(["http://localhost:9099/ok", "http://169.254.169.254/steal"])


def test_pin_request_rewrites_host_and_keeps_sni():
    request = httpx.Request("GET", "https://api.example.com/v1/tasks")
    pinned = pin_request(request, {"api.example.com": ("93.184.216.34",)})
    assert pinned.url.host == "93.184.216.34"
    assert pinned.headers["Host"] == "api.example.com"
    assert pinned.extensions.get("sni_hostname") == "api.example.com"


# ---- 插值净化 ----
@pytest.mark.parametrize(
    "value",
    ["../../etc/passwd", "http://evil.example/x", "a/b", "a:b", "a b", "a?b", "a#b", "..", ""],
)
def test_unsafe_interpolation_rejected(value: str):
    assert is_safe_interpolation_value(value) is False
    with pytest.raises(UnsafeInterpolationValue):
        sanitize_interpolation_value(value)


@pytest.mark.parametrize("value", ["abc-123", "task_1.2", "3f8a9b0c", "UP-0001"])
def test_safe_interpolation_accepted(value: str):
    assert sanitize_interpolation_value(value) == value


# ---- 脱敏 ----
def test_authorization_never_survives_redaction():
    headers = {
        "Authorization": "Bearer sk-live-abcdefghijklmnop",
        "X-Api-Key": "secret-key-value",
        "Cookie": "session=abc",
        "Content-Type": "application/json",
    }
    redacted = redact_headers(headers)
    assert redacted["Authorization"] == "***redacted***"
    assert redacted["X-Api-Key"] == "***redacted***"
    assert redacted["Cookie"] == "***redacted***"
    assert redacted["Content-Type"] == "application/json"


def test_otel_headers_are_whitelist_only():
    headers = {
        "Authorization": "Bearer secret",
        "Content-Type": "application/json",
        "X-Weird": "value",
        "Traceparent": "00-abc-def-01",
    }
    safe = otel_safe_headers(headers)
    assert "Authorization" not in safe
    assert "X-Weird" not in safe
    assert safe["Content-Type"] == "application/json"


def test_scrub_removes_known_secret_values_and_shapes():
    secret = "sk-live-abcdefghijklmnopqrstuvwxyz"
    text = f"upstream said: invalid key {secret} (Bearer {secret})"
    cleaned = scrub_text(text, [secret])
    assert secret not in cleaned
    assert "redacted" in cleaned


def test_scrub_payload_recurses_and_dumps_safely():
    secret = "sk-live-abcdefghijklmnopqrstuvwxyz"
    payload = {"error": {"detail": f"bad key {secret}", "echo": [secret]}}
    cleaned = scrub_payload(payload, [secret])
    assert secret not in safe_json_dump(cleaned)


# ---- 回调验签 ----
def test_signature_roundtrip():
    auth = CallbackAuthenticator(keys={"k1": "secret-one"}, active_kid="k1", tolerance_seconds=300)
    raw = b'{"status":"succeeded"}'
    ts = int(time.time())
    headers = {"x-ag-kid": "k1", "x-ag-timestamp": str(ts), "x-ag-signature": sign("k1", "secret-one", ts, raw)}
    assert auth.verify(headers, raw).ok is True


def test_signature_rejects_tampered_body_and_stale_timestamp():
    auth = CallbackAuthenticator(keys={"k1": "secret-one"}, active_kid="k1", tolerance_seconds=300)
    raw = b'{"status":"succeeded"}'
    ts = int(time.time())
    headers = {"x-ag-kid": "k1", "x-ag-timestamp": str(ts), "x-ag-signature": sign("k1", "secret-one", ts, raw)}
    assert auth.verify(headers, b'{"status":"failed"}').ok is False

    stale = {"x-ag-kid": "k1", "x-ag-timestamp": str(ts - 3600), "x-ag-signature": sign("k1", "secret-one", ts - 3600, raw)}
    result = auth.verify(stale, raw)
    assert result.ok is False and result.replay_suspect is True


def test_signature_rejects_unknown_kid():
    auth = CallbackAuthenticator(keys={"k1": "secret-one"}, active_kid="k1")
    raw = b"{}"
    ts = int(time.time())
    headers = {"x-ag-kid": "nope", "x-ag-timestamp": str(ts), "x-ag-signature": "deadbeef"}
    result = auth.verify(headers, raw)
    assert result.ok is False and "unknown kid" in (result.reason or "")


def test_key_rotation_keeps_previous_valid():
    auth = CallbackAuthenticator(keys={"k1": "secret-one"}, active_kid="k1")
    raw = b"{}"
    ts = int(time.time())
    old_headers = {
        "x-ag-kid": "k1",
        "x-ag-timestamp": str(ts),
        "x-ag-signature": sign("k1", "secret-one", ts, raw),
    }
    auth.rotate("k2", "secret-two")
    assert auth.active_kid == "k2"
    # 旧 kid 仍可验（灰度轮换窗口）
    assert auth.verify(old_headers, raw).ok is True
    new_headers = {
        "x-ag-kid": "k2",
        "x-ag-timestamp": str(ts),
        "x-ag-signature": sign("k2", "secret-two", ts, raw),
    }
    assert auth.verify(new_headers, raw).ok is True


def test_dedup_key_prefers_event_id():
    assert callback_dedup_key(
        upstream_event_id="evt-1", upstream_task_id="t", raw_status="s", body_digest="d"
    ) == "evt:evt-1"
    assert callback_dedup_key(
        upstream_event_id=None, upstream_task_id="t", raw_status="s", body_digest="d"
    ) == "sig:t:s:d"
    assert len(body_digest(b"x")) == 32


def test_opaque_token_is_unpredictable_and_urlsafe():
    tokens = {new_opaque_token() for _ in range(50)}
    assert len(tokens) == 50
    assert all("-" not in t and "_" not in t for t in tokens)
