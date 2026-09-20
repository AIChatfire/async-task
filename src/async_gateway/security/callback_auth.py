"""回调安全（§18.3）：不可预测端点 + HMAC 验签 + 时间窗防重放 + kid 轮换。

设计要点：

* 端点 ``/callbacks/{opaque_token}``，token 由网关为**每个任务**生成（不可预测），
  泄露处置见 §18.3 runbook（吊销单任务 token / 按渠道批量换发）。
* 签名头带 ``kid``，支持 active+previous 双密钥灰度轮换。密钥本体存 vault，
  本模块只消费"kid → secret"映射，不负责持久化。
* 验签失败**只审计不进状态机**；窗口内合法重复投递由 ``callback_event`` 去重表兜底。
"""

from __future__ import annotations

import hashlib
import hmac
import math
import secrets
import time
from dataclasses import dataclass
from typing import Mapping

from ..config import get_settings

DEFAULT_KID_HEADER = "x-ag-kid"
DEFAULT_TS_HEADER = "x-ag-timestamp"
DEFAULT_SIG_HEADER = "x-ag-signature"

_TS_MAX_SKEW_SLACK = 1.0


@dataclass(frozen=True, slots=True)
class CallbackVerifyResult:
    ok: bool
    kid: str | None
    reason: str | None = None
    replay_suspect: bool = False

    @property
    def failure_detail(self) -> str:
        return self.reason or ""


def new_opaque_token(nbytes: int = 32) -> str:
    """每任务一个不可预测 token（URL-safe，去掉可能造成歧义的字符）。"""
    return secrets.token_urlsafe(nbytes).replace("-", "A").replace("_", "B")


def sign(kid: str, secret: str, timestamp: int, raw_body: bytes) -> str:
    """签名串：``HMAC-SHA256(secret, f"{kid}.{timestamp}.{sha256(body)}")``。"""
    body_digest = hashlib.sha256(raw_body).hexdigest()
    message = f"{kid}.{timestamp}.{body_digest}".encode()
    return hmac.new(secret.encode(), message, hashlib.sha256).hexdigest()


class CallbackAuthenticator:
    """验签器（active + previous 双密钥）。"""

    def __init__(
        self,
        keys: Mapping[str, str] | None = None,
        *,
        active_kid: str | None = None,
        tolerance_seconds: int | None = None,
        kid_header: str = DEFAULT_KID_HEADER,
        timestamp_header: str = DEFAULT_TS_HEADER,
        signature_header: str = DEFAULT_SIG_HEADER,
    ) -> None:
        settings = get_settings()
        self.keys: dict[str, str] = dict(keys if keys is not None else settings.callback_hmac_keys)
        self.active_kid = active_kid or settings.callback_active_kid
        self.tolerance_seconds = int(
            tolerance_seconds if tolerance_seconds is not None else settings.callback_tolerance_seconds
        )
        self.kid_header = kid_header.lower()
        self.timestamp_header = timestamp_header.lower()
        self.signature_header = signature_header.lower()

    # ---- 密钥轮换 ----
    def rotate(self, new_kid: str, new_secret: str, *, keep_previous: bool = True) -> None:
        """灰度轮换：新 kid 上位，旧 kid 降为 previous 继续接受。"""
        old_active = self.active_kid
        self.keys[new_kid] = new_secret
        self.active_kid = new_kid
        if not keep_previous:
            for kid in [k for k in self.keys if k != new_kid]:
                self.keys.pop(kid, None)
        elif old_active in self.keys and old_active != new_kid:
            pass  # previous 保留在 keys 中即继续可验

    def header_lookup(self, headers: Mapping[str, str], name: str) -> str | None:
        for key, value in headers.items():
            if key.lower() == name:
                return str(value)
        return None

    def verify(
        self,
        headers: Mapping[str, str],
        raw_body: bytes,
        *,
        now: float | None = None,
    ) -> CallbackVerifyResult:
        kid = self.header_lookup(headers, self.kid_header)
        ts_raw = self.header_lookup(headers, self.timestamp_header)
        sig = self.header_lookup(headers, self.signature_header)
        if not kid or not ts_raw or not sig:
            return CallbackVerifyResult(False, kid, "missing signature headers")
        if kid not in self.keys:
            return CallbackVerifyResult(False, kid, f"unknown kid {kid!r}")
        try:
            timestamp = int(ts_raw)
        except ValueError:
            return CallbackVerifyResult(False, kid, "timestamp not an integer")

        current = now if now is not None else time.time()
        skew = abs(current - timestamp)
        if skew > self.tolerance_seconds + _TS_MAX_SKEW_SLACK:
            return CallbackVerifyResult(
                False,
                kid,
                f"timestamp outside tolerance window ({math.floor(skew)}s > {self.tolerance_seconds}s)",
                replay_suspect=True,
            )
        expected = sign(kid, self.keys[kid], timestamp, raw_body)
        if not hmac.compare_digest(expected, sig):
            return CallbackVerifyResult(False, kid, "signature mismatch")
        return CallbackVerifyResult(True, kid)


def callback_dedup_key(
    *,
    upstream_event_id: str | None,
    upstream_task_id: str | None,
    raw_status: str | None,
    body_digest: str,
) -> str:
    """§13：dedup_key 优先上游 event_id，缺失取 (upstream_task_id, raw_status, body_digest)。"""
    if upstream_event_id:
        return f"evt:{upstream_event_id}"
    return f"sig:{upstream_task_id or '-'}:{raw_status or '-'}:{body_digest}"


def body_digest(raw_body: bytes) -> str:
    return hashlib.sha256(raw_body).hexdigest()[:32]
