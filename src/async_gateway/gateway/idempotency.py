"""派生幂等键（§4.1 / §13 收尾-12）。

口径（照文档，不做"更聪明"的扩展）：

* 调用方显式提供幂等键 → 优先使用；
* New API 按上游原生协议提交、**不携带幂等键** → 网关派生
  ``key_hash + 请求体规范化哈希``；
* 规范化**仅对模板声明的字段集**做（字段按字典序、去首尾空白、声明过的字段忽略大小写）。
  **不做**"默认值显式化"这类不可实现的"全量规范化"。
* 终态 failed 的重试派生 ``{key}#attempt{n}``；
* ``idempotency_key`` 语义 = **窗口期内唯一**：同键在窗口外视为新请求
  （由 ``window_bucket`` 参与唯一约束实现，见 dao/models）。
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from ..templates.schema import NormalizationRules

#: 派生键前缀：区分"显式"与"派生"，便于排障与指标
DERIVED_PREFIX = "auto:"


@dataclass(frozen=True, slots=True)
class IdempotencyKey:
    key: str
    derived: bool
    body_digest: str

    @property
    def explicit(self) -> bool:
        return not self.derived


def _get_dotted(body: dict[str, Any], dotted: str) -> Any:
    node: Any = body
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return None
        node = node[part]
    return node


def canonical_body(body: dict[str, Any], rules: NormalizationRules) -> str:
    """规范化**声明字段集**并序列化（稳定、可复算）。"""
    fields = list(rules.fields)
    if not fields:
        # 未声明时退化为"顶层键全量"，避免把整个嵌套体做不可控规范化
        fields = sorted(str(k) for k in body)
    ci = {f.lower() for f in rules.case_insensitive_fields}
    payload: dict[str, Any] = {}
    for field in sorted(fields):
        value = _get_dotted(body, field)
        if isinstance(value, str) and rules.trim_strings:
            value = value.strip()
        if field in ci and isinstance(value, str):
            value = value.lower()
        payload[field] = value
    return json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"), default=str)


def body_digest(body: dict[str, Any], rules: NormalizationRules) -> str:
    return hashlib.sha256(canonical_body(body, rules).encode()).hexdigest()


def derive_key(key_hash: str, body: dict[str, Any], rules: NormalizationRules) -> IdempotencyKey:
    digest = body_digest(body, rules)
    if not rules.fields and not body:
        # 空体 + 未声明字段集：仅靠 key_hash（可复现，不会与别的请求撞车）
        return IdempotencyKey(f"{DERIVED_PREFIX}{key_hash}|empty", True, digest)
    return IdempotencyKey(f"{DERIVED_PREFIX}{key_hash}|{digest[:32]}", True, digest)


def resolve_key(
    *,
    explicit: str | None,
    key_hash: str,
    body: dict[str, Any],
    rules: NormalizationRules,
) -> IdempotencyKey:
    if explicit:
        return IdempotencyKey(explicit.strip(), False, body_digest(body, rules))
    return derive_key(key_hash, body, rules)


def window_bucket(now: datetime | None, window_seconds: int) -> int:
    """窗口分桶：``floor(now / 窗口秒数)``；(key, bucket) 唯一 = 窗口期内唯一。"""
    ts = (now or datetime.now(UTC)).timestamp()
    return int(ts // max(1, window_seconds))


def retry_key(base_key: str, attempt: int) -> str:
    """终态 failed 的任务级重试：``{key}#attempt{n}``（键不同 = 新业务尝试）。"""
    stripped = base_key.split("#attempt", 1)[0]
    return f"{stripped}#attempt{max(1, attempt)}"


def attempt_of(key: str) -> int:
    if "#attempt" in key:
        try:
            return int(key.rsplit("#attempt", 1)[1])
        except ValueError:  # pragma: no cover - 脏数据
            return 1
    return 1


def key_hash_of(secret_value: str) -> str:
    """凭证 → key_hash。**只做单向哈希**，绝不保存或打印原值。"""
    return hashlib.sha256(secret_value.encode()).hexdigest()[:24]
