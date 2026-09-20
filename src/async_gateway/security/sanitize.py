"""插值变量净化（§18.1）。

``{id}`` 这类变量取自**上游响应体**，属不可信输入；直接拼进 URL 会造成路径穿越
（``../../``）或 scheme 覆盖（``http://evil``）。这里只允许 RFC 3986 unreserved +
少数安全字符，其余一律拒绝；拼接完成后再由 SSRF 校验兜一道。
"""

from __future__ import annotations

import re

_SAFE = re.compile(r"\A[A-Za-z0-9._~\-]{1,256}\Z")
#: 即使被引号/编码包裹也要拒绝的形态
_DANGEROUS = re.compile(r"[/\\:%?#@\s]|\.\.", re.IGNORECASE)


class UnsafeInterpolationValue(ValueError):
    """插值变量含危险字符。"""


def sanitize_interpolation_value(value: object, *, field: str = "id") -> str:
    """校验并返回可安全用于 URL 路径/查询的插值取值。"""
    if isinstance(value, bool) or value is None:
        raise UnsafeInterpolationValue(f"插值变量 {field} 取值非法：{value!r}")
    raw = str(value)
    if not raw:
        raise UnsafeInterpolationValue(f"插值变量 {field} 为空")
    if _DANGEROUS.search(raw):
        # 单独报出方案前缀，便于定位"上游回了个完整 URL"
        raise UnsafeInterpolationValue(f"插值变量 {field} 含路径穿越/方案字符，已拒绝：{raw!r}")
    if not _SAFE.match(raw):
        raise UnsafeInterpolationValue(f"插值变量 {field} 含非白名单字符，已拒绝：{raw!r}")
    return raw


def is_safe_interpolation_value(value: object) -> bool:
    try:
        sanitize_interpolation_value(value)
    except UnsafeInterpolationValue:
        return False
    return True
