"""表达式沙箱：JSONPath **严格子集**（§12.3 / §18.6）。

允许的语法（仅此而已）::

    $
    $.a.b
    $.a[0].b
    $['a']["b"][-1]

**显式禁止**：通配 ``*``、递归 ``..``、过滤器 ``[?(...)]``、脚本 ``[(...)]``、联合
``[a,b]``、切片 ``[1:2]``。任何非法字符直接报错，不做"尽力解析"——模板里出现这些
就等于把上游响应体当代码执行。

求值带三重上限：路径步数、结果字节数、求值截止时间。
"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from typing import Any, Final, Literal

MAX_PATH_STEPS: Final = 32
MAX_RESULT_BYTES: Final = 65536
DEFAULT_DEADLINE_SECONDS: Final = 0.05

_NAME_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_\-]*")
_INDEX_RE = re.compile(r"-?\d+")

#: 明令禁止的构造（出现即报错，且给出定位）
FORBIDDEN_TOKENS: Final[tuple[tuple[str, str], ...]] = (
    ("..", "递归下降"),
    ("*", "通配"),
    ("[?", "过滤器表达式"),
    ("[(", "脚本表达式"),
    ("@", "当前节点引用"),
    ("|", "联合/管道"),
)


class ExpressionError(ValueError):
    """表达式非法（沙箱拒绝）。"""


class PathNotFound(LookupError):
    """路径合法但当前文档中不存在。"""


class EvalBudgetExceeded(RuntimeError):
    """求值超出预算（步数/时间）。"""


@dataclass(frozen=True, slots=True)
class Step:
    kind: Literal["key", "index"]
    value: str | int


@dataclass(frozen=True, slots=True)
class JsonPath:
    source: str
    steps: tuple[Step, ...]

    def __str__(self) -> str:  # pragma: no cover - 展示用
        return self.source


def compile_expression(spec: str) -> JsonPath:
    """把模板里的表达式串编译成 :class:`JsonPath`；非法即抛 :class:`ExpressionError`。"""
    if not isinstance(spec, str) or not spec:
        raise ExpressionError("表达式必须是非空字符串")
    for token, why in FORBIDDEN_TOKENS:
        if token in spec:
            idx = spec.index(token)
            raise ExpressionError(f"表达式含被禁构造 {why!r}（位置 {idx}）：{spec!r}")
    if not spec.startswith("$"):
        raise ExpressionError(f"表达式必须以 $ 开头：{spec!r}")

    steps: list[Step] = []
    i = 1
    n = len(spec)
    while i < n:
        ch = spec[i]
        if ch == ".":
            i += 1
            m = _NAME_RE.match(spec, i)
            if not m:
                raise ExpressionError(f"'.' 后缺少合法键名（位置 {i}）：{spec!r}")
            steps.append(Step("key", m.group(0)))
            i = m.end()
        elif ch == "[":
            end = spec.find("]", i)
            if end < 0:
                raise ExpressionError(f"未闭合的 '['（位置 {i}）：{spec!r}")
            inner = spec[i + 1 : end].strip()
            if len(inner) >= 2 and inner[0] == inner[-1] and inner[0] in ("'", '"'):
                steps.append(Step("key", inner[1:-1]))
            else:
                if not _INDEX_RE.fullmatch(inner):
                    raise ExpressionError(f"下标必须是整数或带引号的键（位置 {i}）：{spec!r}")
                steps.append(Step("index", int(inner)))
            i = end + 1
        else:
            raise ExpressionError(f"非法字符 {ch!r}（位置 {i}）：{spec!r}")

    if len(steps) > MAX_PATH_STEPS:
        raise ExpressionError(f"路径步数 {len(steps)} 超过上限 {MAX_PATH_STEPS}")
    return JsonPath(source=spec, steps=tuple(steps))


def extract(
    doc: Any,
    spec: str | JsonPath,
    *,
    max_result_bytes: int = MAX_RESULT_BYTES,
    deadline_seconds: float = DEFAULT_DEADLINE_SECONDS,
) -> Any:
    """按表达式从上游响应中提取取值。"""
    path = compile_expression(spec) if isinstance(spec, str) else spec
    deadline = time.monotonic() + deadline_seconds
    node = doc
    for step in path.steps:
        if time.monotonic() > deadline:
            raise EvalBudgetExceeded(f"求值超时（>{deadline_seconds}s）：{path.source}")
        if step.kind == "key":
            if not isinstance(node, dict) or step.value not in node:
                raise PathNotFound(f"{path.source}: 缺少键 {step.value!r}")
            node = node[step.value]
        else:
            idx = int(step.value)
            if not isinstance(node, (list, tuple)):
                raise PathNotFound(f"{path.source}: 期望数组，实为 {type(node).__name__}")
            if not (-len(node) <= idx < len(node)):
                raise PathNotFound(f"{path.source}: 下标 {idx} 越界（长度 {len(node)}）")
            node = node[idx]
    try:
        size = len(json.dumps(node, ensure_ascii=False, default=str).encode())
    except (TypeError, ValueError):
        size = 0
    if size > max_result_bytes:
        raise EvalBudgetExceeded(f"结果 {size}B 超过上限 {max_result_bytes}B：{path.source}")
    return node


def try_extract(doc: Any, spec: str | JsonPath, **kwargs: Any) -> tuple[bool, Any]:
    """软失败版本：返回 ``(是否命中, 取值)``。"""
    try:
        return True, extract(doc, spec, **kwargs)
    except (PathNotFound, EvalBudgetExceeded):
        return False, None


def is_valid_expression(spec: str) -> bool:
    try:
        compile_expression(spec)
    except ExpressionError:
        return False
    return True
