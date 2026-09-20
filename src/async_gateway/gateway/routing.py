"""协议面路由匹配（§11：固定前缀，第二段 alias，剩余原样转发）。

New API 在 base_url 模式下把网关当上游：``base_url=https://gw/async/{alias}``，
于是它会把**上游原生路径**（``create_path`` / ``get_path_template`` 渲染后）直接拼在后面。
网关因此不能靠固定端点表路由，而要把"剩余路径"拿去和模板对照：

* 与 ``create_path`` 相同 → 受理
* 与 ``get_path_template`` 同一形状（``{id}`` 位可捕获）→ 查询
* 与 ``cancel_path_template`` 同一形状 → 取消
* 其余 → 404（并区分于"未授权"，见 §18.5 同返 404 的策略）
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal

from ..templates.schema import ResolvedTemplate

RouteKind = Literal["create", "query", "cancel"]


@dataclass(frozen=True, slots=True)
class RouteMatch:
    kind: RouteKind
    upstream_id: str | None = None


def _normalize(path: str) -> str:
    path = path.split("?", 1)[0].strip()
    if not path.startswith("/"):
        path = "/" + path
    return path.rstrip("/") or "/"


def _pattern(template_path: str) -> re.Pattern[str]:
    body = re.escape(_normalize(template_path)).replace(re.escape("{id}"), "([^/]+)")
    return re.compile(rf"^{body}/?$")


def match_route(template: ResolvedTemplate, raw_path: str, method: str) -> RouteMatch | None:
    """按模板把剩余路径归位到 create / query / cancel。"""
    path = _normalize(raw_path)
    method = method.upper()

    if method == template.create_method.upper() and path == _normalize(template.create_path):
        return RouteMatch("create")

    if method == template.cancel_method.upper():
        # **不**看 capabilities.cancel：协议面必须接受"取消"这个语义，
        # 上游不支持时由下游降级为 cancel_requested（§7 核心用例），
        # 而不是让调用方收到 404 —— 那等于对外承诺里少了"取消"这一条。
        matched = _pattern(template.cancel_path_template).match(path)
        if matched:
            return RouteMatch("cancel", matched.group(1))

    if method == template.get_method.upper():
        matched = _pattern(template.get_path_template).match(path)
        if matched:
            return RouteMatch("query", matched.group(1))

    # 无模板时按约定兜底：GET 且末段不是 create_path 的末段 → 视为查询
    return None


def looks_like_absolute_url(value: str) -> bool:
    return bool(re.match(r"^[a-zA-Z][a-zA-Z0-9+.-]*://", value))
