"""DAO 生成 SQL 的**方言可移植性**回归（不连库，纯编译期断言）。

背景（F1，2026-09-20 真机实测）：`func.max(attempts - 1, 0)` 在 SQLite 能跑（双参 `max` 是
**标量**函数）、在 PostgreSQL 直接
`asyncpg.exceptions.UndefinedFunctionError: function max(integer, integer) does not exist`
⇒ 单测全绿、真库上凡 `consumes_attempt=False` 的退款路径（上游 429 / 401 / 403）全部 500。

这类缺陷靠跑 SQLite 套件**永远测不出来**，必须把"编译出来的 SQL"本身钉住：
本文件对 `dao._decremented_attempts()` 按 **postgresql / sqlite 两个方言**编译，
断言是 `CASE` 而不是聚合形态的 `max(...)`。
（`release_submit_intent` 的**语义**——递减不为负——由 SQLite 上的 handler 测试覆盖；
这里只守方言这一层。）
"""

from __future__ import annotations

import re

from sqlalchemy.dialects import postgresql, sqlite

import async_gateway.db.dao as dao


def _compile_pg(expr) -> str:
    # literal_binds：把常量直接渲染进 SQL（否则 0 会变成绑定参数，断言不到"下限是常量 0"）
    return str(expr.compile(dialect=postgresql.dialect(),
                            compile_kwargs={"literal_binds": True}))


def test_decremented_attempts_is_case_not_max_on_postgres():
    """🔴 F1 回归：PG 方言下不得出现聚合形态的 `max(a, b)`。"""
    pg = _compile_pg(dao._decremented_attempts())
    assert "case" in pg.lower(), f"应使用 CASE（跨方言可移植），实际：{pg}"
    assert not re.search(r"\bmax\s*\(", pg.lower()), \
        f"PG 上 max(a,b) 是聚合签名 ⇒ UndefinedFunctionError，实际：{pg}"


def test_decremented_attempts_is_case_on_sqlite_too():
    """同一表达式在 SQLite 下也应是 CASE —— 一处实现、两方言一致，别分叉。"""
    sq = str(dao._decremented_attempts().compile(dialect=sqlite.dialect()))
    assert "case" in sq.lower(), sq
    assert not re.search(r"\bmax\s*\(", sq.lower()), sq


def test_decremented_attempts_semantics_lower_bound_is_zero():
    """语义 = ``max(attempts - 1, 0)``：ELSE 分支必须是常量 0（attempts=0/1 归零）。"""
    pg = _compile_pg(dao._decremented_attempts())
    assert re.search(r"else\s+0(?!\d)", pg.lower()), f"下限不是常量 0：{pg}"
    # WHEN 分支确实是 attempts - 1（而不是别的列）
    assert "attempts - 1" in pg or "attempts-1" in pg, pg
