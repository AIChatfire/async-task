"""治理面（Task-admin）：模板、灰度、重放、dry-run、审计。仅内网可达。

注意：这里**不**再导出 ``app`` 这个名字。包属性 ``app`` 会遮蔽 ``async_gateway.admin.app``
子模块（Python 的 ``import a.b.c as m`` 会优先取父包的属性），导致
``uvicorn async_gateway.admin.app:app`` 这类按模块路径加载的入口解析到错误对象。
"""

from .app import Actor, ApprovalStore, create_admin_app, router

__all__ = ["Actor", "ApprovalStore", "create_admin_app", "router"]
