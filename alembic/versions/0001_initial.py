"""初始 schema（§13）。

实现口径（有意为之，不是偷懒）：

* 建表用 ``Base.metadata.create_all``——**单一事实来源**是 ORM 模型，避免迁移文件
  与模型各写一份 DDL 然后慢慢漂移。代价：本迁移不是一份"冻结的 DDL 快照"，
  后续如需严格可审计的 DDL，请在此基线之上改用显式 ``op.create_table``。
* Postgres 专属项（月度分区、审计表权限撤销）**不放在这里**：
  分区改造脚本见 ``scripts/partition_async_task.sql``；审计表权限见本文件末尾
  的 ``REVOKE``（仅 PG 执行）。

Revision ID: 0001_initial
Revises:
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

from async_gateway.db.base import Base
from async_gateway.db import models  # noqa: F401  确保所有模型已注册

revision = "0001_initial"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    Base.metadata.create_all(bind=bind)

    if bind.dialect.name == "postgresql":
        # 审计表 append-only：撤销治理账号的 UPDATE/DELETE（§18.5）。
        # 迁移由 DBA 账号执行，应用账号是 async_gateway —— 这里显式收口。
        op.execute("REVOKE UPDATE, DELETE ON audit_event FROM PUBLIC")
        for role in ("async_gateway",):
            op.execute(
                sa.text(
                    "DO $$ BEGIN "
                    f"IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '{role}') THEN "
                    f"EXECUTE 'REVOKE UPDATE, DELETE ON audit_event FROM {role}'; "
                    f"EXECUTE 'GRANT SELECT, INSERT ON audit_event TO {role}'; "
                    "END IF; END $$;"
                )
            )
        # 审计序列不允许回绕复用（防"删一条再插一条"掩盖痕迹）
        op.execute("REVOKE USAGE ON SEQUENCE audit_event_id_seq FROM PUBLIC")


def downgrade() -> None:
    bind = op.get_bind()
    Base.metadata.drop_all(bind=bind)
