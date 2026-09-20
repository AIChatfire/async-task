-- 月度分区与归档（§13 / §4.4 D6）
--
-- ⚠️ 本脚本在当前开发环境**未执行过**：本机没有 Postgres，只有 SQLite（用于测试）。
--    它是按文档要求产出的运维工件，必须在 M3 的 PG 实例上**先演练再执行**，
--    并把演练结果（耗时、锁行为、是否触发长事务）补记到 docs/IMPLEMENTATION.md。
--
-- 目标形态：async_task 按 created_at 月度 RANGE 分区；归档 = DETACH PARTITION 后
-- 转为同库 archive schema 的独立表（**零 DELETE**，避免 vacuum 压力与索引膨胀）。

-- ---------------------------------------------------------------------------
-- 1) 一次性改造：普通表 → 分区表（选择一个维护窗口执行）
-- ---------------------------------------------------------------------------
BEGIN;

ALTER TABLE async_task RENAME TO async_task_legacy;

-- 分区父表：结构与原表一致（含所有索引定义挂在父表上，子分区自动继承）
CREATE TABLE async_task (
    LIKE async_task_legacy INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE
) PARTITION BY RANGE (created_at);

-- 唯一约束必须包含分区键，因此幂等唯一索引改为 (idempotency_key, idempotency_bucket, created_at)
-- ⚠️ 语义变化：窗口期内唯一仍成立（bucket 决定窗口），但索引不再能跨分区做全局唯一校验。
--    因此"同键同体 → 重放"的判定必须在应用层用 SELECT 兜住（受理路径已经是这么做的）。
CREATE UNIQUE INDEX uq_task_idem_window ON async_task (idempotency_key, idempotency_bucket, created_at);
CREATE INDEX ix_task_tenant_channel_status ON async_task (tenant, channel, status);
CREATE INDEX ix_task_status_next_poll ON async_task (status, next_poll_at);
CREATE INDEX ix_task_upstream_task_id ON async_task (upstream_task_id);
CREATE INDEX ix_task_origin ON async_task (origin);

-- 建最近 3 个月 + 未来 2 个月分区，并灌入历史数据
DO $$
DECLARE
    m date;
    start_m date;
BEGIN
    FOR i IN -3..2 LOOP
        start_m := date_trunc('month', now())::date + (i || ' month')::interval;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS async_task_%s PARTITION OF async_task FOR VALUES FROM (%L) TO (%L)',
            to_char(start_m, 'YYYYMM'), start_m, start_m + interval '1 month'
        );
    END LOOP;
END $$;

INSERT INTO async_task SELECT * FROM async_task_legacy;
DROP TABLE async_task_legacy;

COMMIT;

-- ---------------------------------------------------------------------------
-- 2) 每月预建下个月分区（建议由定时任务执行；否则插入会因无匹配分区而失败）
-- ---------------------------------------------------------------------------
-- CREATE TABLE IF NOT EXISTS async_task_YYYYMM PARTITION OF async_task
--     FOR VALUES FROM ('YYYY-MM-01') TO ('YYYY-MM-01' + interval '1 month');

-- ---------------------------------------------------------------------------
-- 3) 归档：DETACH → 搬进 archive schema（不做跨库 dump）
--    前置条件：目标分区内**没有非终态行**（§13）
-- ---------------------------------------------------------------------------
BEGIN;

CREATE SCHEMA IF NOT EXISTS archive;

-- 前置检查（应返回 0 行；返回非 0 说明该分区还有活跃任务，本周期不归档）
SELECT count(*) AS active_rows
FROM async_task PARTITION_KEYS_ONLY
WHERE 1 = 0;  -- 占位：实际执行时替换为 ... FROM async_task_YYYYMM
              -- WHERE status NOT IN ('succeeded','cancelled','timeout','dead','dead_awaiting_confirm')

-- ALTER TABLE async_task DETACH PARTITION async_task_YYYYMM;   -- 零 DELETE
-- ALTER TABLE async_task_YYYYMM SET SCHEMA archive;
-- -- 到期/删除时可直接 DELETE（archive schema 不在热表 vacuum 敏感路径上）
-- DELETE FROM archive.async_task_YYYYMM WHERE created_at < now() - interval '90 days';

COMMIT;

-- ---------------------------------------------------------------------------
-- 4) 留存期查询路由约定
--    - 热表未命中 → 查 archive schema（同名表）
--    - 到期/删除后查询返回 410（由应用层 result_ref 状态决定）
-- ---------------------------------------------------------------------------
