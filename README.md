# 异步网关（Async Gateway）

给任意「已任务化」的上游（创建任务 + 查询任务模型）套一层**通用排队异步网关**，上游零改动。
已接入的上游：火山方舟 **Seedance**（视频）、**Seed3D**（图生 3D）；同类上游的接入目标是
「**4 行配置 + 渠道持证**」。New API 侧用其**任务插件**渠道即可对接 ——
方舟形态插件**零改动复用**（渠道 base_url 指向网关），见下。

* 架构文档：[`docs/async-gateway-architecture-v3.md`](docs/async-gateway-architecture-v3.md)
* 落地说明（**含未验证项清单，评估前必读**）：[`docs/IMPLEMENTATION.md`](docs/IMPLEMENTATION.md)
* New API 侧接入（Task Plugin 渠道，含真机实测记录）：
  [`docs/newapi-task-plugin-integration.md`](docs/newapi-task-plugin-integration.md)

## 它承诺什么

| 语义 | 说明 |
|---|---|
| 提交 | `POST /async/{alias}/{上游 create_path}`，返回**上游原生形状**的受理响应（New API 依赖它解析上游 task id） |
| 查询 | `GET /async/{alias}/{上游 get 路径}`，快照优先 + 客户端带凭证时同步透传刷新 |
| 取消 | `DELETE /async/{alias}/{上游取消路径}`，上游不支持则降级为 `cancel_requested` |
| 回调 | `POST /callbacks/{opaque_token}`，HMAC 验签 + 时间窗 + 去重表 |
| 结果 | **不另立路径**：`passthrough`（默认）下查询响应直接给上游直链；`store` 下查询响应的结果字段就是对象存储预签名 URL（转存未完成/失败则留空，并由 envelope `degraded[]` 声明） |

三条它**不**做的事：不参与计费（只负责状态输出契约）、不做 OpenAI 同步协议兼容、
不承诺业务级 exactly-once（业务副作用幂等由上游或调用方保证）。

## 认证模型

**上游凭证随 Authorization 头透传**：New API 渠道持有真实上游 key，网关数据面不存 key、不落盘、
不进日志/审计。调用方须持有效上游凭证，任务隔离由上游按 key 天然保证
（`capabilities.per_key_isolation`）。

> 唯一需要外部裁决的语义缺口：worker 之后的轮询也需要这个 key，而"不落盘"与"另一个进程里调用上游"
> 字面上冲突。落地裁定见 `docs/IMPLEMENTATION.md` §3.2。

## 快速开始

```bash
make install        # pip install -e ".[dev]"
make test           # 199 项测试（SQLite + 内存 broker + MockTransport 假上游，无需 Redis）
make test-all       # 追加 Redis 真机用例（Lua 原子性 / Streams 消费组 / AIMD 直方图）

make gateway        # http://localhost:8000  （/docs 在非 prod 环境开放）
make admin          # http://localhost:8080  Task-admin（仅内网可达）
make worker scheduler inspector
make compose-up     # 起全部依赖与进程（需要 Docker）

make live-seed3d    # 真机跑一次图生 3D（需 ARK_API_KEY；会消耗上游额度）
```

最小请求示例（本地用内置 `echo` 模板）：

```bash
curl -X POST http://localhost:8000/async/echo/v1/tasks \
  -H 'Authorization: Bearer <上游 key>' \
  -H 'Content-Type: application/json' \
  -d '{"prompt":"hello"}'
```

## 目录

```
src/async_gateway/
  domain/       状态机（迁移白名单）、错误三级分类、取值域
  templates/    三级配置面 + 约定推导 + JSONPath 沙箱 + 校验器 + 版本/灰度 + 内置模板
  security/     SSRF、插值净化、凭证脱敏、回调验签
  db/           模型、条件更新 DAO、审计哈希链（append-only）
  gateway/      /async/ 协议面、幂等、状态输出契约、受理服务、URL 直配
  upstream/     上游客户端（凭证透传 / resolve-and-pin / 响应脱敏 / 结果拉取）
  infra/        Redis、两级并发槽与配额、自适应轮询、对象存储、凭证存放
  bus/ tasks/   Streams broker、六个 handler、派发
  workers/      worker / scheduler / inspector（故障域隔离）
  admin/        治理面（RBAC、职责分离、双人复核、重放、dry-run、审计）
scripts/
  live_seed3d.py      真机联调：经网关跑一次图生 3D（含 New API 插件同口径路径）
  smoke_workers.py    后台进程冒烟（scheduler/inspector/worker 各一轮，走真实 Redis Streams）
```

## 设计上的几个硬约束

1. **不猜成功**：响应超时/丢失/5xx → `submit_unknown`，只能由 compensate 按
   `confirm_strategy` 确认后驱动；`manual_only` 上游**永不自动重发创建**。
2. **状态输出契约**：真实、单调收敛不翻转、重复查询一致；网关内部终态
   （`timeout/dead/dead_awaiting_confirm`）按模板映射重写为**上游原生失败类取值**，
   否则 New API 永远读不到终态、退款不触发。
3. **所有写路径带前置条件**：条件更新（CAS）+ 迁移白名单双重校验，终态迁移只允许一次。
4. **治理不绕过安全**：Task-admin 的每次变更走同一个校验器；高危操作双人复核；审计 append-only
   且只存引用与哈希。
5. **模板即代码**：表达式限定 JSONPath 严格子集（禁通配/递归/过滤器/脚本），带步数、结果大小、
   求值超时上限；AI 产物不直写生产（校验 → 预览 → dry-run → 评审四道闸）。
6. **结果策略默认不转存**：`AG_RESULT_MODE_DEFAULT=passthrough` —— 结果字段直接用上游直链，
   规避尚未验证的对象存储链路；代价是链接依赖上游有效期（envelope `degraded[]` 会声明）。
   切到 `store` 转存时必须保证**数据面 IO 用原始 URL**：上游结果多为预签名地址，
   脱敏只作用于对外输出（见 `docs/IMPLEMENTATION.md` §3.11、§3.13）。
