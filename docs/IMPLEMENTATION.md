# 落地说明（架构文档 → 代码）

> 对应架构文档：`docs/async-gateway-architecture-v3.md`（v3.0）
> 代码根目录：`src/async_gateway/`
> 测试：`tests/`（184 项，全绿；其中 5 项为 Redis 真机用例）

本文只讲三件事：**文档的哪一条落在哪个文件**、**哪些地方我做了裁定（以及为什么）**、
**哪些还没验证**。第三部分请务必读完再评估可用性。

---

## 1. 怎么跑

```bash
# 依赖（已在本机建好：/Users/betterme/.workbuddy/binaries/python/envs/async-gateway）
make install

make test          # 默认测试（SQLite + 内存 broker + 假上游，不需要 Redis）
make test-all      # 含 Redis 真机用例（Lua 原子性 / Streams 消费组 / AIML 直方图）

make gateway       # 起 gateway-api（:8000）
make admin         # 起 Task-admin（:8080，仅内网）
make worker        # worker（--pools heavy-poll,light-poll）
make scheduler     # 单副本调度器
make inspector     # 单副本巡检面
make compose-up    # 全部依赖 + 进程（需要 Docker，本机没有）
```

本地最小闭环不依赖任何外部服务：默认 `AG_RESULT_STORE_MODE=memory`、
`AG_QUEUE_DRIVER=memory`（测试环境）、假上游用 `httpx.MockTransport` 注入。

---

## 2. 文档 → 代码映射

| 文档章节 | 落点 | 验证 |
|---|---|---|
| §3 三层能力（接入/模板/治理） | `gateway/` · `templates/` · `admin/` | 三套 e2e 测试 |
| §4.1 幂等与重试分级 | `gateway/idempotency.py` · `domain/errors.py` | `test_idempotency*` / `test_domain.py` |
| §4.1 提交意图标记 | `db/dao.py:mark_submit_intent` · `tasks/handlers.py` | `test_handlers.py`（崩溃窗口两子项） |
| §4.2 状态输出契约 | `gateway/output.py` | `test_api_e2e.py::test_internal_terminal_is_rewritten...` |
| §4.3 背压（两级槽 + 配额分离 + 校准只纠泄漏） | `infra/concurrency.py`（Lua） | `test_infra.py`（内存版 + Redis 版各一套） |
| §4.4 unknown 有界化 | `workers/inspector.py:bound_unknowns` | `test_handlers.py` |
| §5 接入新上游 6 步 | `admin/app.py`（校验→预览→dry-run→审批→灰度） | `test_admin.py` |
| §7 业务用例（受理/查询/取消/回调/模板/异常治理/灰度） | `gateway/routes.py` · `workers/` · `admin/` | 全部覆盖 |
| §8.1/§8.2 生命周期与异常路径 | `domain/state_machine.py` · `tasks/handlers.py` | `test_domain.py` / `test_handlers.py` |
| §8.3 模板变更流程 | `templates/registry.py:classify_change` | `test_templates.py` |
| §9 业务实体与审计事件清单 | `db/models.py` · `db/audit.py` | `test_handlers.py::test_audit_*` |
| §10 进程划分 | `workers/{worker,scheduler,inspector}.py` · `tasks/queues.py` | 进程级入口 + 池化队列 |
| §11 `/async/` 协议面 | `gateway/routes.py` · `gateway/routing.py` | `test_api_e2e.py` |
| §11 归属校验（统一 404） | `gateway/container.py:assert_owned` | `test_ownership_mismatch_is_404` |
| §11 URL 直配过渡通道 | `gateway/direct.py`（独立指标 + 开关 + 前缀白名单） | `test_url_direct_*` |
| §12.1 三级配置面 | `templates/schema.py:template_tier` | `test_templates.py::test_tier_detection` |
| §12.2 约定推导 + capabilities schema | `templates/derive.py` · `templates/schema.py` | `test_derivation_defaults_and_provenance` |
| §12.2 查询面（快照优先 / 终态读 result_ref / 原生重写） | `gateway/routes.py:_handle_query` · `gateway/output.py` | `test_query_*` |
| §12.3 校验器 + 预览 + 热更新限定 + 就地勘误 | `templates/validator.py` · `templates/registry.py` | `test_templates.py` / `test_admin.py` |
| §12.4 AI 生成流水线（四道闸） | `admin/app.py`（AI 产物入闸：校验/预览/dry-run/审批） | 见 §4 待确认 |
| §12.5 Seedance 极简模板 + 平台差异补丁 | `templates/builtin/*.yaml` | `test_platform_patch_applies_to_seedance` |
| §13 数据模型与写路径前置条件 | `db/models.py` · `db/dao.py` | `test_domain.py::test_write_path_table_*` |
| §14 状态机与终态仲裁 | `domain/state_machine.py` · `gateway/routes.py:_apply_callback` | `test_domain.py` / `test_callback_arbitration_*` |
| §15 Taskiq/队列与自适应轮询 | `bus/` · `infra/polling.py` | `test_infra.py` |
| §16 New API 集成（状态输出契约） | `gateway/output.py` · `gateway/accept.py` | `test_create_returns_upstream_native_shape` |
| §17 可观测（Logfire / 脱敏 / SLI） | `observability/` · `security/redaction.py` | `test_security.py` |
| §18.1 SSRF（含结果拉取 + resolve-and-pin） | `security/ssrf.py` · `upstream/client.py:pin_request` | `test_security.py` |
| §18.2 凭证分层 | `infra/credentials.py` · `gateway/container.py` | `test_upstream_credential_is_passed_through` |
| §18.3 回调安全 | `security/callback_auth.py` · `gateway/routes.py:receive_callback` | `test_callback_*` |
| §18.4 Task-admin 治理 | `admin/app.py` | `test_admin.py` |
| §18.5 审计防篡改 | `db/audit.py`（哈希链 + 只存引用） | `test_audit_chain_detects_tampering` |
| §18.6/§18.7/§18.8/§18.9 模板沙箱/出口/合规/多租户 | `templates/expression.py` · `infra/object_store.py` · tenant 维度 | `test_expression*` |
| §19 部署 | `docker-compose.yml` · `Dockerfile`(待补) · `alembic/` | 见 §4 |
| §20/§21 里程碑与验收 | 见下方 §5 逐条对照 | — |

---

## 3. 落地的关键裁定（与文档有出入的地方，都在这里）

### 3.1 受理时**同步**完成上游创建（`AG_SUBMIT_MODE=inline`，默认）

文档同时要求两件互相拉扯的事：

* §7「任务受理 → 返回**受理响应（上游原生形状）**」、§16「响应形状保真：面向 New API 的 create
  响应必须是上游原生形状（否则 New API 的上游 adaptor 解析不出上游 task id）」；
* §10「worker 在另一个进程里调用上游」。

若 create 由 worker 异步完成，受理请求就无法返回含上游 id 的原生响应 —— New API 侧会直接判失败。
因此默认 `inline`：**受理请求内同步 create**，凭证不出请求、响应天然是上游原生形状。
`AG_SUBMIT_MODE=queued` 保留异步路径（受理延迟优先场景），代价是需要 §3.2 的凭证存放。

### 3.2 数据面凭证的短生命周期存放（`AG_CREDENTIAL_CHANNEL`）

文档 §18.2 要求"网关不存 key、不落盘"，但 worker 之后的轮询必须用这个 key。二者字面上冲突。
落地裁定：

* 凭证进 **Redis**（内存语义），键 `cred:{task_id}`，**TTL ≤ 任务 deadline + 5min**，
  不写 Postgres、不落磁盘、不进日志；任务进终态立即删除；
* `AG_CREDENTIAL_CHANNEL=none` 提供"绝对不驻留"形态——此时只有客户端请求能驱动透传
  （查询面会在客户端带凭证时同步刷新）；
* **inline 模式下提交阶段完全不依赖它**；只有后台轮询/补偿/转存用得到。

这一条建议在评审上明确确认，它是文档里唯一一处需要外部裁决的语义缺口。

### 3.3 状态机白名单的 5 处补全（每处都有原因）

文档 §14 的"迁移白名单显式"少了几个必需出口，直接照抄会让任务卡死。补全并写进代码注释：

| 补充边 | 为什么必需 |
|---|---|
| `accepted → dead` | 提交阶段**确定性**失败（4xx 参数错误 / SSRF 拒绝 / 凭证已失效）原表没有出口 |
| `accepted → timeout` | 提交阶段就超期（deadline 巡检覆盖 accepted） |
| `submit_unknown → accepted` | compensate「确认未创建 → 重新发起创建」需要一个能重新抢占提交意图的落点 |
| `poll_unrecognized → {succeeded, failed}` | 模板勘误生效后，恢复的轮询直接读到终态 |
| `in_progress → submit_unknown` | §14 的"活动态停滞 → 疑似孤儿转 submit_unknown 由 compensate 归位"原表不接受该边 |
| `dead_awaiting_confirm → dead` | §14「人工确认后重放**或关闭**」的"关闭"分支；failure→failure 收敛，不构成成功/失败翻转 |

另外：`db/dao.py:advance()` 在**显式传入 expected 时也会逐个校验白名单**——否则"条件更新"
会成为绕过白名单的后门（只要状态对得上就能跳任意一步）。这个校验在开发期抓到了一个真 bug。

### 3.4 `failed` 不是终态；终态重写集合是 `{timeout, dead, dead_awaiting_confirm}`

按 §14 的图，`failed` 是"失败待重试"的调度态（出边到 `upstream_submitted` / `dead`），
因此它**不在** §12.2 的"内部终态对外重写"集合里，也不会以终态形态泄露给 New API。
§4.2 里"failed → 退款路径"指的是**上游原生返回 failed** 的情况，由查询面透传原生取值触发。

### 3.5 轮询：只有"提取不到状态字段"才归 `poll_unrecognized`

§12.2 说"提取不到判定的响应归 poll_unrecognized"。落地时严格区分两件事：

* 状态字段**取得到**但不是成功/失败/过期 → 那就是上游的**进行中**状态（queued/running/…）：
  记快照 + 按自适应间隔继续轮询，**不**归 unknown；
* 状态字段**取不到**（响应形状变了、包裹层变了）→ 才归 `poll_unrecognized`。

否则任何一次正常轮询都会被误标成"不可判定"，直接把 unknown 指标打爆。

### 3.6 `UTCDateTime` 类型装饰器

SQLite 不支持带时区时间戳，读回来是 naive，于是所有"是否到期/停滞/存活多久"的判断都会
`TypeError: can't subtract offset-naive and offset-aware`；Postgres 又是 aware。
在 `db/models.py` 加了一层类型装饰器把**读语义**拉齐，业务代码只写一种时间比较。

### 3.7 重放 = **新任务行**（`origin=replay`），原任务原地不动

§13 的 `attempts` 口径要求"含 failed 重试重建"在**同一行**累加，而 §14 又要求终态单调不翻转。
两者的调和：重试留在同一行（`failed → upstream_submitted`，attempts+1），**重放**（死信人工确认后）
派生新行，派生键 `{key}#attempt{n}`、`replay_of` 指向父任务。这样唯一约束与单调性都不破。

重放还有两条硬约束（§18.4）：已有 `upstream_task_id` 直接拒绝（**禁止重放创建类动作**）；
会触发真实上游创建 → 必须随请求提供数据面凭证（网关不持久化）。

### 3.8 URL 直配 = 渠道策略内联的极简模板

§11 的"约定型直配兜底"落地为：渠道策略里内联一份极简配置（`base_url/create_path/result_location`），
走**同一个**校验器与同一条流水线，但登记独立指标 `ag_url_direct_total`、按渠道开关、
`AG_URL_DIRECT_CONFIG_ENABLED=false` 一键全局关闭、path 前缀白名单约束漫游。

### 3.9 队列：Streams 为默认实现，Taskiq 为可选运行时

文档写"Taskiq 队列（Redis Streams）"。为了不让核心链路绑在框架 API 版本上，默认用
`StreamBus` 直接实现 XADD/XREADGROUP/XAUTOCLAIM/DLQ 语义（消费组、ACK、pending 重投行为完全可预测，
也便于故障注入测试）；`AG_QUEUE_DRIVER=taskiq` 时投递走 Taskiq broker、消费交给
`taskiq worker async_gateway.bus.taskiq_adapter:broker`，**同一套 handler**（`dispatch_message`）。

### 3.10 测试与冒烟抓到的真实缺陷（都已修）

开发过程中测试与冒烟脚本抓出的问题，列在这里是因为它们比"功能清单"更能说明可信度：

| 缺陷 | 后果 | 发现方式 |
|---|---|---|
| `AG_APP_ENV=dev` 被判为"测试环境" → broker 静默退化成**进程内**内存队列 | 网关投的消息 worker 永远收不到，且不报任何错；docker-compose 正是 dev 环境 | `scripts/smoke_workers.py` 冒烟（深度恒为 0） |
| 轮询时把"取得到但非终态"的状态误判为 `poll_unrecognized` | 每次正常轮询都把 unknown 指标打爆，退避变 30s + 触发人工收敛 | `test_poll_unrecognized_status_value` |
| 后台提交成功后不回写 create 响应存档 | 幂等重放会把**上一次的失败响应**一直返回给调用方 | 自行复查受理链路时发现 |
| `advance()` 显式传 expected 时绕过白名单 | 条件更新变成"跳任意状态"的后门 | 加了白名单校验后立刻被 `FAILED→FAILED` 用例抓到 |
| 审批票据缺失时静默放行（`consume(None)` 返回 None） | 高危操作只需不带头就绕过双人复核 | `test_replay_requires_approval_ticket` |
| `backoff` 的 jitter 冲过 cap | "上限 60s"形同虚设 | `test_backoff_is_bounded_and_jittered` |
| SQLite naive 时间戳 | 所有"到期/停滞/存活期"判断直接 TypeError | 端到端用例 |
| 取消路径匹配依赖 `capabilities.cancel` | 上游不支持取消时 DELETE 返回 404，等于对外承诺里少了"取消" | `test_cancel_degrades_when_upstream_cannot_cancel` |
| 包属性 `app` 遮蔽 `admin.app` 子模块 | `uvicorn async_gateway.admin.app:app` 这类按模块路径加载的入口会解析到错误对象 | 复查 `admin/__init__.py` |

冒烟脚本：`scripts/smoke_workers.py`（起 scheduler 2 tick → inspector 1 tick → worker 消费并 ACK 一条消息，
走真实 Redis Streams 消费组）。实测输出：`SMOKE OK`。

---

## 4. 未验证 / 待确认（评估可用性前必读）

### 4.1 本环境没有的依赖（因此没验）

| 项 | 状态 |
|---|---|
| **Postgres** | 本机没有。全部测试跑 SQLite。`alembic/versions/0001_initial.py` 未执行过；`scripts/partition_async_task.sql`（月度分区 + DETACH 归档）**未执行**，是按文档产出的运维工件，必须先在 PG 上演练 |
| **MinIO** | `MinioResultStore` 未连真机（测试用 `MemoryResultStore`，接口一致）。预签名 URL、SSE-S3 桶加密均未实测 |
| **Docker / K8s** | 本机无 docker CLI，`docker compose up` 未跑过；`Dockerfile` 尚未补（compose 已引用）。HPA 复合指标、探针、单副本约束都只是配置声明 |
| **Logfire** | 未配 token；`logfire.configure` 分支未执行。指标走内置注册表 + `/metrics`（Prometheus 文本格式），未接真实采集 |
| **New API 侧** | 完全没有联调。§16 的"状态输出契约"只在网关侧自证（原生形状、内部终态重写、结果字段重写都已测），**M3 出口要求的"New API adaptor 对转存永久失败形状不误判为 failed"未验证** |

### 4.2 实现上的已知缺口

1. **`confirm_strategy=list_and_match` 未实现**：`_confirm()` 里返回"inconclusive"（会一直退避），
   只实现了 `query_by_client_key` 与 `manual_only`。需要上游 list 端点的字段映射，属 M3 剩余项。
2. **PG → Streams 显式重建程序未写**：当前靠"真相在 PG + scheduler 按 `next_poll_at` 派发"天然覆盖
   "Redis 丢消息"场景；文档要求的"批量 XADD + 幂等键防重"与**季度演练**未做（M3 注入清单第 1 项）。
3. **审批票据是进程内实现**（`admin/app.py:ApprovalStore`）：单副本内网服务可接受，重启后票据失效
   （需重新审批，属安全的一侧）。生产建议落库并纳入审计链。
4. **AI 生成流水线只有"入闸"部分**：`POST /admin/templates/preview` 覆盖校验/展开/渲染样例/diff，
   dry-run 与审批齐备；**没有**调用 LLM 从文档生成初稿（M6 内容），也没有文档注入防护的实测
   （只在 `templates/validator.py` 里对 AI 产出的 URL/表达式做机器校验）。
5. **回放/轮换 runbook 未演练**：`opaque_token` 泄露处置、`kid` 双密钥轮换代码已实现（`rotate()` + 测试），
   但 runbook 的操作步骤未在真实上游上走过。
6. **对账任务未上线**：§16「每日比对网关终态任务集 vs New API tasks/消费日志」没有实现，
   只有审计链与指标。
7. **容量数字未压测**：§17 的容量目标表（受理 ≥200 QPS、poll ≥2000 QPS、转存 250MB/s、
   scheduler ≥5000 msg/s）**一个都没实测**，M3 基线压测未做。
8. **RTO ≤ 15min 未验证**：PG→Streams 重建未演练，RTO 目标值无实测支撑。
9. **`dry_run` 的"计费与配额统计显式排除"** 只做到 `origin=dry_run` 标记 + 审计；
   accept 配额仍然消耗（同渠道共享计数器），生产需按 origin 分桶。

### 4.3 一处测试环境特有的偏差

`AG_MIN_REFRESH_INTERVAL=0` 等测试用配置让"快照新鲜度"判断总是成立，
因此查询面每次都做同步透传刷新。生产默认是 3–5s 窗口（快照优先），这条路径的
"快照优先 + retry_after 引导降频"只在 `response_headers` 的单测层面被覆盖，未做高并发验证。

---

## 5. 验收标准逐条对照（§21）

| # | 验收项 | 状态 |
|---|---|---|
| 1 | 新约定型上游 = 4 行模板 + 渠道持证 + dry-run，零代码 | ✅ 代码零改动；`test_templates.py` + `test_admin.py::test_dry_run_*` |
| 2 | 同幂等键同 attempt 只产生一个上游任务 | ✅ `test_derived_idempotency_key_makes_resubmit_a_replay` |
| 3 | 重启不丢任务；accepted 悬挂按提交意图分流；创建超时进 unknown 且补偿收敛；状态输出单调不翻转 | ✅ 分流两子项 + 单调性 + 重复查询一致均已测；**重启不丢**只覆盖到"Redis 丢消息 → scheduler 重投"这一层 |
| 4 | store 模式留存期内结果可回读；到期/删除 410 | ⚠️ 可回读 + 410 已测（内存存储）；**留存到期清理任务未实现**（无定时任务） |
| 5 | 单 task_id 全链路可查；核心指标/SLI/告警齐备 | ⚠️ 指标注册表 + `/metrics` 有；**Logfire 链路与告警规则未接** |
| 6 | Task-admin SSO+RBAC+职责分离；全操作审计 append-only；无绕过校验器的通道 | ✅ 除 SSO 真实对接（当前是 OIDC 中间件注入头的契约） |
| 7 | capabilities 全字段声明 + degraded[] 显式降级 | ✅ `test_envelope_only_when_requested` |
| 8 | M3 基线；N 倍容量下核心 SLO 达标 | ❌ 未压测 |
| 9 | 归属校验开启时统一 404 | ✅ `test_ownership_mismatch_is_404` |
| 10 | URL 直配可一键全局关闭 | ✅ `test_url_direct_kill_switch` |

---

## 6. 建议的下一步（按风险排序）

1. **起一台真 PG + MinIO**，跑 `alembic upgrade head`，演练分区脚本，补 M3 故障注入清单（7 项）。
2. **New API 侧联调**（M3 出口）：重点是 §4.2 的"转存永久失败报 succeeded"形状与内部终态重写形状。
3. **M3 性能基线压测**：单 scheduler 上限、Streams 吞吐、热表 UPDATE 速率（当前 §17 全是目标值）。
4. 补 `Dockerfile` 与探针/HPA 清单，跑通 `docker compose up`。
5. 补 `list_and_match`、每日对账任务、留存到期清理任务。
6. 补 AI 生成流水线（LLM 调用 + 文档注入防护实测）。

---

## 7. 目录速览

```
src/async_gateway/
  config.py                 策略组默认值（渠道/模板可覆盖）
  domain/                   状态机、错误三级分类、取值域（无 IO）
  templates/                三级配置面、约定推导、JSONPath 沙箱、校验器、版本/灰度、渲染、内置模板
  security/                 SSRF、插值净化、脱敏、回调验签
  db/                       模型、条件更新 DAO、审计哈希链
  gateway/                  /async/ 协议面、幂等、输出契约、策略解析、直配、受理服务
  upstream/                 上游客户端（透传/resolve-and-pin/脱敏）
  infra/                    Redis、并发与配额、自适应轮询、对象存储、凭证存放
  bus/                      Streams broker / 内存 broker / Taskiq 适配
  tasks/                    队列命名、六个 handler、派发
  workers/                  worker / scheduler / inspector
  admin/                    治理面（RBAC、双人复核、模板、重放、dry-run、审计）
  observability/            日志（强制脱敏）与指标
alembic/                    迁移（0001_initial 用 metadata 建表 + PG 审计权限收口）
scripts/partition_async_task.sql   月度分区与 DETACH 归档（未执行，需演练）
```
