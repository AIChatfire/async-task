# 异步网关方案（解决方案 + 业务架构 + 技术架构）

> 版本：v3.0
> 变更：落实评审报告 v1.3 全部优化项——可靠性状态机细化（unknown 拆两态、竞态补全、写路径前置条件）、计费转为状态输出契约（网关不参与计费）、凭证透传分层（数据面 header 透传 / 控制面 vault）、安全治理补全（SSRF 运行时校验、数据合规、Task-admin RBAC、审计防篡改）、容量与 SLO 补齐
> 范围：通用排队异步网关；仅覆盖异步任务，不涉及 OpenAI 同步协议兼容

---

# 第一部分 解决方案

## 1. 背景与目标

### 1.1 背景

- 需要一套通用排队异步能力，承接任意"已任务化"的上游（创建任务 + 查询任务模型）。
- 服务形态为**异步网关**：给既有上游套一层，上游零代码改动。
- 网关作为 New API 的渠道接入：渠道 base_url 指向网关，New API 用现有上游 adaptor 把网关当上游解析；任务计费动作（提交预扣、失败退款、完成差额结算）全部在 New API 侧由其既有任务计费链路承担。
- 认证模型：上游凭证随 alias 透传——New API 渠道持有真实上游 key，Authorization 头穿透网关直达上游；调用方须持有效上游凭证，任务隔离由上游按 key 天然保证。
- 首个上游：火山方舟 Seedance；同类上游接入应达到"4 行配置 + 渠道持证"。

### 1.2 目标

- 上游零侵入、New API 一次性薄接入（注册渠道类型），之后接入新上游只改配置。
- 可靠性：至少一次投递 + 幂等去重；不确定失败走确认，不盲目重试创建；**状态输出真实、单调收敛不翻转、重复查询一致**（New API 计费 exactly-once 的前提）。
- 接入效率：模板三级配置 + AI 辅助生成 + dry-run 实测，把"读懂一套 API"降到"贴一份文档，确认几个假设"。
- 治理：Task-admin 统一管理模板、凭证引用、灰度、重放与审计；SSO+RBAC 归因到人，治理操作不绕过注册校验。

### 1.3 非目标

- 不做 OpenAI 同步协议兼容，不做同步转异步。
- **不参与计费**：网关不记账、不预扣、不退款、不结算，对计费的全部责任收敛为状态输出契约（见 §4.2）。
- 不承诺业务级 exactly-once（业务副作用幂等由上游或调用方保证，能力声明中体现）。

## 2. 设计原则

1. **网关是任务真相，渠道只是配置载体**：New API 管鉴权/计费/日志，网关管排队/调度/状态收敛。
2. **约定优于配置**：能推导的绝不手写；模板只写"偏离约定的部分"。
3. **AI 生成、机器校验、人工确认**：AI 产物永远不直写生产；AI 输入文档按不可信数据处理。
4. **先落库后入队**；**不猜成功**（unknown 两态机制）；**大结果不入队**；**所有写路径带前置条件**。
5. **治理不绕过安全**：Task-admin 的任何变更经同一校验器与白名单；凭证分层——数据面 header 透传不落盘，控制面 vault。

## 3. 方案概述

```
                 ┌─────────────── Task-admin（治理面）────────────────┐
                 │ 模板CRUD/预览/校验 · AI生成入口 · 凭证引用(控制面)     │
                 │ 灰度开关 · unknown/死信重放 · dry-run · 审计          │
                 │ SSO+RBAC · 职责分离 · 高危双人复核                   │
                 └───────────────┬────────────────────────────────────┘
                                 │ 审批流 + 审计（不绕过校验器）
客户端 ──► New API ──► 异步网关 ──► Taskiq 队列/worker ──► 上游（零改动）
           (渠道/计费/持key)  (FastAPI,     (Redis Streams)      (Seedance...)
                            Authorization头透传)
```

三层能力：

- **接入层**：`/async/` 网关 + New API `async_gateway` 渠道（base_url 接入，凭证随头透传）。
- **模板层**：三级配置面（极简/差异/完整）+ 推导规则 + 校验器 + 预览接口 + AI 生成流水线。
- **治理层**：Task-admin 对模板、凭证引用、灰度、异常任务进行全生命周期管理。

## 4. 关键机制（决策口径）

### 4.1 幂等与重试分级

幂等约束的是"创建次数 ≤ 1"，不是"尝试次数 = 1"。

**幂等键来源**：调用方显式提供优先；New API 按上游原生协议提交、不携带幂等键时，网关**派生幂等键** = `key_hash + 请求体规范化哈希`（**仅对模板声明的规范化字段集**做规范化：字段按字典序排序、去除首尾无关空白、字符串大小写规则按模板声明；不做"默认值显式化"等不可实现的全量规范化），时间窗内去重；终态 failed 的重试派生 `{key}#attempt{n}`。去重窗口长度进策略组（默认 24h）；`idempotency_key` 列语义 = **窗口期内唯一**，窗口外同名键视为新请求（§13）。

**错误三级分类**（retryable 由此推导，不允许模板手配）：

| 级别 | 取值 | 动作 |
|---|---|---|
| 可安全重试（请求未发出/无副作用） | 连接拒绝、DNS 失败、**连接建立超时**；429（respect_retry_after） | 直接重试；429 不消耗业务 attempts（独立退避计数+上限），其"无副作用"是上游假设，进 capabilities `rate_limit_side_effect_free` 与 AI 假设清单 |
| 判失败不重试 | 4xx 参数错误（400/422） | 任务级失败；细分：401/403→渠道级故障（熔断渠道+告警，不烧任务 attempts；熔断反馈回路：网关告警→New API 渠道禁用该 key→换 key 恢复——透传模式下网关不持 key，熔断动作=告警+渠道侧联动）；409→转确认（恰是去重抓手） |
| 不可盲目重试（请求可能已到达） | **响应超时/丢失**、5xx | 标 `submit_unknown`，由 compensate 按 confirm_strategy 确认后驱动 |

**不变量**：**不确定场景**的创建重发只能由确认结果驱动（compensate 是唯一入口）；终态 failed 的任务级重试属新业务尝试，经 `submit_upstream` 重建并递增 attempts，不违反幂等约束（键不同 attempt）。错误分类映射进模板可覆盖（默认按上表）。

**提交意图标记（防 accepted 悬挂盲建）**：worker 领取任务后、调上游**之前**先条件更新落"提交意图"（`submit_started_at` 时间戳 + attempts 预递增），随后才调上游。accepted 悬挂巡检按有无该标记分流：**无提交意图**（入队失败/消息丢失，上游必未创建）→ 条件更新重投；**有提交意图**（已调上游未落库，worker 崩溃窗口）→ 转 `submit_unknown` 走 compensate 确认，禁止未经确认直接重投（对 manual_only 的 Seedance 即真实重复创建风险）。

### 4.2 状态输出契约（替代计费口径）

网关完全不参与计费。网关对计费的全部责任 = **状态输出契约**：

1. **真实**：输出状态反映上游真实状态，不伪造成功、不伪造失败。
2. **单调收敛不翻转**：一旦输出终态永不翻转（终态迁移只允许一次，条件更新保证）。
3. **重复查询一致**：同一任务任意时刻重复查询返回一致结果（终态后读 result_ref，见 §12.2）。
4. **内部终态有原生输出路径**：timeout/dead/dead_awaiting_confirm 是网关内部终态，上游原生枚举没有；对外响应保持上游原生形状，但状态字段按模板终态映射**重写为上游原生失败类终态取值**、失败原因字段按模板映射 error_code 为原生格式——形状保真且 New API 退款链路可触发（规则细节见 §12.2 查询面）。

New API 侧计费映射（均在 New API，网关无感知）：提交→预扣；failed/timeout/cancelled→退款路径；succeeded→差额结算。

**转存永久失败的上游成功任务**：报 `succeeded` + 结果不可用（envelope `degraded[]` 声明），平台承担上游成本——不伪造失败骗退款。原生表达：状态字段=上游原生成功终态，结果字段重写为网关结果端点 URL（转存永久失败时该 URL 返回 410 语义）——New API 按成功正常结算，用户取结果时得到明确不可用信号；M3 契约联调须验证 New API adaptor 对该形状不误判为 failed。转存独立重试、耗尽告警、支持重放转存。

**响应形状保真（硬约束）**：base_url 模式下 New API 按上游原生 schema 解析 create/get 响应，面向 New API 的响应必须是上游原生形状；envelope 双字段（`raw_status`/`terminal`/`degraded[]`）仅面向网关自有调用方。

### 4.3 背压

- 渠道+租户两级并发限制：Redis Lua 原子 check-and-incr（终态中间件 DECR）；键结构 `conc:{channel}` 与 `conc:{channel}:{tenant}` 两级，限额配置在渠道配置/策略组；每 30s 用 PG 实数校准防计数泄漏，**校准只纠泄漏方向**（Redis 值 > PG 实数才下调，或带 epoch 单调递增防回退，防校准误伤在途任务）。
- **配额分离**：受理占"受理速率"配额（gateway-api 受理时消耗，QPS 令牌桶），submit 占"上游并发"槽（worker 调上游时 check-and-incr），两配额独立计数、独立限额——突发受理不挤占上游并发槽。
- 队列深度限制；打满返回 429/503 + retry_after，客户端可用同一幂等键安全重试。

### 4.4 机制与策略决策（引用式，详见正文）

- **统一结果转存**：结果一律转存对象存储、回引用——详见 §12.2 result_policy。
- **无队列内优先级**：渠道级并发隔离 + 全局 FIFO——详见 §4.3、§15。
- **状态来源主备**：`status_source: callback|poll` 主备不双轨——详见 §12.2。
- **envelope 双字段 + degraded[]**：仅面向网关自有调用方——详见 §12.2、§16。
- **URL 直配过渡管控**：独立指标 + 按渠道开关 + 一键全局关闭 + 下线 milestone——详见 §11、§20。
- **unknown 有界化**：单渠道上限 + 最大存活时间，超限统一转 `dead_awaiting_confirm`；覆盖 submit_unknown 与 poll_unrecognized 两态，模板勘误不至时不会无限悬挂——详见 §14。

## 5. 接入一个新上游的标准动作（业务视角）

1. 准备：上游 API 文档或 OpenAPI spec、一条真实 curl；凭证由 New API 渠道持有（数据面），dry-run 用控制面凭证存 vault 记下引用名。
2. 生成：AI 生成极简模板初稿，显式列出假设（id 位置、查询路径、状态枚举、confirm_strategy、rate_limit_side_effect_free）；输入文档按不可信数据处理。
3. 校验：注册校验器跑 schema + 推导完整性 + 表达式沙箱校验；不通过则按报错补字段。
4. 预览：模板预览接口展示展开后完整形态、每字段来源、**渲染请求样例**（实际将发出的 HTTP 请求）与版本 diff。
5. 实测：Task-admin 触发 dry-run（`origin=dry_run` 隔离），真实创建一笔最小任务并自动查询确认、清理。
6. 上线：配置仓评审（CODEOWNERS + 双人 approve）→ 灰度（渠道维度按比例绑新版本）→ 全量；全程审计留痕。

---

# 第二部分 业务架构

## 6. 业务角色

| 角色 | 关注点 | 主要触点 |
|---|---|---|
| 接入方（业务团队） | 快速把上游变成异步任务 | 贴文档 → 确认 AI 假设 → dry-run → 灰度 |
| 平台运营 | 模板质量、接入SLA、上游健康 | Task-admin 模板库、校验记录、看板 |
| New API 运营 | 渠道配置、计费准确、任务可查 | 渠道管理（持有上游 key）、计费对账 |
| SRE | 稳定性、异常收敛、容量 | Logfire 告警、unknown/死信重放、灰度开关 |
| 审计员（auditor） | 治理操作归因、数据访问合规 | 审计日志（append-only，只读） |

## 7. 核心业务用例

| 用例 | 说明 | 关键约束 |
|---|---|---|
| 任务受理 | 提交创建请求，返回受理响应（上游原生形状） | 先落库后入队；幂等键（显式或派生）唯一 |
| 任务查询 | 查询状态与结果 | 快照优先 + per-task min_refresh_interval（默认 3–5s）+ retry_after 引导；终态后 store 模式读 result_ref |
| 任务取消 | 取消未开始/执行中任务 | 上游不支持则降级为 cancel_requested，以轮询终态收敛 |
| 回调处理 | 上游完成通知 | 验签 + callback_event 去重 + 状态机合法迁移；orphan 回调暂存对账 |
| 模板接入 | 新上游注册 | 三级配置面 + 校验 + 预览（渲染样例+diff）+ dry-run |
| AI 辅助生成 | 文档→模板初稿 | 输入不可信；产物不直写生产；假设显式列出 |
| 异常治理 | submit_unknown 确认、dead_awaiting_confirm 重放、orphan 对账 | 重放前置幂等检查 + 双人复核；操作全审计 |
| 灰度发布 | 渠道维度按比例绑模板新版本 | 权重归零即回滚；错误率超基线自动归零 |

## 8. 关键业务流程

### 8.1 任务生命周期（正常路径）

```
受理(accepted) → 上游创建(upstream_submitted) → 执行中(in_progress)
  → 成功(succeeded)：拉结果（请求时刻 SSRF 校验）→ 转存对象存储 → 回短 TTL 预签名引用
  → 失败(failed)：按三级错误分类判可重试 → 耗尽进死信(dead)
  → 取消(cancelled) / 超时(timeout)：deadline_at 判定；上游 expired 统一映射进 timeout
```

计费由 New API 依据读到的状态驱动（预扣/退款/结算），网关只保证状态输出契约（§4.2）。

### 8.2 异常路径

```
提交响应超时/丢失/5xx → submit_unknown → compensate 按 confirm_strategy 确认
  ├─ 确认未创建 → 重新发起创建（不确定场景唯一允许重发创建的入口）
  ├─ 确认已创建 → 补 upstream_task_id → 走正常轮询
  ├─ manual_only 上游 → 永不自动重发创建，转人工确认
  └─ 确认不了 → 保持 + 告警；超上限/超存活期 → dead_awaiting_confirm（人工确认后重放/关闭）

轮询响应判不出终态 → poll_unrecognized
  （保留 upstream_task_id，任务确定已存在；模板勘误后恢复轮询，永不触发创建）

accepted 悬挂（超时未推进）→ inspector 巡检按提交意图分流：
  ├─ 无提交意图（submit_started_at 为空 = 入队失败/消息丢失，上游必未创建）→ 条件更新重投
  └─ 有提交意图（已调上游未落库）→ 转 submit_unknown 走 compensate 确认，禁止未经确认盲建
orphan callback（网关侧查无此行）→ 暂存 orphan_callback 表 → compensate/巡检对账归位
```

### 8.3 模板变更流程

```
AI/人工起草 → 校验器 → Task-admin 预览（渲染请求样例 + 版本 diff）→ dry-run 实测
  → 审批（策略/能力/URL/表达式/callback 参数变更强制双人评审；字段勘误走快速通道）
  → 灰度发布（渠道维度按比例绑新版本，权重归零即回滚）→ 审计归档
```

## 9. 业务实体

- **渠道（New API）**：选路、权重、分组、计费入口；绑定网关 alias；持有真实上游 key（多 key 轮询/熔断为渠道侧原生能力）。
- **模板**：上游接入契约（三级配置面，版本化管理）；不含数据面凭证。
- **任务**：网关核心实体，状态机驱动；受理时记录 `template_alias/template_version`、`deadline_at`、`origin`，全生命周期审计。
- **凭证（分层）**：数据面 = Authorization 头透传（网关不存 key、不落盘）；控制面 = vault 仅存 Task-admin dry-run 凭证，`credential_ref` 仅控制面用途。
- **审计事件**：受理、取消、回调、终态、模板变更、治理操作、凭证引用变更、灰度切换、验签失败、结果回读/导出、dry-run 执行；append-only/WORM 存储。

---

# 第三部分 技术架构

## 10. 总体架构

```
                         ┌────────────────────────────┐
                         │ Task-admin（治理面，独立部署）│
                         │ SSO+RBAC · 模板/灰度/重放    │
                         │ dry-run · 审计(append-only)  │
                         └────────────┬───────────────┘
                                  审批+审计 │ 校验器（同一套）
┌─────────┐   ┌──────────────┐   ┌───────┴────────┐   ┌──────────────┐
│ New API │──►│ gateway-api  │──►│  Taskiq 队列    │──►│ 上游 Seedance │
│渠道/计费/│   │ (FastAPI)    │   │  Redis Streams  │   │  (零改动)     │
│持有上游key│  └──────┬───────┘   └───────┬────────┘   └──────────────┘
└─────────┘          │                   │
   Authorization  Postgres（真相/审计）  worker 池 / transfer-worker / scheduler / inspector
   头透传 ▲           │                   │
                  对象存储（请求/结果）   Logfire（OTel，header 强制脱敏）
```

进程划分：

| 进程 | 技术 | 职责 |
|---|---|---|
| gateway-api | FastAPI | 受理/查询/取消/回调，无状态，多副本 |
| worker（QoS 池） | Taskiq worker | 上游调用、状态推进；按池分组（heavy-poll/light-poll/灰度隔离），池内 per-channel 信号量+Redis 计数做渠道并发隔离；模板 `pool` 字段默认共享池 |
| transfer-worker | Taskiq worker | `store_result` 大结果转存，独立队列 + 独立 Deployment/HPA，与轻量 poll 隔离 |
| scheduler | Taskiq scheduler | 延迟轮询调度（next_poll_at 派发）；生产固定单副本 + 就绪探针 |
| inspector | Taskiq beat/worker | 巡检面：accepted 悬挂巡检（按提交意图分流，§14）、deadline 巡检、orphan 对账、unknown 有界化巡检；独立 Deployment、单副本，与 scheduler 故障域隔离 |
| task-admin | FastAPI + 内部 UI | 治理面，SSO+RBAC，不接生产流量；看板/审计走只读副本 |
| postgres / redis / minio | - | 真相与审计 / broker 与缓存 / 大对象（桶级加密） |

## 11. `/async/` 协议面

- 固定前缀，第二段为 alias，剩余原样转发；版本不进 URL。
- 对外承诺三语义：提交、查询、取消；回调端点 `/callbacks/{opaque_token}`（路径不可预测），验签接收。
- **认证 = 上游凭证透传**：Authorization 头穿透网关直达上游；网关数据面不存 key、不落盘。调用方须持有效上游凭证；任务隔离由上游按 key 天然保证（前提：capabilities `per_key_isolation: true`，逐上游确认后声明）。
- **归属校验（可选纵深，默认建议开启）**：透传查询/取消前按 `(channel, upstream_task_id)` 查 `async_task`；命中且归属匹配才放行，未命中统一 404（不区分"不存在"与"无权"，防枚举/IDOR 放大）。
- 双轨解析：alias 命中走模板 → 未命中过白名单的约定型直配兜底（**过渡能力**：独立调用量指标 + 按渠道开关 + 一键全局关闭，path 前缀白名单约束漫游，下线计划见 §4.4/§20）→ 否则拒绝。

## 12. 模板系统（核心）

### 12.1 三级配置面

- **极简模式**：约定型上游 4 行（alias / base_url / create_path / result_location）。数据面凭证不进模板（随头透传）；`credential_ref` 仅控制面 dry-run 用途，可出现在控制面配置而非数据面模板。
- **差异模式**：只写偏离约定的字段（id_location、get_path_template、终态判定取值、capabilities、pool）。
- **完整模式**：不规则上游展开七组全字段。

### 12.2 约定推导规则与查询面

查询面原则：`GET /async/{alias}/{上游get路径}` 原样透传上游 get 响应，调用方看到**上游原生状态与原生响应形状**；网关不维护全量状态映射表，仅从响应提取"是否终态/是否成功/结果位置"，用于状态输出、结果转存与停止轮询；提取不到判定的响应归 `poll_unrecognized`。

| 字段 | 默认 |
|---|---|
| get_path_template | create_path + `/{id}` |
| id_location | `$.id` |
| cancel_path_template | create_path + `/{id}`（DELETE） |
| 终态判定 | 默认 `terminal_success: [succeeded]`、`terminal_failure: [failed]`；上游 `expired` 统一映射进网关 `timeout`；特殊取值在模板中覆盖 |
| status_source | 默认 `poll`；上游回调可靠则配 `callback`，轮询降为低频兜底，不做双轨并行；主源模式下兜底源的终态判定须与主源仲裁或经固定确认窗口才落库，防兜底误判永久锁死正确回调（§14 终态冲突仲裁） |
| result_policy | 默认 `mode: store`（终态转存回引用，短 TTL 预签名 URL，TTL 策略可配）；上游永久直链可配 `passthrough`（须在 envelope `degraded[]` 声明依赖上游直链有效期）；`redirect` 按需；pipeline 插件位预留，第一版只实现 store/passthrough |
| callback 注入 | 约定参数 `callback_url` |
| pool | 默认共享池；重轮询/灰度隔离渠道可指定专属池 |
| 策略组 | 全局默认；New API 渠道侧可按渠道覆盖 |

**capabilities schema（全字段，缺省 false）**：

| 字段 | 含义 |
|---|---|
| upstream_idempotent | 上游创建是否幂等；false 时创建去重责任在网关 |
| confirm_strategy | `query_by_client_key` / `list_and_match`（列举+请求体 hash 匹配，需存 create 请求摘要；**前提**：仅当上游 list 记录含可匹配请求属性——匹配字段集逐上游确认并进 capabilities——才可用，否则降级 manual_only；分页深度/时间界受限：仅列举近 10 分钟创建记录、分页上限 3 页；多结果歧义 → 转人工）/ `manual_only`（**永不自动重发创建**，转人工） |
| cancel / callback / list_tasks | 上游是否支持取消 / 回调 / 任务列举（孤儿对账用） |
| max_payload_bytes | 请求体上限 |
| permanent_result_url | 结果 URL 是否永久有效（passthrough 前提） |
| rate_limit_side_effect_free | 429 是否无副作用假设项 |
| per_key_isolation | 上游是否按 key/账户隔离任务空间（透传认证模型与归属校验豁免的前提，逐上游确认） |

**查询面行为**：

- **快照优先**：默认返回网关侧最后一次轮询/回调的状态快照；透传刷新改为异步触发 + per-task `min_refresh_interval`（默认 3–5s，渠道可配）；进行中响应携带 `retry_after` 引导客户端降频；渠道级查询熔断：上游 429 率超阈值时查询强制走快照。
- **终态后读路径**：终态且 store 模式时，响应结果字段重写为 `result_ref`（预签名 URL）指向对象存储；上游 URL 过期/任务归档后由 result_ref 服务；留存到期或删除后返回 410。
- **状态输出契约**：终态输出单调不翻转、重复查询一致（§4.2），写入本查询面为硬约束。
- **网关内部终态对外输出（硬规则）**：任务进入网关内部终态（timeout/dead/dead_awaiting_confirm）时，响应保持上游原生形状，但状态字段按模板终态映射**重写为上游原生的失败类终态取值**，失败原因字段按模板映射 error_code 为原生格式——形状保真且 New API 退款链路可触发；否则快照停在最后非终态、New API 永远读不到终态，退款不触发、状态输出契约落空。
- **转存永久失败的原生表达**：状态字段=上游原生成功终态，结果字段重写为网关结果端点 URL（转存永久失败时该 URL 返回 410 语义）——保证 New API 正常结算、用户取结果时得到明确不可用信号；**M3 契约联调需验证 New API adaptor 对该形状不误判为 failed**。
- **envelope（仅网关自有调用方）**：`raw_status` 透传上游原生状态（提交响应携带上游 create 原始响应）、`terminal` 三值（in_progress/succeeded/failed）、`degraded[]` 显式声明能力降级（如不支持的 cancel、passthrough 直链依赖、结果不可用）。

### 12.3 校验与预览

- 注册校验：schema + 推导完整性（推导不出的必填项报错定位到字段）+ **表达式沙箱校验**（JSONPath 严格子集，禁 filter/脚本表达式，求值超时与结果大小上限）；终态判定取值提取不到标 warning，无法判定的响应归 `poll_unrecognized`。
- 模板预览接口：输入简化配置，返回展开后完整形态 + 每字段来源（显式/推导/默认）+ **渲染请求样例**（实际将发出的 HTTP 请求）+ **版本 diff 视图**；URL/表达式/callback 参数变更自动归入强制评审；预览接口永不输出凭证值。
- 模板热更新限定：**非策略字段勘误即时生效；策略/能力变更走版本化灰度**（渠道维度按比例绑新版本，权重归零即回滚）。任务按受理时 `template_alias + template_version` 执行，老任务用旧版收敛，新版只影响新任务。
- **勘误就地修订例外（版本钉住的调和）**：终态判定/提取表达式类勘误允许对既有版本**就地修订（in-place patch）**——仅改判定逻辑、不改请求形状，修订单列审计；存量 poll_unrecognized 任务不换版本即恢复轮询（§14）。其余变更仍走版本化。

### 12.4 AI 生成流水线

```
API文档/OpenAPI/curl ──► AI提取端点/ID位置/终态取值/结果字段/回调机制
  ──► 极简模板初稿 + 假设清单 ──► 校验器 ──► 预览 ──► dry-run ──► 评审上线
```

- 输入四样：文档或 spec、真实 curl、模板规范、参照样例（如 Seedance）。**输入文档按不可信数据处理**（防 prompt 注入篡改 base_url）；AI 输出的 URL/表达式必须过机器校验。
- 边界：凭证永不进 prompt；不规则上游 AI 出初稿、字段级人工核对。
- dry-run 隔离：`origin=dry_run` 字段支撑，计费与配额统计显式排除；确认后自动 cancel → 清理 → 归档（cancel 不支持的上游打标记等自然终态）；dry-run 详情 RBAC 可见 + 自动脱敏。

### 12.5 模板样例（Seedance，极简模式）

```yaml
alias: volc-seedance
base_url: https://ark.cn-beijing.volces.com/api/v3
create_path: /contents/generations/tasks
result_location: $.content.video_url
```

关键点：

- 真实上游 key 由 New API 渠道持有，随 Authorization 头透传；模板不含数据面凭证（原 `credential_ref` 行移除，接入故事改口径为"4 行配置 + 渠道持证"）。
- `capabilities.upstream_idempotent: false` + `confirm_strategy: manual_only` 由**平台内置上游差异补丁**自动附加：声明"创建去重责任在网关，提交不确定时永不自动重发创建、转人工确认"。
- 多 key 轮询/单 key 限流熔断切换为 New API 渠道侧原生能力，非网关职责。

## 13. 数据模型（Postgres）

`async_task`：`task_id`（PK）、`idempotency_key`（窗口期内唯一；显式或派生 = `key_hash + 请求体规范化哈希`，重试派生 `{key}#attempt{n}`；窗口长度进策略组，默认 24h，窗口外同名键视为新请求）、`tenant/channel/task_type`、`status`、`upstream_task_id`、`attempts/max_attempts`（attempts=**提交尝试次数**：每次 submit_upstream 执行前随提交意图预递增、每次恰好 +1，含首次提交与 failed 重试重建；指标/告警统一按此口径解释，勿当作"失败重试次数"）、`submit_started_at`（提交意图标记：worker 调上游前条件更新写入并预递增 attempts，区分"入队失败"与"已调上游未落库"）、`template_alias/template_version`（受理时写入，随行归档）、`deadline_at`（受理时按渠道/模板 TTL 写入，timeout 判定依据）、`origin`（user/dry_run/replay）、`create_req_ref`（create 请求摘要，list_and_match 用）、`result_ref/result_summary`、`error_code/error_message/retryable`、`cancel_requested`、`created/started/finished/next_poll_at`、`trace_id`。

- 错误码取值域：`error_code ∈ {UPSTREAM_4XX, UPSTREAM_5XX, UPSTREAM_TERMINAL, TRANSPORT, RATE_LIMITED, TIMEOUT, CANCELLED, TRANSFER_FAILED, UNKNOWN_EXHAUSTED}`；`retryable` 由 §4.1 三级分类推导，不允许模板手配。
- 索引：`(tenant, channel, status)`、`(status, next_poll_at)`、`upstream_task_id` 单列索引（回调反查 / orphan 对账）；大字段一律引用。
- **所有写路径带前置条件**（期望状态或乐观版本号，条件更新）：

| 写路径 | 前置条件 |
|---|---|
| cancel API 置 cancel_requested | status ∈ {accepted, upstream_submitted, in_progress, submit_unknown}（submit_unknown 仅置 cancel_requested，不直接迁移，由 compensate 检查收敛） |
| submit 意图标记写入 | status = accepted 且 `submit_started_at` 为空（CAS 抢占，防多 worker 并发提交） |
| submit 成功落库 | status = accepted 且 `submit_started_at` 已置（意图先行）且未置 cancel_requested（否则立即触发 cancel 流程） |
| 状态推进 | 白名单迁移 + 期望状态匹配 |
| store_result 写 result_ref | status = succeeded 且 result_ref 为空 |
| scheduler 写 next_poll_at | 非终态 |
| compensate 归位 | status = submit_unknown |

- 附属表：`callback_event(channel, dedup_key UNIQUE, payload, received_at)`（dedup_key 优先上游 event_id，缺失取 `(upstream_task_id, raw_status, body_digest)`）；`orphan_callback(channel, upstream_task_id, payload, received_at)`；`upstream_template`（alias + 命名空间归属唯一、version、enabled、config JSON、hash、变更审计）；`audit_event` 独立表（append-only/WORM，与任务表分离；看板聚合走只读副本或预聚合表）。
- 分区归档：`async_task` 按 `created_at` 月度声明式分区；归档形态选定 = **DETACH PARTITION 后转为同库 `archive` schema 独立表**（不做跨库 dump；DETACH 零 DELETE，避免 vacuum 压力与索引膨胀）。DETACH 前置条件 = 分区内无非终态行；月末长任务滞留老分区至终态后随下一周期归档。留存期内归档查询路由 = 热表未命中查 archive schema；**删除权在归档侧执行 DELETE 可接受**（archive schema 不在热表 vacuum 敏感路径上），到期/删除后返回 410。
- 对象存储生命周期显式化：默认 30 天（租户可配），到期 `result_ref` 置 expired、查询返回 410；存储用量/成本纳入 §17 指标。

## 14. 状态机

内部状态机仅服务状态输出、调度与终态收敛；对外查询透传上游原生状态，两者解耦：透传管"看见"，终态判定管"输出与停止"。输出路径补全：任务进网关内部终态（timeout/dead/dead_awaiting_confirm）时，对外响应保持上游原生形状，但状态字段按模板终态映射**重写为上游原生失败类终态取值**、失败原因按模板映射 error_code 为原生格式（§12.2/§4.2），保证 New API 退款链路可触发。内部状态机简化为 `accepted → upstream_submitted → in_progress → 终态`；queued/running 为上游侧镜像，不进内部状态机。

```
accepted ──► upstream_submitted ──► in_progress ──► succeeded
   │               │                    ├─► failed（可重试 → submit_upstream 重建 attempts+1 → 耗尽 dead）
   ├─► cancelled   ├─► submit_unknown   ├─► cancelled
   │(未提交直接取消) │(响应超时/丢失/5xx)  └─► timeout（deadline_at 判定；上游 expired 映射进此）
   │               └─► cancelled（submit 成功后发现 cancel_requested 置位 → 立即触发上游取消）
   │
   └─ accepted 悬挂：inspector 巡检超时未推进，按提交意图（submit_started_at）分流
      ├─ 无提交意图（入队失败/消息丢失）→ 条件更新重投
      └─ 有提交意图（已调上游未落库）→ 转 submit_unknown 走 compensate，禁止直接重投

submit_unknown ──compensate──► 归位（未创建→重发 / 已创建→补 ID 轮询）
   │    （重发创建/补 ID 前强制检查 cancel_requested，置位则转人工关闭）
   └── 超上限/超存活期 ──► dead_awaiting_confirm（人工确认后重放或关闭；重放第一步强制确认查询）

in_progress ──响应不可判定──► poll_unrecognized ──模板勘误──► 恢复轮询（永不触发创建）
   │    （勘误允许对既有版本就地修订——仅改判定逻辑，§12.3；任务不换版本即恢复）
   └── 有界化：unknown 上限/最大存活 → dead_awaiting_confirm；deadline 巡检同样覆盖，
       勘误不至时不会无限悬挂（见 §4.4）

dry-run（origin=dry_run）：确认后自动 cancel → 清理 → 归档；cancel 不支持则打标记等自然终态
```

- **迁移白名单显式**：合法迁移含 `accepted→{upstream_submitted, cancelled, submit_unknown}`、`upstream_submitted→{in_progress, 终态, submit_unknown, cancelled}`（`upstream_submitted→终态` 合法——快速任务回调直达场景，回调先于轮询到达）、`in_progress→终态`、`submit_unknown→{upstream_submitted, dead_awaiting_confirm}`、`poll_unrecognized→{in_progress, dead_awaiting_confirm, timeout}`；其余一律拒绝。
- **cancel 竞态**：补 `accepted→cancelled` 条件迁移；submit 成功落库后强制检查 `cancel_requested`，置位则立即触发 cancel 流程，杜绝"用户已取消、上游仍跑"；cancel 前置条件含 submit_unknown（仅置 cancel_requested），compensate 重发创建/补 ID 前强制检查，置位则转人工关闭。
- **终态冲突仲裁**：`status_source` 主源模式下，兜底源（如 callback 主模式下的低频轮询）判出的终态若与主源不一致或先于主源到达，须与主源仲裁（复查主源/确认查询）或经固定确认窗口后才允许落库——终态迁移只一次，兜底误判会永久锁死正确回调结果。
- **timeout 胜出的不对称代价**：timeout 判定后上游姗姗来迟的 succeeded 回调不再翻转终态，该笔上游费用平台白付（New API 已按 timeout 退款）——显式接受此不对称（宁退款不误锁），计入每日对账差异指标（§16）。
- **orphan 归位**：orphan_callback 暂存表 + inspector 对账；inspector 定期按 `capabilities.list_tasks` 对账上游任务列表发现孤儿，不支持 list 的上游以 `next_poll_at` 停滞 + 心跳缺失判疑似孤儿转 `submit_unknown` 由 compensate 归位。
- **死信重放**：第一步强制执行确认查询（manual_only 上游强制人工确认）；已有 `upstream_task_id` 禁止重放创建类动作；审计记录确认结果。
- **回调去重**：验签通过的回调先查 `callback_event`，命中直接 200 不进状态机，杜绝竞态窗口内双写 result_ref。
- 终态迁移只允许一次（条件更新），保证状态输出单调不翻转。

---

## 15. Taskiq 任务与中间件

任务拆分：`submit_upstream` / `poll_upstream` / `finalize_task` / `cancel_upstream` / `compensate_orphan` / `store_result`（独立队列 + 独立 Deployment/HPA，大结果 IO 不与轻量 poll 混部）。

中间件统一实现：trace 上下文传递（header 强制脱敏）、幂等检查、超时控制（poll 硬超时 10s，超时按 `next_poll_at` 重排）、指数退避+jitter、异常三级分类（§4.1）、渠道并发计数 DECR、指标打点。

**自适应轮询**（按渠道维护）：

| 参数 | 取值/规则 |
|---|---|
| 间隔公式 | `interval = clamp(base × 2^⌊elapsed / P50⌋, min 3s, max 60s)`；首 poll = max(P10, 3s)，base 默认 3s |
| 冷启动 | 渠道无样本时用模板 `poll_initial_interval`（默认 5s）；积累 ≥ 30 个终态时长样本后切换直方图驱动 |
| 直方图存储 | Redis 桶计数滑动窗口 7 天（按 10s 分桶，HINCRBY + 键 TTL 滚动过期），P10/P50/P95 每 5min 重算并缓存 |
| 429 AIMD | 该渠道 429 时间隔 ×2（上限 60s）并暂停派发至 retry_after；每过一个无 429 周期恢复 -10% |
| 收益量级 | 万级并发 180s 任务：固定 5s 退避 ≈2000 poll/s，自适应 ≈500 poll/s，省 4 倍上游配额 |

## 16. 与 New API 集成（状态输出契约）

- **接入方式**：渠道类型 `async_gateway`，渠道 base_url 指向网关；New API 渠道持有真实上游 key 随 Authorization 头透传；New API 用现有上游 adaptor 解析网关响应，按上游原生协议提交任务（不携带幂等键，网关派生，见 §4.1）。
- **响应形状保真（硬约束）**：面向 New API 的 create/get 响应必须是上游原生形状（§12.2 透传原则已满足）；envelope（`raw_status`/`terminal`/`degraded[]`）仅面向网关自有调用方。
- **状态输出契约**：真实、单调收敛不翻转、重复查询一致（§4.2）；失败原因与进度字段按上游原生格式透传，New API adaptor 按上游 schema 解析；网关内部终态（timeout/dead/dead_awaiting_confirm）按模板映射重写为上游原生失败类终态取值输出（§12.2），保证退款链路可触发。
- **计费映射（全部在 New API 侧）**：提交→预扣；failed/timeout/cancelled→退款路径；succeeded→差额结算；转存永久失败的上游成功任务报 `succeeded`+结果不可用（平台承担上游成本，不伪造失败骗退款）。
- **对账**：每日比对网关终态任务集 vs New API tasks 表/消费日志，差异告警（防 New API 侧退款/结算异常成为静默资损）。
- **职责边界**：New API 管鉴权/计费/日志/展示/多 key 轮询；网关管排队/调度/状态收敛。渠道配置面以 base_url 实际能力为限，超出项（任务类型映射、策略覆盖）依赖 New API 侧定制，不视为既有能力。

## 17. 可观测（Logfire）

- FastAPI 请求打点；Taskiq 中间件任务级 span，跨投递透传 trace context；httpx 上游打点（body 只记摘要/引用）。
- **强制脱敏清单**：Authorization 头永不进日志/trace/审计，OTel 采集器显式剔除 header 采集（白名单制）；上游错误响应入库/展示前按凭证值反向扫描抹除（防 4xx 回显 token 片段）。
- 核心指标：受理速率、队列深度、任务年龄、attempts（提交尝试次数，口径见 §13）分布、终态分布、submit_unknown/poll_unrecognized 数、死信速率、上游 429/5xx 比率、端到端时延、直配调用量（独立指标）、存储用量/成本。
- **分渠道健康度**：成功率/时延/429 率全量打点；灰度门控自动化——错误率 > 基线+2σ 持续 5min 自动权重归零；**最小样本量护栏**：统计窗口内样本 < 100 请求不触发自动归零（防小样本误熔断）。
- 告警一页收敛（队列深度、任务年龄 P95、unknown 两态数、死信速率、上游错误率、端到端时延、回调验签失败数、scheduler/inspector 停顿超阈值（阈值目标值待定））；阈值改基线+静态兜底双层，其余仅看板。
- 验签失败防 DoS：分渠道/来源 IP 限流，审计按分钟聚合计数保留样本，告警联动自动封禁。

**SLI/SLO 表**（结构留位，标注"目标值待定"处可调）：

| SLI | SLO |
|---|---|
| 受理时延 | P99 < 500ms |
| poll 及时率（实际 poll 与 next_poll_at 偏差 < 5s 占比） | > 99% |
| 转存成功率 | > 99.9% |
| submit_unknown 收敛时长 | P95（目标值待定） |
| 终态输出翻转次数 | 0 |
| 回调验签可用性 | （目标值待定） |

**容量目标表**（M3 基线压测验证，M6 满容量验收）：

| 指标 | 目标 |
|---|---|
| 受理 QPS | ≥ 200/s |
| 并发任务数 | ≥ 2 万 |
| poll QPS | ≥ 2000/s |
| 转存吞吐 | ≥ 250MB/s 峰值 |
| scheduler 派发速率 | ≥ 5000 msg/s |

## 18. 安全

### 18.1 SSRF（请求时刻校验）

- 所有出站请求（提交/查询/取消/**结果拉取/转存**）在**请求时刻**对最终 URL 重做白名单 + 内网/metadata 判定——`result_location` 提取的 URL 来自上游响应体，属不可信输入。
- 显式 `follow_redirects=false`；确需跟随则逐跳重验。
- 插值变量净化：`{id}` 等取自上游响应的变量拒绝含 `/`、`..`、`:`、scheme 的取值；join 后再验一次。
- resolve-and-pin 或校验已连接 socket 对端地址，防 DNS rebinding/TOCTOU。
- 直配过渡期管控：path 前缀白名单 + 独立监控指标 + 一键 kill switch（见 §11/§20）。

### 18.2 凭证分层（写死）

- **数据面 = header 透传**：真实上游 key 由 New API 渠道持有，Authorization 头穿透网关直达上游；网关不存 key、不落盘、不持久化 header；日志/trace/审计强制脱敏 Authorization 头（见 §17）。
- **控制面 = vault**：仅存 Task-admin dry-run 用凭证；`credential_ref` 仅控制面用途。
- 多 key 轮询/熔断切换为 New API 渠道侧能力；key 与任务的粘性与归因由渠道↔key 天然对应保证。
- **兼容性前提（per_key_isolation × 多 key × 派生幂等键）**：`per_key_isolation: true` 的上游要求 New API 渠道单 key 或任务级 key 粘性（创建与轮询同 key）；多 key 轮换会导致 poll 404（任务空间按 key 隔离）与派生幂等键 key_hash 漂移失效（同请求换 key 视为新键、去重失效）；逐上游确认，列入 M2/M6 联调出口（§12.2/§16/§4.1）。

### 18.3 回调安全

- 端点 `/callbacks/{opaque_token}` 不可预测；分渠道 HMAC 验签 + 时间窗防重放。
- 密钥存 vault；签名头带 `kid`，active+previous 双密钥灰度轮换 + 轮换 runbook；窗口内合法重复投递由 `callback_event` 去重表兜底（§14）。
- 验签失败只审计不进状态机；防 DoS 限流与封禁见 §17。
- **opaque_token 泄露处置 runbook**：单任务泄露→吊销该 token（映射表删除 + 缓存失效）→ 换发新 token → 通知上游更新 callback_url → 审计归因；批量泄露→按渠道轮换 token 生成密钥并全量换发。

### 18.4 Task-admin 治理（认证授权模型）

- 接企业 SSO + MFA；RBAC 角色：`template-author` / `approver` / `operator` / `auditor`。
- **职责分离**：提交人与审批人不得同一人；"内网可达"仅作网络纵深，不作授权依据。
- **高危操作二级管控**（死信/unknown 重放、灰度变更、凭证引用变更、模板高危字段变更）：双人复核 + 执行前展示影响面（任务数、预计上游调用数）。
- 重放强制前置幂等检查（已有 `upstream_task_id` 禁止重放创建类动作）；批量重放设单次上限与速率限制（如 100 任务/min）。
- 永远复用同一校验器与白名单，无特权旁路。

### 18.5 审计防篡改

- `audit_event` 写 append-only 存储，定期哈希锚定/导出 WORM 桶；DB 层撤销治理账号对审计表的 UPDATE/DELETE。
- **审计只存引用/哈希**：审计记录仅含任务 ID、payload digest、凭证引用名等不可逆引用，不含个人负载与凭证值——WORM 留存与删除权不冲突（删除权作用于任务数据，审计仅存引用，§18.8）。
- alias 加命名空间归属，未授权与不存在同返 404（防枚举平台上游）。

### 18.6 模板与配置安全

- 模板即代码须有沙箱：表达式限定 JSONPath 严格子集 + 求值超时 + 结果大小上限。
- AI 输入文档声明为不可信数据；AI 输出的 URL/表达式必须过机器校验。
- 预览加渲染请求样例 + 版本 diff；URL/表达式/callback 参数变更自动归强制评审。
- 配置仓 CODEOWNERS + 双人 approve + 分支保护。

### 18.7 结果引用与出口收敛

- 结果引用用短 TTL 预签名 URL（TTL 策略写进 `result_policy`）；或网关代理读 + 归属校验，代理读支持 **Range 透传**（大视频分段下载/断点续传）。
- worker 流量经固定 egress IP/出口代理，白名单在代理层再执行一次（平台层兜底）。

### 18.8 数据合规

- **加密**：对象存储桶级加密（可选租户级 KMS key）。
- **留存**：按租户可配的留存策略（热表/冷表/对象分层），对象默认 30 天到期自动清除；日志/trace 摘要留存期显式声明。
- **删除权**：任务删除语义级联删除 `create_req_ref`/`result_ref` 对象及归档记录（归档侧 DELETE 可接受，§13），记删除审计（审计仅含引用/哈希，§18.5），删除后查询返回 410。
- **与"归档任务可回读"的调和**：留存期内归档任务经 `result_ref` 可回读；到期或删除后返回 410——可回读以留存期为界，不承诺永久。

### 18.9 多租户

- tenant 维度配额/并发/命名空间隔离；归属校验作为可选纵深见 §11。

## 19. 部署

- 开发联调：compose 起全部进程。
- 生产 K8s：gateway-api 多副本；worker 按 QoS 池分组（heavy-poll/light-poll/灰度隔离池，渠道增长不再一拆一 Deployment）；transfer-worker 独立 Deployment；HPA 复合指标（lag + 最老消息年龄 + 槽位占用率）+ 缩容冷却窗。
- scheduler 生产固定单副本 + 就绪探针（统一口径，弃"带锁多副本"）；恢复后按 `next_poll_at` 分批放出，防积压洪峰。
- inspector（巡检面：accepted 悬挂/deadline/orphan 对账/unknown 有界化）独立 Deployment 单副本部署，与 scheduler 故障域隔离——调度器故障不阻塞巡检收敛，巡检故障不影响派发；巡检停顿纳入告警（§17）。
- Redis AOF（**RPO ≤ 1s**）、Postgres HA、对象存储托管；实现并演练"PG→Streams 重建"恢复程序（按 status 扫活跃任务批量 XADD + 幂等键防重），**RTO 目标 ≤ 15min**（与 PG→Streams 重建程序季度演练挂钩验证，演练实测超标则下调目标或优化程序）；容灾每季度联合演练。
- Task-admin 独立命名空间，仅内网可达，不接生产流量路径；看板/审计/批量操作走只读副本，重放接口限速。

## 20. 容量与里程碑

容量目标见 §17 表（M3 基线验证、M6 满容量验收）。

| 阶段 | 内容 | 出口 |
|---|---|---|
| M1 骨架 | API + 状态机 + Taskiq 打通 echo 上游 | 受理-查询-取消闭环 |
| M2 Seedance | 极简模板、轮询、回调、结果转存 | 真实视频可回读 + **回调验签+防重放用例通过** |
| M3 可靠性 | 幂等分级、unknown 两态补偿、死信、背压 | **状态输出契约经测试渠道联调通过** + 故障注入场景清单逐项通过 + **性能基线压测**（单 scheduler 上限、Streams 吞吐、热表 UPDATE 速率） |
| M4 可观测 | Logfire 全链路 + SLI/SLO + 告警 | 单 task_id 查全链路；对账任务上线 |
| M5 治理面 | Task-admin：SSO/RBAC、模板库、灰度、重放、dry-run | 治理操作全审计（append-only）；直配按渠道开关可用 |
| M6 平台化 | AI 生成流水线 + New API 联调 + **满容量验收** | 新上游=文档+确认假设；N 倍目标容量下核心 SLO 达标；**直配一键全局关闭验证**（下线 milestone） |

**故障注入场景清单（M3 逐项验收，7 项）**：Redis 数据丢失/AOF 重放；PG 主备切换（观测 unknown 洪峰与 compensate 限速）；回调洪峰（单任务千级重复 + 全渠道风暴）；上游 429 持续风暴；scheduler 宕机（停顿告警 + 恢复分批放出）；对象存储不可用；worker 崩溃窗口两子项——入队失败/消息丢失（任务停 accepted、无提交意图→inspector 条件更新重投）/已调上游未落库（有提交意图→转 submit_unknown 走 compensate，断言不重复创建）。

## 21. 验收标准

1. 新约定型上游接入 = 4 行模板 + 渠道持证 + dry-run 通过，零代码。
2. 同一幂等键（显式或派生：`key_hash + 请求体规范化哈希`）同一 attempt 只产生一个上游任务；重复回调/轮询/提交无副作用。
3. 重启不丢任务；accepted 悬挂按提交意图分流收敛（无提交意图巡检重投 / 有提交意图转 submit_unknown，不重复创建）；创建超时进 submit_unknown 且补偿收敛；**状态输出单调收敛不翻转、重复查询一致**（New API 计费 exactly-once 的前提）。
4. **store 模式且留存期内**：结果转存后可回读，上游 URL 过期不影响归档任务；到期/删除后返回 410；passthrough 模式须在 envelope `degraded[]` 声明依赖上游直链有效期。
5. 单 task_id 全链路可查；核心指标、SLI/SLO、告警齐备。
6. Task-admin SSO+RBAC+职责分离；全操作审计 append-only/WORM；无任何绕过校验器的变更通道。
7. capabilities schema 全字段声明；上游能力缺失时 envelope `degraded[]` 显式降级声明，不静默承诺。
8. **容量**：M3 性能基线达标；N 倍目标容量下核心 SLO 达标（M6 满容量验收）。
9. 归属校验（开启时）：跨渠道/租户的任务 ID 枚举统一 404，不区分"不存在"与"无权"。
10. **URL 直配可一键全局关闭**，alias 路径不受影响。

## 22. 风险与对策

| 风险 | 对策 |
|---|---|
| Redis 丢任务 | AOF（RPO≤1s）+ 真相在 Postgres + PG→Streams 重建程序演练 |
| 轮询风暴 | 快照优先 + min_refresh_interval + retry_after + 自适应退避 + 渠道级查询熔断 |
| AI 生成幻觉配置 | 输入不可信声明 + 校验器 + 预览（渲染样例/diff）+ dry-run 四道闸，产物不直写生产 |
| 治理面板变旁路 | SSO+RBAC+职责分离；同一校验器；高危双人复核；审计 append-only |
| SSRF/凭证泄露 | 请求时刻 URL 校验 + 禁重定向 + resolve-and-pin + egress 代理；header 透传不落盘 + 强制脱敏 |
| unknown 积压 | 两态拆分 + confirm_strategy + 巡检 + 阈值告警 + dead_awaiting_confirm 人工收敛 |
| 上游限流 | 渠道并发 Redis Lua + 429 AIMD 减半 + retry_after 透传 + 限流重试不烧 attempts |
| 状态翻转致资损 | 终态迁移只允许一次（条件更新）+ 每日对账 New API tasks/消费日志 |
| scheduler/inspector 单点 | 各自单副本+就绪探针+停顿告警+恢复分批放出；故障域隔离，互不染指 |
| 数据合规 | 桶级加密 + 留存策略 + 删除权级联 + 日志摘要留存期 |

## 附录 A：名词

- **受理（accepted）**：已持久化未调上游。
- **submit_unknown**：提交响应超时/丢失/5xx，上游可能已创建；由 compensate 按 confirm_strategy 确认驱动，禁自动重发（manual_only 永不自动重发）。
- **poll_unrecognized**：任务确定已存在但轮询响应判不出终态；保留 upstream_task_id，模板勘误后恢复轮询，**永不触发创建**。
- **死信（dead）**：重试耗尽的终态任务；**dead_awaiting_confirm**：unknown 超上限/超存活期的归宿，人工确认后重放或关闭。
- **孤儿任务**：上游已创建但网关侧失联，由 compensate/list 对账消除；orphan callback 先暂存对账表。
- **envelope**：网关自有调用方的响应包装：`raw_status`（上游原生状态）+ `terminal`（in_progress/succeeded/failed）+ `degraded[]`（能力降级声明）；面向 New API 的响应为上游原生形状，不用 envelope。
- **capabilities**：模板声明的上游能力集（upstream_idempotent/confirm_strategy/cancel/callback/list_tasks/max_payload_bytes/permanent_result_url/rate_limit_side_effect_free/per_key_isolation）。
- **confirm_strategy**：submit_unknown 的确认方式：query_by_client_key / list_and_match / manual_only。
- **alias**：渠道绑定的上游标识，URL 第二段；带命名空间归属。
- **URL 直配**：不经模板、按约定直接拼上游路径的过渡接入方式，限期下线。
- **compensate**：不确定任务的确认补偿任务，是不确定场景唯一允许重发创建的入口。
- **三级配置面**：极简/差异/完整，能推导的不手写。
- **派生幂等键**：`key_hash + 请求体规范化哈希`（重试 `{key}#attempt{n}`）。
- **cancel_requested**：上游不支持取消时的降级标记，以轮询终态收敛。
- **平台差异补丁**：平台内置的逐上游能力/方言修正（如 Seedance 附加 upstream_idempotent:false + confirm_strategy:manual_only）。
- **归档**：async_task 月度分区 DETACH 转同库 archive schema 独立表；热表活跃任务 / archive schema 归档任务；留存期内可回读，到期 410。
- **状态输出契约**：网关对计费的全部责任——真实、单调收敛不翻转、重复查询一致。
- **约定型上游**：创建响应可取 id、查询=创建路径+`/{id}`，支持极简模板。

---

## 附录 B：修订对照表（评审 v1.3 → v3.0）

| 评审条目 | 落实章节 | 一句话说明 |
|---|---|---|
| P0-1 | §4.1/§8.2/§12.2/§14 | unknown 拆 submit_unknown/poll_unrecognized 两态；capabilities 增 confirm_strategy 三值，manual_only 永不自动重发创建 |
| P0-3 | §11/验收9 | 透传认证模型写明（per_key_isolation 前提）；归属校验为可选纵深，未命中统一 404 |
| P0-4 | §18.1/§11 | 请求时刻最终 URL 校验（含结果拉取/转存）、禁重定向、插值净化、resolve-and-pin、直配 kill switch |
| P0-5 | §12.2 查询面/§22 | 快照优先 + per-task min_refresh_interval（3–5s）+ retry_after + 渠道级查询熔断 |
| P0-6 | §18.8/§13/验收4 | 数据合规子节：桶级加密、留存策略、删除权级联（410）、日志摘要留存；与归档回读调和 |
| P0-7 | §18.4 | Task-admin SSO+MFA+RBAC 四角色+职责分离+高危双人复核，内网仅网络纵深 |
| P0-8 | §13/§12.3/§8.3 | async_task 增 template_alias/template_version；热更新限定勘误；灰度=渠道维度绑版本 |
| P0-9 | §17 容量表/§20/验收8 | 容量目标表结构留位；压测左移 M3 基线、M6 满容量验收 |
| A1 | §14/§8.2/验收3/M3 | accepted 悬挂 scheduler 巡检条件更新重投，故障注入含此用例 |
| A2 | §14/§13 | accepted→cancelled 迁移；submit 成功后强制检查 cancel_requested 立即触发取消 |
| A3 | §13/§8.2/§14 | orphan_callback 暂存对账表；upstream_task_id 单列索引 |
| A4 | §13/§8.1/§14 | deadline_at 受理时写入、scheduler 巡检判 timeout；上游 expired 映射进 timeout |
| A5 | §8.2/§14/§18.4 | unknown 归宿统一 dead_awaiting_confirm；重放第一步强制确认查询并记审计 |
| A6 | §14/§8.1 | 内部状态机简化 accepted→upstream_submitted→in_progress→终态，queued/running 注明为上游镜像 |
| A7 | §13/§14 | callback_event 去重表（event_id 优先，缺失取任务+状态+摘要），命中直接 200 |
| A 另（写路径） | §13 | 各写路径合法前置状态表（cancel/submit/推进/store_result/next_poll_at/compensate） |
| B0 | §4.2/§16/§12.2/验收3 | §4.2 整节删记账动词改写状态输出契约；转存永久失败报 succeeded+结果不可用；单调不翻转写查询面 |
| B1 | §4.1 | 三级分类清单：连接建立超时（安全）vs 响应超时/丢失（须确认）；4xx 归入判失败 |
| B2 | §4.1 | 4xx 细分：401/403→渠道熔断告警、409→转确认、400/422→任务级失败 |
| B3 | §4.1/§12.2 | 429 不消耗业务 attempts（独立退避计数）；capabilities 增 rate_limit_side_effect_free |
| B4 | §4.1/验收2 | 不变量限定"不确定场景"；failed 重试经 submit_upstream 重建，派生键 {key}#attempt{n} |
| B 另（错误码） | §13 | error_code 九值取值域；retryable 由三级分类推导禁手配 |
| C0 | §1.1/§9/§12.1/§12.5/§18.2 | 凭证分层写死：数据面 header 透传不落盘 / 控制面 vault；credential_ref 仅控制面 |
| C1 | §18.2/§16 | 透传模式下失效并注明：key 粘性与归因=渠道↔key 天然对应，归 New API 渠道侧 |
| C2 | §17/§12.4 | header 强制脱敏清单（OTel 剔除/错误响应反扫/dry-run 脱敏/RBAC 可见/预览不输出凭证） |
| C3 | §11/§18.3 | 回调 /callbacks/{opaque_token}；kid 双密钥轮换+runbook；去重表兜底窗口内重放 |
| C4 | §12.3/§12.4/§18.6/§5 | JSONPath 严格子集沙箱+超时+大小上限；AI 输入不可信；预览渲染样例+diff；CODEOWNERS 双人 approve |
| C5 | §18.4 | 高危操作二级管控双人复核+影响面展示；重放前置幂等检查；批量上限+限速 |
| C6 | §13/§18.5 | audit_event append-only/WORM+哈希锚定；DB 撤销 UPDATE/DELETE；alias 命名空间同返 404 |
| C7 | §18.7/§12.2 | 结果引用短 TTL 预签名 URL（TTL 进 result_policy）；worker 固定 egress/出口代理复验 |
| C 另（凭证双源） | §12.1/§12.5/§18.2 | 透传模式消除双源：模板不含数据面凭证，渠道持证为单一来源 |
| D1 | §15 | 渠道上游时长直方图自适应轮询：首 poll=P10、随等待/P50 拉长、429 AIMD 倍增 |
| D2 | §10/§19/§20 | scheduler 统一单副本+就绪探针；恢复按 next_poll_at 分批放出；停顿告警；M3 注入用例 |
| D3 | §10/§15/§19 | store_result 独立队列/Deployment/HPA；HPA 复合指标+缩容冷却；poll 硬超时 10s 重排 |
| D4 | §10/§12.2/§19 | 渠道分组池化（heavy-poll/light-poll/灰度隔离），模板 pool 字段默认共享池 |
| D5 | §4.3 | 渠道并发 Redis Lua check-and-incr + PG 30s 校准 + 429 桶减半暂停派发 |
| D6 | §13/§17 | async_task 月度分区 DETACH 归档；对象生命周期默认 30 天租户可配（410）；存储成本入指标 |
| D7 | §17/§7/§16 | SLI/SLO 表+分渠道健康度+灰度自动门控（基线+2σ）+每日对账 New API tasks+阈值双层 |
| D8 | §19/§20 | RPO≤1s 写明；PG→Streams 重建程序并演练；只读副本隔离看板；重放限速；M3 PG 切换用例 |
| E1 | §16/§4.2/§12.2 | §16 瘦身改状态输出契约：原生形状保真硬约束、envelope 仅自有调用方、每日对账、配置面以 base_url 为限 |
| E2 | §12.2/附录A/验收7 | capabilities schema 全字段+per_key_isolation；envelope 增 degraded[]；"方言补丁"改"平台差异补丁"入附录 |
| E3 | §12.2/验收4 | 终态后 store 模式读 result_ref（410 语义）；passthrough 须 degraded 声明直链有效期 |
| E4 | §13/§12.4/§14 | origin(user/dry_run/replay) 字段；dry-run 收敛路径进状态机（cancel→清理→归档/打标记等终态） |
| E5 | §14 | 孤儿发现：list_tasks 对账；不支持则 next_poll_at 停滞+心跳缺失转 submit_unknown |
| E6 | §4.1/§13/验收2 | 派生幂等键 key_hash+请求体规范化哈希，时间窗去重；验收 2 口径同步 |
| F1 | §20 | M2 出口加回调验签+防重放；M3 加状态输出契约联调+性能基线；M6 满容量验收 |
| F2 | §20 | M3 故障注入场景清单（7 项）逐项验收 |
| P2-1 | §4.1 | connect timeout（安全）vs 响应超时/丢失（须确认）显式区分 |
| P2-2 | §9 | 审计事件清单补全：凭证引用变更/灰度切换/验签失败/结果回读导出/dry-run 执行 |
| P2-3 | §11/§4.4/§20/验收10 | 直配独立指标+按渠道开关+一键关闭+下线 milestone（M6 验证） |
| P2-4 | §17/§18.3 | 验签失败分渠道/IP 限流+分钟聚合样本+联动自动封禁 |
| P2-5 | §4.4 | §4.4 改引用式写法：一句决策+详见正文，消除双写漂移 |
| P2-6 | 附录A | 术语补全：unknown 两态/死信/envelope/raw_status/terminal/capabilities/alias/直配/compensate/归档/cancel_requested/平台差异补丁等 |
| P2-7 | §17 | SLI/SLO 与容量目标表结构留位（含"目标值待定"标注） |
| P2-8 | §13/§19 | audit_event 独立表分离；看板聚合走只读副本/预聚合表 |
| v3 收尾-1 | §4.1/§8.2/§13/§14/§20/验收3 | accepted 悬挂 vs compensate 唯一入口矛盾修复：submit_started_at 提交意图标记（调上游前条件更新+attempts 预递增）；巡检分流（无意图→条件重投/有意图→submit_unknown 禁盲建）；M3 清单第 8 项拆两子项 |
| v3 收尾-2 | §4.2/§12.2/§14/§16 | 内部终态对外输出路径：timeout/dead/dead_awaiting_confirm 按模板映射重写为上游原生失败类终态+error_code 原生格式（形状保真、退款可触发）；转存永久失败=原生成功终态+结果 URL（410 语义），M3 联调验证 adaptor 不误判 failed |
| v3 收尾-3 | §10/§19/§14/§17 | 巡检拆独立 inspector 进程（独立 Deployment 单副本、与 scheduler 故障域隔离）；scheduler 只留延迟轮询调度 |
| v3 收尾-4 | §19 | RTO 定值 ≤15min，与 PG→Streams 重建程序季度演练挂钩验证 |
| v3 收尾-5 | §12.3/§14 | poll_unrecognized 恢复：终态判定/提取表达式勘误允许 in-place patch（仅改判定逻辑，单列审计），其余变更仍版本化 |
| v3 收尾-6 | §13/§14 | submit_unknown 可取消：cancel 前置条件补 submit_unknown（仅置 cancel_requested）；compensate 重发/补 ID 前强制检查，置位转人工关闭 |
| v3 收尾-7 | §14 | 迁移白名单显式化：upstream_submitted→终态合法（快速任务回调直达场景） |
| v3 收尾-8 | §14/§4.4 | poll_unrecognized 有界化：unknown 上限/最大存活→dead_awaiting_confirm 与 deadline 巡检均覆盖，勘误不至不无限悬挂 |
| v3 收尾-9 | §14/§12.2 | 终态冲突仲裁：主源模式下兜底源终态判定须与主源仲裁或经确认窗口才落库，防兜底误判永久锁死正确回调 |
| v3 收尾-10 | §15 | 自适应轮询参数表：间隔公式 clamp(base×2^⌊elapsed/P50⌋,3s,60s)、冷启动 poll_initial_interval 默认 5s、Redis 桶计数滑窗 7 天、429 AIMD ×2/-10% |
| v3 收尾-11 | §4.3 | 并发计数细化：校准只纠泄漏方向（或 epoch 单调）；受理速率配额与上游并发槽分离；渠道+租户两级键结构与限额配置位置 |
| v3 收尾-12 | §4.1/§13 | 派生幂等键：删"默认值显式化"不可实现表述，仅对模板声明字段集规范化；去重窗口进策略组默认 24h；idempotency_key=窗口期内唯一 |
| v3 收尾-13 | §12.2/§13 | list_and_match 可行性前提：上游 list 记录须含可匹配请求属性（字段集逐上游确认进 capabilities），否则降级 manual_only；近 10 分钟/3 页时间界；多结果歧义转人工 |
| v3 收尾-14 | §13/附录A | 分区归档落地：DETACH 转同库 archive schema 独立表；DETACH 前置=无非终态行，滞留长任务随下一周期归档；留存期查询路由热表未命中查归档；删除权在归档侧 DELETE 可接受 |
| v3 收尾-15 | §18.2/§4.1/§16 | P1：per_key_isolation × 多 key × 派生幂等键兼容性前提——要求渠道单 key 或任务级 key 粘性，多 key 轮换致 poll 404 与 key_hash 漂移失效；列入 M2/M6 联调出口 |
| v3 收尾-16 | §4.1/§18.3/§18.5/§18.8/§17/§18.7/§14 | P2 六项：401/403 熔断反馈回路；opaque_token 泄露轮换/吊销 runbook；审计只存引用/哈希声明；灰度门控最小样本量护栏；代理读 Range 透传；timeout 胜出后上游成功=平台白付不对称代价声明 |
| v3 收尾-17 | §20/附录B | M3 故障注入清单去重：worker 崩溃窗口两子项并入一项（8→7 项），消除验收重复计数 |
| v3 收尾-18 | §13/§17 | attempts 口径统一：定义=提交尝试次数（含意图预递增，每次 submit_upstream 恰好 +1），指标/告警按此口径 |
