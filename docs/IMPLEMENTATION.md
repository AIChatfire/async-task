# 落地说明（架构文档 → 代码）

> 对应架构文档：`docs/async-gateway-architecture-v3.md`（v3.0）
> 代码根目录：`src/async_gateway/`
> 测试：`tests/`（199 项，全绿；其中 5 项为 Redis 真机用例）

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

本地最小闭环不依赖任何外部服务：测试环境注入内存 broker / 内存 request store、
假上游用 `httpx.MockTransport` 注入；**对象存储完全不参与**（未配置 ⇒ 转存自动关闭，见 §3.16）。

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

### 3.1 受理默认**不等上游、立刻 202**（`AG_SUBMIT_MODE=queued`；2026-09-21 反转裁定）

文档同时要求两件互相拉扯的事：

* §7「任务受理 → 返回**受理响应（上游原生形状）**」、§16「响应形状保真：面向 New API 的 create
  响应必须是上游原生形状（否则 New API 的上游 adaptor 解析不出上游 task id）」；
* §10「worker 在另一个进程里调用上游」＋ 异步网关的解耦目标（上游受理慢不能拖住受理）。

两次落地的结论不同，根因是"响应形状保真"被重新拆解为两件更小的事——**可解析**（调用方
必须拿到一个能回查的 id）与**可判定**（查询响应必须带得起状态词）。据此的现行裁定：

* **默认 `queued`（2026-09-21 起）**：受理只落库 + 入队，**立刻返回 202**
  `{"id", "task_id", "status": "queued"}`（`id`/`task_id` = **网关任务 id**），上游 create
  由 worker 后台完成（凭证走 §3.2 的短生命周期存放）。New API 两族插件都接得上：
  ark 系（`parseSubmitResponse` 取 `body.id`）与 `generic-async-v1`（取 `task_id || id`）；
  随后按该 id 查询 —— 查询/取消面**上游 id 优先、网关 task id 兜底**（`routes._find_task`）。
  预创建期的查询响应由 `output._effective_status_value` 合成非终态取值 `queued`，
  否则空快照 + 无 status 会被 New API 判成 UNKNOWN。
  **代价（知情项）**：New API 侧存的是网关 task id（提交时上游 id 尚不存在）；
  收益是上游受理再慢（数十秒~分钟级）也不拖住受理应答。
* **`inline`（显式配置）**：受理请求内同步 create，凭证不出请求、响应天然是上游原生形状
  （200 + 上游 task id 原样透出）——适合"必须同步拿上游原生响应"的直连调用方。

> 变更史：09-19 版默认 `inline`（当时把"响应形状保真"理解为"必须是上游原生形状"）；
> 09-21 依据"提交必须不等上游（对齐异步网关解耦目标，治上游同步受理慢）"改为 `queued`，
> 并补齐三处配套：202 体可解析 id / 查询面网关 id 兜底 / 预创建期合成状态词。

### 3.2 数据面凭证的短生命周期存放（Redis 短存）

文档 §18.2 要求"网关不存 key、不落盘"，但 worker 之后的轮询必须用这个 key。二者字面上冲突。
落地裁定：

* 凭证进 **Redis**（内存语义），键 `cred:{task_id}`，**TTL ≤ 任务 deadline + 5min**，
  不写 Postgres、不落磁盘、不进日志；任务进终态立即删除；
* 「绝对不驻留」形态（worker 拿不到凭证、只有客户端请求能驱动透传）原由 `CREDENTIAL_CHANNEL=none`
  提供，2026-09-21 参数精简时随其它未接线开关一并移除；当前后端是 Redis（生产）/ 进程内（测试）；
* **`queued`（默认）下提交阶段的 create 在 worker 里跑，依赖这份短驻留凭证**；`inline` 下
  提交阶段完全不依赖它（凭证不出请求），只有后台轮询/补偿/转存用得到。

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

### 3.11 真机联调（火山方舟图生 3D）抓到的三个缺陷（都已修）

2026-09-20 用真实方舟 `doubao-seed3d-2-0-260328` 经网关跑通一次（脚本
`scripts/live_seed3d.py`；详见 `docs/newapi-task-plugin-integration.md` §8）。三个缺陷都只在
"真实上游 + 真实产物"下才会暴露：

| 缺陷 | 后果 | 触发条件 |
|---|---|---|
| 结果字段照抄 New API 插件 README 的 `$.content.url` | 任务 succeeded 但"结果 url not extractable" | 直连方舟时产物在 `content.file_url`（`content.url` 是其 BFF 归一化后的形状） |
| 响应体入口脱敏把预签名 URL 的签名抹成 `***redacted***`，而快照就是转存取 URL 的唯一来源 | 转存永久失败 → `result_degraded=['transfer_failed']` → 结果端点 410 | 上游结果 URL 带签名查询串（TOS 预签名，踩中 `_SECRET_SHAPES` 的 32+ 字符兜底规则） |
| 结果**文件**拉取上限与上游 API 响应上限共用硬编码 4 MiB | 41 MB 产物必然"result too large"，同样落到 410 | 3D/视频类产物本体远大于 JSON 状态响应 |

修复要点（都在配置面与数据面，不改变对外契约）：

1. 模板 `result_location` 用实测值（提取表达式属纯勘误，可就地修订、存量任务即时恢复）。
2. `UpstreamResponse` 保留一份**未脱敏**原始体（`raw_json_body`）；写快照时只对
   **结果字段**回填原始值，其余字段与一切对外输出仍走脱敏（§17 C2 防护不变）。
3. 新增 `AG_RESULT_MAX_BYTES`（默认 256 MiB）与 `max_response_bytes`（JSON 响应，4 MiB）解耦；
   大小超限判为**不可重试**的确定性失败。

第 2、3 条经**变异验证**：临时移除修复后回归用例确实变红。注意假上游也必须校验签名
（`FakeUpstream.result_expected_signature`）—— 否则 `MockTransport` 只看路径，
"签名被抹"这一缺陷在测试里测不出来（这一点是第一次变异验证失败才发现并补上的）。

### 3.12 方舟系模板的路径口径与 New API 任务插件对齐

New API 的任务插件是**单文件 JS**，上游路径是**硬编码**的
（`volcengine-ark-3d`：`ctx.baseUrl + "/api/v3/contents/generations/tasks"`）；插件作者只能通过
渠道 base_url 换上游，改不了这段后缀。而网关按 alias 之后的剩余路径与 `create_path` **逐字**比对归位。
因此方舟系模板统一采取「`base_url` 收到站点根 + `/api/v3` 并进 `create_path`」，
让「渠道 base_url 指向网关 → **零改插件复用**」与「直连上游 URL 不变」同时成立。

`tests/test_templates.py::test_ark_templates_align_with_newapi_plugin_path` 对**全部**方舟系模板
（`volc-seedance` / `volc-seed3d`）守住这条约束 —— 新增同族上游时若口径写错会立刻变红。

⚠️ 可复用的只有**同构**插件：`aivideomaker`、`senseaudio-video` 的**入站**契约虽然也是方舟原生格式，
但其**上游**是各自第三方服务（`/api/v1/generate/{model}`、`/v1/video/create`），
把渠道 base_url 指向本网关后它们要的路径不是方舟路径，因此**不能**用来接方舟上游。

### 3.13 结果策略默认取 `passthrough`（先不转存）

**裁定**：`ResultPolicy.mode` 的全局默认改为可配项 `AG_RESULT_MODE_DEFAULT`，**当前默认 `passthrough`**
（不转存、结果字段直接给上游直链）。理由：转存链路依赖两件在本环境**尚未验证**的事 ——
对象存储（MinIO 未连真机、预签名与桶加密未实测）与"存储地址对调用方可达"（New API 会对结果 URL
及其每次重定向做 SSRF 校验），而上游产物常见几十 MB。先直链让链路一次跑通，
待对象存储验完再**按模板/渠道逐个**显式切回 `store`。

实现要点（两处都踩过）：

1. `templates/derive.py` 用 `model_fields_set` 区分"真的没写 `mode`"与"写了 `mode: store`"——
   否则一旦默认切到 passthrough，模板再想显式切回 store 也会被全局默认盖掉（**切不回去**）。
2. `gateway/routes.py:get_result` 的 passthrough 判定**必须前置**在 `result_ref is None` 之前：
   passthrough 下 `result_ref` 永远为空，判定若在其后，成功任务会拿到 `202 转存中`，
   调用方会无限重试一个永不发生的转存。已修，并有变异验证过的回归用例
   （`test_passthrough_keeps_upstream_url_and_results_endpoint_is_409`）。

**代价（必须知情）**：结果链接依赖上游有效期（火山 TOS 预签名 24h），且上游直链会暴露给调用方
（envelope `degraded[]` 会声明 `passthrough_result`）。若要求"对外只给自有链接"，
则需要转存链路，即回到 `store` 并先解决对象存储侧的验证项。

测试基座（`tests/conftest.py`）显式把默认设为 `store`，以保持既有用例"成功即转存"的语义；
"默认取配置"这条逻辑由 `test_result_mode_default_comes_from_settings` 单独覆盖。

### 3.14 结果获取不走独立端点（已移除 `/results/{task_id}`）

原实现里 store 模式把结果字段重写为 `{AG_CALLBACK_BASE_URL}/results/{task_id}`，再由该端点 302 到
对象存储的预签名 URL。它同时踩了三条：

| 问题 | 说明 |
|---|---|
| 违背动态路由原则 | `gateway/routing.py` 开篇即是"网关**不能**靠固定端点表路由，而要把剩余路径拿去和模板对照"；而它是全体系**唯一**的固定端点 |
| 用内部主键当能力 URL | 路径参数是 `async_task` 主键，该值还会经 `X-AG-Task-Id` 响应头外露 ⇒ 可被枚举取件 |
| 归属校验形同虚设 | `x-ag-channel` 带了才校验、不带直接放行 ⇒ 匿名可读；与 §18.x「代理读须带归属校验」不符，也与 New API 插件的 `credentialless` 回源**直接冲突**（加严校验会让插件取件 404） |

**裁定**：按 §12.2 首选改为**结果直给** —— store 模式下查询响应里的结果字段就是对象存储的预签名 URL
（`routes._presigned_result_url` 由 `result_ref` 现算）。网关对外**只透传上游原生路径**
（提交/查询/取消），不再发明任何结果路径。连带变化：

- 移除 `RESULT_ENDPOINT` / `result_endpoint_url`；`build_native_query_response` 的入参由
  `public_base_url` 改为 `result_url`（调用方预先算好，函数保持纯同步，便于测试）；
- "结果不可用"不再靠 410 表达：**结果字段留空** + `_result_hint`，语义由 envelope 的
  `degraded[]`（`result_pending` / `result_unavailable`）承担；
- 附带消掉一处语义混用：结果 URL 原先借用 `AG_CALLBACK_BASE_URL`（**回调**基址）拼接，
  现已不需要该基址 —— 它现在只用于回调地址拼接；
- 相关用例改写为"从查询响应取结果"：`test_store_mode_query_gives_presigned_url`、
  `test_store_mode_result_field_empty_when_transfer_failed`、
  `test_passthrough_keeps_upstream_url_in_query_response`（并断言该路径已 404）。

若将来确有"网关代理读"需求（Range 透传 / 隐藏桶），须以**带归属校验**的独立形态重新引入
（§12.2 / §485 的备选），不得回到匿名 + 内部主键的形态。

### 3.15 env 契约：模板 / 代码 / 编排三向对齐（2026-09-20）

| 方向 | 发现 | 处置 |
|---|---|---|
| **代码读、模板没登记** | `Settings` 共 75 个字段，`.env.example` 只登记 44 个 ⇒ **31 个开关运维根本不知道存在**（含 `AG_QUEUE_DRIVER`、`RESULT_STORE_MODE`、`AG_SSRF_DENY_PRIVATE`、`AG_MIN_REFRESH_INTERVAL`、`CANARY_*` 等），照模板配永远用默认值 | 模板补齐为**逐字段对齐的权威清单**，并在顶部列出「生产部署必改项」 |
| **模板有、编排不注入** | `docker-compose.yml` 的 `x-app-env` 只显式列 10 项、且**没有 `env_file`** ⇒ 其余 **34 项**照模板配了在容器里**全部不生效**（不报错、不告警） | 给 `x-app-build` 加 `env_file: [{path: .env, required: false}]`（app 服务经锚点继承）；那 10 项改成 `${XXX:-默认}` —— `environment` 优先级**高于** `env_file`，不这么写 `.env` 覆盖不了 |
| **形态陷阱** | `KEY=  # 注释` 会被解析器把注释整段当成值（fail-open；空值语义静默失效） | 门禁静态扫该形态；模板约定「留空就写 `KEY=`，要注释就写 `KEY=值  # 注释`」 |
| **空串顶默认值** | 模板里留空的项，若代码默认非 `None`，显式空串会把默认值顶掉 | 门禁断言「模板留空项 ⇒ 代码默认必须是 `None`」 |

门禁：`tests/test_env_contract.py`（5 项）—— 双向键一致 + 空值形态 + 空值回落 + compose 注入 + compose 可覆盖。
**回放验证**：用上一版模板跑同一判据 ⇒ 漏登记 31 个（证明门禁不是空转）。
另把 `pyyaml` 显式加进 dev 依赖（门禁要解析 compose，别依赖间接包）。

**未验证**：本机**无 Docker**，`docker compose config` 跑不了 ⇒ 容器路径只做了**静态**断言
（证明"声明的键会被注入"），**没有**做运行态对差（`docker inspect` 实注入 env）。生产不以这份
compose 部署（生产为 K8s）。

**附带（同日）**：`AG_S3_ENDPOINT` / `AG_S3_BUCKET` 的**代码默认值**改为外部对象存储
（`https://oss.s3ai.cn` / `cdn`；`S3_SECURE` 已于 09-21 移除、改为按端点写法推导），取消"模板给生产值、
代码默认给本机值"的两套口径；**凭据默认值改为空串** —— 不留 `minioadmin` 这类假默认。
（**2026-09-21 更新**：compose 里的本地 MinIO 已删除；转存只走外部、未配置即自动关闭，见 §3.16。）

冒烟脚本：`scripts/smoke_workers.py`（起 scheduler 2 tick → inspector 1 tick → worker 消费并 ACK 一条消息，
走真实 Redis Streams 消费组）。实测输出：`SMOKE OK`。

### 3.16 架构精简：对象存储降级为可选件、配置去前缀（2026-09-21 裁定）

用户指令：「**转存只走外部 minio、不配置自动不转存、精简架构；配置参数优化，环境变量去 `AG_` 前缀**」。
落地五件：

1. **受理不再依赖对象存储**：create 请求体与受理/上游响应存档（原 `requests/{tenant}/{task_id}/…`
   对象）迁到 **Redis 短生命周期存放**（`infra/request_store.py`，键 `req:` / `reqresp:`，TTL 分别按
   任务 deadline 与幂等窗口）——`queued` 语义要的"跨进程共享"交给 Redis（它本来就在关键路径上）。
   收益：本地/单机部署不用起 MinIO。代价：Redis 数据丢失时提交**显式失败**（新错误码 `REQUEST_LOST`，
   不可重试），不再拿空体去调上游（空体只会被上游判成客户端的错）。
2. **对象存储只服务结果转存、只走外部**：`get_result_store()` 在 `s3_configured`（端点/凭据/桶四项
   齐全）为假时返回 `None` ⇒ **转存自动关闭**：不派发 transfer、不重试、不告警；查询响应保留上游直链，
   并在 envelope `degraded[]` 加 `transfer_disabled` 声明（不假装转存过）。
3. **compose 精简**：删掉 `minio` / `minio-init` 两个容器与 `miniodata` 卷；要转存就在 `.env` 配 `S3_*`。
4. **配置参数精简**：去掉 13 个**从未接线**的开关（`TRANSFER_MAX_INLINE_BYTES`、
   `POLL_HISTOGRAM_WINDOW_DAYS`、`POLL_RECOMPUTE_SECONDS`、`POLL_HARD_TIMEOUT_SECONDS`、
   `QUERY_429_CIRCUIT_RATIO`、`QUERY_429_CIRCUIT_WINDOW_SECONDS`、`CREDENTIAL_CHANNEL`、
   `CALLBACK_RATE_PER_MINUTE_PER_CHANNEL`、`ADMIN_REQUIRE_APPROVAL`、`CANARY_*`×3、
   `CONCURRENCY_CALIBRATE_SECONDS`）、合并 2 个（`S3_SECURE` ⇒ 按端点写法推导；
   `RESULT_STORE_MODE` ⇒ 后端固定对象存储、测试注入内存替身）、`AG_RESULT_MODE_DEFAULT` 去掉未实现的
   `redirect`。**未接线就不该出现在权威清单里**——那正是 §3.15 要防的"配了不生效"。
5. **环境变量**：`Settings.env_prefix` 一度改为空（"去前缀"），**同日复核后恢复 `AG_` 前缀** ——
   部署环境常导出同名的通用变量（`DATABASE_URL`/`REDIS_URL`/`LOG_LEVEL`…），无前缀会被静默读到并顶掉
   默认值，前缀即隔离。（镜像自证标签 `AG_IMAGE_VERSION` 是制品元数据、非配置项，照旧保留。）

**未验证**：本机**无 Docker** ⇒ 精简后的 compose 只做静态断言（`test_env_contract` 的 yaml 解析）；
Redis 请求存储的真实链路（多进程 create body 交付）需在容器/真机上复验。

---

## 4. 未验证 / 待确认（评估可用性前必读）

### 4.1 本环境没有的依赖（因此没验）

| 项 | 状态 |
|---|---|
| **Postgres** | 本机没有。全部测试跑 SQLite。`alembic/versions/0001_initial.py` 未执行过；`scripts/partition_async_task.sql`（月度分区 + DETACH 归档）**未执行**，是按文档产出的运维工件，必须先在 PG 上演练 |
| **MinIO / 对象存储** | ✅ 2026-09-20 已连真机并跑通 **store 端到端**（`oss.s3ai.cn` / 桶 `cdn`）：真实转存 42.4 MB 产物 → `result_ref=20260920/{tenant}/{task_id}/attempt-1.bin` → 查询响应里结果字段 = 该对象的**预签名 URL** → `Range` 取回 **HTTP 206**；另改为**不自动建桶**（见 `test_minio_store_does_not_auto_create_bucket`）。🔻**桶读权限已确认保持公开**（`Principal:*` 的 `s3:GetObject` + `s3:ListBucket`）⇒ 转存产物属**公开资源、且可被枚举**，此为**有意取舍**（勿放敏感产物）。SSE-S3 桶加密、留存到期清理仍未接线 |
| **Docker / K8s** | 本机无 docker CLI，`docker compose up` 未跑过；`Dockerfile` 尚未补（compose 已引用）。HPA 复合指标、探针、单副本约束都只是配置声明 |
| **Logfire** | 未配 token；`logfire.configure` 分支未执行。指标走内置注册表 + `/metrics`（Prometheus 文本格式），未接真实采集 |
| **真实上游（火山方舟）** | ✅ 2026-09-20 已**完整端到端**验证：`doubao-seed3d-2-0-260328` 图生 3D 经网关跑通两次（端到端 4m38s / 4m48s）——受理原生形状 → 轮询 → 终态 → 结果字段（含签名）→ 直链抽检 `HTTP 206` / 42.4 MB 可取，当前默认 `passthrough`。脚本 `scripts/live_seed3d.py`，记录见 `docs/newapi-task-plugin-integration.md` §8。**其余上游（Seedance 视频等）仍未真机跑过**；`store`（转存）路径的真机验证只有一次（见 §3.11），且对象存储未连真机 |
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
| 1 | 新约定型上游 = 4 行模板 + 渠道持证 + dry-run，零代码 | ✅ 代码零改动；第二个上游 `volc-seed3d`（图生 3D）就是 4 行模板接入，且已按 New API 任务插件的拼接口径对齐并经真机跑通（`tests/test_volc_seed3d.py`） |
| 2 | 同幂等键同 attempt 只产生一个上游任务 | ✅ `test_derived_idempotency_key_makes_resubmit_a_replay` |
| 3 | 重启不丢任务；accepted 悬挂按提交意图分流；创建超时进 unknown 且补偿收敛；状态输出单调不翻转 | ✅ 分流两子项 + 单调性 + 重复查询一致均已测；**重启不丢**只覆盖到"Redis 丢消息 → scheduler 重投"这一层 |
| 4 | store 模式留存期内结果可回读；到期/删除后不可读 | ⚠️ 可回读已测（内存存储；结果在查询响应里直给预签名 URL）。**留存到期清理任务未实现**（无定时任务）；到期/删除后由对象存储侧失效，不再有 410 语义（见 §3.14） |
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
scripts/live_seed3d.py             真机联调：经网关跑一次图生 3D（含 New API 插件的同口径路径）
scripts/smoke_workers.py           后台进程冒烟（scheduler/inspector/worker 各一轮，真实 Redis Streams）
scripts/partition_async_task.sql   月度分区与 DETACH 归档（未执行，需演练）
```
