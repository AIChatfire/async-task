# New API 侧接入（Task Plugin 渠道）

> 结论先说：**两侧都不需要改代码**。New API 已有「任务插件」体系与官方插件
> `volcengine-ark-3d`；本网关已内置模板 `volc-seed3d.yaml`。要做的只有三处配置：
> 上传/激活插件、建一条 **Task Plugin** 渠道并把 `base_url` 指向网关、把真实上游 key 放进渠道。
>
> 本文档的路径口径与字段口径均经 **2026-09-20 真机实测**（见 §8）。

---

## 1. 为什么这么接：两层协议面是同构的

| 层 | 事实依据（源码/文档） |
|---|---|
| New API 任务插件 | `relay/channel/task/jsplugin/adaptor.go:863` 把 `ctx["baseUrl"] = info.ChannelBaseUrl`；插件 `buildSubmitRequest` 拼 `ctx.baseUrl + "/api/v3/contents/generations/tasks"`（`plugins/tasks/volcengine-ark-3d/1.0.1/plugin.js:140`） |
| 插件请求 URL 校验 | `pkg/jsplugin/request.go:ValidateRequestURL` —— 请求 host 必须等于**渠道 base_url 的 host**（除非插件声明 `meta.allowedHosts`） |
| 网关协议面 | `gateway/routing.py`：`/async/{alias}/{剩余路径}`，剩余路径与模板 `create_path` / `get_path_template` **逐字比对**归位为 create/query/cancel |
| 网关凭证模型 | 渠道持有的 key 随 `Authorization` 头透传，网关不落盘（`upstream/client.py`） |

也就是说：**把渠道 base_url 指向网关，插件拼出来的路径就变成网关的 rest 段**。
只要 `create_path` 与插件后缀逐字相同，链路即通。

---

## 2. 路径对齐（最容易出错的一处）

| 环节 | 值 |
|---|---|
| 渠道 `base_url` | `http://<gateway>:8000/async/volc-seed3d` |
| 插件硬编码后缀 | `/api/v3/contents/generations/tasks` |
| 插件实际请求 | `http://<gateway>:8000/async/volc-seed3d/api/v3/contents/generations/tasks` |
| 网关收到的 rest | `/api/v3/contents/generations/tasks` |
| 模板 `create_path` | `/api/v3/contents/generations/tasks` ✅ 逐字相等 |
| 网关发往上游 | `https://ark.cn-beijing.volces.com` + `/api/v3/contents/generations/tasks` |

因此内置模板刻意把 `/api/v3` 从 `base_url` 挪进 `create_path`：

```yaml
alias: volc-seed3d
base_url: https://ark.cn-beijing.volces.com
create_path: /api/v3/contents/generations/tasks
result_location: $.content.file_url
```

一条配置同时满足「直连上游正确」与「经 New API 拼接口径一致」。这条约束
有回归测试守着：`tests/test_volc_seed3d.py::test_newapi_plugin_concatenation_lands_on_create_route`。

> ✅ **方舟系模板口径已统一**：`volc-seedance`（视频）与 `volc-seed3d`（3D）都是
> 「站点根 + `/api/v3/...` 全路径」，因此方舟形态的插件只需把**渠道 base_url** 换到对应 alias
> 即可复用，插件侧零改动。该约束由
> `tests/test_templates.py::test_ark_templates_align_with_newapi_plugin_path` 对全部方舟系模板守着。
>
> ⚠️ 可复用的只有**同构**插件。插件仓库里另有 `aivideomaker`、`senseaudio-video`，它们面向客户端的
> **入站**路由同样是 `/…/api/v3/contents/generations/tasks`（方舟原生格式），但其**上游**是各自第三方服务
> （`/api/v1/generate/{model}`、`/v1/video/create`）。把渠道 base_url 指向本网关后，它们需要的路径
> 不是方舟路径 —— **不能**用它们来接方舟上游。

---

## 3. 配置步骤

### 3.1 上传并激活插件（Root 权限）

```bash
# 上传（source 为 plugin.js 全文；服务端会先编译校验再落库）
python - <<'PY' > /tmp/plugin_upload.json
import json
src = open("plugins/tasks/volcengine-ark-3d/1.0.1/plugin.js").read()
print(json.dumps({"source": src, "remark": "ark 3d via async-gateway"}))
PY

curl -X POST http://<newapi>/api/plugin/task \
  -H "Authorization: Bearer <access_token>" \
  -H "New-Api-User: <user_id>" \
  -H 'Content-Type: application/json' \
  --data-binary @/tmp/plugin_upload.json

# 激活
curl -X POST http://<newapi>/api/plugin/task/volcengine-ark-3d/activate \
  -H "Authorization: Bearer <access_token>" -H "New-Api-User: <user_id>"

# 核对注册结果（插件级编译/路由错误都在这里暴露）
curl http://<newapi>/api/plugin/task/runtime/status \
  -H "Authorization: Bearer <access_token>" -H "New-Api-User: <user_id>"
```

管理界面同样可以：**任务插件**页上传 → 激活。上传是管理员级信任决定，激活前请过一遍源码 diff。

### 3.2 建渠道（Task Plugin 类型）

```jsonc
// POST /api/channel/   —— 注意必须包一层 {"mode":"single","channel":{...}}
{
  "mode": "single",
  "channel": {
    "type": 10001,                                  // 插件 meta.channelTypes 声明的类型
    "name": "ark-3d-via-gateway",
    "key": "<真实火山方舟 key>",                      // 放在渠道里，不落网关盘
    "base_url": "http://<gateway>:8000/async/volc-seed3d",
    "models": "doubao-seed3d-2-0-260328",
    "group": "default",
    "setting": "{\"task_plugin_key\":\"volcengine-ark-3d\"}"
  }
}
```

要点：

* `type` 取插件声明的 `channelTypes: [10001]`（界面显示为 **Task Plugin**）；
* `setting.task_plugin_key` 是绑定字段（`relaykit/dto/channel_settings.go:14`，JSON 名
  `task_plugin_key`）；绑定需要 `TaskPluginBind` 权限；
* `base_url` 必须显式填写，且只需要写到 `/async/{alias}` 为止 —— 插件自己带路径；
* 最小侵入原则：这条渠道**只挂要测的那一个模型**，避免与既有渠道争抢同名模型。

### 3.3 调用

```bash
# 插件声明的原生路由
curl -X POST http://<newapi>/volcengine/3d/api/v3/contents/generations/tasks \
  -H "Authorization: Bearer <newapi token>" -H 'Content-Type: application/json' \
  -d '{"model":"doubao-seed3d-2-0-260328","content":[{"type":"text","text":" --subdivisionlevel high --fileformat obj"},{"type":"image_url","image_url":{"url":"https://ark-project.tos-cn-beijing.volces.com/doc_image/i23d_flower.jpeg"}}]}'
```

通用管理面（不依赖插件原生路由）：

| 用途 | 端点 |
|---|---|
| 提交 | `POST /v1/tasks/volcengine-ark-3d` |
| 查询 | `GET /v1/tasks/{taskId}` |
| 产物清单 | `GET /v1/tasks/{taskId}/artifacts` |
| 产物内容 | `GET|HEAD /v1/tasks/{taskId}/artifacts/{key}/content` |

---

## 4. 状态与结果的契约对齐

| 关注点 | 网关侧 | 插件侧 | 是否一致 |
|---|---|---|---|
| 任务 id | `id_location: $.id` | `parseSubmitResponse` 取 `body.id` | ✅ |
| 状态字段 | `status_field: $.status` | `statusResult` 取 `t.status \|\| t.task_status` | ✅ |
| 成功取值 | `succeeded` → SUCCEEDED | `succeeded` → SUCCESS | ✅ |
| 失败取值 | `failed` → FAILED；`expired` → 内部 timeout 后**重写为 `failed`** | `failed`/`expired`/`timeout` → FAILURE | ✅ 都收敛到失败 |
| 结果字段 | `result_location: $.content.file_url`（**实测值**）。成功时结果字段**直接可用**：`store` = 对象存储预签名 URL，`passthrough` = 上游直链，**均不经网关中转** | `resultURL()` 候选顺序含 `content.url`；artifact 从 data 提取 | ✅ |
| 产物类型 | 结果字段本身就是取件入口；网关**不提供**独立结果端点（只透传上游原生路径） | `listArtifacts` → key `file`、mime `model/gltf-binary` | ✅ |

> ⚠️ **`content.url` vs `content.file_url` 是本次实测纠正的一处口径差异**：
> 插件 README 记的是 `content.url`，那是其 BFF（`tasks.oneapis.cn`）归一化后的形状；
> **直连方舟**时产物在 `content.file_url`。照抄 README 会导致
> 「任务 succeeded 但结果取不到」（当时表现为转存永久失败 + 结果端点 410；该中转端点现已移除，
> 见 `IMPLEMENTATION.md` §3.14）。

网关侧「形状契约」意味着：New API 看到的**查询响应**始终是上游原生形状（envelope 只给网关自有
调用方），内部终态（timeout/dead/dead_awaiting_confirm）会按模板映射重写为上游原生失败类取值 ——
退款链路才触发。

> **2026-09-21 起受理默认 `queued`（不等上游、立刻 202）**：提交响应是网关形状
> `{"id","task_id","status":"queued"}`——`id`/`task_id` 为**网关任务 id**（提交时上游 id 尚不存在），
> 两族插件均可解析（ark 系取 `body.id`，generic-async-v1 取 `task_id || id`），随后按该 id 查询
> 即可命中（查询面：上游 id 优先、网关 task id 兜底；预创建期查询响应会合成 `queued` 状态词）。
> 需要"受理内同步创建、上游 id 原样透出"时，显式配置 `AG_SUBMIT_MODE=inline`。
> 配套说明见 `IMPLEMENTATION.md` §3.1。

---

## 5. 计费口径

插件 `extractUsage` 上报的用量是：`doubao-seed3d-2-0-260328` → `output_tokens: 1`／次，
`hyper3d-gen2-260112` → `30000`／次。

⚠️ **实测提醒**：真机返回的上游 `usage` 是 `{"completion_tokens": 30000, "total_tokens": 30000}`，
而插件**不采信**上游该字段、自行按 1 上报。若 New API 按 token 计费，会出现"上游按 30000 计、
网关按 1 计"的口径落差 —— 配价前请先确认这条链路按哪一侧结算。

| 口径 | 配置 | 换算 |
|---|---|---|
| 按 token 倍率 | `ModelRatio`（+ `CompletionRatio`） | ratio=1 ↔ $2 per 1M tokens |
| 按次 | `ModelPrice`（USD/次） | `quota = ModelPrice × 500000` |

商用前必须以 `GET /api/log/` 的 `quota` 实算核对（判断「配了但没生效」的唯一可靠证据是**日志里有没有记录**）。

---

## 6. 结果回源：当前默认**不转存**（passthrough）

`result_policy.mode` 的全局默认由 `AG_RESULT_MODE_DEFAULT` 决定，**当前取 `passthrough`**：
网关不下载产物，成功响应里的结果字段就是**上游直链**（火山 TOS 预签名，实测有效期 86400s）。
插件按 `content.file_url` / `resultURL()` 取到该直链后用 `credentialless: true` 回源 ——
TOS 是公网地址、不触碰内网，因此**不涉及**下面 6.1 的 SSRF 约束。

两条使用注意：

1. **没有"结果端点"可走**：网关只透传上游原生路径，结果一律从**查询响应**里取
   （即 `GET /async/{alias}/<上游 get 路径>` 的结果字段）。原先的 `/results/{task_id}` 已移除（现返回 404），
   理由见 `IMPLEMENTATION.md` §3.14。
2. **链接有效期由上游决定**：TOS 预签名 24h 过期，之后用户取不到产物。需要长期可回读，
   就必须切到 `store`（见 6.1）。

### 6.1 若切回 `store`：两个约束随之生效

```bash
AG_RESULT_MODE_DEFAULT=store     # 全局切回；或按模板显式 result_policy: {mode: store}
```

- **对象存储必须让 New API 可达**：插件回源用 `credentialless: true`，而 New API 会对
  **初始 URL 与每一次重定向**做 SSRF 校验。若对象存储是内网/回环地址（典型：MinIO 在内网）
  → 取产物失败。另外 `AG_CALLBACK_BASE_URL` 必须是 New API 能访问到的网关地址。
- **结果文件上限独立可配**（3D 产物实测 **41.2 MB**，曾因与 API 响应上限共用 4 MiB 而确定性失败）：

  ```bash
  AG_RESULT_MAX_BYTES=268435456    # 结果文件上限（默认 256 MiB）
  ```

  它与「上游 API 响应体上限」（4 MiB，用于 JSON 状态响应）是两件事，不要混用。
  超出该上限会被判为**不可重试**的确定性失败 → 任务保持 succeeded，结果字段留空 + `degraded[]` 声明。

> 切到 `store` 后，成功响应的结果字段就是**对象存储的预签名 URL**（网关按
> `result_policy.presign_ttl_seconds` 现算，默认 900s）；转存未完成或永久失败时该字段留空，
> 由 envelope 的 `degraded[]`（`result_pending` / `result_unavailable`）声明该状态。

---

## 7. 未验证项（评估可用性前必读）

1. **本机没有 New API 实例**：§3 的接口与字段取自源码（`relaykit/dto/channel_settings.go`、
   `pkg/jsplugin/request.go`、`relay/channel/task/jsplugin/adaptor.go`、`docs/plugin-api/README.md`）
   与插件源码，**未在真实 New API 上端到端跑过**。网关侧已按真机验证（§8）。
2. **计费口径未实测**：见 §5。
3. **取消**：插件未声明 cancel 路由；网关侧 `DELETE` 对 3D 的上游语义也未实测
   （补丁按 Seedance 口径标 `capabilities.cancel: false` → 降级为 `cancel_requested`，靠轮询收敛）。
4. **结果回源的 SSRF 边界**：见 §6.1，属部署期决定，未在真实网络拓扑上验证。

---

## 8. 真机实测记录（2026-09-20）

> 注：本次实测时受理语义为 `inline`（同步创建、返回上游原生形状）——下表"受理"一行描述的是
> `inline` 形态。2026-09-21 起默认改为 `queued`（立刻 202，见 §4 注与 `IMPLEMENTATION.md` §3.1）。

**被测链路**：`POST /async/volc-seed3d/api/v3/contents/generations/tasks`（即 New API 插件会拼出的同一路径）
→ 火山方舟 `doubao-seed3d-2-0-260328`，图片用方舟文档示例图，参数 `--subdivisionlevel high --fileformat obj`。

复现命令（零额外依赖，SQLite + 内存结果存储 + 内存队列）：

```bash
ARK_API_KEY=<key> make live-seed3d   # 或
ARK_API_KEY=<key> python scripts/live_seed3d.py --interval 10 --max-polls 50
```

| 环节 | 实测结果（两次独立任务一致） |
|---|---|
| 受理（inline 同步创建） | HTTP 200，返回**上游原生形状**：`{"id":"cgt-20260920142120-655rn","model":"doubao-seed3d-2-0-260328","status":"running",...,"execution_expires_after":172800}`。上游任务 id 形如 `cgt-<时间戳>-<后缀>` |
| 轮询 | 27–28 次查询后转终态，端到端约 **4m38s / 4m48s**。中途响应带 `x-ag-snapshot: cached` + `retry-after: 1`；终态 `x-ag-snapshot: terminal` |
| 状态映射 | 上游 `running` → 网关 `in_progress`（继续轮询，**未**误判 poll_unrecognized）；`succeeded` → `succeeded` |
| 查询响应形状 | 上游字段原样保留，参数被上游接受并回显（`subdivisionlevel: "high"`、`fileformat: "obj"`） |
| 结果字段 | **`content.file_url`**，TOS 预签名（`*.zip?X-Tos-Algorithm=…&X-Tos-Signature=…`），**签名以完整形式返回**（未被入口脱敏破坏） |
| 交付物可用性 | 对结果直链做 `Range: bytes=0-1023` 抽检 → **HTTP 206**、`content-type: application/zip`、总长 **42,374,044 B**（42.4 MB）——直链确实能取到产物 |
| 结果策略 | 默认 `passthrough`：`result_ref` 为空、`result_degraded` 为空（不转存，也没有"转存失败"） |
| 结果获取 | 结果一律从**查询响应**的结果字段取：实测 passthrough 下该字段 = 上游 TOS 预签名直链（签名完整）。当时另有 `GET /results/{task_id}` 返回 409，该端点现已移除（见 §6） |
| 上游 usage | `{"completion_tokens": 30000, "total_tokens": 30000}`（注意：插件自报 1，见 §5） |
| 上游消耗 | **2 个图生 3D 任务**（第二次为「修复后的完整端到端」补测）；其余复验均只读 |

> **端到端最终判定：通过。** 受理 → 轮询 → 终态 → 结果字段（含签名）→ 直链可取，全链路一次跑通。

### 本轮由此暴露并修复的三个真实缺陷

| # | 缺陷 | 症状 | 修复 |
|---|---|---|---|
| 1 | 结果字段照抄 BFF 口径 `$.content.url` | 任务 succeeded 但"结果 url not extractable" | 模板改为实测的 `$.content.file_url`（提取表达式属纯勘误，可就地修订） |
| 2 | 响应体入口脱敏把预签名 URL 的签名抹成 `***redacted***`，快照存的就是坏 URL | 转存永久失败 → `result_degraded=['transfer_failed']` → 结果端点 410 | 上游响应保留一份**未脱敏**原始体（`UpstreamResponse.raw_json_body`），写快照时仅对**结果字段**回填原始值；其余字段与对外输出仍走脱敏 |
| 3 | 结果文件上限与 API 响应上限共用硬编码 4 MiB | 41 MB 产物必然"result too large" | 新增 `AG_RESULT_MAX_BYTES`（默认 256 MiB）与 `max_response_bytes` 解耦；大小超限改判**不可重试**的确定性失败 |

三个缺陷都有回归测试（`tests/test_volc_seed3d.py`），其中第 2、3 条经**变异验证**：
临时移除修复后用例确实变红（因此"假上游"也加了签名校验，否则 MockTransport 只看路径、
测不出签名被抹）。
