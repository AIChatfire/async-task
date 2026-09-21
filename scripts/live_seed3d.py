"""真机联调：经异步网关提交一次火山方舟图生 3D（doubao-seed3d）。

验证的是"网关 → 真实上游"这一段：提交（inline 同步创建）、轮询、状态映射、
结果字段提取、面向 New API 的原生形状保真；并把 New API 任务插件
（``volcengine-ark-3d``）的拼接口径也走一遍，确认两边逐字对齐。

⚠️ 本脚本会**真实提交 1 个生成任务**，消耗上游额度。只读探测请改用
``GET /api/v3/models``。

用法::

    ARK_API_KEY=... python scripts/live_seed3d.py
    python scripts/live_seed3d.py --bearer ark-xxx --interval 10 --timeout 600

本地依赖：SQLite + 内存结果存储 + 内存队列（不需要 Postgres / MinIO / Redis）。
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from typing import Any

# 必须在导入 async_gateway 之前落定配置：derive.STRATEGY_KEYS 等在导入期只读一次。
os.environ.setdefault("AG_APP_ENV", "dev")
os.environ.setdefault("AG_DATABASE_URL", "sqlite+aiosqlite:////tmp/ag-live-seed3d.db")
os.environ.setdefault("RESULT_STORE_MODE", "memory")
os.environ.setdefault("AG_QUEUE_DRIVER", "memory")
# 请求体/限流器等本脚本单进程运行：清空 AG_REDIS_URL ⇒ 全部走进程内替身（不需要 Redis）
os.environ.setdefault("AG_REDIS_URL", "")
os.environ.setdefault("AG_MIN_REFRESH_INTERVAL", "0")
os.environ.setdefault("AG_LOG_LEVEL", "WARNING")
os.environ.setdefault("AG_CALLBACK_BASE_URL", "http://localhost:8000")

ALIAS = "volc-seed3d"
#: New API 任务插件 volcengine-ark-3d 里硬编码的路径后缀（渠道 base_url + 该后缀）
PLUGIN_PATH = "/api/v3/contents/generations/tasks"
MODEL = "doubao-seed3d-2-0-260328"
IMAGE_URL = "https://ark-project.tos-cn-beijing.volces.com/doc_image/i23d_flower.jpeg"

BUSINESS_TERMINAL = {"succeeded", "failed", "timeout", "cancelled", "dead", "dead_awaiting_confirm"}

BODY: dict[str, Any] = {
    "model": MODEL,
    "content": [
        {"type": "text", "text": " --subdivisionlevel high --fileformat obj"},
        {"type": "image_url", "image_url": {"url": IMAGE_URL}},
    ],
}


def _short(payload: Any, limit: int = 700) -> str:
    text = payload if isinstance(payload, str) else json.dumps(payload, ensure_ascii=False)
    return text if len(text) <= limit else text[:limit] + f"...(+{len(text) - limit}B)"


def _report(label: str, response) -> Any:
    print(f"\n----- {label} -----")
    print(f"HTTP {response.status_code}")
    for key in ("x-ag-task-id", "x-ag-internal-status", "x-ag-snapshot", "retry-after", "location"):
        value = response.headers.get(key)
        if value:
            print(f"  {key}: {value}")
    try:
        body = response.json()
    except Exception:  # noqa: BLE001 - 非 JSON 响应
        body = response.text
    print(f"  body: {_short(body)}")
    return body


async def _pump_background(bus, container, limit: int = 32) -> int:
    """把网关投进内存队列的消息按 worker 的同一条 handler 路径跑掉（转存等）。"""
    from async_gateway.tasks.handlers import dispatch_message
    from async_gateway.tasks.queues import all_queues

    done = 0
    for queue in all_queues():
        while done < limit:
            messages = await bus.consume(queue, consumer="live-seed3d", count=10, block_ms=0)
            if not messages:
                break
            for message in messages:
                await dispatch_message(dict(message.payload), message.name, bus, container)
                await bus.ack(queue, message.message_id)
                done += 1
    return done


async def _probe_result_url(url: Any) -> bool:
    """抽检交付物直链：只取前 1 KB（避免下载几十 MB），并确认签名未被脱敏破坏。

    passthrough 模式下这条链接**就是交付物**，所以必须验证它真的能取到东西，
    而不只是"字段里有值"。
    """
    import httpx

    if not isinstance(url, str) or not url.startswith(("http://", "https://")):
        print("  !! 结果字段不是可用的 http(s) 链接")
        return False
    if "***redacted***" in url:
        print("  !! 结果链接里的签名被脱敏抹掉 —— 交付物不可用")
        return False
    try:
        async with httpx.AsyncClient(timeout=30.0, follow_redirects=False) as probe:
            resp = await probe.get(url, headers={"Range": "bytes=0-1023"})
    except Exception as exc:  # noqa: BLE001 - 抽检失败即视为不可用
        print(f"  !! 直链请求失败: {type(exc).__name__}: {exc}")
        return False
    print(
        f"  直链抽检: HTTP {resp.status_code} bytes={len(resp.content)} "
        f"content-type={resp.headers.get('content-type')}"
    )
    return resp.status_code in (200, 206)


async def main(args: argparse.Namespace) -> int:
    import httpx

    from async_gateway.db.base import dispose_engine, init_schema, session_scope
    from async_gateway.db.dao import TaskDAO
    from async_gateway.domain.enums import TaskStatus
    from async_gateway.gateway.app import create_app
    from async_gateway.gateway.container import get_container

    bearer = args.bearer or os.environ.get("ARK_API_KEY", "")
    if not bearer:
        print("缺少凭证：用 --bearer 或 ARK_API_KEY 提供上游 key")
        return 2

    # httpx.ASGITransport 不会触发 FastAPI 的 lifespan，建表要自己来
    # （生产走 alembic upgrade head，这里 SQLite 直接建）。
    await init_schema()

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    headers = {"Authorization": f"Bearer {bearer}", "Content-Type": "application/json"}

    async with httpx.AsyncClient(transport=transport, base_url="http://gw.local") as client:
        container = get_container()

        # New API 任务插件打过来的就是这条路径（base_url={网关}/async/{alias} + 后缀）
        print(f"POST /async/{ALIAS}{PLUGIN_PATH}   model={MODEL}")
        created = await client.post(f"/async/{ALIAS}{PLUGIN_PATH}", headers=headers, json=BODY)
        create_body = _report("create（上游原生形状）", created)
        if created.status_code >= 400:
            return 1

        upstream_id = (create_body or {}).get("id") if isinstance(create_body, dict) else None
        if not upstream_id:
            print("!! 受理响应里没有上游任务 id —— New API 侧会解析失败")
            return 1

        # 轮询直到终态
        terminal_body: Any = None
        for attempt in range(1, args.max_polls + 1):
            await asyncio.sleep(args.interval)
            queried = await client.get(f"/async/{ALIAS}{PLUGIN_PATH}/{upstream_id}", headers=headers)
            query_body = _report(f"query #{attempt}", queried)
            internal = queried.headers.get("x-ag-internal-status", "")
            if internal in BUSINESS_TERMINAL:
                terminal_body = query_body
                break
        else:
            print(f"\n!! {args.max_polls} 次轮询后仍未到终态（网关内部状态持续在途）")

        # 终态后：跑掉后台消息（只有 store 模式才会产生转存消息），再看任务行
        processed = await _pump_background(container.bus, container)
        print(f"\n----- 后台消息处理 -----\n处理了 {processed} 条（转存/收尾等）")

        async with session_scope() as session:
            # 受理时未带 X-AG-Channel，故 channel == alias（见 container.authenticate）
            row = await TaskDAO(session).find_by_upstream_id(ALIAS, upstream_id)
        if row is None:
            print("!! 查不到任务行")
            await dispose_engine()
            return 1

        template = container.registry.require(row.template_alias, row.template_version).resolved
        mode = template.result_policy.mode.value
        print("\n----- 任务行（网关侧真相）-----")
        print(f"  task_id={row.task_id} status={row.status} raw_status={row.raw_status}")
        print(f"  attempts={row.attempts} template={row.template_alias}@{row.template_version}")
        print(f"  result_policy.mode={mode}  (AG_RESULT_MODE_DEFAULT={container.settings.result_mode_default})")
        print(f"  result_ref={row.result_ref} result_degraded={row.result_degraded}")

        endpoint = await client.get(f"/results/{row.task_id}")
        print(f"\n----- GET /results/{row.task_id} -----")
        print(f"HTTP {endpoint.status_code} location={endpoint.headers.get('location')}")
        print(f"  body: {_short(endpoint.text)}")

        # ---- 交付物可获取性：按结果策略分流 ----
        ok = False
        if row.status != TaskStatus.SUCCEEDED.value:
            print(f"\n!! 任务未成功（{row.status}），无法验证交付物")
        elif mode == "passthrough":
            url = (terminal_body or {}).get("content", {}).get("file_url") if isinstance(terminal_body, dict) else None
            print("\n----- passthrough：查询响应里的结果字段就是交付物 -----")
            print(f"  content.file_url={str(url)[:120]}")
            ok = await _probe_result_url(url)
            if endpoint.status_code != 409:
                print(f"  !! passthrough 下 /results 应为 409，实际 {endpoint.status_code}")
                ok = False
        else:
            ok = row.result_ref is not None and endpoint.status_code == 302

        await dispose_engine()
        print(f"\nLIVE SEED3D {'OK' if ok else 'FAILED'}")
        return 0 if ok else 1


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="经异步网关真机跑一次图生 3D")
    parser.add_argument("--bearer", default="", help="上游 key（也可用 ARK_API_KEY）")
    parser.add_argument("--interval", type=float, default=10.0, help="轮询间隔秒")
    parser.add_argument("--max-polls", type=int, default=40, help="最大轮询次数")
    parser.add_argument("--timeout", type=float, default=900.0, help="整体超时秒")
    return parser.parse_args()


if __name__ == "__main__":
    _args = parse_args()
    sys.exit(asyncio.run(asyncio.wait_for(main(_args), timeout=_args.timeout)))
