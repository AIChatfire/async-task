#!/usr/bin/env python3
"""容器健康探针（**角色感知**；2026-09-21 补，对应 livetest-ai 报告 F2）。

镜像里的 HEALTHCHECK 原先只探 ``http://127.0.0.1:8000/healthz``：这对 **web 进程**
（gateway-api / task-admin）是对的，对 **后台进程**（worker / scheduler / inspector）
则是错的——它们不在 8000 上监听，于是容器恒为 unhealthy，探针从"健康信号"退化成噪音。

本探针按角色分流：

* ``http`` 角色（gateway / admin）：``GET {AG_PROBE_URL 或 127.0.0.1:<port>}/healthz`` 期望 200；
* ``loop`` 角色（worker / scheduler / inspector）：心跳文件（见
  ``async_gateway.observability.heartbeat``）年龄 ≤ ``AG_PROBE_MAX_AGE_SECONDS``
  ⇒ 判据是"循环还在推进"，而不是"容器还在"。

角色解析顺序：``--role`` > 环境变量 ``AG_PROBE_ROLE`` > 从 **PID 1 的 cmdline** 推断。
最后那条兜底让**默认 CMD**（``uvicorn async_gateway.gateway.app:app``）开箱可用，
不必在每一个编排文件里重复声明；显式声明优先，便于 K8s 的 exec 探针复用同一脚本。

用法：
    python /app/scripts/healthcheck.py                 # 自动判角色
    python /app/scripts/healthcheck.py --role worker    # 显式指定
退出码：0 = 健康；1 = 不健康（stdout 打一行判据，``docker inspect`` / ``kubectl describe`` 可见）。
"""

from __future__ import annotations

import argparse
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

# 包已随镜像安装（pip install -e .）；粗心的环境里退化为本地默认值，保证探针自身不会 import 失败。
try:
    from async_gateway.observability.heartbeat import (  # type: ignore[import-not-found]
        DEFAULT_HEARTBEAT_DIR,
        DEFAULT_PROBE_MAX_AGE_SECONDS,
        LOOP_ROLES,
        heartbeat_age,
    )
except Exception:  # pragma: no cover - 只在包不可导入的裸环境里走到
    DEFAULT_HEARTBEAT_DIR = "/tmp/ag-heartbeat"
    DEFAULT_PROBE_MAX_AGE_SECONDS = 120.0
    LOOP_ROLES = ("worker", "scheduler", "inspector")

    def heartbeat_age(role: str, *, now: float | None = None) -> float | None:  # type: ignore[misc]
        import time

        try:
            mtime = (Path(os.environ.get("AG_HEARTBEAT_DIR") or DEFAULT_HEARTBEAT_DIR) / f"{role}.beat").stat().st_mtime
        except OSError:
            return None
        return max(0.0, (now if now is not None else time.time()) - mtime)


#: PID 1 的模块名 → 角色（web 角色走 HTTP，循环角色走心跳）
ROLE_BY_MODULE: dict[str, str] = {
    "async_gateway.gateway.app": "gateway",
    "async_gateway.admin.app": "admin",
    "async_gateway.workers.worker": "worker",
    "async_gateway.workers.scheduler": "scheduler",
    "async_gateway.workers.inspector": "inspector",
}

HTTP_ROLES = frozenset({"gateway", "admin", "http"})
LOOP_ALIASES = frozenset(set(LOOP_ROLES) | {"loop"})
DEFAULT_HTTP_PORT = 8000
DEFAULT_TIMEOUT = 3.0


def read_pid1_cmdline(proc: Path = Path("/proc/1/cmdline")) -> list[str]:
    """PID 1 的 argv（部署侧不传 ``--role`` 时的兜底判据）。读不到返回空表。"""
    try:
        raw = proc.read_bytes()
    except OSError:
        return []
    return [part for part in raw.decode("utf-8", "replace").split("\0") if part]


def infer_role(cmdline: list[str]) -> str:
    """从 cmdline 推断角色；认不出就按 ``http`` 处理（镜像默认 CMD 是 web 进程）。"""
    for token in cmdline:
        for module, role in ROLE_BY_MODULE.items():
            if module in token:
                return role
    return "http"


def infer_port(cmdline: list[str], default: int = DEFAULT_HTTP_PORT) -> int:
    """从 ``--port N`` 推断监听端口（``python -m` / uvicorn 两种写法都覆盖）。"""
    for index, token in enumerate(cmdline):
        if token == "--port" and index + 1 < len(cmdline):
            try:
                return int(cmdline[index + 1])
            except ValueError:
                return default
        if token.startswith("--port="):
            try:
                return int(token.split("=", 1)[1])
            except ValueError:
                return default
    return default


def http_url(cmdline: list[str]) -> str:
    """显式 ``AG_PROBE_URL`` 优先；否则按推断端口拼本地回环地址。"""
    explicit = os.environ.get("AG_PROBE_URL")
    if explicit:
        return explicit
    return f"http://127.0.0.1:{infer_port(cmdline)}/healthz"


def check_http(url: str, *, timeout: float = DEFAULT_TIMEOUT) -> tuple[bool, str]:
    """回环 HTTP 探针。

    ⚠️ **显式禁用代理**（``ProxyHandler({})``）：探针打的是本容器回环地址，而环境里的
    ``HTTP_PROXY`` 会把回环请求也代理走（实测：拿到的不是"连不上"而是代理的 502）——
    那会把"进程没起来"误报成"网关/代理故障"，判据失真。
    """
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    try:
        with opener.open(url, timeout=timeout) as response:  # noqa: S310 - 回环地址
            ok = 200 <= response.status < 300
            return ok, f"http status={response.status} url={url}"
    except urllib.error.HTTPError as exc:
        return False, f"http status={exc.code} url={url}"
    except Exception as exc:  # noqa: BLE001 - 探针只报结论与原因
        return False, f"http unreachable url={url} err={type(exc).__name__}: {exc}"


def check_loop(role: str, *, max_age: float) -> tuple[bool, str]:
    age = heartbeat_age(role)
    path = (Path(os.environ.get("AG_HEARTBEAT_DIR") or DEFAULT_HEARTBEAT_DIR) / f"{role}.beat")
    if age is None:
        return False, f"heartbeat missing path={path}（进程从未打过心跳：起不来或目录不可写）"
    if age > max_age:
        return False, f"heartbeat stale age={age:.1f}s > max_age={max_age:.1f}s path={path}（循环卡住）"
    return True, f"heartbeat fresh age={age:.1f}s <= max_age={max_age:.1f}s"


def resolve_role(explicit: str | None, cmdline: list[str]) -> str:
    role = (explicit or os.environ.get("AG_PROBE_ROLE") or "").strip().lower()
    return role or infer_role(cmdline)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="async-gateway 容器健康探针（角色感知）")
    parser.add_argument("--role", default=None, help="gateway|admin|worker|scheduler|inspector|http|loop")
    parser.add_argument("--url", default=None, help="覆盖 HTTP 探针地址（等价 AG_PROBE_URL）")
    parser.add_argument("--max-age", type=float, default=None, help="覆盖心跳过期阈值（秒）")
    args = parser.parse_args(argv)

    if args.url:
        os.environ["AG_PROBE_URL"] = args.url

    cmdline = read_pid1_cmdline()
    role = resolve_role(args.role, cmdline)
    if role not in HTTP_ROLES and role not in LOOP_ALIASES:
        print(f"unhealthy role={role} reason=unknown-role（可用角色：http|loop|{'|'.join(sorted(set(ROLE_BY_MODULE.values())))})")
        return 1

    if role in HTTP_ROLES:
        ok, detail = check_http(http_url(cmdline))
    else:
        probe_role = "worker" if role == "loop" else role
        max_age = args.max_age
        if max_age is None:
            raw = os.environ.get("AG_PROBE_MAX_AGE_SECONDS")
            try:
                max_age = float(raw) if raw else DEFAULT_PROBE_MAX_AGE_SECONDS
            except ValueError:
                max_age = DEFAULT_PROBE_MAX_AGE_SECONDS
        ok, detail = check_loop(probe_role, max_age=max_age)

    print(f"{'healthy' if ok else 'unhealthy'} role={role} {detail}")
    return 0 if ok else 1


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
