"""容器探针（角色感知）与后台心跳：行为判据 + 接线门禁。

对应 livetest-ai 报告 `E2E-ASYNC-TASK-001` 的 F2：

> worker / scheduler / inspector / transfer-worker 容器恒为 unhealthy ——
> 镜像 Dockerfile 里的 HEALTHCHECK 探的是 ``127.0.0.1:8000/healthz``，而这四个进程
> 都不在 8000 上监听。部署侧为这四个服务各自声明探针后才 healthy（=产品侧缺陷被部署绕开）。

本模块钉三件事：

1. **角色判定**：web 角色走 HTTP、循环角色走心跳（``--role`` / ``AG_PROBE_ROLE`` /
   PID 1 cmdline 三级来源）；
2. **判据真的会红**：心跳缺失/过期必须返回 1（否则等于"永远健康"）；
3. **接线不脱节**：Dockerfile 的 HEALTHCHECK 指向这个脚本、compose 为每个服务声明角色、
   三个后台循环各自真的打心跳（含行为验证，不只是源码里出现过）。
"""

from __future__ import annotations

import importlib.util
import os
import socket
import threading
import time
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import yaml

from async_gateway.observability import heartbeat as hb

ROOT = Path(__file__).resolve().parents[1]
DOCKERFILE = ROOT / "Dockerfile"
COMPOSE = ROOT / "docker-compose.yml"


def _load_probe():
    spec = importlib.util.spec_from_file_location(
        "ag_healthcheck", ROOT / "scripts" / "healthcheck.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


PROBE = _load_probe()


# ---------------------------------------------------------------- 1) 角色判定
def test_role_inferred_from_pid1_cmdline():
    cases = {
        ("uvicorn", "async_gateway.gateway.app:app", "--port", "8000"): ("gateway", 8000),
        ("uvicorn", "async_gateway.admin.app:app", "--host", "0.0.0.0", "--port", "8080"): (
            "admin",
            8080,
        ),
        ("python", "-m", "async_gateway.workers.worker", "--pools", "transfer"): ("worker", 8000),
        ("python", "-m", "async_gateway.workers.scheduler"): ("scheduler", 8000),
        ("python", "-m", "async_gateway.workers.inspector"): ("inspector", 8000),
    }
    for cmdline, (role, port) in cases.items():
        argv = list(cmdline)
        assert PROBE.infer_role(argv) == role, cmdline
        assert PROBE.infer_port(argv) == port, cmdline
    # 认不出 → 按 web 处理（镜像默认 CMD 是 gateway）
    assert PROBE.infer_role(["/bin/sh"]) == "http"


def test_explicit_role_beats_inference(monkeypatch):
    monkeypatch.setenv("AG_PROBE_ROLE", "scheduler")
    assert PROBE.resolve_role(None, ["uvicorn", "async_gateway.gateway.app:app"]) == "scheduler"
    assert PROBE.resolve_role("worker", ["uvicorn"]) == "worker"


def test_probe_rejects_unknown_role(capsys):
    assert PROBE.main(["--role", "nonsense"]) == 1
    assert "unknown-role" in capsys.readouterr().out


# ---------------------------------------------------------------- 2) 循环角色：心跳判据
def test_loop_role_heartbeat_missing_then_fresh_then_stale(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("AG_HEARTBEAT_DIR", str(tmp_path))

    # 从未打过心跳 → 不健康（这是 F2 里最容易被忽略的一种：探针不能"默认健康"）
    assert PROBE.main(["--role", "worker"]) == 1
    assert "missing" in capsys.readouterr().out

    assert hb.beat("worker") is True
    assert PROBE.main(["--role", "worker"]) == 0
    assert "fresh" in capsys.readouterr().out

    # 心跳过期（模拟循环卡住）→ 不健康，且判据里带年龄与阈值（可诊断）
    stale = time.time() - 999
    os.utime(tmp_path / "worker.beat", (stale, stale))
    assert PROBE.main(["--role", "worker"]) == 1
    out = capsys.readouterr().out
    assert "stale" in out and "999" in out and "max_age" in out


def test_loop_role_uses_threshold_env(tmp_path, monkeypatch):
    monkeypatch.setenv("AG_HEARTBEAT_DIR", str(tmp_path))
    assert hb.beat("scheduler")
    stale = time.time() - 30
    os.utime(tmp_path / "scheduler.beat", (stale, stale))
    # 阈值 10s ⇒ 30s 前的跳判死；阈值 120s（默认）⇒ 仍算健康
    assert PROBE.main(["--role", "scheduler", "--max-age", "10"]) == 1
    assert PROBE.main(["--role", "scheduler"]) == 0


def test_heartbeat_write_failure_is_not_fatal(tmp_path, monkeypatch):
    """心跳是观测层：目录不可写只能告警，绝不能让业务循环抛异常。"""
    blocker = tmp_path / "blocked"
    blocker.write_text("not a directory", encoding="utf-8")
    monkeypatch.setenv("AG_HEARTBEAT_DIR", str(blocker / "sub"))
    assert hb.beat("worker") is False  # 不抛
    assert hb.heartbeat_age("worker") is None


# ---------------------------------------------------------------- 3) web 角色：HTTP 判据
class _HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802 - http.server 接口
        code = 200 if self.path == "/healthz" else 404
        self.send_response(code)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, *args):  # 静音
        return


def _serve_healthz() -> tuple[HTTPServer, int]:
    server = HTTPServer(("127.0.0.1", 0), _HealthHandler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, server.server_port


def _dead_port() -> int:
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def test_http_role_probes_healthz(monkeypatch, capsys):
    server, port = _serve_healthz()
    try:
        monkeypatch.setenv("AG_PROBE_URL", f"http://127.0.0.1:{port}/healthz")
        assert PROBE.main(["--role", "gateway"]) == 0
        assert "status=200" in capsys.readouterr().out

        # 路径不对（404）也算不健康 —— 探针要真看状态码
        monkeypatch.setenv("AG_PROBE_URL", f"http://127.0.0.1:{port}/nope")
        assert PROBE.main(["--role", "admin"]) == 1
        assert "status=404" in capsys.readouterr().out
    finally:
        server.shutdown()


def test_http_role_unreachable_is_unhealthy(monkeypatch, capsys):
    monkeypatch.setenv("AG_PROBE_URL", f"http://127.0.0.1:{_dead_port()}/healthz")
    assert PROBE.main(["--role", "gateway"]) == 1
    assert "unreachable" in capsys.readouterr().out


# ---------------------------------------------------------------- 4) 接线门禁
def test_dockerfile_healthcheck_points_at_role_aware_probe():
    text = DOCKERFILE.read_text(encoding="utf-8")
    line = next(ln for ln in text.splitlines() if ln.strip().startswith("HEALTHCHECK"))
    assert "scripts/healthcheck.py" in line, "镜像探针必须走角色感知脚本"
    assert "127.0.0.1:8000/healthz" not in line, "不得再写死 web 端口（F2 的根因）"


def test_compose_declares_probe_role_for_every_app_service():
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = doc["services"]
    app_services = {n: s for n, s in services.items() if isinstance(s, dict) and s.get("build")}
    assert len(app_services) >= 6
    roles = {n: (svc.get("environment") or {}).get("AG_PROBE_ROLE") for n, svc in app_services.items()}
    assert roles == {
        "migrate": None,  # 一次性任务（跑完即退出），不声明角色
        "gateway-api": "gateway",
        "worker": "worker",
        "transfer-worker": "worker",
        "scheduler": "scheduler",
        "inspector": "inspector",
        "task-admin": "admin",
    }, roles


async def _noop() -> None:
    return None


def _isolate_loop_teardown(monkeypatch) -> None:
    """把进程收尾（关 Redis 客户端 / 卸引擎）置空。

    本条测的是"循环打心跳"，而收尾钩子动的是**进程级单例**（`infra.redis._client`）——
    它的连接绑定在"首次使用它的那个 event loop"上，跨用例调用会撞
    ``RuntimeError: Event loop is closed``（与断言目标无关的测试基座问题）。
    """
    for module in ("worker", "scheduler", "inspector"):
        monkeypatch.setattr(f"async_gateway.workers.{module}.close_redis", _noop)
        monkeypatch.setattr(f"async_gateway.workers.{module}.dispose_engine", _noop)


async def test_scheduler_and_inspector_loops_beat_heartbeat(tmp_path, monkeypatch, container, db):
    """循环**真的**打心跳（行为验证：跑一轮真进程循环后文件必须新鲜）。"""
    monkeypatch.setenv("AG_HEARTBEAT_DIR", str(tmp_path))
    _isolate_loop_teardown(monkeypatch)
    from async_gateway.workers.inspector import run_inspector
    from async_gateway.workers.scheduler import run_scheduler

    await run_scheduler(max_ticks=1, tick_seconds=0.01)
    assert hb.heartbeat_age("scheduler") is not None, "scheduler 循环未打心跳"
    await run_inspector(max_ticks=1, tick_seconds=0.01)
    assert hb.heartbeat_age("inspector") is not None, "inspector 循环未打心跳"
    assert PROBE.main(["--role", "scheduler"]) == 0
    assert PROBE.main(["--role", "inspector"]) == 0


async def test_worker_loop_beats_heartbeat(tmp_path, monkeypatch, container, db):
    monkeypatch.setenv("AG_HEARTBEAT_DIR", str(tmp_path))
    _isolate_loop_teardown(monkeypatch)
    # worker 自己造 bus；这里换成容器里的内存 bus，好让循环处理一条消息即退出
    monkeypatch.setattr("async_gateway.workers.worker.make_bus", lambda *a, **k: container.bus)
    from async_gateway.workers.worker import run_worker

    await container.bus.enqueue("finalize:default", "finalize_task", {"task_id": "no-such-task"})
    await run_worker(["light-poll"], consumer="probe-test", stop_after=1)
    assert hb.heartbeat_age("worker") is not None, "worker 循环未打心跳"
    assert PROBE.main(["--role", "worker"]) == 0
