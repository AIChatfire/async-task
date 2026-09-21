"""测试基座：内存 broker / 内存结果存储 / 内存凭证 + MockTransport 假上游。

必须在导入 ``async_gateway`` 之前设置环境变量：``derive.STRATEGY_KEYS`` 等在**导入期**
就会读一次配置，晚了就固化了默认值。
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

_TMP = tempfile.mkdtemp(prefix="ag-test-")
os.environ.update(
    {
        "AG_APP_ENV": "test",
        "AG_DATABASE_URL": f"sqlite+aiosqlite:///{_TMP}/test.db",
        # 本机 .env 里 AG_REDIS_URL 可能是生产占位符（<redis-host>）：显式钉住，测试不依赖它。
        # 只有 redis 标记的用例会真的连它（其余全部走内存替身，见 is_test / uses_ephemeral_infra）。
        "AG_REDIS_URL": "redis://localhost:6379/0",
        # 对象存储：测试**一律不接真机**（防误写生产桶）。需要转存的用例由 result_store fixture
        # 注入内存替身（set_result_store）；「未配置 ⇒ 转存自动关闭」另有专门用例覆盖。
        "AG_S3_ENDPOINT": "",
        "AG_S3_ACCESS_KEY": "",
        "AG_S3_SECRET_KEY": "",
        "AG_S3_BUCKET": "",
        # 结果**策略**模式（store/passthrough），与请求数据的存储后端无关：
        # 请求体/响应存档走 request store（本基座注入内存实现，见 request_store fixture）。
        # 既有用例假定"成功即转存"（store），故测试环境显式指定；
        # "默认取配置"这条逻辑另有专门用例（test_templates.py）覆盖。
        "AG_RESULT_MODE_DEFAULT": "store",
        "AG_QUEUE_DRIVER": "memory",
        "AG_SSRF_ALLOW_HOSTS": '["localhost","127.0.0.1","gw.test"]',
        "AG_SSRF_PIN_DNS": "false",
        "AG_CALLBACK_HMAC_KEYS": '{"k1":"secret-one","k0":"secret-zero"}',
        "AG_CALLBACK_ACTIVE_KID": "k1",
        "AG_CALLBACK_BASE_URL": "http://gw.test",
        "AG_ADMIN_TOKEN": "test-admin",
        "AG_URL_DIRECT_CONFIG_ENABLED": "true",
        # 既有用例全部按 inline（受理内同步 create）语义写：这里显式钉住。
        # "默认是 queued"由 tests/test_submit_mode.py 单独守住（含模型默认值断言）。
        "AG_SUBMIT_MODE": "inline",
        "AG_MAX_ATTEMPTS": "3",
        "AG_TASK_DEADLINE_SECONDS": "600",
        "AG_UNKNOWN_MAX_LIFETIME_SECONDS": "3600",
        "AG_ACCEPT_RATE_PER_SECOND": "1000",
        "AG_ACCEPT_BURST": "1000",
        "AG_POLL_INITIAL_INTERVAL": "0.01",
        "AG_POLL_BASE_INTERVAL": "0.01",
        "AG_POLL_MIN_INTERVAL": "0.01",
        "AG_MIN_REFRESH_INTERVAL": "0.0",
    }
)

import httpx  # noqa: E402
import pytest  # noqa: E402
import pytest_asyncio  # noqa: E402
from sqlalchemy import delete  # noqa: E402

from async_gateway.bus.memory import MemoryBus  # noqa: E402
from async_gateway.config import get_settings  # noqa: E402
from async_gateway.db.base import Base, dispose_engine, get_engine, init_schema, session_scope  # noqa: E402
from async_gateway.db.models import (  # noqa: E402
    AsyncTask,
    AuditEvent,
    CallbackEvent,
    ChannelPolicyRow,
    MetricsRollup,
    OrphanCallback,
    UpstreamTemplateRow,
)
from async_gateway.gateway.container import Container, configure_container  # noqa: E402
from async_gateway.infra.concurrency import MemoryAcceptRateLimiter, MemoryConcurrencyLimiter  # noqa: E402
from async_gateway.infra.credentials import MemoryEphemeralCredentials, set_credential_store  # noqa: E402
from async_gateway.infra.object_store import MemoryResultStore, set_result_store  # noqa: E402
from async_gateway.infra.request_store import MemoryRequestStore, set_request_store  # noqa: E402
from async_gateway.infra.polling import MemoryPollingController  # noqa: E402
from async_gateway.security.callback_auth import CallbackAuthenticator  # noqa: E402
from async_gateway.templates.registry import TemplateRegistry  # noqa: E402

FIXTURES = Path(__file__).parent / "fixtures" / "templates"
BUILTIN = Path(__file__).resolve().parents[1] / "src" / "async_gateway" / "templates" / "builtin"


class FakeUpstream:
    """可编程假上游：覆盖创建/查询/取消/结果拉取与各类故障。"""

    def __init__(self, *, base_url: str = "http://localhost:9099") -> None:
        self.base_url = base_url
        self.tasks: dict[str, dict] = {}
        self.create_calls = 0
        self.get_calls = 0
        self.cancel_calls = 0
        self.result_fetches = 0
        #: 创建阶段剧本：ok / http_500 / timeout / read_timeout / refused / rate_limited /
        #:               bad_request / unauthorized / created_without_id
        self.create_script: list[str] = []
        #: 查询阶段剧本：None 表示按 task 状态正常返回；否则用该状态串回答
        self.poll_script: list[str] = []
        self.poll_status_override: str | None = None
        self.result_bytes = b"FAKE-RESULT-BYTES"
        self.result_status = 200
        #: 结果拉取校验的签名期望值。真机上游的结果 URL 多为预签名（TOS 等），
        #: 签名被篡改或抹除即 403 —— 设了它，MockTransport 才具备"签名必须原样送达"
        #: 的区分度（默认 None = 不校验，只看路径）。
        self.result_expected_signature: str | None = None
        self.request_log: list[tuple[str, str]] = []

    # ---- 剧本控制 ----
    def queue_create(self, *scripts: str) -> None:
        self.create_script.extend(scripts)

    def add_task(self, status: str = "running", *, result_url: str | None = None) -> str:
        task_id = f"up-{len(self.tasks) + 1}"
        self.tasks[task_id] = {
            "id": task_id,
            "status": status,
            "output": {"url": result_url or f"{self.base_url}/files/{task_id}.bin"},
        }
        return task_id

    def set_status(self, task_id: str, status: str) -> None:
        self.tasks[task_id]["status"] = status

    # ---- transport ----
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self.handler)

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.request_log.append((request.method, path))
        auth = request.headers.get("authorization", "")
        # 记录收到的凭证头：用于断言"数据面凭证确实透传到了上游"。
        # 不能靠响应体回显来断言——响应体过脱敏层，回显的凭证会被抹成 ***redacted***。
        self.last_auth = auth

        if request.method == "POST" and path == "/v1/tasks":
            self.create_calls += 1
            script = self.create_script.pop(0) if self.create_script else "ok"
            if script == "http_500":
                return httpx.Response(500, json={"error": "upstream exploded"})
            if script == "read_timeout":
                raise httpx.ReadTimeout("read timed out", request=request)
            if script == "connect_timeout":
                raise httpx.ConnectTimeout("connect timed out", request=request)
            if script == "refused":
                raise httpx.ConnectError("connection refused", request=request)
            if script == "rate_limited":
                return httpx.Response(429, headers={"Retry-After": "1"}, json={"error": "slow down"})
            if script == "bad_request":
                return httpx.Response(422, json={"error": "invalid model"})
            if script == "unauthorized":
                return httpx.Response(403, json={"error": "bad key"})
            if script == "conflict":
                return httpx.Response(409, json={"error": "duplicate client key"})
            if script == "created_without_id":
                return httpx.Response(200, json={"unexpected": "shape"})
            task_id = self.add_task()
            return httpx.Response(200, json={"task_id": task_id, "status": "queued"})

        if request.method == "GET" and path.startswith("/v1/tasks/"):
            self.get_calls += 1
            task_id = path.rsplit("/", 1)[1]
            if self.poll_script:
                script = self.poll_script.pop(0)
                if script == "http_500":
                    return httpx.Response(503, json={"error": "temporarily unavailable"})
                if script == "not_found":
                    return httpx.Response(404, json={"error": "no such task"})
                if script == "unrecognized":
                    return httpx.Response(200, json={"id": task_id, "state": "weird-value"})
                if script == "rate_limited":
                    return httpx.Response(429, headers={"Retry-After": "1"}, json={})
            task = self.tasks.get(task_id)
            if task is None:
                return httpx.Response(404, json={"error": "no such task"})
            if self.poll_status_override:
                task = {**task, "status": self.poll_status_override}
            return httpx.Response(200, json=task)

        if request.method == "DELETE" and path.startswith("/v1/tasks/"):
            self.cancel_calls += 1
            task_id = path.rsplit("/", 1)[1]
            if task_id not in self.tasks:
                return httpx.Response(404, json={"error": "no such task"})
            self.tasks[task_id]["status"] = "cancelled"
            return httpx.Response(200, json={"id": task_id, "status": "cancelled"})

        if request.method == "GET" and path.startswith("/files/"):
            self.result_fetches += 1
            if self.result_expected_signature is not None:
                if request.url.params.get("X-Tos-Signature") != self.result_expected_signature:
                    return httpx.Response(403, content=b"signature mismatch")
            if self.result_status != 200:
                return httpx.Response(self.result_status, content=b"nope")
            return httpx.Response(200, content=self.result_bytes)

        return httpx.Response(404, json={"error": "unknown path", "path": path})


@pytest.fixture(scope="session")
def settings():
    return get_settings()


@pytest_asyncio.fixture
async def db():
    await init_schema()
    async with session_scope() as s:
        for model in (
            AsyncTask,
            CallbackEvent,
            OrphanCallback,
            UpstreamTemplateRow,
            ChannelPolicyRow,
            AuditEvent,
            MetricsRollup,
        ):
            await s.execute(delete(model))
    yield
    async with session_scope() as s:
        for model in (
            AsyncTask,
            CallbackEvent,
            OrphanCallback,
            UpstreamTemplateRow,
            ChannelPolicyRow,
            AuditEvent,
            MetricsRollup,
        ):
            await s.execute(delete(model))


@pytest.fixture
def fake_upstream() -> FakeUpstream:
    return FakeUpstream()


@pytest.fixture
def result_store() -> MemoryResultStore:
    store = MemoryResultStore()
    set_result_store(store)
    return store


@pytest.fixture
def request_store() -> MemoryRequestStore:
    store = MemoryRequestStore()
    set_request_store(store)
    return store


@pytest.fixture
def credentials() -> MemoryEphemeralCredentials:
    store = MemoryEphemeralCredentials()
    set_credential_store(store)
    return store


@pytest.fixture
def registry() -> TemplateRegistry:
    return TemplateRegistry(builtin_dir=BUILTIN, extra_dirs=[FIXTURES])


@pytest.fixture
def authenticator() -> CallbackAuthenticator:
    from async_gateway.gateway import routes

    auth = CallbackAuthenticator(
        keys={"k1": "secret-one", "k0": "secret-zero"}, active_kid="k1", tolerance_seconds=300
    )
    routes.set_authenticator(auth)
    return auth


@pytest.fixture
def container(
    registry: TemplateRegistry,
    result_store: MemoryResultStore,
    request_store: MemoryRequestStore,
    credentials: MemoryEphemeralCredentials,
    fake_upstream: FakeUpstream,
) -> Container:
    c = Container(
        registry=registry,
        limiter=MemoryConcurrencyLimiter(),
        accept_limiter=MemoryAcceptRateLimiter(rate=1000.0, burst=1000),
        polling=MemoryPollingController(),
        result_store=result_store,
        request_store=request_store,
        channel_policies={},
    )
    c.set_bus(MemoryBus())
    c.transport_factory = fake_upstream.transport
    configure_container(c)
    return c


@pytest_asyncio.fixture
async def client(container: Container, db, authenticator) -> httpx.AsyncClient:
    from async_gateway.gateway.app import create_app

    app = create_app()
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://gw.test") as c:
        yield c


@pytest_asyncio.fixture(autouse=True)
async def _cleanup_engine():
    yield
    await dispose_engine()


def auth_headers(key: str = "sk-test-key-123456", **extra: str) -> dict[str, str]:
    headers = {"Authorization": f"Bearer {key}"}
    headers.update(extra)
    return headers


def make_callback(app_secret: str, kid: str, body: bytes, timestamp: int | None = None) -> dict[str, str]:
    import time

    from async_gateway.security.callback_auth import sign

    ts = int(timestamp if timestamp is not None else time.time())
    return {
        "x-ag-kid": kid,
        "x-ag-timestamp": str(ts),
        "x-ag-signature": sign(kid, app_secret, ts, body),
    }


def payload_bytes(payload: dict) -> bytes:
    return json.dumps(payload).encode()
