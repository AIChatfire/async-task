"""基础设施：并发槽/配额（内存版 + Redis Lua 版）、自适应轮询、Streams broker。"""

from __future__ import annotations

import asyncio

import pytest

from async_gateway.bus.base import QueueSaturated
from async_gateway.bus.factory import make_bus
from async_gateway.bus.memory import MemoryBus
from async_gateway.infra.concurrency import (
    MemoryAcceptRateLimiter,
    MemoryConcurrencyLimiter,
    RedisAcceptRateLimiter,
    RedisConcurrencyLimiter,
)
from async_gateway.infra.polling import MemoryPollingController, RedisPollingController

# __ 内存实现（与 Lua 版语义一致，保证默认测试不依赖 Redis） __


async def test_two_level_concurrency_slots():
    limiter = MemoryConcurrencyLimiter()
    d = await limiter.acquire("ch", "t1", channel_limit=2, tenant_limit=1)
    assert d.granted
    # 租户打满：渠道槽**不能**被占用（否则是泄漏）
    d = await limiter.acquire("ch", "t1", channel_limit=2, tenant_limit=1)
    assert not d.granted and d.reason == "tenant_concurrency"
    d = await limiter.acquire("ch", "t2", channel_limit=2, tenant_limit=1)
    assert d.granted
    d = await limiter.acquire("ch", "t3", channel_limit=2, tenant_limit=1)
    assert not d.granted and d.reason == "channel_concurrency"


async def test_release_is_idempotent_and_never_negative():
    limiter = MemoryConcurrencyLimiter()
    await limiter.acquire("ch", "t", channel_limit=5, tenant_limit=5)
    await limiter.release("ch", "t")
    await limiter.release("ch", "t")
    await limiter.release("ch", "t")
    # 重复释放不得把计数打成负数——否则上限就形同虚设
    d = await limiter.acquire("ch", "t", channel_limit=1, tenant_limit=1)
    assert d.granted


async def test_calibration_only_corrects_downward():
    limiter = MemoryConcurrencyLimiter()
    for _ in range(3):
        await limiter.acquire("ch", "t", channel_limit=10, tenant_limit=10)
    # PG 实数只有 1（其余是泄漏）→ 下调
    drift = await limiter.calibrate_channel("ch", 1)
    assert drift == 2
    # PG 实数为 5（大于 Redis 的 1）→ 说明 Redis 偏小是在途任务，绝不上调
    drift2 = await limiter.calibrate_channel("ch", 5)
    assert drift2 == 0


async def test_accept_rate_quota_is_separate_from_upstream_slots():
    accept = MemoryAcceptRateLimiter(rate=1.0, burst=2)
    assert (await accept.try_acquire("ch")).granted
    assert (await accept.try_acquire("ch")).granted
    denied = await accept.try_acquire("ch")
    assert not denied.granted and denied.reason == "accept_rate"
    assert denied.retry_after and denied.retry_after > 0


async def test_polling_cold_start_and_adaptive_growth():
    ctrl = MemoryPollingController(base=3.0, minimum=3.0, maximum=60.0, initial=5.0, hot_start_samples=3)
    # 冷启动：用模板 poll_initial_interval
    assert await ctrl.next_interval("ch", elapsed=0.0, first_poll=True) == 5.0

    for _ in range(3):
        await ctrl.record_terminal("ch", 180.0)
    stats = await ctrl.stats("ch")
    assert stats.warm is True

    # 首次轮询取 max(P10, min)
    first = await ctrl.next_interval("ch", elapsed=0.0, first_poll=True)
    assert first >= 3.0

    early = await ctrl.next_interval("ch", elapsed=10.0)
    late = await ctrl.next_interval("ch", elapsed=600.0)
    assert early <= late
    assert late <= 60.0


async def test_polling_aimd_on_429():
    ctrl = MemoryPollingController(base=3.0, minimum=3.0, maximum=60.0, initial=5.0)
    m1 = await ctrl.note_rate_limited("ch", retry_after=2.0)
    assert m1 == 2.0
    assert await ctrl.paused_for("ch") > 0
    m2 = await ctrl.note_rate_limited("ch", None)
    assert m2 == 4.0
    # 之后：一个无 429 周期恢复 -10%
    m3 = await ctrl.note_clean_period("ch")
    assert m3 < m2
    for _ in range(20):
        m3 = await ctrl.note_clean_period("ch")
    assert m3 == 1.0  # 下限


async def test_memory_bus_enqueue_consume_ack_and_dlq():
    bus = MemoryBus(max_depth=2)
    await bus.enqueue("q", "poll_upstream", {"task_id": "t1"})
    await bus.enqueue("q", "poll_upstream", {"task_id": "t2"})
    with pytest.raises(QueueSaturated):
        await bus.enqueue("q", "poll_upstream", {"task_id": "t3"})
    messages = await bus.consume("q", consumer="c", count=10, block_ms=1)
    assert [m.task_id for m in messages] == ["t1", "t2"]
    await bus.dead_letter("q", messages[0], "boom")
    assert bus.dlq["q"][0][1] == "boom"


def test_bus_factory_defaults_to_memory_in_test_env(settings):
    assert isinstance(make_bus(settings), MemoryBus)


# __ Redis 实机实现（Lua 原子性只有真 Redis 才验证得了） __


@pytest.fixture
async def redis_client():
    from async_gateway.infra.redis import close_redis, get_redis

    # pytest-asyncio 每个用例一个新事件循环：必须丢掉上一轮绑定在旧循环上的连接，
    # 否则 ping 会报 "Event loop is closed"，真机 Lua 用例就被静默跳过了。
    try:
        await close_redis()
    except Exception:  # noqa: BLE001 - 旧循环已关闭时的清理失败无关紧要
        pass
    client = get_redis()
    try:
        await client.ping()
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"redis unavailable: {exc}")
    yield client
    await client.flushdb()
    await close_redis()


@pytest.mark.redis
async def test_redis_concurrency_lua_is_atomic(redis_client):
    limiter = RedisConcurrencyLimiter(ttl_seconds=60)

    async def acquire_many(n: int) -> int:
        results = await asyncio.gather(
            *(limiter.acquire("chL", "tL", channel_limit=5, tenant_limit=100) for _ in range(n))
        )
        return sum(1 for r in results if r.granted)

    granted = await acquire_many(20)
    # Lua 原子性：恰好 5 个拿到槽，既不多也不少
    assert granted == 5
    await limiter.release("chL", "tL")
    assert await redis_client.get("conc:{chL}") == "4"


@pytest.mark.redis
async def test_redis_concurrency_tenant_limit_does_not_leak_channel(redis_client):
    limiter = RedisConcurrencyLimiter()
    d1 = await limiter.acquire("chT", "tT", channel_limit=10, tenant_limit=1)
    assert d1.granted
    d2 = await limiter.acquire("chT", "tT", channel_limit=10, tenant_limit=1)
    assert not d2.granted and d2.reason == "tenant_concurrency"
    # 渠道计数必须仍是 1（没被无谓占用）
    assert await redis_client.get("conc:{chT}") == "1"


@pytest.mark.redis
async def test_redis_calibration_only_downward(redis_client):
    limiter = RedisConcurrencyLimiter()
    for _ in range(3):
        await limiter.acquire("chC", "tC", channel_limit=10, tenant_limit=10)
    assert await limiter.calibrate_channel("chC", 1) == 2
    assert await limiter.calibrate_channel("chC", 9) == 0


@pytest.mark.redis
async def test_redis_accept_rate_token_bucket(redis_client):
    limiter = RedisAcceptRateLimiter(rate=1.0, burst=2)
    assert (await limiter.try_acquire("chA")).granted
    assert (await limiter.try_acquire("chA")).granted
    assert not (await limiter.try_acquire("chA")).granted


@pytest.mark.redis
async def test_redis_polling_histogram_and_aimd(redis_client):
    ctrl = RedisPollingController(base=3.0, minimum=3.0, maximum=60.0, initial=5.0, hot_start_samples=3)
    for _ in range(3):
        await ctrl.record_terminal("chP", 185.0)
    stats = await ctrl.stats("chP")
    assert stats.warm is True and stats.p50 >= 180.0
    assert await ctrl.paused_for("chP") == 0.0
    await ctrl.note_rate_limited("chP", retry_after=1.0)
    assert await ctrl.paused_for("chP") > 0


@pytest.mark.redis
async def test_stream_bus_roundtrip_and_dlq(settings):
    from async_gateway.bus.stream import StreamBus

    bus = StreamBus(prefix="ag:test:", group="ag-test", max_depth=100)
    await bus.ensure_queue("unit")
    await bus.enqueue("unit", "poll_upstream", {"task_id": "abc"})
    messages = await bus.consume("unit", consumer="c1", count=1, block_ms=200)
    assert messages and messages[0].task_id == "abc"
    assert await bus.pending("unit") == 1
    await bus.dead_letter("unit", messages[0], "unit-test")
    assert await bus.pending("unit") == 0
    entries = await bus.claim_stale("unit", consumer="c2", min_idle_ms=0, count=5)
    assert isinstance(entries, list)


# __ 结果存储：桶缺失不得静默建桶 __
def test_minio_store_does_not_auto_create_bucket():
    """桶缺失属部署配置错误：必须立刻失败，不得静默 `make_bucket`。

    结果桶通常是部署侧预建的**共享桶**（可能挂 CDN 与既有策略）。若网关在桶名配错时
    自动建桶，结果会写进一个没人管理的新桶里 —— 既不报警、也不可发现。
    """
    from types import SimpleNamespace

    from async_gateway.infra.object_store import MinioResultStore

    store = MinioResultStore.__new__(MinioResultStore)  # 跳过 __init__（避免连真实存储）
    store.bucket = "cdn"
    store._bucket_ready = False
    created: list[str] = []
    store.client = SimpleNamespace(
        bucket_exists=lambda name: False,
        make_bucket=lambda name: created.append(name),
    )

    with pytest.raises(RuntimeError, match="not available"):
        store._ensure_bucket()
    assert created == []  # 关键：绝不去建桶


def test_result_key_prefixes_creation_date():
    """结果 key 一级前缀 = 任务**创建日期（UTC）**，便于按天分目录/生命周期清理。"""
    from datetime import UTC, datetime, timedelta, timezone

    from async_gateway.infra.object_store import result_key

    created = datetime(2026, 9, 20, 23, 30, tzinfo=UTC)
    assert result_key("default", "t1", created_at=created) == "20260920/default/t1/attempt-1.bin"

    # 时区必须折算到 UTC：东八区 09-21 00:30 == UTC 09-20 16:30
    cst = timezone(timedelta(hours=8))
    assert (
        result_key("default", "t1", created_at=datetime(2026, 9, 21, 0, 30, tzinfo=cst))
        == "20260920/default/t1/attempt-1.bin"
    )

    # naive 时间按 UTC 解释（不随部署机器时区漂移）；attempt / ext 可覆盖
    naive = datetime(2026, 9, 20, 1, 0)
    assert (
        result_key("d", "t2", created_at=naive, attempt=3, ext="glb")
        == "20260920/d/t2/attempt-3.glb"
    )

# __ 请求数据存储（create 请求体 / 响应存档；对象存储只服务转存） __


async def test_request_store_roundtrip_and_missing():
    from async_gateway.infra.request_store import (
        MemoryRequestStore,
        RequestDataNotFound,
        body_ttl_seconds,
        request_key,
        response_key,
        response_ttl_seconds,
    )

    store = MemoryRequestStore()
    key = request_key("default", "t1")
    assert key == "req:default:t1" and response_key("default", "t1") == "reqresp:default:t1"
    await store.put_json(key, {"prompt": "你好"}, 60)
    assert await store.get_bytes(key) == '{"prompt": "你好"}'.encode()

    await store.drop(key)
    with pytest.raises(RequestDataNotFound):
        await store.get_bytes(key)

    # TTL 由既有配置派生，不引入新开关
    assert 300 <= body_ttl_seconds() <= 3600
    assert response_ttl_seconds() >= 60


def test_result_store_is_none_when_s3_unconfigured():
    """未配置对象存储 ⇒ get_result_store() 返回 None（转存自动关闭的判据）。

    测试基座把 S3_* 四项钉成空串（conftest）⇒ 这里断言的是"真实未配置"路径；
    需要转存的用例走 result_store fixture 注入内存替身，与 S3_* 是否配置无关。
    注意：**不要**在这里动 get_settings 的缓存（会话级 settings fixture 持有的是同一对象，
    重置会让其它用例的 monkeypatch 打空）。
    """
    from async_gateway.config import get_settings
    from async_gateway.infra.object_store import MemoryResultStore, get_result_store, set_result_store

    assert get_settings().s3_configured is False
    set_result_store(None)
    try:
        assert get_result_store() is None
    finally:
        set_result_store(MemoryResultStore())
