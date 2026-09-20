"""背压：渠道/租户两级并发槽 + 受理速率配额（§4.3）。

三处关键设计（评审 P0/D5 的落实点）：

1. **两级原子 check-and-incr**：渠道槽与租户槽必须在同一段 Lua 里判+占，否则
   "渠道占了、租户满"会留下泄漏；Lua 里发现租户满就不 INCR 渠道，天然无泄漏。
2. **配额分离**：受理占"受理速率"配额（token bucket，受理时消耗）；submit 占
   "上游并发"槽（worker 调上游时占用）。**两个配额独立计数、独立限额**——
   突发受理不挤占上游并发槽。
3. **校准只纠泄漏方向**：每 30s 用 PG 实数校准，只在 ``Redis 值 > PG 实数`` 时下调，
   否则会把"在途任务"误伤成"已释放"而超发。
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol

from .redis import get_redis

#: KEYS[1]=渠道槽 KEYS[2]=租户槽
#: ARGV[1]=渠道限额 ARGV[2]=租户限额 ARGV[3]=槽位 TTL 秒
#: 返回 0=占位成功 1=渠道打满 2=租户打满
ACQUIRE_LUA = """
local ch, tn = KEYS[1], KEYS[2]
local ch_limit, tn_limit, ttl = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local cur_ch = tonumber(redis.call('GET', ch) or '0')
if cur_ch >= ch_limit then
  return 1
end
local cur_tn = tonumber(redis.call('GET', tn) or '0')
if cur_tn >= tn_limit then
  return 2
end
redis.call('INCR', ch)
redis.call('EXPIRE', ch, ttl)
redis.call('INCR', tn)
redis.call('EXPIRE', tn, ttl)
return 0
"""

#: 释放：相同槽位各减一；**不为负**（重复 DECR 不会把计数打成负数反过来超发）
RELEASE_LUA = """
local changed = 0
for i = 1, #KEYS do
  local v = tonumber(redis.call('GET', KEYS[i]) or '0')
  if v > 0 then
    redis.call('DECR', KEYS[i])
    changed = changed + 1
  end
end
return changed
"""

#: 校准：仅当 Redis 值 > PG 实数时下调（只纠泄漏方向，不倒灌）
CALIBRATE_LUA = """
local v = tonumber(redis.call('GET', KEYS[1]) or '0')
local real = tonumber(ARGV[1])
local ttl = tonumber(ARGV[2])
if v > real then
  if real > 0 then
    redis.call('SET', KEYS[1], real)
    redis.call('EXPIRE', KEYS[1], ttl)
  else
    redis.call('DEL', KEYS[1])
  end
  return v - real
end
return 0
"""

#: 令牌桶（受理速率）：KEYS[1]=桶；ARGV: rate, burst, now_ms, cost
TOKEN_BUCKET_LUA = """
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local cost = tonumber(ARGV[4])
local data = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts = tonumber(data[2])
if tokens == nil then tokens = burst end
if ts == nil then ts = now end
local elapsed = math.max(0, now - ts) / 1000.0
tokens = math.min(burst, tokens + elapsed * rate)
local allowed = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 60000)
return allowed
"""


@dataclass(frozen=True, slots=True)
class SlotDecision:
    granted: bool
    reason: str = "ok"
    retry_after: float | None = None

    def as_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if not self.granted and self.retry_after is not None:
            headers["Retry-After"] = str(max(1, int(self.retry_after)))
        return headers


class ConcurrencyLimiter(Protocol):
    async def acquire(self, channel: str, tenant: str, *, channel_limit: int, tenant_limit: int) -> SlotDecision: ...
    async def release(self, channel: str, tenant: str) -> None: ...
    async def calibrate_channel(self, channel: str, real_active: int, *, ttl_seconds: int = 3600) -> int: ...


def _keys(channel: str, tenant: str) -> tuple[str, str]:
    return f"conc:{{{channel}}}", f"conc:{{{channel}}}:{{{tenant}}}"


class RedisConcurrencyLimiter:
    def __init__(self, ttl_seconds: int = 3600) -> None:
        self.ttl = ttl_seconds
        self._acquire = None
        self._release = None
        self._calibrate = None

    def _scripts(self):  # pragma: no cover - 薄封装
        client = get_redis()
        if self._acquire is None:
            self._acquire = client.register_script(ACQUIRE_LUA)
            self._release = client.register_script(RELEASE_LUA)
            self._calibrate = client.register_script(CALIBRATE_LUA)
        return self._acquire, self._release, self._calibrate

    async def acquire(
        self, channel: str, tenant: str, *, channel_limit: int, tenant_limit: int
    ) -> SlotDecision:
        acquire, _, _ = self._scripts()
        code = await acquire(
            keys=list(_keys(channel, tenant)),
            args=[channel_limit, tenant_limit, self.ttl],
        )
        code = int(code)
        if code == 0:
            return SlotDecision(True)
        reason = "channel_concurrency" if code == 1 else "tenant_concurrency"
        return SlotDecision(False, reason, retry_after=1.0)

    async def release(self, channel: str, tenant: str) -> None:
        _, release, _ = self._scripts()
        await release(keys=list(_keys(channel, tenant)))

    async def calibrate_channel(self, channel: str, real_active: int, *, ttl_seconds: int = 3600) -> int:
        _, _, calibrate = self._scripts()
        drift = await calibrate(keys=[_keys(channel, "x")[0]], args=[real_active, ttl_seconds])
        return int(drift)


class MemoryConcurrencyLimiter:
    """进程内实现（测试 / 无 Redis 的本地联调）。语义与 Lua 版一致。"""

    def __init__(self) -> None:
        self.counts: dict[str, int] = {}

    async def acquire(
        self, channel: str, tenant: str, *, channel_limit: int, tenant_limit: int
    ) -> SlotDecision:
        ch, tn = _keys(channel, tenant)
        if self.counts.get(ch, 0) >= channel_limit:
            return SlotDecision(False, "channel_concurrency", 1.0)
        if self.counts.get(tn, 0) >= tenant_limit:
            return SlotDecision(False, "tenant_concurrency", 1.0)
        self.counts[ch] = self.counts.get(ch, 0) + 1
        self.counts[tn] = self.counts.get(tn, 0) + 1
        return SlotDecision(True)

    async def release(self, channel: str, tenant: str) -> None:
        for key in _keys(channel, tenant):
            if self.counts.get(key, 0) > 0:
                self.counts[key] -= 1

    async def calibrate_channel(self, channel: str, real_active: int, *, ttl_seconds: int = 3600) -> int:
        ch = _keys(channel, "x")[0]
        current = self.counts.get(ch, 0)
        if current > real_active:
            self.counts[ch] = real_active
            return current - real_active
        return 0

    async def aclose(self) -> None:  # pragma: no cover - 接口对齐
        return None


class RedisAcceptRateLimiter:
    """受理速率配额（QPS 令牌桶），与上游并发槽**分开计数**。"""

    def __init__(self, rate: float, burst: int) -> None:
        self.rate = float(rate)
        self.burst = int(burst)
        self._script = None

    def _load(self):  # pragma: no cover
        if self._script is None:
            self._script = get_redis().register_script(TOKEN_BUCKET_LUA)
        return self._script

    async def try_acquire(self, channel: str, *, cost: int = 1) -> SlotDecision:
        script = self._load()
        allowed = await script(
            keys=[f"accept:{{{channel}}}"],
            args=[self.rate, self.burst, int(time.time() * 1000), cost],
        )
        if int(allowed) == 1:
            return SlotDecision(True)
        return SlotDecision(False, "accept_rate", retry_after=max(0.05, cost / max(self.rate, 0.001)))


class MemoryAcceptRateLimiter:
    def __init__(self, rate: float, burst: int) -> None:
        self.rate = float(rate)
        self.burst = int(burst)
        self.tokens = float(burst)
        self.ts = time.monotonic()

    async def try_acquire(self, channel: str, *, cost: int = 1) -> SlotDecision:
        now = time.monotonic()
        self.tokens = min(self.burst, self.tokens + (now - self.ts) * self.rate)
        self.ts = now
        if self.tokens >= cost:
            self.tokens -= cost
            return SlotDecision(True)
        need = (cost - self.tokens) / max(self.rate, 0.001)
        return SlotDecision(False, "accept_rate", retry_after=need)
