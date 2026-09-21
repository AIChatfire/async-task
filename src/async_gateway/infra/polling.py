"""自适应轮询（§15）。

两条曲线分开管，互不干扰：

* **时长驱动**：``interval = clamp(base × 2^⌊elapsed / P50⌋, min, max)``。
  首次轮询取 ``max(P10, min)``；渠道无样本时用模板 ``poll_initial_interval``（默认 5s）；
  积累 ≥ 30 个终态时长样本后切换到直方图驱动。
* **限流驱动（AIMD）**：429 时该渠道间隔 ×2（上限 60s）并暂停派发至 ``retry_after``；
  每过一个无 429 周期恢复 -10%，下限 1.0。

⚠️ **乘子只乘轮询，不乘重投**（2026-09-21 修 F5，见 ``docs/IMPLEMENTATION.md`` §3.18）：
重投（创建被 429 拒绝后的重试）走 :meth:`_BasePolling.base_interval`——它不含乘子，
上限由 ``AG_SUBMIT_RETRY_MAX_SECONDS`` 兜。乘子的本意是"按上游容忍度调慢**轮询**节奏"，
把它乘到重投上会让一次限流风暴把**创建**也拖到 60s 级，而任务在创建成功前毫无进展 ⇒
被 deadline 收尾成 timeout（livetest-ai 报告 E2E-ASYNC-TASK-001 的 F5 现场）。

直方图存 Redis 桶计数滑动窗口 7 天（10s 分桶，HINCRBY + 键 TTL 滚动过期），
P10/P50/P95 每 5 分钟重算并缓存——万级并发下这条曲线是省上游配额的主要来源。
"""

from __future__ import annotations

import json
import math
import time
from dataclasses import dataclass
from typing import Protocol

from ..config import get_settings
from .redis import get_redis

BUCKET_SECONDS = 10
HIST_TTL_SECONDS = 7 * 24 * 3600
PCT_CACHE_SECONDS = 300
AIMD_MIN = 1.0


@dataclass(frozen=True, slots=True)
class PollStats:
    p10: float
    p50: float
    p95: float
    samples: int
    warm: bool

    @property
    def cold(self) -> bool:
        return not self.warm


def _bucket_label(seconds: float) -> str:
    return str(int(seconds // BUCKET_SECONDS) * BUCKET_SECONDS)


def _percentile(sorted_buckets: list[tuple[int, int]], total: int, q: float, fallback: float) -> float:
    if total <= 0:
        return fallback
    target = total * q
    acc = 0
    for bucket_end, count in sorted_buckets:
        acc += count
        if acc >= target:
            # 用桶上界近似（10s 粒度足够，轮询调度不需要秒级精度）
            return float(max(bucket_end, BUCKET_SECONDS))
    return float(sorted_buckets[-1][0]) if sorted_buckets else fallback


class PollingController(Protocol):
    async def stats(self, channel: str) -> PollStats: ...
    async def record_duration(self, channel: str, seconds: float) -> None: ...
    async def record_terminal(self, channel: str, seconds: float) -> None: ...
    async def next_interval(self, channel: str, *, elapsed: float, first_poll: bool = False) -> float: ...
    async def base_interval(self, channel: str, *, elapsed: float, first_poll: bool = False) -> float: ...
    async def note_rate_limited(self, channel: str, retry_after: float | None) -> float: ...
    async def note_clean_period(self, channel: str) -> float: ...
    async def decay_if_clean(self, channel: str, *, quiet_seconds: float) -> float | None: ...
    async def paused_for(self, channel: str) -> float: ...


class _BasePolling:
    """与存储无关的算法部分（Redis 版与内存版共用）。"""

    def __init__(
        self,
        *,
        base: float | None = None,
        minimum: float | None = None,
        maximum: float | None = None,
        initial: float | None = None,
        hot_start_samples: int | None = None,
    ) -> None:
        s = get_settings()
        self.base = base if base is not None else s.poll_base_interval
        self.minimum = minimum if minimum is not None else s.poll_min_interval
        self.maximum = maximum if maximum is not None else s.poll_max_interval
        self.initial = initial if initial is not None else s.poll_initial_interval
        self.hot_start_samples = hot_start_samples if hot_start_samples is not None else s.poll_hot_start_samples

    def compute_interval(self, elapsed: float, stats: PollStats, *, first_poll: bool = False) -> float:
        if stats.cold:
            return self.clamp(self.initial if first_poll else max(self.initial, self.minimum))
        if first_poll:
            return self.clamp(max(stats.p10, self.minimum))
        p50 = max(stats.p50, self.minimum)
        exponent = int(math.floor(max(0.0, elapsed) / p50))
        exponent = min(exponent, 8)  # 2^8 × base 已是 12+ 分钟量级，够用且防溢出
        return self.clamp(self.base * (2**exponent))

    def clamp(self, value: float) -> float:
        return float(max(self.minimum, min(self.maximum, value)))

    def aimd_up(self, multiplier: float) -> float:
        return min(self.maximum / max(self.base, 1e-6), max(AIMD_MIN, multiplier * 2.0))

    def aimd_down(self, multiplier: float) -> float:
        return max(AIMD_MIN, multiplier * 0.9)


class RedisPollingController(_BasePolling):
    """Redis 版：桶计数直方图 + 百分位缓存 + AIMD 乘子/暂停位。"""

    def _hist_key(self, channel: str) -> str:
        return f"poll:hist:{{{channel}}}"

    def _pct_key(self, channel: str) -> str:
        return f"poll:pct:{{{channel}}}"

    def _aimd_key(self, channel: str) -> str:
        return f"poll:aimd:{{{channel}}}"

    def _pause_key(self, channel: str) -> str:
        return f"poll:pause:{{{channel}}}"

    def _rl_key(self, channel: str) -> str:
        return f"poll:rl:{{{channel}}}"

    @property
    def _rl_ttl_seconds(self) -> int:
        # 比静默期长即可；给足余量避免键先过期而被误判为"从未 429"
        return max(600, int(get_settings().aimd_quiet_seconds * 3))

    async def record_duration(self, channel: str, seconds: float) -> None:
        client = get_redis()
        key = self._hist_key(channel)
        await client.hincrby(key, _bucket_label(seconds), 1)
        await client.expire(key, HIST_TTL_SECONDS)

    async def record_terminal(self, channel: str, seconds: float) -> None:
        """终态时长样本：既进直方图，也用于判断是否已"热启动"。"""
        await self.record_duration(channel, seconds)

    async def stats(self, channel: str) -> PollStats:
        client = get_redis()
        cached = await client.get(self._pct_key(channel))
        if cached:
            payload = json.loads(cached)
            return PollStats(**payload)
        raw = await client.hgetall(self._hist_key(channel))
        buckets = sorted((int(k), int(v)) for k, v in raw.items())
        total = sum(v for _, v in buckets)
        stats = PollStats(
            p10=_percentile(buckets, total, 0.10, self.initial),
            p50=_percentile(buckets, total, 0.50, self.initial),
            p95=_percentile(buckets, total, 0.95, self.initial),
            samples=total,
            warm=total >= self.hot_start_samples,
        )
        await client.set(
            self._pct_key(channel),
            json.dumps(
                {"p10": stats.p10, "p50": stats.p50, "p95": stats.p95, "samples": stats.samples, "warm": stats.warm}
            ),
            ex=PCT_CACHE_SECONDS,
        )
        return stats

    async def next_interval(self, channel: str, *, elapsed: float, first_poll: bool = False) -> float:
        stats = await self.stats(channel)
        interval = self.compute_interval(elapsed, stats, first_poll=first_poll)
        multiplier = await self._multiplier(channel)
        return self.clamp(interval * multiplier)

    async def base_interval(self, channel: str, *, elapsed: float, first_poll: bool = False) -> float:
        """**不含 AIMD 乘子**的间隔（重投退避用，见模块 docstring 与 §3.18）。"""
        stats = await self.stats(channel)
        return self.compute_interval(elapsed, stats, first_poll=first_poll)

    async def note_rate_limited(self, channel: str, retry_after: float | None = None) -> float:
        client = get_redis()
        current = await self._multiplier(channel)
        new = self.aimd_up(current)
        await client.set(self._aimd_key(channel), new, ex=HIST_TTL_SECONDS)
        # 记录"上一次 429 时刻"：AIMD 恢复（decay_if_clean）靠它判断静默期
        await client.set(self._rl_key(channel), time.time(), ex=self._rl_ttl_seconds)
        pause = max(0.0, float(retry_after or 0.0))
        if pause > 0:
            await client.set(self._pause_key(channel), time.time() + pause, ex=max(60, int(pause) + 60))
        return new

    async def note_clean_period(self, channel: str) -> float:
        client = get_redis()
        current = await self._multiplier(channel)
        new = self.aimd_down(current)
        await client.set(self._aimd_key(channel), new, ex=HIST_TTL_SECONDS)
        return new

    async def decay_if_clean(self, channel: str, *, quiet_seconds: float) -> float | None:
        """距上次 429 已静默 ``quiet_seconds`` ⇒ 乘子回落一步；否则不动（返回 ``None``）。"""
        raw = await get_redis().get(self._rl_key(channel))
        try:
            last = float(raw) if raw is not None else None
        except (TypeError, ValueError):
            last = None
        if last is not None and (time.time() - last) < quiet_seconds:
            return None
        return await self.note_clean_period(channel)

    async def paused_for(self, channel: str) -> float:
        client = get_redis()
        raw = await client.get(self._pause_key(channel))
        if not raw:
            return 0.0
        return max(0.0, float(raw) - time.time())

    async def _multiplier(self, channel: str) -> float:
        raw = await get_redis().get(self._aimd_key(channel))
        try:
            return float(raw) if raw is not None else AIMD_MIN
        except (TypeError, ValueError):  # pragma: no cover - 脏数据兜底
            return AIMD_MIN


class MemoryPollingController(_BasePolling):
    """内存版：同一套算法，供无 Redis 的测试与本地联调使用。"""

    def __init__(self, **kwargs: float | int | None) -> None:
        super().__init__(**kwargs)  # type: ignore[arg-type]
        self.buckets: dict[str, dict[int, int]] = {}
        self.multiplier: dict[str, float] = {}
        self.pause_until: dict[str, float] = {}
        self.last_rate_limited: dict[str, float] = {}

    async def record_duration(self, channel: str, seconds: float) -> None:
        label = int(seconds // BUCKET_SECONDS) * BUCKET_SECONDS
        self.buckets.setdefault(channel, {})
        self.buckets[channel][label] = self.buckets[channel].get(label, 0) + 1

    async def record_terminal(self, channel: str, seconds: float) -> None:
        await self.record_duration(channel, seconds)

    async def stats(self, channel: str) -> PollStats:
        raw = self.buckets.get(channel, {})
        items = sorted(raw.items())
        total = sum(v for _, v in items)
        return PollStats(
            p10=_percentile(items, total, 0.10, self.initial),
            p50=_percentile(items, total, 0.50, self.initial),
            p95=_percentile(items, total, 0.95, self.initial),
            samples=total,
            warm=total >= self.hot_start_samples,
        )

    async def next_interval(self, channel: str, *, elapsed: float, first_poll: bool = False) -> float:
        stats = await self.stats(channel)
        base_interval = self.compute_interval(elapsed, stats, first_poll=first_poll)
        return self.clamp(base_interval * self.multiplier.get(channel, AIMD_MIN))

    async def base_interval(self, channel: str, *, elapsed: float, first_poll: bool = False) -> float:
        """**不含 AIMD 乘子**的间隔（重投退避用，见模块 docstring 与 §3.18）。"""
        stats = await self.stats(channel)
        return self.compute_interval(elapsed, stats, first_poll=first_poll)

    async def note_rate_limited(self, channel: str, retry_after: float | None = None) -> float:
        current = self.multiplier.get(channel, AIMD_MIN)
        new = self.aimd_up(current)
        self.multiplier[channel] = new
        self.last_rate_limited[channel] = time.monotonic()
        if retry_after:
            self.pause_until[channel] = time.monotonic() + float(retry_after)
        return new

    async def note_clean_period(self, channel: str) -> float:
        new = self.aimd_down(self.multiplier.get(channel, AIMD_MIN))
        self.multiplier[channel] = new
        return new

    async def decay_if_clean(self, channel: str, *, quiet_seconds: float) -> float | None:
        """距上次 429 已静默 ``quiet_seconds`` ⇒ 乘子回落一步；否则不动（返回 ``None``）。"""
        last = self.last_rate_limited.get(channel)
        if last is not None and (time.monotonic() - last) < quiet_seconds:
            return None
        return await self.note_clean_period(channel)

    async def paused_for(self, channel: str) -> float:
        return max(0.0, self.pause_until.get(channel, 0.0) - time.monotonic())
