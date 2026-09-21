"""指标（§16 / §17）。

不引 Prometheus 客户端依赖：用一个极小的计数器/直方图注册表 + 文本导出
（Prometheus exposition format）即可，避免为几个指标拖进一个 C 扩展。

指标清单按 §17 的"核心指标"组织：受理速率、队列深度、任务年龄、attempts 分布、
终态分布、unknown 两态数、死信速率、上游 429/5xx 比率、端到端时延、**直配调用量
（独立指标）**、存储用量/成本。
"""

from __future__ import annotations

import threading
from collections import defaultdict
from dataclasses import dataclass, field

_LOCK = threading.Lock()


@dataclass
class Counter:
    name: str
    help: str
    samples: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict)

    def inc(self, labels: dict[str, str] | None = None, value: float = 1.0) -> None:
        key = tuple(sorted((labels or {}).items()))
        with _LOCK:
            self.samples[key] = self.samples.get(key, 0.0) + value

    def render(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} counter"]
        with _LOCK:
            items = list(self.samples.items())
        for key, value in items:
            label_str = ",".join(f'{k}="{v}"' for k, v in key)
            lines.append(f"{self.name}{{{label_str}}} {value}" if label_str else f"{self.name} {value}")
        return lines


@dataclass
class Gauge:
    name: str
    help: str
    values: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict)

    def set(self, value: float, labels: dict[str, str] | None = None) -> None:
        key = tuple(sorted((labels or {}).items()))
        with _LOCK:
            self.values[key] = value

    def add(self, value: float, labels: dict[str, str] | None = None) -> None:
        key = tuple(sorted((labels or {}).items()))
        with _LOCK:
            self.values[key] = self.values.get(key, 0.0) + value

    def render(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} gauge"]
        with _LOCK:
            items = list(self.values.items())
        for key, value in items:
            label_str = ",".join(f'{k}="{v}"' for k, v in key)
            lines.append(f"{self.name}{{{label_str}}} {value}" if label_str else f"{self.name} {value}")
        return lines


@dataclass
class Histogram:
    name: str
    help: str
    buckets: tuple[float, ...] = (0.005, 0.025, 0.1, 0.5, 1.0, 5.0, 30.0, 300.0)
    counts: dict[tuple[tuple[str, str], ...], list[int]] = field(default_factory=dict)
    sums: dict[tuple[tuple[str, str], ...], float] = field(default_factory=dict)
    totals: dict[tuple[tuple[str, str], ...], int] = field(default_factory=dict)

    def observe(self, value: float, labels: dict[str, str] | None = None) -> None:
        key = tuple(sorted((labels or {}).items()))
        with _LOCK:
            counts = self.counts.setdefault(key, [0] * len(self.buckets))
            for i, bound in enumerate(self.buckets):
                if value <= bound:
                    counts[i] += 1
            self.sums[key] = self.sums.get(key, 0.0) + value
            self.totals[key] = self.totals.get(key, 0) + 1

    def render(self) -> list[str]:
        lines = [f"# HELP {self.name} {self.help}", f"# TYPE {self.name} histogram"]
        with _LOCK:
            items = list(self.counts.items())
            sums = dict(self.sums)
            totals = dict(self.totals)
        for key, counts in items:
            label_str = ",".join(f'{k}="{v}"' for k, v in key)
            for i, bound in enumerate(self.buckets):
                suffix = f'le="{bound}"'
                all_labels = f"{label_str},{suffix}" if label_str else suffix
                lines.append(f"{self.name}_bucket{{{all_labels}}} {counts[i]}")
            inf_labels = f"{label_str},le=\"+Inf\"" if label_str else 'le="+Inf"'
            lines.append(f"{self.name}_bucket{{{inf_labels}}} {totals.get(key, 0)}")
            base = f"{{{label_str}}}" if label_str else ""
            lines.append(f"{self.name}_sum{base} {sums.get(key, 0.0)}")
            lines.append(f"{self.name}_count{base} {totals.get(key, 0)}")
        return lines


class _Registry:
    def __init__(self) -> None:
        self.counters: dict[str, Counter] = {}
        self.gauges: dict[str, Gauge] = {}
        self.histograms: dict[str, Histogram] = {}

    def counter(self, name: str, help_text: str) -> Counter:
        if name not in self.counters:
            self.counters[name] = Counter(name, help_text)
        return self.counters[name]

    def gauge(self, name: str, help_text: str) -> Gauge:
        if name not in self.gauges:
            self.gauges[name] = Gauge(name, help_text)
        return self.gauges[name]

    def histogram(self, name: str, help_text: str) -> Histogram:
        if name not in self.histograms:
            self.histograms[name] = Histogram(name, help_text)
        return self.histograms[name]

    def render(self) -> str:
        blocks: list[str] = []
        for counter in self.counters.values():
            blocks.extend(counter.render())
        for gauge in self.gauges.values():
            blocks.extend(gauge.render())
        for hist in self.histograms.values():
            blocks.extend(hist.render())
        return "\n".join(blocks) + "\n"

    def reset(self) -> None:
        self.counters.clear()
        self.gauges.clear()
        self.histograms.clear()


REGISTRY = _Registry()


# ---- 具名指标（避免散落的字符串）----
ACCEPT_TOTAL = REGISTRY.counter("ag_accept_total", "受理请求数（按结果与渠道）")
ACCEPT_REJECTED = REGISTRY.counter("ag_accept_rejected_total", "受理被拒数（配额/队列深度）")
TASK_TERMINAL_TOTAL = REGISTRY.counter("ag_task_terminal_total", "终态任务数")
TASK_UNKNOWN_TOTAL = REGISTRY.counter("ag_task_unknown_total", "unknown 两态发生数")
UPSTREAM_CALLS = REGISTRY.counter("ag_upstream_calls_total", "上游调用数（按上下文与结果）")
UPSTREAM_STATUS = REGISTRY.counter("ag_upstream_status_total", "上游 HTTP 状态码分布")
POLL_429 = REGISTRY.counter("ag_poll_rate_limited_total", "轮询 429 次数")
TRANSFER_TOTAL = REGISTRY.counter("ag_transfer_total", "结果转存结果分布")
URL_DIRECT_TOTAL = REGISTRY.counter("ag_url_direct_total", "URL 直配调用量（独立指标，用于下线）")
CALLBACK_TOTAL = REGISTRY.counter("ag_callback_total", "回调处理结果分布")
SIGNATURE_FAILURES = REGISTRY.counter("ag_callback_signature_failure_total", "回调验签失败数")
GOVERNANCE_TOTAL = REGISTRY.counter("ag_governance_total", "治理操作数")
QUEUE_DEPTH = REGISTRY.gauge("ag_queue_depth", "队列深度")
TASK_AGE = REGISTRY.gauge("ag_oldest_queue_message_age_seconds", "队列最老消息年龄")
ACTIVE_TASKS = REGISTRY.gauge("ag_active_tasks", "活动任务数（按渠道）")
UNKNOWN_GAUGE = REGISTRY.gauge("ag_unknown_tasks", "unknown 任务数（按渠道与状态）")
DEAD_TOTAL = REGISTRY.counter("ag_dead_total", "死信速率")
QUERY_REFRESH_TOTAL = REGISTRY.counter(
    "ag_query_refresh_total",
    "查询面透传刷新结果分布（ok / upstream_not_ok / error / skipped_*）",
)
ACCEPTED_RESUBMIT_TOTAL = REGISTRY.counter(
    "ag_accepted_resubmit_dispatch_total", "accepted 安全重投的派发数（无提交意图且到期）"
)
ENQUEUE_LAG = REGISTRY.histogram("ag_enqueue_lag_seconds", "消息从入队到被消费的时延")
TASK_E2E_LATENCY = REGISTRY.histogram("ag_task_e2e_latency_seconds", "任务端到端时延")
STORAGE_BYTES = REGISTRY.gauge("ag_result_storage_bytes", "结果存储用量估算")
HISTOGRAM_BY_CHANNEL: dict[str, Counter] = defaultdict(lambda: REGISTRY.counter("ag_terminal_duration_total", "终态时长样本"))


def render_metrics() -> str:
    return REGISTRY.render()


def reset_metrics() -> None:
    REGISTRY.reset()
