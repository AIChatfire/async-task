"""异步网关：给既有「已任务化」上游套一层排队异步能力，上游零改动。

分层：
    domain    —— 状态机、错误分级、取值域（无 IO）
    templates —— 三级配置面 + 约定推导 + 沙箱校验 + 版本/灰度
    security  —— SSRF、凭证脱敏、回调验签
    db        —— 真相与审计（Postgres）
    gateway   —— /async/ 协议面（受理/查询/取消/回调）
    bus/tasks —— Redis Streams 队列与任务
    workers   —— worker / scheduler / inspector
    admin     —— 治理面（Task-admin）
"""

__version__ = "0.1.0"
