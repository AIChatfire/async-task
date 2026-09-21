# 异步网关镜像（⚠️ 本机无 docker CLI，本文件未被本地构建验证；构建由 CI 承担）
FROM python:3.12-slim

# 镜像版本自证：CI 传 `--build-arg APP_VERSION=<版本>`（镜像 tag **无 v 前缀**）。
# 它不是应用配置项（Settings 不读），只为 `docker inspect` 时能核对镜像版本。
ARG APP_VERSION=dev
ENV AG_IMAGE_VERSION=$APP_VERSION

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# 先装依赖（利用层缓存）：只依赖 pyproject，不依赖源码
COPY pyproject.toml ./
RUN pip install --no-cache-dir \
        "fastapi>=0.115" "uvicorn[standard]>=0.30" "pydantic>=2.8" "pydantic-settings>=2.4" \
        "sqlalchemy[asyncio]>=2.0" asyncpg aiosqlite alembic "redis>=5.0" httpx pyyaml minio \
        "python-multipart>=0.0.9"

COPY src ./src
COPY alembic.ini ./
COPY alembic ./alembic
COPY scripts ./scripts
COPY pyproject.toml README.md ./
RUN pip install --no-cache-dir -e .

# 非 root 运行
RUN useradd --create-home --uid 10001 app && chown -R app:app /app
USER app

EXPOSE 8000

# 健康探针（**角色感知**，2026-09-21 修 F2）：
#   镜像原先只会探 127.0.0.1:8000/healthz —— 那是**默认 CMD**（gateway-api）的路径。
#   worker / scheduler / inspector / transfer-worker 不在 8000 上监听，于是恒为 unhealthy
#   （livetest-ai 报告 E2E-ASYNC-TASK-001 的 F2：探的不是自己的进程）。
#   现在探针自己判角色：web 角色走 HTTP；循环角色比对「循环心跳文件」的新鲜度
#   （见 src/async_gateway/observability/heartbeat.py）。
#   角色来源：AG_PROBE_ROLE 环境变量 > 从 PID 1 的 cmdline 推断（默认 CMD 无需声明）。
#   阈值/目录：AG_PROBE_MAX_AGE_SECONDS（默认 120s）/ AG_HEARTBEAT_DIR（默认 /tmp/ag-heartbeat）。
HEALTHCHECK --interval=15s --timeout=3s --start-period=15s --retries=5 CMD ["python", "/app/scripts/healthcheck.py"]

CMD ["uvicorn", "async_gateway.gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
