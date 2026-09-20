# 异步网关镜像（⚠️ 本机无 docker CLI，本文件未被构建/运行验证过）
FROM python:3.12-slim

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

# slim 镜像没有 curl，用 Python 做探针
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/healthz', timeout=2).status==200 else 1)"

CMD ["uvicorn", "async_gateway.gateway.app:app", "--host", "0.0.0.0", "--port", "8000"]
