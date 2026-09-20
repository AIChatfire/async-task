# 异步网关常用目标。本地无需 Docker：测试与联调默认走 SQLite + 内存 broker + 假上游。
PY ?= /Users/betterme/.workbuddy/binaries/python/envs/async-gateway/bin/python

.PHONY: help install test test-all lint gateway worker scheduler inspector admin migrate revision
.PHONY: compose-up compose-down live-seed3d

help:
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## 安装依赖（可编辑安装）
	$(PY) -m pip install -e ".[dev]"

test: ## 默认测试（不依赖 Redis）
	$(PY) -m pytest -q -m "not redis"

test-all: ## 全量测试（含 Redis 真机 Lua / Streams）
	$(PY) -m pytest -q

gateway: ## 起 gateway-api
	$(PY) -m uvicorn async_gateway.gateway.app:app --host 0.0.0.0 --port 8000 --reload

admin: ## 起 Task-admin（治理面，仅内网）
	$(PY) -m uvicorn async_gateway.admin.app:app --host 0.0.0.0 --port 8080 --reload

worker: ## 起 worker（默认消费 poll 池）
	$(PY) -m async_gateway.workers.worker --pools heavy-poll,light-poll

scheduler: ## 起 scheduler（生产固定单副本）
	$(PY) -m async_gateway.workers.scheduler

inspector: ## 起 inspector（独立单副本，与 scheduler 故障域隔离）
	$(PY) -m async_gateway.workers.inspector

migrate: ## 执行数据库迁移
	$(PY) -m alembic upgrade head

revision: ## 生成迁移（make revision m="add xxx"）
	$(PY) -m alembic revision --autogenerate -m "$(m)"

live-seed3d: ## 真机联调：经网关跑一次图生 3D（需 ARK_API_KEY；会消耗上游额度）
	$(PY) scripts/live_seed3d.py --interval 10

compose-up: ## 起全部依赖与进程（需要 Docker）
	docker compose up -d --build

compose-down:
	docker compose down
