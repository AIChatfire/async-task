"""env 契约门禁：模板 / 代码 / 编排三向一致。

防的是两类静默事故：

* **漏登记** —— 代码在读某个键、模板里没有 ⇒ 运维照模板配 `.env` 时不知道这个开关存在，
  永远用默认值跑，且不觉得少了什么（比"配错"更难发现）；
* **配了不生效** —— 模板声明了键、编排层没注入 ⇒ 容器里读到的是代码默认值，不报错、不告警。

另附两条形态检查：`KEY=  # 注释`（解析器把注释当值，fail-open）与"留空项必须回落代码默认"。
"""

from __future__ import annotations

import re
from pathlib import Path

import yaml

from async_gateway.config import Settings

ROOT = Path(__file__).resolve().parents[1]
ENV_EXAMPLE = ROOT / ".env.example"
COMPOSE = ROOT / "docker-compose.yml"

_EMPTY_WITH_INLINE_COMMENT = re.compile(r"^\s*[A-Z][A-Z0-9_]*=\s+#")


def template_keys() -> dict[str, str]:
    """模板里所有 `KEY=value`（去掉行内注释后的值）。"""
    out: dict[str, str] = {}
    for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        key, value = s.split("=", 1)
        out[key.strip()] = re.split(r"\s+#", value, maxsplit=1)[0].strip()
    return out


def _settings_keys() -> set[str]:
    return {f"AG_{name.upper()}" for name in Settings.model_fields}


def test_env_template_matches_settings_fields_both_ways():
    """双向一致，且两侧都不得为空（防"扫描范围写错 ⇒ 门禁空转"）。"""
    expected = _settings_keys()
    declared = set(template_keys())

    # 防空转：数量下界 + 权威文件必须在场
    assert len(expected) >= 55, f"Settings 字段只有 {len(expected)} 个 ⇒ 导入/映射规则有问题"
    assert len(declared) >= 55, f"模板只解析出 {len(declared)} 个键 ⇒ 解析规则有问题"

    missing = sorted(expected - declared)
    extra = sorted(declared - expected)
    assert missing == [], f"代码会读、但模板没登记（漏登记）: {missing}"
    assert extra == [], f"模板里有、但代码不读（死配置或需改为别名说明）: {extra}"


def test_env_template_has_no_empty_value_with_inline_comment():
    """`KEY=  # 注释` 会被 python-dotenv 系解析成值 = `'# 注释'`（空值语义静默失效）。"""
    bad = [
        line
        for line in ENV_EXAMPLE.read_text(encoding="utf-8").splitlines()
        if _EMPTY_WITH_INLINE_COMMENT.match(line)
    ]
    assert bad == [], f"存在「空值 + 行内注释」形态（会被当成值）: {bad}"


def test_empty_template_values_fall_back_to_code_default():
    """模板里留空的项，代码默认必须是 None —— 否则显式空串会顶掉默认值。"""
    empty = [k for k, v in template_keys().items() if v == ""]
    assert empty, "模板里应至少有一个留空项（如 AG_LOGFIRE_TOKEN / AG_S3_ACCESS_KEY）⇒ 门禁可能在空转"
    for key in empty:
        field = Settings.model_fields[key[len("AG_") :].lower()]
        assert field.default is None or field.default == "", (
            f"{key} 在模板里留空，但代码默认是 {field.default!r}："
            "非空默认值会被空串顶掉（要么改成 `or DEFAULT` 语义，要么模板别留空）"
        )


def test_compose_injects_env_file_into_app_services():
    """编排层必须把 .env 注入 app 服务 —— 否则照模板配的项在容器里全部不生效。"""
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    services = doc["services"]
    app_services = {n: s for n, s in services.items() if isinstance(s, dict) and s.get("build")}
    assert len(app_services) >= 6, f"app 服务只解析出 {len(app_services)} 个 ⇒ 解析有问题"

    for name, svc in app_services.items():
        env_file = svc.get("env_file") or []
        paths = [e["path"] if isinstance(e, dict) else e for e in env_file]
        assert ".env" in paths, f"{name} 未注入 .env ⇒ 模板声明的配置项在容器里不生效"


def test_compose_app_env_allows_dotenv_override():
    """`environment:` 优先级高于 `env_file:`，所以那几项必须写成 ${VAR:-默认} 才能被 .env 覆盖。"""
    doc = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    app_env = doc["x-app-env"]
    assert len(app_env) >= 5, f"x-app-env 只解析出 {len(app_env)} 项 ⇒ 解析有问题"
    for key, value in app_env.items():
        assert isinstance(value, str) and value.startswith("${") and ":-" in value, (
            f"{key} 的值是字面量 ⇒ 会覆盖 .env，用户在 .env 里改了不生效"
        )
