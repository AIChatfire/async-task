"""URL 直配过渡通道（§11 / §4.4 / §20 下线 milestone）。

"URL 直配"= 不经模板库注册、也未经评审，直接由**渠道策略内联一份极简配置**就放流量。
它是过渡能力，因此被三样东西管住：

1. **独立指标** ``ag_url_direct_total``（含 channel/alias/kind），下线时看得到存量；
2. **按渠道开关**（``url_direct_enabled``）+ **一键全局关闭**（``AG_URL_DIRECT_CONFIG_ENABLED=false``）；
3. **path 前缀白名单**约束漫游范围——白名单为空则只允许该内联配置自己的路径。

M6 验收项："直配一键全局关闭验证"。
"""

from __future__ import annotations

import logging
from typing import Any

from ..templates.derive import resolve
from ..templates.registry import TemplateVersion
from ..templates.schema import FieldSource, TemplateDraft
from ..templates.validator import validate
from .container import Container

logger = logging.getLogger(__name__)

MINIMAL_KEYS = ("base_url", "create_path", "result_location")
OPTIONAL_KEYS = (
    "id_location",
    "get_path_template",
    "cancel_path_template",
    "result_policy",
    "capabilities",
    "terminal",
    "normalize",
    "strategy",
    "pool",
)


def direct_config(container: Container, channel: str) -> dict[str, Any] | None:
    policy = container.channel_policies.get(channel) or {}
    cfg = policy.get("url_direct")
    return cfg if isinstance(cfg, dict) else None


def path_allowed(container: Container, channel: str, path: str) -> bool:
    cfg = direct_config(container, channel)
    if not cfg:
        return False
    prefixes = cfg.get("allowed_prefixes") or container.settings.url_direct_allowed_prefixes
    if not prefixes:
        # 未声明前缀白名单时，只允许该内联配置自己声明的路径，避免"任意漫游"
        prefixes = [str(cfg.get("create_path", "/"))]
    return any(path.startswith(str(p)) for p in prefixes)


def build_direct_version(
    container: Container,
    channel: str,
    alias: str,
    path: str = "/",
) -> TemplateVersion | None:
    """用渠道内联配置合成一个临时 TemplateVersion（不落模板库、不进灰度）。"""
    cfg = direct_config(container, channel)
    if not cfg:
        return None
    missing = [k for k in MINIMAL_KEYS if not cfg.get(k)]
    if missing:
        logger.warning("url_direct config for channel %s missing keys: %s", channel, missing)
        return None
    if not path_allowed(container, channel, path):
        logger.warning("url_direct path not allowlisted: channel=%s path=%s", channel, path)
        return None

    raw: dict[str, Any] = {"alias": alias}
    for key in MINIMAL_KEYS + OPTIONAL_KEYS:
        if key in cfg and cfg[key] is not None:
            raw[key] = cfg[key]

    report = validate(raw)
    if not report.ok or report.resolved is None:
        logger.warning(
            "url_direct config for channel %s rejected by validator: %s",
            channel,
            [i.as_dict() for i in report.errors],
        )
        return None

    resolved = resolve(TemplateDraft.model_validate(raw))
    resolved.provenance["_direct"] = FieldSource.EXPLICIT.value
    return TemplateVersion(
        alias=alias,
        namespace=f"direct:{channel}",
        version=0,
        enabled=True,
        resolved=resolved,
        raw=raw,
        patch_ids=["url_direct"],
    )
