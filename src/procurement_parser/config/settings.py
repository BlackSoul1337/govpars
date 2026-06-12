from __future__ import annotations

import json
import os
import tomllib
from pathlib import Path
from typing import Any, Literal

from dotenv import load_dotenv
from pydantic import BaseModel, Field, SecretStr

from procurement_parser.domain.models import Source


class AppSettings(BaseModel):
    log_level: str = "INFO"
    log_to_file: bool = True
    log_dir: str = "logs"
    log_filename: str = "procurement-parser.jsonl"
    log_max_bytes: int = 10_485_760
    log_backup_count: int = 5
    revision_mode: Literal["changes", "off"] = "changes"
    metrics_port: int = 9108
    history_retention_days: int = 90


class DatabaseSettings(BaseModel):
    url: str
    pool_size: int = 10
    max_overflow: int = 20
    statement_timeout_seconds: int = 60


class SourceConcurrency(BaseModel):
    direct: int = 4
    proxy: int = 8


class SourceDiscovery(BaseModel):
    direct: int = Field(default=1, ge=1, le=64)
    proxy: int = Field(default=1, ge=1, le=64)


class SourceEndpoints(BaseModel):
    lots: str = ""
    adverts: str = ""
    plan_items: str = ""


class SourceSettings(BaseModel):
    name: str
    base_url: str
    request_timeout_seconds: int = 30
    per_page: int = 50
    max_attempts: int = 5
    active_refresh_seconds: int = 900
    list_refresh_seconds: int = 300
    closed_refresh_seconds: int = 86400
    old_refresh_seconds: int = 604800
    full_reconcile_seconds: int = 604800
    recently_closed_window_seconds: int = 1209600
    browser_profile_dir: str | None = None
    concurrency: SourceConcurrency = Field(default_factory=SourceConcurrency)
    discovery: SourceDiscovery = Field(default_factory=SourceDiscovery)
    endpoints: SourceEndpoints = Field(default_factory=SourceEndpoints)


class NetworkSettings(BaseModel):
    kind: Literal["direct", "static_proxy", "sticky_residential", "sticky_mobile"]
    proxy_url: SecretStr | None = None
    proxy_pool_file: str | None = None
    proxy_pool_index: int = 0
    proxy_pool_urls: list[SecretStr] = Field(default_factory=list)
    rotate_url: SecretStr | None = None
    rotate_method: Literal["GET", "POST"] = "GET"
    rotate_timeout_seconds: int = 30
    sticky_ttl_seconds: int = 0
    max_lanes: int = 1
    cooldown_seconds: int = 600
    block_threshold: int = 3


class CaptchaSettings(BaseModel):
    provider: Literal["disabled", "manual", "2captcha"]
    api_key: SecretStr | None = None
    max_attempts: int = 2
    poll_interval_seconds: int = 5
    timeout_seconds: int = 180
    max_cost_per_hour: float = 1.0
    fallback_provider: str | None = None


class RuntimeSettings(BaseModel):
    worker_count: int = 4
    claim_batch_size: int = 10
    lease_seconds: int = 180
    backfill_capacity_percent: int = 20
    browser_lanes: int = 1
    browser_navigation_timeout_seconds: int = 60
    browser_capture_timeout_seconds: int = 60
    browser_response_timeout_seconds: int = 60
    browser_disk_cache_mb: int = 512
    browser_preload_main_bundle: bool = False
    metrics_sample_seconds: int = 15


class Settings(BaseModel):
    app: AppSettings
    database: DatabaseSettings
    source: SourceSettings
    network: NetworkSettings
    captcha: CaptchaSettings
    runtime: RuntimeSettings
    config_dir: Path


def _read_toml(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _env_secret(config: dict[str, Any], key: str, fallback_env: str) -> str | None:
    env_name = config.pop(key, None) or fallback_env
    return os.getenv(env_name) or None


def _source_file(source: Source) -> str:
    return source.value.replace("-", "_")


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_settings(
    *,
    source: Source,
    runtime_profile: str = "local",
    network_profile: str = "direct",
    captcha_profile: str = "disabled",
    config_dir: Path | None = None,
) -> Settings:
    load_dotenv(override=False)
    root = config_dir or Path(os.getenv("PROCUREMENT_CONFIG_DIR", "config"))
    app_data = _read_toml(root / "app.toml")["app"]
    db_data = _read_toml(root / "database.toml")["database"]
    source_doc = _read_toml(root / "sources" / f"{_source_file(source)}.toml")
    network_data = _read_toml(root / "network" / f"{network_profile}.toml")["network"]
    captcha_data = _read_toml(root / "captcha" / f"{captcha_profile}.toml")["captcha"]
    runtime_data = _read_toml(root / "runtime" / f"{runtime_profile}.toml")["runtime"]
    runtime_data["worker_count"] = int(
        os.getenv("WORKER_COUNT") or runtime_data.get("worker_count", 4)
    )
    runtime_data["browser_lanes"] = int(
        os.getenv("BROWSER_LANES") or runtime_data.get("browser_lanes", 1)
    )

    db_url_env = db_data.pop("url_env", "DATABASE_URL")
    db_data["url"] = os.getenv(
        db_url_env,
        "postgresql+asyncpg://procurement:procurement@localhost:5432/procurement",
    )

    app_data["log_level"] = os.getenv("LOG_LEVEL", app_data["log_level"])
    app_data["log_to_file"] = _env_bool(
        "LOG_TO_FILE",
        app_data.get("log_to_file", True),
    )
    app_data["log_dir"] = os.getenv("LOG_DIR", app_data.get("log_dir", "logs"))
    app_data["log_filename"] = os.getenv(
        "LOG_FILENAME",
        app_data.get("log_filename", "procurement-parser.jsonl"),
    )
    app_data["revision_mode"] = os.getenv("REVISION_MODE", app_data["revision_mode"])
    app_data["metrics_port"] = int(
        os.getenv("METRICS_PORT", app_data["metrics_port"])
    )

    network_data["proxy_url"] = _env_secret(network_data, "proxy_url_env", "PROXY_URL")
    pool_file_env = network_data.pop("proxy_pool_file_env", "PROXY_POOL_FILE")
    if pool_file := os.getenv(pool_file_env):
        network_data["proxy_pool_file"] = pool_file
    network_data["max_lanes"] = int(
        os.getenv("NETWORK_MAX_LANES") or network_data.get("max_lanes", 1)
    )
    pool_index_env = network_data.pop("proxy_pool_index_env", "PROXY_POOL_INDEX")
    network_data["proxy_pool_index"] = int(
        os.getenv(pool_index_env, network_data.get("proxy_pool_index", 0))
    )
    if not network_data["proxy_url"] and network_data.get("proxy_pool_file"):
        pool_path = Path(network_data["proxy_pool_file"])
        if not pool_path.is_absolute():
            pool_path = root / pool_path
        with pool_path.open(encoding="utf-8") as handle:
            pool_document = json.load(handle)
        proxies = pool_document.get("proxies", pool_document)
        if not proxies:
            raise ValueError(f"Proxy pool is empty: {pool_path}")
        index = network_data["proxy_pool_index"] % len(proxies)
        selected = proxies[index]
        network_data["proxy_url"] = (
            selected["url"] if isinstance(selected, dict) else selected
        )
        network_data["proxy_pool_urls"] = [
            item["url"] if isinstance(item, dict) else item for item in proxies
        ]
    network_data["rotate_url"] = _env_secret(
        network_data,
        "rotate_url_env",
        "PROXY_ROTATE_URL",
    )
    captcha_data["api_key"] = _env_secret(
        captcha_data,
        "api_key_env",
        "TWOCAPTCHA_API_KEY",
    )
    max_cost_env = captcha_data.pop("max_cost_per_hour_env", "CAPTCHA_MAX_COST_PER_HOUR")
    captcha_data["max_cost_per_hour"] = float(
        os.getenv(max_cost_env, captcha_data.get("max_cost_per_hour", 1.0))
    )

    source_data = dict(source_doc["source"])
    source_data["browser_profile_dir"] = os.getenv(
        "BROWSER_PROFILE_DIR",
        source_data.get("browser_profile_dir"),
    )
    source_data["concurrency"] = source_doc.get("source", {}).get("concurrency", {})
    source_data["discovery"] = source_doc.get("source", {}).get("discovery", {})
    source_data["endpoints"] = source_doc.get("source", {}).get("endpoints", {})

    return Settings(
        app=AppSettings.model_validate(app_data),
        database=DatabaseSettings.model_validate(db_data),
        source=SourceSettings.model_validate(source_data),
        network=NetworkSettings.model_validate(network_data),
        captcha=CaptchaSettings.model_validate(captcha_data),
        runtime=RuntimeSettings.model_validate(runtime_data),
        config_dir=root,
    )


def discovery_concurrency(
    settings: Settings,
    override: int | None = None,
) -> int:
    if settings.source.name != Source.EEP_MITWORK.value:
        return 1
    if override is not None:
        if not 1 <= override <= 64:
            raise ValueError("discovery concurrency must be between 1 and 64")
        return override
    if settings.network.kind == "direct":
        return settings.source.discovery.direct
    return settings.source.discovery.proxy


def configure_discovery_concurrency(
    settings: Settings,
    override: int | None = None,
) -> int:
    effective = discovery_concurrency(settings, override)
    if settings.source.name == Source.EEP_MITWORK.value:
        settings.source.concurrency.direct = max(
            settings.source.concurrency.direct,
            effective,
        )
        settings.source.concurrency.proxy = max(
            settings.source.concurrency.proxy,
            effective,
        )
    return effective
