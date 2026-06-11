from pathlib import Path

from procurement_parser.config.settings import load_settings
from procurement_parser.domain.models import Source


def test_profiles_are_independent(monkeypatch) -> None:
    monkeypatch.setenv("PROXY_URL", "http://proxy.example:8080")
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="residential",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )

    assert settings.runtime.worker_count == 4
    assert settings.network.kind == "sticky_residential"
    assert settings.network.proxy_url.get_secret_value() == "http://proxy.example:8080"
    assert settings.captcha.provider == "disabled"
    assert settings.source.name == "zakup-sk"


def test_empty_optional_secrets_are_treated_as_missing(monkeypatch) -> None:
    monkeypatch.setenv("PROXY_URL", "")
    monkeypatch.setenv("PROXY_ROTATE_URL", "")
    monkeypatch.setenv("TWOCAPTCHA_API_KEY", "")
    settings = load_settings(
        source=Source.EEP_MITWORK,
        runtime_profile="local",
        network_profile="direct",
        captcha_profile="2captcha",
        config_dir=Path("config"),
    )

    assert settings.network.proxy_url is None
    assert settings.network.rotate_url is None
    assert settings.captcha.api_key is None


def test_slow_internet_runtime_extends_browser_timeouts(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_COUNT", "2")
    monkeypatch.setenv("BROWSER_LANES", "1")
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="slow_internet",
        network_profile="direct",
        captcha_profile="manual",
        config_dir=Path("config"),
    )

    assert settings.runtime.worker_count == 2
    assert settings.runtime.lease_seconds == 900
    assert settings.runtime.browser_navigation_timeout_seconds == 360
    assert settings.runtime.browser_capture_timeout_seconds == 360
    assert settings.runtime.browser_response_timeout_seconds == 360
    assert settings.runtime.browser_disk_cache_mb == 512
    assert settings.runtime.browser_preload_main_bundle is True


def test_empty_concurrency_overrides_use_profile_values(monkeypatch) -> None:
    monkeypatch.setenv("WORKER_COUNT", "")
    monkeypatch.setenv("BROWSER_LANES", "")
    monkeypatch.setenv("NETWORK_MAX_LANES", "")

    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="slow_internet",
        network_profile="direct",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )

    assert settings.runtime.worker_count == 2
    assert settings.runtime.browser_lanes == 1
    assert settings.network.max_lanes == 1


def test_public_pool_loads_all_validated_proxies(monkeypatch, tmp_path) -> None:
    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.setenv("PROXY_POOL_INDEX", "1")
    pool = tmp_path / "pool.json"
    pool.write_text(
        '{"proxies": ['
        '{"url": "http://proxy-1.example:8000"},'
        '{"url": "http://proxy-2.example:8000"}'
        "]}",
        encoding="utf-8",
    )
    monkeypatch.setenv("PROXY_POOL_FILE", str(pool))
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="public_pool",
        captcha_profile="manual",
        config_dir=Path("config"),
    )

    assert len(settings.network.proxy_pool_urls) > 1
    assert settings.network.proxy_url == settings.network.proxy_pool_urls[1]
