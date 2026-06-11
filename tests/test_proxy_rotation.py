import logging
import time
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import respx
import structlog
from httpx import Response

from procurement_parser.config.settings import load_settings
from procurement_parser.domain.models import Source
from procurement_parser.infrastructure.captcha.solvers import DisabledCaptchaSolver
from procurement_parser.infrastructure.network.proxy_pool import ProxyPoolCursor
from procurement_parser.infrastructure.sources.eep_mitwork.client import EepHttpClient
from procurement_parser.infrastructure.sources.zakup_sk.adapter import ZakupSkAdapter
from procurement_parser.infrastructure.sources.zakup_sk.strategies import ApiResponse
from procurement_parser.observability import configure_logging

PUBLIC_POOL_EXAMPLE = str(
    Path("config/proxy_pools/public_pool.example.json").resolve()
)


def test_proxy_pool_cursor_skips_active_and_cooling_proxies() -> None:
    cursor = ProxyPoolCursor(
        ["http://proxy-1", "http://proxy-2", "http://proxy-3"],
        cooldown_seconds=60,
    )
    cursor.mark_failed("http://proxy-1")

    assert cursor.next(exclude={"http://proxy-2"}) == "http://proxy-3"


@pytest.mark.asyncio
async def test_eep_public_pool_uses_parallel_lanes_and_reserve(monkeypatch) -> None:
    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.setenv("PROXY_POOL_INDEX", "0")
    monkeypatch.setenv(
        "PROXY_POOL_FILE",
        PUBLIC_POOL_EXAMPLE,
    )
    settings = load_settings(
        source=Source.EEP_MITWORK,
        runtime_profile="local",
        network_profile="public_pool",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    client = EepHttpClient(settings.source, settings.network)
    old = client._lanes[0]
    try:
        assert client.lane_count == settings.network.max_lanes
        assert await client._replace_lane(0, old, failed=True)
        assert client._lanes[0].proxy_url != old.proxy_url
        assert client._lanes[0].generation == 1
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_public_pool_replaces_lane_from_reserve(monkeypatch) -> None:
    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.setenv("PROXY_POOL_INDEX", "0")
    monkeypatch.setenv(
        "PROXY_POOL_FILE",
        PUBLIC_POOL_EXAMPLE,
    )
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="public_pool",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    adapter = ZakupSkAdapter(
        settings.source,
        settings.network,
        settings.captcha,
        DisabledCaptchaSolver(),
        settings.runtime,
    )
    old = adapter.stacks[0]
    old_proxy = old.browser.network.proxy_url
    try:
        adapter._lane_in_use[0] = True
        assert await adapter._release_stack(0, old, exhausted=True, failed=True)
        replacement = adapter.stacks[0]
        assert replacement is not old
        assert replacement.browser.network.proxy_url != old_proxy
        assert replacement.browser.session_generation == 1
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_zakup_transport_failure_rotates_and_retries_same_request(
    monkeypatch,
) -> None:
    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.setenv("NETWORK_MAX_LANES", "1")
    monkeypatch.setenv("PROXY_POOL_INDEX", "0")
    monkeypatch.setenv("PROXY_POOL_FILE", PUBLIC_POOL_EXAMPLE)
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="public_pool",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    adapter = ZakupSkAdapter(
        settings.source,
        settings.network,
        settings.captcha,
        DisabledCaptchaSolver(),
        settings.runtime,
    )
    first = adapter.stacks[0]
    replacement = adapter._build_stack(
        0,
        settings.network.model_copy(
            update={"proxy_url": settings.network.proxy_pool_urls[1]}
        ),
    )
    first.request = AsyncMock(side_effect=httpx.ProxyError("dead proxy"))
    replacement.request = AsyncMock(
        return_value=ApiResponse(
            status=200,
            headers={},
            data={"content": []},
            strategy="curl-cffi",
        )
    )
    monkeypatch.setattr(
        adapter,
        "_replacement_stack",
        AsyncMock(return_value=replacement),
    )
    try:
        response = await adapter._request(
            "POST",
            "https://zakup.sk.kz/eprocsearch/api/external/lots/filter",
            body={},
        )
    finally:
        await adapter.close()

    assert response.status == 200
    assert adapter.stacks[0] is replacement
    replacement.request.assert_awaited_once()


@pytest.mark.asyncio
async def test_eep_transport_failure_rotates_before_next_attempt(
    monkeypatch,
) -> None:
    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.setenv("NETWORK_MAX_LANES", "1")
    monkeypatch.setenv("PROXY_POOL_INDEX", "0")
    monkeypatch.setenv("PROXY_POOL_FILE", PUBLIC_POOL_EXAMPLE)
    settings = load_settings(
        source=Source.EEP_MITWORK,
        runtime_profile="local",
        network_profile="public_pool",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    client = EepHttpClient(settings.source, settings.network)
    first = client._lanes[0]
    replacement = client._build_lane("http://proxy-replacement.example:8000")
    first.client.get = AsyncMock(side_effect=httpx.ProxyError("dead proxy"))
    replacement.client.get = AsyncMock(
        return_value=httpx.Response(
            200,
            text="<html></html>",
            request=httpx.Request(
                "GET",
                "https://eep.mitwork.kz/ru/publics/lots",
            ),
        )
    )

    async def replace(_lane_index, _lane, *, failed):
        assert failed is True
        client._lanes[0] = replacement
        return True

    monkeypatch.setattr(client, "_replace_lane", replace)
    try:
        response = await client.get("/ru/publics/lots")
    finally:
        await client.close()

    assert response.status_code == 200
    replacement.client.get.assert_awaited_once()


@pytest.mark.asyncio
async def test_direct_zakup_uses_runtime_browser_lane_count(monkeypatch) -> None:
    monkeypatch.delenv("PROXY_URL", raising=False)
    monkeypatch.delenv("BROWSER_LANES", raising=False)
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="direct",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    adapter = ZakupSkAdapter(
        settings.source,
        settings.network,
        settings.captcha,
        DisabledCaptchaSolver(),
        settings.runtime,
    )
    try:
        assert len(adapter.stacks) == settings.runtime.browser_lanes == 2
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_direct_zakup_does_not_retry_or_rotate_an_open_lane(monkeypatch) -> None:
    monkeypatch.delenv("PROXY_URL", raising=False)
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="direct",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    adapter = ZakupSkAdapter(
        settings.source,
        settings.network,
        settings.captcha,
        DisabledCaptchaSolver(),
        settings.runtime,
    )
    try:
        open_stack = adapter.stacks[0]
        for _ in range(settings.network.block_threshold):
            await open_stack.lane_breaker.record_transport_failure()
        adapter._next_lane = 0

        acquired = await adapter._acquire_stack(set())

        assert acquired is not None
        lane_index, healthy_stack = acquired
        assert lane_index == 1
        assert healthy_stack is adapter.stacks[1]
        assert not await adapter._release_stack(
            lane_index,
            healthy_stack,
            exhausted=True,
        )
        assert adapter.stacks[lane_index] is healthy_stack
    finally:
        await adapter.close()


@pytest.mark.asyncio
@respx.mock
async def test_residential_provider_rotation_rebuilds_session(monkeypatch) -> None:
    monkeypatch.setenv("PROXY_URL", "http://user:password@proxy.example:8000")
    monkeypatch.setenv("PROXY_ROTATE_URL", "https://proxy.example/rotate")
    respx.get("https://proxy.example/rotate").mock(return_value=Response(200))
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="residential",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    adapter = ZakupSkAdapter(
        settings.source,
        settings.network,
        settings.captcha,
        DisabledCaptchaSolver(),
        settings.runtime,
    )
    old = adapter.stacks[0]
    try:
        adapter._lane_in_use[0] = True
        assert await adapter._release_stack(0, old, exhausted=True, failed=False)
        replacement = adapter.stacks[0]
        assert replacement is not old
        assert replacement.browser.network.proxy_url == old.browser.network.proxy_url
        assert replacement.browser.session_generation == 1
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_sticky_lane_expires_after_configured_ttl(monkeypatch) -> None:
    monkeypatch.setenv("PROXY_URL", "http://proxy.example:8000")
    monkeypatch.setenv("PROXY_ROTATE_URL", "https://proxy.example/rotate")
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="mobile",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    adapter = ZakupSkAdapter(
        settings.source,
        settings.network,
        settings.captcha,
        DisabledCaptchaSolver(),
        settings.runtime,
    )
    try:
        adapter._lane_started_at[0] = (
            time.monotonic() - settings.network.sticky_ttl_seconds - 1
        )
        assert adapter._lane_ttl_expired(0)
    finally:
        await adapter.close()


def test_logging_writes_rotating_jsonl_file(tmp_path) -> None:
    configure_logging(
        "INFO",
        log_dir=str(tmp_path),
        log_filename="worker.jsonl",
        log_max_bytes=1024,
        log_backup_count=1,
    )
    structlog.get_logger().info("test_file_log", task_id=42)
    for handler in logging.getLogger().handlers:
        handler.flush()

    content = (tmp_path / "worker.jsonl").read_text(encoding="utf-8")
    assert '"event": "test_file_log"' in content
    assert '"task_id": 42' in content


def test_logging_serializes_exception_details(tmp_path) -> None:
    configure_logging(
        "INFO",
        log_dir=str(tmp_path),
        log_filename="worker.jsonl",
    )
    try:
        raise RuntimeError("proxy failed")
    except RuntimeError:
        structlog.get_logger().exception("test_exception")
    for handler in logging.getLogger().handlers:
        handler.flush()

    content = (tmp_path / "worker.jsonl").read_text(encoding="utf-8")
    assert '"exception":' in content
    assert "RuntimeError: proxy failed" in content
