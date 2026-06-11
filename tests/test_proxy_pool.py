import json
from types import SimpleNamespace

import pytest
import respx
from httpx import Response
from pydantic import SecretStr

from procurement_parser.config.settings import NetworkSettings
from procurement_parser.infrastructure.network import proxy_pool


def test_build_proxy_pool_normalizes_all_supported_inputs(tmp_path) -> None:
    structured = tmp_path / "structured.json"
    structured.write_text(
        json.dumps(
            {
                "proxies": [
                    {
                        "protocol": "https",
                        "ip": "127.0.0.1",
                        "port": 8080,
                        "uptime": 99,
                        "ip_data": {"countryCode": "KZ"},
                    },
                    {"protocol": "ftp", "ip": "127.0.0.2", "port": 21},
                ]
            }
        ),
        encoding="utf-8",
    )
    generic = tmp_path / "generic.json"
    generic.write_text(
        json.dumps([{"ip_address": "127.0.0.3", "port": 3128}]),
        encoding="utf-8",
    )
    spys = tmp_path / "spys.txt"
    spys.write_text(
        "127.0.0.4:1080 SOCKS5\n127.0.0.5:8000 HTTP\n",
        encoding="utf-8",
    )

    result = proxy_pool.build_proxy_pool(
        structured_json=structured,
        generic_json=generic,
        spys_text=spys,
    )

    urls = {item["url"] for item in result["proxies"]}
    assert urls == {
        "https://127.0.0.1:8080",
        "http://127.0.0.3:3128",
        "socks5://127.0.0.4:1080",
        "http://127.0.0.5:8000",
    }
    assert result["proxies"][0]["country"] == "KZ"


@pytest.mark.asyncio
@respx.mock
async def test_provider_rotation_uses_configured_method_and_secret_url() -> None:
    route = respx.post("https://proxy.example/rotate").mock(
        return_value=Response(204)
    )
    settings = NetworkSettings(
        kind="sticky_residential",
        rotate_url=SecretStr("https://proxy.example/rotate"),
        rotate_method="POST",
    )

    await proxy_pool.request_provider_rotation(settings)

    assert route.called


@pytest.mark.asyncio
async def test_check_proxy_pool_keeps_only_successful_candidates(
    monkeypatch,
) -> None:
    class FakeSession:
        def __init__(self, *, proxies, **_kwargs):
            self.proxy = proxies["http"]

        async def get(self, _target_url):
            status = 200 if self.proxy.endswith(":8000") else 503
            return SimpleNamespace(status_code=status)

        async def close(self):
            return None

    monkeypatch.setattr(proxy_pool, "AsyncSession", FakeSession)
    document = {
        "proxies": [
            {"url": "http://127.0.0.1:8000"},
            {"url": "http://127.0.0.2:8001"},
        ]
    }

    result = await proxy_pool.check_proxy_pool(
        document,
        target_url="https://example.test/",
        limit=10,
        concurrency=2,
        timeout_seconds=1,
    )

    assert result["tested"] == 2
    assert result["working_count"] == 1
    assert result["proxies"][0]["url"].endswith(":8000")
