from __future__ import annotations

import asyncio
import json
import re
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from curl_cffi.requests import AsyncSession

from procurement_parser.config.settings import NetworkSettings

PROXY_LINE = re.compile(
    r"^(?P<address>\d{1,3}(?:\.\d{1,3}){3}:\d+)\s+"
    r"(?P<protocol>SOCKS5|HTTPS|HTTP)",
    re.MULTILINE,
)


class ProxyPoolCursor:
    def __init__(
        self,
        urls: list[str],
        *,
        start_index: int = 0,
        cooldown_seconds: int = 600,
    ) -> None:
        self.urls = urls
        self.cursor = start_index % len(urls) if urls else 0
        self.cooldown_seconds = cooldown_seconds
        self.cooldown_until: dict[str, float] = {}

    def mark_failed(self, url: str) -> None:
        self.cooldown_until[url] = time.monotonic() + self.cooldown_seconds

    def next(self, *, exclude: set[str]) -> str | None:
        now = time.monotonic()
        for _ in self.urls:
            url = self.urls[self.cursor]
            self.cursor = (self.cursor + 1) % len(self.urls)
            if url in exclude:
                continue
            if self.cooldown_until.get(url, 0) > now:
                continue
            return url
        return None


async def request_provider_rotation(network: NetworkSettings) -> None:
    if not network.rotate_url:
        raise RuntimeError("Proxy provider rotation URL is not configured")
    async with httpx.AsyncClient(timeout=network.rotate_timeout_seconds) as client:
        response = await client.request(
            network.rotate_method,
            network.rotate_url.get_secret_value(),
        )
        response.raise_for_status()


def build_proxy_pool(
    *,
    structured_json: Path | None = None,
    generic_json: Path | None = None,
    spys_text: Path | None = None,
) -> dict[str, Any]:
    entries: dict[str, dict[str, Any]] = {}

    if structured_json:
        structured = json.loads(structured_json.read_text(encoding="utf-8"))
        for item in structured.get("proxies", []):
            protocol = item.get("protocol", "").lower()
            if protocol not in {"http", "https", "socks5"}:
                continue
            url = item.get("proxy") or f"{protocol}://{item['ip']}:{item['port']}"
            entries[url] = {
                "url": url,
                "source": structured_json.name,
                "protocol_confidence": "declared",
                "country": item.get("ip_data", {}).get("countryCode"),
                "reported_uptime": item.get("uptime"),
            }

    if spys_text:
        text = spys_text.read_text(encoding="utf-8")
        for match in PROXY_LINE.finditer(text):
            protocol = match.group("protocol").lower()
            scheme = "socks5" if protocol == "socks5" else "http"
            url = f"{scheme}://{match.group('address')}"
            entries.setdefault(
                url,
                {
                    "url": url,
                    "source": spys_text.name,
                    "protocol_confidence": "declared",
                    "country": "KZ",
                },
            )

    if generic_json:
        generic = json.loads(generic_json.read_text(encoding="utf-8"))
        for item in generic:
            url = f"http://{item['ip_address']}:{item['port']}"
            entries.setdefault(
                url,
                {
                    "url": url,
                    "source": generic_json.name,
                    "protocol_confidence": "assumed_http",
                    "country": None,
                },
            )

    proxies = sorted(
        entries.values(),
        key=lambda item: (
            item["protocol_confidence"] != "declared",
            item.get("reported_uptime") is None,
            -(item.get("reported_uptime") or 0),
            item["url"],
        ),
    )
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "warning": (
            "Untrusted public proxies. Validate before use and do not reuse "
            "sessions across proxies."
        ),
        "proxies": proxies,
    }


async def check_proxy_pool(
    document: dict[str, Any],
    *,
    target_url: str,
    limit: int,
    concurrency: int,
    timeout_seconds: int,
) -> dict[str, Any]:
    candidates = document["proxies"][:limit]
    semaphore = asyncio.Semaphore(concurrency)

    async def check(entry: dict[str, Any]) -> dict[str, Any] | None:
        async with semaphore:
            started = asyncio.get_running_loop().time()
            proxy = entry["url"]
            session = AsyncSession(
                impersonate="chrome",
                proxies={"http": proxy, "https": proxy},
                timeout=timeout_seconds,
            )
            try:
                response = await session.get(target_url)
                if response.status_code >= 400:
                    return None
                return {
                    **entry,
                    "checked_at": datetime.now(UTC).isoformat(),
                    "latency_ms": round(
                        (asyncio.get_running_loop().time() - started) * 1000
                    ),
                    "target_status": response.status_code,
                }
            except Exception:
                return None
            finally:
                await session.close()

    results = await asyncio.gather(*(check(entry) for entry in candidates))
    working = [entry for entry in results if entry is not None]
    working.sort(key=lambda item: item["latency_ms"])
    return {
        "generated_at": datetime.now(UTC).isoformat(),
        "target_url": target_url,
        "tested": len(candidates),
        "working_count": len(working),
        "proxies": working,
    }
