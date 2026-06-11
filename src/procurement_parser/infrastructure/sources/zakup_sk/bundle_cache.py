from __future__ import annotations

import asyncio
import os
import re
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import urljoin

import httpx
import structlog
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = structlog.get_logger()
MAIN_BUNDLE_PATTERN = re.compile(r'src=["\']([^"\']*main-es2015\.[^"\']+\.js)["\']')


class ZakupBundleCache:
    def __init__(
        self,
        *,
        base_url: str,
        cache_dir: Path,
        proxy_url: str | None,
        timeout_seconds: int,
    ) -> None:
        self.base_url = base_url
        self.cache_dir = cache_dir
        self.proxy_url = proxy_url
        self.timeout_seconds = timeout_seconds

    async def prepare(self) -> tuple[str, Path] | None:
        await asyncio.to_thread(self.cache_dir.mkdir, parents=True, exist_ok=True)
        timeout = httpx.Timeout(self.timeout_seconds, connect=60)
        headers = {
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ru-RU,ru;q=0.9",
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/136.0.0.0 Safari/537.36"
            ),
        }
        async with httpx.AsyncClient(
            timeout=timeout,
            proxy=self.proxy_url,
            follow_redirects=True,
            headers=headers,
        ) as client:
            index = await client.get(f"{self.base_url}/")
            index.raise_for_status()
            match = MAIN_BUNDLE_PATTERN.search(index.text)
            if not match:
                logger.warning("zakup_main_bundle_not_found")
                return None
            url = urljoin(f"{self.base_url}/", match.group(1))
            destination = self.cache_dir / Path(url).name
            total = await self._total_size(client, url)
            if destination.exists() and destination.stat().st_size == total:
                logger.info(
                    "zakup_bundle_cache_hit",
                    url=url,
                    path=str(destination),
                    size_bytes=total,
                )
                return url, destination
            async with self._download_lock(destination):
                if destination.exists() and destination.stat().st_size == total:
                    logger.info(
                        "zakup_bundle_cache_hit_after_wait",
                        url=url,
                        path=str(destination),
                        size_bytes=total,
                    )
                    return url, destination
                await self._download(client, url, destination, total)
            return url, destination

    @asynccontextmanager
    async def _download_lock(self, destination: Path):
        lock_path = destination.with_suffix(f"{destination.suffix}.lock")
        stale_after = max(1800, self.timeout_seconds * 3)
        while True:
            acquired = await asyncio.to_thread(_try_create_lock, lock_path)
            if acquired:
                break
            try:
                age = time.time() - lock_path.stat().st_mtime
            except FileNotFoundError:
                continue
            if age > stale_after:
                logger.warning(
                    "zakup_bundle_cache_stale_lock_removed",
                    path=str(lock_path),
                    age_seconds=round(age, 1),
                )
                try:
                    await asyncio.to_thread(lock_path.unlink)
                except FileNotFoundError:
                    pass
                continue
            logger.info("zakup_bundle_cache_waiting_for_lock", path=str(lock_path))
            await asyncio.sleep(2)
        try:
            yield
        finally:
            try:
                await asyncio.to_thread(lock_path.unlink)
            except FileNotFoundError:
                pass

    def cached(self) -> tuple[str, Path] | None:
        if not self.cache_dir.exists():
            return None
        candidates = sorted(
            self.cache_dir.glob("main-es2015.*.js"),
            key=lambda path: path.stat().st_mtime,
            reverse=True,
        )
        if not candidates:
            return None
        path = candidates[0]
        return urljoin(f"{self.base_url}/", path.name), path

    async def _total_size(self, client: httpx.AsyncClient, url: str) -> int:
        response = await client.get(url, headers={"Range": "bytes=0-0"})
        response.raise_for_status()
        content_range = response.headers.get("content-range", "")
        match = re.search(r"/(\d+)$", content_range)
        if not match:
            raise RuntimeError(f"Zakup bundle does not support byte ranges: {url}")
        return int(match.group(1))

    async def _download(
        self,
        client: httpx.AsyncClient,
        url: str,
        destination: Path,
        total: int,
    ) -> None:
        partial = destination.with_suffix(f"{destination.suffix}.part")
        offset = partial.stat().st_size if partial.exists() else 0
        if offset > total:
            await asyncio.to_thread(partial.unlink)
            offset = 0
        chunk_size = 1024 * 1024
        while offset < total:
            end = min(total - 1, offset + chunk_size - 1)
            response = await self._get_range(client, url, offset, end)
            await asyncio.to_thread(_append_bytes, partial, response.content)
            offset += len(response.content)
            logger.info(
                "zakup_bundle_cache_progress",
                downloaded_bytes=offset,
                total_bytes=total,
                percent=round(offset * 100 / total, 1),
                path=str(partial),
            )
        await asyncio.to_thread(partial.replace, destination)
        logger.info(
            "zakup_bundle_cache_complete",
            url=url,
            path=str(destination),
            size_bytes=total,
        )

    @staticmethod
    @retry(
        retry=retry_if_exception_type(
            (httpx.TimeoutException, httpx.NetworkError, httpx.RemoteProtocolError)
        ),
        wait=wait_exponential(multiplier=2, min=2, max=60),
        stop=stop_after_attempt(5),
        reraise=True,
    )
    async def _get_range(
        client: httpx.AsyncClient,
        url: str,
        start: int,
        end: int,
    ) -> httpx.Response:
        response = await client.get(
            url,
            headers={"Range": f"bytes={start}-{end}"},
        )
        response.raise_for_status()
        expected_range = f"bytes {start}-{end}/"
        if response.status_code != 206 or not response.headers.get(
            "content-range",
            "",
        ).startswith(expected_range):
            raise httpx.RemoteProtocolError(
                f"Invalid range response for bytes {start}-{end}"
            )
        expected_length = end - start + 1
        if len(response.content) != expected_length:
            raise httpx.RemoteProtocolError(
                f"Incomplete range {start}-{end}: received {len(response.content)} bytes"
            )
        return response


def _append_bytes(path: Path, content: bytes) -> None:
    with path.open("ab") as handle:
        handle.write(content)


def _try_create_lock(path: Path) -> bool:
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    os.close(descriptor)
    return True
