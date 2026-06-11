from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

import httpx
from curl_cffi.requests import AsyncSession
from structlog import get_logger
from structlog.contextvars import bind_contextvars
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential_jitter,
)

from procurement_parser.config.settings import NetworkSettings, SourceSettings
from procurement_parser.domain.ports import RuntimeStatePort
from procurement_parser.infrastructure.network.circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
)
from procurement_parser.infrastructure.network.proxy_pool import (
    ProxyPoolCursor,
    request_provider_rotation,
)
from procurement_parser.metrics import LANE_ROTATIONS, LANE_STATE

BLOCKED_OR_TRANSIENT_STATUSES = {403, 407, 418, 429, 502, 503, 504}
WAF_STATUSES = {403, 418, 429}
logger = get_logger()


@dataclass(slots=True)
class EepResponse:
    status_code: int
    text: str
    headers: dict[str, str]
    strategy: str


@dataclass(slots=True)
class EepHttpLane:
    proxy_url: str
    client: httpx.AsyncClient
    breaker: CircuitBreaker
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    generation: int = 0


class EepHttpClient:
    def __init__(
        self,
        settings: SourceSettings,
        network: NetworkSettings,
        runtime_state: RuntimeStatePort | None = None,
    ) -> None:
        self.settings = settings
        self.network = network
        self.runtime_state = runtime_state
        self.max_attempts = settings.max_attempts
        self._direct_client: httpx.AsyncClient | None = None
        self._lanes: list[EepHttpLane] = []
        self._next_lane = 0
        self._selection_lock = asyncio.Lock()
        self._rotation_lock = asyncio.Lock()
        self._runtime_state_loaded = False
        self._runtime_state_lock = asyncio.Lock()

        pool_urls = [item.get_secret_value() for item in network.proxy_pool_urls]
        if pool_urls:
            lane_count = min(network.max_lanes, len(pool_urls))
            for lane_index in range(lane_count):
                pool_index = (network.proxy_pool_index + lane_index) % len(pool_urls)
                self._lanes.append(self._build_lane(pool_urls[pool_index]))
            self._proxy_pool = ProxyPoolCursor(
                pool_urls,
                start_index=network.proxy_pool_index + lane_count,
                cooldown_seconds=network.cooldown_seconds,
            )
        elif network.proxy_url:
            self._lanes.append(
                self._build_lane(network.proxy_url.get_secret_value())
            )
            self._proxy_pool = None
        else:
            self._direct_client = self._build_client(None)
            self._proxy_pool = None

    @property
    def lane_count(self) -> int:
        return len(self._lanes)

    def _build_client(self, proxy_url: str | None) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.settings.base_url,
            timeout=self.settings.request_timeout_seconds,
            follow_redirects=True,
            proxy=proxy_url,
            http2=True,
            limits=httpx.Limits(
                max_connections=max(
                    self.settings.concurrency.direct,
                    self.settings.concurrency.proxy,
                ),
                max_keepalive_connections=max(
                    self.settings.concurrency.direct,
                    self.settings.concurrency.proxy,
                ),
            ),
            headers={
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                ),
                "Accept-Language": "ru-RU,ru;q=0.9,kk;q=0.7,en;q=0.5",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/136.0.0.0 Safari/537.36"
                ),
            },
        )

    def _build_lane(self, proxy_url: str, *, generation: int = 0) -> EepHttpLane:
        return EepHttpLane(
            proxy_url=proxy_url,
            client=self._build_client(proxy_url),
            breaker=CircuitBreaker(
                threshold=self.network.block_threshold,
                cooldown_seconds=self.network.cooldown_seconds,
                blocked_statuses=BLOCKED_OR_TRANSIENT_STATUSES,
            ),
            generation=generation,
        )

    async def get(self, path: str, *, params: dict | None = None) -> EepResponse:
        if self._direct_client:
            return await self._get_direct(path, params=params)
        return await self._get_via_lanes(path, params=params)

    async def _get_direct(
        self,
        path: str,
        *,
        params: dict | None,
    ) -> EepResponse:
        assert self._direct_client is not None

        @retry(
            retry=retry_if_exception_type(
                httpx.TransportError
            ),
            wait=wait_exponential_jitter(initial=1, max=30),
            stop=stop_after_attempt(self.max_attempts),
            reraise=True,
        )
        async def request() -> httpx.Response:
            response = await self._direct_client.get(path, params=params)
            if response.status_code not in WAF_STATUSES:
                response.raise_for_status()
            return response

        bind_contextvars(strategy="httpx", proxy_id="direct")
        try:
            response = await request()
        except httpx.TransportError:
            return await self._get_curl_fallback(path, params=params)
        if response.status_code in WAF_STATUSES:
            return await self._get_curl_fallback(path, params=params)
        return EepResponse(
            status_code=response.status_code,
            text=response.text,
            headers=dict(response.headers),
            strategy="httpx",
        )

    async def _get_via_lanes(
        self,
        path: str,
        *,
        params: dict | None,
    ) -> EepResponse:
        await self._ensure_runtime_state()
        last_error: Exception | None = None
        last_proxy: str | None = None
        for _ in range(self.max_attempts):
            lane_index, lane = await self._acquire_lane()
            last_proxy = lane.proxy_url
            try:
                bind_contextvars(
                    strategy="httpx-proxy",
                    proxy_id=self._proxy_id(lane.proxy_url),
                    session_lane_id=self._lane_id(lane_index, lane),
                )
                response = await lane.client.get(path, params=params)
                if response.status_code in BLOCKED_OR_TRANSIENT_STATUSES:
                    await lane.breaker.record_status(response.status_code)
                    await self._persist_lane(lane_index, lane)
                    last_error = httpx.HTTPStatusError(
                        f"EEP proxy returned HTTP {response.status_code}",
                        request=response.request,
                        response=response,
                    )
                    if self._proxy_pool or self.network.rotate_url:
                        await self._replace_lane(lane_index, lane, failed=True)
                    continue
                response.raise_for_status()
                await lane.breaker.record_success()
                await self._persist_lane(lane_index, lane)
                return EepResponse(
                    status_code=response.status_code,
                    text=response.text,
                    headers=dict(response.headers),
                    strategy="httpx-proxy",
                )
            except httpx.TransportError as exc:
                last_error = exc
                await lane.breaker.record_transport_failure()
                await self._persist_lane(lane_index, lane)
                if self._proxy_pool or self.network.rotate_url:
                    await self._replace_lane(lane_index, lane, failed=True)
            finally:
                lane.lock.release()
            await asyncio.sleep(0.25)
        if last_proxy:
            try:
                return await self._get_curl_fallback(
                    path,
                    params=params,
                    proxy_url=last_proxy,
                )
            except Exception as exc:
                if last_error:
                    raise last_error from exc
                raise
        if last_error:
            raise last_error
        raise RuntimeError("No EEP proxy lane is available")

    async def _get_curl_fallback(
        self,
        path: str,
        *,
        params: dict | None,
        proxy_url: str | None = None,
    ) -> EepResponse:
        bind_contextvars(
            strategy="curl-cffi",
            proxy_id=(
                self._proxy_id(proxy_url)
                if proxy_url
                else "direct"
            ),
        )
        proxies = (
            {"http": proxy_url, "https": proxy_url}
            if proxy_url
            else None
        )
        session = AsyncSession(
            impersonate="chrome",
            proxies=proxies,
            timeout=self.settings.request_timeout_seconds,
        )
        try:
            response = await session.get(
                f"{self.settings.base_url}{path}",
                params=params,
                headers={
                    "Accept-Language": "ru-RU,ru;q=0.9,kk;q=0.7,en;q=0.5",
                },
            )
            if response.status_code >= 400:
                raise RuntimeError(
                    "EEP curl-cffi fallback returned "
                    f"HTTP {response.status_code}"
                )
            return EepResponse(
                status_code=response.status_code,
                text=response.text,
                headers=dict(response.headers),
                strategy="curl-cffi",
            )
        finally:
            await session.close()

    async def _acquire_lane(self) -> tuple[int, EepHttpLane]:
        while True:
            async with self._selection_lock:
                locked_lane_exists = False
                for _ in self._lanes:
                    lane_index = self._next_lane
                    self._next_lane = (self._next_lane + 1) % len(self._lanes)
                    lane = self._lanes[lane_index]
                    if lane.lock.locked():
                        locked_lane_exists = True
                        continue
                    if not await lane.breaker.is_available():
                        continue
                    await lane.lock.acquire()
                    return lane_index, lane
                if not locked_lane_exists:
                    raise CircuitOpenError("All EEP proxy lanes are in cooldown")
            await asyncio.sleep(0.05)

    async def _replace_lane(
        self,
        lane_index: int,
        lane: EepHttpLane,
        *,
        failed: bool,
    ) -> bool:
        async with self._rotation_lock:
            if self._lanes[lane_index] is not lane:
                return True
            next_proxy: str | None = None
            rotation_kind = "none"
            if self._proxy_pool:
                if failed:
                    self._proxy_pool.mark_failed(lane.proxy_url)
                active = {
                    item.proxy_url
                    for index, item in enumerate(self._lanes)
                    if index != lane_index
                }
                next_proxy = self._proxy_pool.next(
                    exclude=active | {lane.proxy_url}
                )
                rotation_kind = "pool"
            elif self.network.rotate_url:
                await request_provider_rotation(self.network)
                next_proxy = lane.proxy_url
                rotation_kind = "provider"
            if not next_proxy:
                logger.warning(
                    "eep_proxy_lane_exhausted_without_replacement",
                    lane_index=lane_index,
                    proxy_id=self._proxy_id(lane.proxy_url),
                )
                return False
            replacement = self._build_lane(
                next_proxy,
                generation=lane.generation + 1,
            )
            self._lanes[lane_index] = replacement
            await lane.client.aclose()
            LANE_ROTATIONS.labels(
                source="eep-mitwork",
                rotation_kind=rotation_kind,
            ).inc()
            await self._persist_lane(lane_index, replacement)
            logger.warning(
                "eep_proxy_lane_replaced",
                lane_index=lane_index,
                generation=replacement.generation,
                rotation_kind=rotation_kind,
                proxy_id=self._proxy_id(next_proxy),
                session_lane_id=self._lane_id(lane_index, replacement),
            )
            return True

    async def _ensure_runtime_state(self) -> None:
        if self._runtime_state_loaded:
            return
        async with self._runtime_state_lock:
            if self._runtime_state_loaded:
                return
            if self.runtime_state:
                for lane_index, lane in enumerate(self._lanes):
                    row = await self.runtime_state.load_lane(
                        self._lane_id(lane_index, lane)
                    )
                    if row:
                        await lane.breaker.restore(
                            state=row["state"],
                            consecutive_blocks=row["consecutive_blocks"],
                            cooldown_until=row["cooldown_until"],
                        )
                    await self._persist_lane(lane_index, lane)
            self._runtime_state_loaded = True

    async def _persist_lane(
        self,
        lane_index: int,
        lane: EepHttpLane,
    ) -> None:
        lane_id = self._lane_id(lane_index, lane)
        for state in ("closed", "open", "half_open"):
            LANE_STATE.labels(
                source="eep-mitwork",
                lane_id=lane_id,
                state=state,
            ).set(1 if lane.breaker.state.value == state else 0)
        if not self.runtime_state:
            return
        await self.runtime_state.save_lane(
            lane_id=lane_id,
            source="eep-mitwork",
            proxy_id=self._proxy_id(lane.proxy_url),
            state=lane.breaker.state.value,
            consecutive_blocks=lane.breaker.consecutive_blocks,
            cooldown_until=lane.breaker.cooldown_until,
            profile_payload={"generation": lane.generation},
        )

    def _lane_id(self, lane_index: int, lane: EepHttpLane) -> str:
        return f"eep-{lane_index}-{self._proxy_id(lane.proxy_url)}"

    @staticmethod
    def _proxy_id(proxy_url: str) -> str:
        from hashlib import sha256

        return sha256(proxy_url.encode()).hexdigest()[:12]

    async def close(self) -> None:
        if self._direct_client:
            await self._direct_client.aclose()
        await asyncio.gather(*(lane.client.aclose() for lane in self._lanes))
