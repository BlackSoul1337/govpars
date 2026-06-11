from __future__ import annotations

import asyncio
import time
from typing import Any

from pydantic import SecretStr
from structlog import get_logger

from procurement_parser.config.settings import (
    CaptchaSettings,
    NetworkSettings,
    RuntimeSettings,
    SourceSettings,
)
from procurement_parser.domain.errors import SourceBlockedError
from procurement_parser.domain.models import (
    DiscoveredEntity,
    EntityIdentity,
    EntityType,
    ExtractedBatch,
    Source,
)
from procurement_parser.domain.ports import CaptchaSolverPort, RuntimeStatePort
from procurement_parser.infrastructure.network.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitOpenError,
)
from procurement_parser.infrastructure.network.proxy_pool import (
    ProxyPoolCursor,
    request_provider_rotation,
)
from procurement_parser.infrastructure.sources.zakup_sk.browser import ZakupBrowserSession
from procurement_parser.infrastructure.sources.zakup_sk.parser import (
    attach_notice_lots,
    parse_detail,
    parse_discovery,
)
from procurement_parser.infrastructure.sources.zakup_sk.strategies import (
    ApiResponse,
    BrowserFetchStrategy,
    CurlCffiApiStrategy,
    DomFallbackStrategy,
    NetworkInterceptStrategy,
    ZakupStrategyStack,
)
from procurement_parser.metrics import (
    LANE_ROTATIONS,
    LANE_STATE,
    SOURCE_LATENCY,
    SOURCE_RESPONSES,
)

logger = get_logger()


class ZakupBlockedError(SourceBlockedError):
    def __init__(self, status: int, strategy: str) -> None:
        super().__init__(status, strategy, source="Zakup")
        self.status = status
        self.strategy = strategy


class ZakupSkAdapter:
    source = Source.ZAKUP_SK

    def __init__(
        self,
        settings: SourceSettings,
        network: NetworkSettings,
        captcha: CaptchaSettings,
        captcha_solver: CaptchaSolverPort,
        runtime: RuntimeSettings | None = None,
        runtime_state: RuntimeStatePort | None = None,
    ) -> None:
        self.settings = settings
        self.network = network
        self.captcha_solver = captcha_solver
        self.runtime = runtime
        self.runtime_state = runtime_state
        pool_size = len(network.proxy_pool_urls)
        if pool_size:
            lane_count = min(network.max_lanes, pool_size)
        elif network.proxy_url:
            lane_count = network.max_lanes
        else:
            lane_count = runtime.browser_lanes if runtime else 1
        lane_count = max(1, lane_count)
        self.source_breaker = CircuitBreaker(
            threshold=max(network.block_threshold * 2, 5),
            cooldown_seconds=network.cooldown_seconds,
        )
        pool_urls = [item.get_secret_value() for item in network.proxy_pool_urls]
        self.proxy_pool = (
            ProxyPoolCursor(
                pool_urls,
                start_index=network.proxy_pool_index + lane_count,
                cooldown_seconds=network.cooldown_seconds,
            )
            if pool_urls
            else None
        )
        self.stacks: list[ZakupStrategyStack] = []
        self._lane_generations = [0] * lane_count
        self._lane_in_use = [False] * lane_count
        self._lane_started_at = [time.monotonic()] * lane_count
        for lane_index in range(lane_count):
            lane_network = network
            if pool_size:
                pool_index = (network.proxy_pool_index + lane_index) % pool_size
                lane_network = network.model_copy(
                    update={"proxy_url": network.proxy_pool_urls[pool_index]}
                )
            self.stacks.append(self._build_stack(lane_index, lane_network))
        self.stack = self.stacks[0]
        self.browser = self.stack.browser
        self._next_lane = 0
        self._lane_condition = asyncio.Condition()
        self._rotation_lock = asyncio.Lock()
        self._runtime_state_loaded = False
        self._runtime_state_lock = asyncio.Lock()

    def _build_stack(
        self,
        lane_index: int,
        network: NetworkSettings,
    ) -> ZakupStrategyStack:
        runtime = self.runtime
        browser = ZakupBrowserSession(
            self.settings,
            network,
            self.captcha_solver,
            lane_index=lane_index,
            session_generation=self._lane_generations[lane_index],
            navigation_timeout_seconds=(
                runtime.browser_navigation_timeout_seconds if runtime else 60
            ),
            capture_timeout_seconds=(
                runtime.browser_capture_timeout_seconds if runtime else 60
            ),
            response_timeout_seconds=(
                runtime.browser_response_timeout_seconds if runtime else 60
            ),
            disk_cache_mb=runtime.browser_disk_cache_mb if runtime else 512,
            preload_main_bundle=(
                runtime.browser_preload_main_bundle if runtime else False
            ),
        )
        return ZakupStrategyStack(
            CurlCffiApiStrategy(self.settings, network),
            BrowserFetchStrategy(browser),
            NetworkInterceptStrategy(browser),
            DomFallbackStrategy(browser),
            browser,
            lane_breaker=CircuitBreaker(
                threshold=network.block_threshold,
                cooldown_seconds=network.cooldown_seconds,
            ),
            source_breaker=self.source_breaker,
        )

    async def _acquire_stack(
        self,
        attempted: set[int],
    ) -> tuple[int, ZakupStrategyStack] | None:
        async with self._lane_condition:
            while True:
                unattempted_exists = False
                busy_unattempted_exists = False
                for _ in self.stacks:
                    lane_index = self._next_lane
                    self._next_lane = (self._next_lane + 1) % len(self.stacks)
                    stack = self.stacks[lane_index]
                    if id(stack) in attempted:
                        continue
                    unattempted_exists = True
                    if self._lane_in_use[lane_index]:
                        busy_unattempted_exists = True
                        continue
                    if await stack.lane_breaker.is_available():
                        self._lane_in_use[lane_index] = True
                        return lane_index, stack
                if not unattempted_exists:
                    return None
                if not busy_unattempted_exists:
                    return None
                await self._lane_condition.wait()

    async def _release_stack(
        self,
        lane_index: int,
        stack: ZakupStrategyStack,
        *,
        exhausted: bool,
        failed: bool = True,
    ) -> bool:
        replacement: ZakupStrategyStack | None = None
        current_proxy = (
            stack.browser.network.proxy_url.get_secret_value()
            if stack.browser.network.proxy_url
            else None
        )
        can_rotate = bool(
            current_proxy and (self.proxy_pool or self.network.rotate_url)
        )
        if exhausted and can_rotate:
            async with self._rotation_lock:
                try:
                    replacement = await self._replacement_stack(
                        lane_index,
                        stack,
                        failed=failed,
                    )
                except Exception:
                    logger.exception(
                        "proxy_lane_replacement_failed",
                        lane_index=lane_index,
                    )
                async with self._lane_condition:
                    if replacement and self.stacks[lane_index] is stack:
                        self.stacks[lane_index] = replacement
                        self._lane_started_at[lane_index] = time.monotonic()
                        if lane_index == 0:
                            self.stack = replacement
                            self.browser = replacement.browser
                    elif not failed:
                        self._lane_started_at[lane_index] = time.monotonic()
                    self._lane_in_use[lane_index] = False
                    self._lane_condition.notify_all()
        else:
            async with self._lane_condition:
                self._lane_in_use[lane_index] = False
                self._lane_condition.notify_all()
        if replacement:
            await stack.close()
            LANE_ROTATIONS.labels(
                source=self.source.value,
                rotation_kind="replacement",
            ).inc()
        return replacement is not None

    async def _replacement_stack(
        self,
        lane_index: int,
        stack: ZakupStrategyStack,
        *,
        failed: bool = True,
    ) -> ZakupStrategyStack | None:
        current_proxy = (
            stack.browser.network.proxy_url.get_secret_value()
            if stack.browser.network.proxy_url
            else None
        )
        next_proxy: str | None = None
        rotation_kind = "none"
        if self.proxy_pool and current_proxy:
            if failed:
                self.proxy_pool.mark_failed(current_proxy)
            active = {
                item.browser.network.proxy_url.get_secret_value()
                for item in self.stacks
                if item.browser.network.proxy_url and item is not stack
            }
            next_proxy = self.proxy_pool.next(exclude=active | {current_proxy})
            rotation_kind = "pool"
        elif self.network.rotate_url and current_proxy:
            try:
                await request_provider_rotation(self.network)
            except Exception:
                logger.exception(
                    "proxy_provider_rotation_failed",
                    lane_index=lane_index,
                )
                return None
            next_proxy = current_proxy
            rotation_kind = "provider"
        if not next_proxy:
            logger.warning(
                "proxy_lane_exhausted_without_replacement",
                lane_index=lane_index,
                proxy_id=(
                    stack.browser.identity.proxy_id if stack.browser.identity else None
                ),
            )
            return None
        self._lane_generations[lane_index] += 1
        lane_network = self.network.model_copy(
            update={
                "proxy_url": SecretStr(next_proxy),
                "proxy_pool_urls": [],
            }
        )
        replacement = self._build_stack(lane_index, lane_network)
        logger.warning(
            "proxy_lane_replaced",
            lane_index=lane_index,
            rotation_kind=rotation_kind,
            generation=self._lane_generations[lane_index],
            session_lane_id=replacement.browser.lane_id,
            proxy_id=self._proxy_id(replacement),
        )
        return replacement

    def _lane_ttl_expired(self, lane_index: int) -> bool:
        ttl = self.network.sticky_ttl_seconds
        if ttl <= 0 or not (self.proxy_pool or self.network.rotate_url):
            return False
        return time.monotonic() - self._lane_started_at[lane_index] >= ttl

    async def _request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> ApiResponse:
        await self._ensure_runtime_state()
        last_response: ApiResponse | None = None
        last_error: Exception | None = None
        rotate_statuses = {403, 407, 418, 429, 502, 503, 504}
        attempted: set[int] = set()
        attempts = 0
        can_rotate = bool(self.proxy_pool or self.network.rotate_url)
        max_attempts = (
            max(len(self.stacks), self.settings.max_attempts)
            if can_rotate
            else len(self.stacks)
        )
        while attempts < max_attempts:
            acquired = await self._acquire_stack(attempted)
            if acquired is None:
                break
            lane_index, stack = acquired
            if self._lane_ttl_expired(lane_index):
                await self._release_stack(
                    lane_index,
                    stack,
                    exhausted=True,
                    failed=False,
                )
                continue
            attempted.add(id(stack))
            attempts += 1
            exhausted = False
            try:
                response = await stack.request(
                    method,
                    url,
                    params=params,
                    body=body,
                )
                if (
                    response.status in rotate_statuses
                    and response.status not in {403, 418, 429}
                ):
                    await stack.lane_breaker.record_transport_failure()
                if response.status in rotate_statuses and can_rotate:
                    exhausted = True
            except CircuitOpenError as exc:
                last_error = exc
                exhausted = True
                continue
            except Exception as exc:
                opened = await stack.lane_breaker.record_transport_failure()
                exhausted = opened or can_rotate
                last_error = exc
                continue
            finally:
                if stack.lane_breaker.state == BreakerState.OPEN:
                    exhausted = True
                await self._release_stack(
                    lane_index,
                    stack,
                    exhausted=exhausted,
                )
                await self._persist_runtime_state(self.stacks[lane_index])
            if response.status not in rotate_statuses:
                return response
            last_response = response
        if last_response is not None:
            return last_response
        if last_error is not None:
            raise last_error
        raise RuntimeError("No Zakup session lane is available")

    async def _ensure_runtime_state(self) -> None:
        if self._runtime_state_loaded:
            return
        async with self._runtime_state_lock:
            if self._runtime_state_loaded:
                return
            if self.runtime_state:
                source_row = await self.runtime_state.load_lane(
                    "zakup-sk:source-breaker"
                )
                if source_row:
                    await self.source_breaker.restore(
                        state=source_row["state"],
                        consecutive_blocks=source_row["consecutive_blocks"],
                        cooldown_until=source_row["cooldown_until"],
                    )
                for stack in self.stacks:
                    row = await self.runtime_state.load_lane(
                        stack.browser.lane_id
                    )
                    if row:
                        await stack.lane_breaker.restore(
                            state=row["state"],
                            consecutive_blocks=row["consecutive_blocks"],
                            cooldown_until=row["cooldown_until"],
                        )
                    else:
                        await self._persist_runtime_state(stack)
            self._runtime_state_loaded = True

    async def _persist_runtime_state(
        self,
        stack: ZakupStrategyStack,
    ) -> None:
        self._observe_breaker_metric(
            stack.browser.lane_id,
            stack.lane_breaker.state,
        )
        self._observe_breaker_metric(
            "source-breaker",
            self.source_breaker.state,
        )
        if not self.runtime_state:
            return
        await self.runtime_state.save_lane(
            lane_id=stack.browser.lane_id,
            source=self.source.value,
            proxy_id=(
                stack.browser.identity.proxy_id
                if stack.browser.identity
                else self._proxy_id(stack)
            ),
            state=stack.lane_breaker.state.value,
            consecutive_blocks=stack.lane_breaker.consecutive_blocks,
            cooldown_until=stack.lane_breaker.cooldown_until,
            profile_payload={
                "generation": stack.browser.session_generation,
                "browser_only_profiles": sorted(
                    key
                    for key, profile in stack.profiles.items()
                    if profile.browser_only
                ),
            },
        )
        await self.runtime_state.save_lane(
            lane_id="zakup-sk:source-breaker",
            source=self.source.value,
            proxy_id=None,
            state=self.source_breaker.state.value,
            consecutive_blocks=self.source_breaker.consecutive_blocks,
            cooldown_until=self.source_breaker.cooldown_until,
            profile_payload={},
        )

    @staticmethod
    def _proxy_id(stack: ZakupStrategyStack) -> str | None:
        proxy = stack.browser.network.proxy_url
        if not proxy:
            return None
        from hashlib import sha256

        return sha256(proxy.get_secret_value().encode()).hexdigest()[:12]

    def _observe_breaker_metric(
        self,
        lane_id: str,
        state: BreakerState,
    ) -> None:
        for current_state in BreakerState:
            LANE_STATE.labels(
                source=self.source.value,
                lane_id=lane_id,
                state=current_state.value,
            ).set(1 if current_state == state else 0)

    def _endpoint(self, entity_type: EntityType) -> str:
        if entity_type == EntityType.LOT:
            return self.settings.endpoints.lots
        if entity_type == EntityType.NOTICE:
            return self.settings.endpoints.adverts
        if entity_type == EntityType.PLAN_ITEM:
            return self.settings.endpoints.plan_items
        raise ValueError(f"Unsupported Zakup entity type: {entity_type}")

    async def discover(
        self,
        entity_type: EntityType,
        *,
        page: int = 1,
        priority: int = 0,
        filters: dict | None = None,
    ) -> list[DiscoveredEntity]:
        endpoint = self._endpoint(entity_type)
        body = dict(filters or {})
        if entity_type in {EntityType.LOT, EntityType.NOTICE}:
            body.setdefault("tenderSubjectTypes", [])
        started = time.monotonic()
        response = await self._request(
            "POST",
            f"{self.settings.base_url}{endpoint}/filter",
            params={
                "page": max(0, page - 1),
                "size": self.settings.per_page,
                "sort": "id,desc",
            },
            body=body,
        )
        self._observe(response.status, response.strategy, time.monotonic() - started)
        self._ensure_success(response.status, response.strategy)
        return parse_discovery(response.data, entity_type, priority=priority)

    async def extract(self, identity: EntityIdentity) -> ExtractedBatch:
        endpoint = self._endpoint(identity.entity_type)
        started = time.monotonic()
        response = await self._request(
            "GET",
            f"{self.settings.base_url}{endpoint}/{identity.source_entity_id}",
        )
        self._observe(response.status, response.strategy, time.monotonic() - started)
        self._ensure_success(response.status, response.strategy)
        payload = response.data if isinstance(response.data, dict) else {"data": response.data}
        batch = parse_detail(
            payload,
            identity.entity_type,
            source_entity_id=identity.source_entity_id,
            http_status=response.status,
        )
        if identity.entity_type == EntityType.NOTICE:
            lots_response = await self._request(
                "GET",
                (
                    f"{self.settings.base_url}{self.settings.endpoints.adverts}"
                    f"/lots/{identity.source_entity_id}"
                ),
                params={"page": 0, "size": 1000, "sort": "id,asc"},
            )
            if lots_response.status < 400:
                attach_notice_lots(batch, identity, lots_response.data)
        for envelope in batch.entities:
            envelope.response_headers = response.headers
        return batch

    @staticmethod
    def _ensure_success(status: int, strategy: str) -> None:
        if status in {403, 418, 429}:
            raise ZakupBlockedError(status, strategy)
        if status >= 400:
            raise RuntimeError(f"Zakup HTTP {status} via {strategy}")

    @staticmethod
    def _observe(status: int, strategy: str, elapsed: float) -> None:
        SOURCE_RESPONSES.labels(
            source=Source.ZAKUP_SK.value,
            strategy=strategy,
            status=str(status),
        ).inc()
        SOURCE_LATENCY.labels(
            source=Source.ZAKUP_SK.value,
            strategy=strategy,
        ).observe(elapsed)

    async def spike(self) -> dict[str, Any]:
        stack = self.stacks[0]
        identity = await stack.browser.start()
        profile = await stack.browser.capture_profile()
        return {
            "lane_id": identity.lane_id,
            "proxy_id": identity.proxy_id,
            "user_agent": identity.user_agent,
            "captured": profile.model_dump(mode="json") if profile else None,
        }

    async def close(self) -> None:
        await asyncio.gather(*(stack.close() for stack in self.stacks))
