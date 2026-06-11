from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any, Protocol

from curl_cffi.requests import AsyncSession
from structlog.contextvars import bind_contextvars

from procurement_parser.config.settings import NetworkSettings, SourceSettings
from procurement_parser.infrastructure.network.circuit_breaker import CircuitBreaker
from procurement_parser.infrastructure.network.session import RequestProfile, SessionIdentity
from procurement_parser.infrastructure.sources.zakup_sk.browser import ZakupBrowserSession
from procurement_parser.infrastructure.sources.zakup_sk.request_profiles import (
    request_profile_key,
)


@dataclass(slots=True)
class ApiResponse:
    status: int
    headers: dict[str, str]
    data: Any
    strategy: str


class ZakupStrategy(Protocol):
    name: str

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        profile: RequestProfile | None = None,
    ) -> ApiResponse: ...


class CurlCffiApiStrategy:
    name = "curl-cffi"

    def __init__(
        self,
        source: SourceSettings,
        network: NetworkSettings,
        session_identity: SessionIdentity | None = None,
    ) -> None:
        self.source = source
        self.identity = session_identity
        proxy = network.proxy_url.get_secret_value() if network.proxy_url else None
        proxies = {"http": proxy, "https": proxy} if proxy else None
        self.session = AsyncSession(
            impersonate="chrome",
            proxies=proxies,
            timeout=source.request_timeout_seconds,
        )

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        profile: RequestProfile | None = None,
    ) -> ApiResponse:
        headers = profile.transferable_headers() if profile else {}
        if self.identity:
            headers.setdefault("User-Agent", self.identity.user_agent)
        response = await self.session.request(
            method,
            url,
            params=params,
            json=body,
            headers=headers,
            cookies=self.identity.cookie_dict() if self.identity else None,
        )
        try:
            data = response.json()
        except Exception:
            data = {"raw": response.text}
        return ApiResponse(
            status=response.status_code,
            headers=dict(response.headers),
            data=data,
            strategy=self.name,
        )

    async def close(self) -> None:
        await self.session.close()


class BrowserFetchStrategy:
    name = "browser-fetch"

    def __init__(self, browser: ZakupBrowserSession) -> None:
        self.browser = browser

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        profile: RequestProfile | None = None,
    ) -> ApiResponse:
        if params:
            from urllib.parse import urlencode

            url = f"{url}?{urlencode(params, doseq=True)}"
        status, headers, data = await self.browser.fetch_json(
            method,
            url,
            body=body,
            headers=profile.browser_transferable_headers() if profile else None,
        )
        return ApiResponse(status=status, headers=headers, data=data, strategy=self.name)


class NetworkInterceptStrategy:
    name = "network-intercept"

    def __init__(self, browser: ZakupBrowserSession) -> None:
        self.browser = browser

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        profile: RequestProfile | None = None,
    ) -> ApiResponse:
        del method, profile
        route = self._route_for(url, params=params or {}, body=body or {})
        endpoint_fragment = url.removeprefix(self.browser.source.base_url)
        status, headers, data = await self.browser.intercept_route_json(
            route,
            endpoint_fragment,
        )
        return ApiResponse(status=status, headers=headers, data=data, strategy=self.name)

    @staticmethod
    def _route_for(
        url: str,
        *,
        params: dict[str, Any],
        body: dict[str, Any],
    ) -> str:
        page = int(params.get("page", 0)) + 1
        if url.endswith("/lots/filter"):
            route = "/ext?tabs=lot"
            if advert_status := body.get("advertStatus"):
                route += f"&adst={advert_status}"
            if lot_status := body.get("lotStatus"):
                route += f"&lst={lot_status}"
            route += f"&page={page}"
            return route
        if url.endswith("/4dv3rts/filter"):
            route = "/ext?tabs=advert"
            if advert_status := body.get("advertStatus"):
                route += f"&adst={advert_status}"
            route += f"&page={page}"
            return route
        if "/4dv3rts/lots/" in url:
            advert_id = url.rsplit("/", 1)[-1]
            return (
                f"/ext(popup:item/{advert_id}/advert)"
                "?tabs=advert&tabstatus=active&page=1"
            )
        if "/4dv3rts/" in url:
            advert_id = url.rsplit("/", 1)[-1]
            return (
                f"/ext(popup:item/{advert_id}/advert)"
                "?tabs=advert&tabstatus=active&page=1"
            )
        if "/lots/" in url:
            lot_id = url.rsplit("/", 1)[-1]
            return (
                f"/ext(popup:item/{lot_id}/lot)"
                "?tabs=lot&adst=PUBLISHED&lst=PUBLISHED&page=1"
            )
        if "/plan-items/" in url:
            plan_id = url.rsplit("/", 1)[-1]
            return f"/ext(popup:item/{plan_id}/plan)"
        raise ValueError(f"No SPA route is known for Zakup endpoint: {url}")


class DomFallbackStrategy:
    name = "dom-fallback"

    def __init__(self, browser: ZakupBrowserSession) -> None:
        self.browser = browser

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
        profile: RequestProfile | None = None,
    ) -> ApiResponse:
        del method, profile
        if not url.endswith(("/lots/filter", "/4dv3rts/filter")):
            raise RuntimeError(
                "Zakup DOM fallback is intentionally limited to discovery "
                "pages to avoid overwriting detail data with partial fields"
            )
        route = NetworkInterceptStrategy._route_for(
            url,
            params=params or {},
            body=body or {},
        )
        status, headers, data = await self.browser.dom_discovery_json(route)
        return ApiResponse(
            status=status,
            headers=headers,
            data=data,
            strategy=self.name,
        )


class ZakupStrategyStack:
    def __init__(
        self,
        direct: CurlCffiApiStrategy,
        browser_fetch: BrowserFetchStrategy,
        network_intercept: NetworkInterceptStrategy,
        dom_fallback: DomFallbackStrategy,
        browser: ZakupBrowserSession,
        *,
        lane_breaker: CircuitBreaker,
        source_breaker: CircuitBreaker,
    ) -> None:
        self.direct = direct
        self.browser_fetch = browser_fetch
        self.network_intercept = network_intercept
        self.dom_fallback = dom_fallback
        self.browser = browser
        self.lane_breaker = lane_breaker
        self.source_breaker = source_breaker
        self.profiles: dict[str, RequestProfile] = {}
        self._request_lock = asyncio.Lock()

    async def request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        body: dict[str, Any] | None = None,
    ) -> ApiResponse:
        async with self._request_lock:
            return await self._request_locked(
                method,
                url,
                params=params,
                body=body,
            )

    async def _request_locked(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None,
        body: dict[str, Any] | None,
    ) -> ApiResponse:
        await self.source_breaker.allow()
        await self.lane_breaker.allow()
        bind_contextvars(
            session_lane_id=(
                self.browser.identity.lane_id if self.browser.identity else None
            ),
            proxy_id=(
                self.browser.identity.proxy_id if self.browser.identity else None
            ),
            strategy=self.direct.name,
        )
        profile_key = request_profile_key(method, url)
        profile = self.profiles.get(profile_key) or self.browser.get_profile(method, url)
        if profile:
            self.profiles[profile_key] = profile
        if profile and profile.browser_only:
            bind_contextvars(strategy=self.network_intercept.name)
            response = await self.network_intercept.request(
                method,
                url,
                params=params,
                body=body,
                profile=profile,
            )
            if response.status in {403, 418, 429}:
                await self.lane_breaker.record_status(response.status)
                await self.source_breaker.record_status(response.status)
                return response
            await self.lane_breaker.record_success()
            await self.source_breaker.record_success()
            return response

        response = await self.direct.request(
            method, url, params=params, body=body, profile=profile
        )
        if response.status not in {403, 418, 429}:
            await self.lane_breaker.record_success()
            await self.source_breaker.record_success()
            return response

        await self.lane_breaker.record_status(response.status)
        await self.source_breaker.record_status(response.status)
        await self.browser.start()
        assert self.browser.identity is not None
        self.direct.identity = self.browser.identity
        captured = self.browser.get_profile(method, url)
        if captured:
            bind_contextvars(
                session_lane_id=self.browser.identity.lane_id,
                proxy_id=self.browser.identity.proxy_id,
            )
            self.profiles[profile_key] = captured
            if not captured.browser_only:
                refreshed = await self.direct.request(
                    method, url, params=params, body=body, profile=captured
                )
                if refreshed.status not in {403, 418, 429}:
                    await self.lane_breaker.record_success()
                    await self.source_breaker.record_success()
                    return refreshed
        else:
            bind_contextvars(strategy=self.network_intercept.name)
            intercepted = await self.network_intercept.request(
                method,
                url,
                params=params,
                body=body,
            )
            if captured := self.browser.get_profile(method, url):
                self.profiles[profile_key] = captured
            if intercepted.status not in {403, 418, 429}:
                await self.lane_breaker.record_success()
                await self.source_breaker.record_success()
                return intercepted
            await self.lane_breaker.record_status(intercepted.status)
            await self.source_breaker.record_status(intercepted.status)
        return await self._browser_request(
            method,
            url,
            params=params,
            body=body,
            profile=captured,
        )

    async def _browser_request(
        self,
        method: str,
        url: str,
        *,
        params: dict[str, Any] | None,
        body: dict[str, Any] | None,
        profile: RequestProfile | None,
    ) -> ApiResponse:
        bind_contextvars(strategy=self.browser_fetch.name)
        response = await self.browser_fetch.request(
            method, url, params=params, body=body, profile=profile
        )
        if response.status in {403, 418, 429}:
            await self.lane_breaker.record_status(response.status)
            await self.source_breaker.record_status(response.status)
            bind_contextvars(strategy=self.network_intercept.name)
            response = await self.network_intercept.request(
                method,
                url,
                params=params,
                body=body,
                profile=profile,
            )
        if (
            response.status in {403, 418, 429}
            and url.endswith(("/lots/filter", "/4dv3rts/filter"))
        ):
            bind_contextvars(strategy=self.dom_fallback.name)
            response = await self.dom_fallback.request(
                method,
                url,
                params=params,
                body=body,
                profile=profile,
            )
        if response.status in {403, 418, 429}:
            await self.lane_breaker.record_status(response.status)
            await self.source_breaker.record_status(response.status)
            return response
        await self.lane_breaker.record_success()
        await self.source_breaker.record_success()
        return response

    async def close(self) -> None:
        await self.direct.close()
        await self.browser.close()
