from __future__ import annotations

import asyncio
import hashlib
import os
import re
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from playwright.async_api import (
    BrowserContext,
    Page,
    Request,
    async_playwright,
)
from playwright.async_api import (
    Error as PlaywrightError,
)
from pydantic import SecretStr
from structlog import get_logger
from structlog.contextvars import bind_contextvars

from procurement_parser.config.settings import NetworkSettings, SourceSettings
from procurement_parser.domain.models import CaptchaChallenge, CaptchaKind
from procurement_parser.domain.ports import CaptchaSolverPort
from procurement_parser.infrastructure.network.session import (
    HOP_BY_HOP_HEADERS,
    RequestProfile,
    SessionIdentity,
)
from procurement_parser.infrastructure.sources.zakup_sk.bundle_cache import (
    ZakupBundleCache,
)
from procurement_parser.infrastructure.sources.zakup_sk.request_profiles import (
    request_profile_key,
)

SESSION_HEADER_NAMES = {
    "authorization",
    "cookie",
    "x-auth-token",
    "x-csrf-token",
    "x-xsrf-token",
}
REQUEST_SCOPED_HEADER_NAMES = {"e-tag", "tor"}
REQUEST_SCOPED_PATTERNS = ("signature", "timestamp", "nonce", "request-id")
BLOCKED_THIRD_PARTY_URLS = [
    "*://connect.facebook.net/*",
    "*://www.facebook.com/tr/*",
    "*://www.googletagmanager.com/*",
    "*://www.google-analytics.com/*",
    "*://mc.yandex.ru/*",
    "*://www.youtube.com/*",
    "*://youtube.com/*",
    "*://*.googlevideo.com/*",
]
BLOCKED_THIRD_PARTY_HOSTS = {
    "connect.facebook.net",
    "www.facebook.com",
    "www.googletagmanager.com",
    "www.google-analytics.com",
    "mc.yandex.ru",
    "www.youtube.com",
    "youtube.com",
}
logger = get_logger()


class ZakupBrowserSession:
    def __init__(
        self,
        source: SourceSettings,
        network: NetworkSettings,
        captcha_solver: CaptchaSolverPort,
        *,
        lane_index: int = 0,
        session_generation: int = 0,
        navigation_timeout_seconds: int = 60,
        capture_timeout_seconds: int = 60,
        response_timeout_seconds: int = 60,
        disk_cache_mb: int = 512,
        preload_main_bundle: bool = False,
    ) -> None:
        self.source = source
        self.network = network
        self.captcha_solver = captcha_solver
        self.lane_index = lane_index
        self.session_generation = session_generation
        self.navigation_timeout_seconds = navigation_timeout_seconds
        self.capture_timeout_seconds = capture_timeout_seconds
        self.response_timeout_seconds = response_timeout_seconds
        self.disk_cache_mb = disk_cache_mb
        self.preload_main_bundle = preload_main_bundle
        self.playwright = None
        self.context: BrowserContext | None = None
        self.page: Page | None = None
        self.identity: SessionIdentity | None = None
        self._capture_event = asyncio.Event()
        self._response_cache: list[tuple[str, int, dict[str, str], Any]] = []
        self._initial_navigation_started = False
        self._request_started_at: dict[int, datetime] = {}
        self._cdp_session = None

    @property
    def lane_id(self) -> str:
        proxy_url = (
            self.network.proxy_url.get_secret_value()
            if self.network.proxy_url
            else None
        )
        proxy_id = (
            hashlib.sha256(proxy_url.encode()).hexdigest()[:12]
            if proxy_url
            else "direct"
        )
        profile_root = str(
            Path(self.source.browser_profile_dir or "playwright/.auth/zakup")
            .resolve()
        )
        profile_id = hashlib.sha256(profile_root.encode()).hexdigest()[:8]
        return f"zakup-{profile_id}-{self.lane_index}-{proxy_id}"

    def _lane_log_context(self) -> dict[str, str | None]:
        return {
            "session_lane_id": (
                self.identity.lane_id if self.identity else self.lane_id
            ),
            "proxy_id": self.identity.proxy_id if self.identity else None,
        }

    async def start(self) -> SessionIdentity:
        if self.identity and self.context and self.page:
            return self.identity
        self.playwright = await async_playwright().start()
        proxy_url = self.network.proxy_url.get_secret_value() if self.network.proxy_url else None
        proxy_id = (
            hashlib.sha256(proxy_url.encode()).hexdigest()[:12] if proxy_url else "direct"
        )
        bind_contextvars(
            session_lane_id=self.lane_id,
            proxy_id=None if proxy_id == "direct" else proxy_id,
        )
        profile_dir = (
            Path(self.source.browser_profile_dir or "playwright/.auth/zakup")
            / f"lane-{self.lane_index}-{proxy_id}-g{self.session_generation}"
        )
        await asyncio.to_thread(profile_dir.mkdir, parents=True, exist_ok=True)
        proxy = {"server": proxy_url} if proxy_url else None
        bundle_cache = ZakupBundleCache(
            base_url=self.source.base_url,
            cache_dir=Path("playwright/.cache/zakup"),
            proxy_url=proxy_url,
            timeout_seconds=self.navigation_timeout_seconds,
        )
        cached_bundle = bundle_cache.cached()
        if self.preload_main_bundle:
            cached_bundle = cached_bundle or await bundle_cache.prepare()
        explicit_headless = os.getenv("PLAYWRIGHT_HEADLESS")
        headless = (
            explicit_headless != "0"
            if explicit_headless is not None
            else not bool(getattr(self.captcha_solver, "interactive", False))
        )
        self.context = await self.playwright.chromium.launch_persistent_context(
            user_data_dir=str(profile_dir),
            headless=headless,
            proxy=proxy,
            locale="ru-RU",
            viewport={"width": 1440, "height": 1000},
            args=[
                f"--disk-cache-size={self.disk_cache_mb * 1024 * 1024}",
                "--media-cache-size=1",
                (
                    "--host-resolver-rules="
                    "MAP www.googletagmanager.com ~NOTFOUND, "
                    "MAP www.google-analytics.com ~NOTFOUND, "
                    "MAP connect.facebook.net ~NOTFOUND, "
                    "MAP mc.yandex.ru ~NOTFOUND, "
                    "MAP www.youtube.com ~NOTFOUND, "
                    "MAP youtube.com ~NOTFOUND, "
                    "MAP *.googlevideo.com ~NOTFOUND"
                ),
            ],
        )
        self.page = self.context.pages[0] if self.context.pages else await self.context.new_page()
        self._cdp_session = await self.context.new_cdp_session(self.page)
        await self._cdp_session.send("Network.enable")
        await self._cdp_session.send(
            "Network.setBlockedURLs",
            {"urls": BLOCKED_THIRD_PARTY_URLS},
        )
        if cached_bundle:
            bundle_url, bundle_path = cached_bundle

            async def serve_cached_bundle(route) -> None:
                await route.fulfill(
                    path=bundle_path,
                    content_type="application/javascript; charset=utf-8",
                    headers={"Cache-Control": "public, max-age=31536000, immutable"},
                )

            await self.page.route(bundle_url, serve_cached_bundle)
        self.page.set_default_navigation_timeout(self.navigation_timeout_seconds * 1000)
        self.page.set_default_timeout(self.response_timeout_seconds * 1000)
        user_agent = await self.page.evaluate("navigator.userAgent")
        self.identity = SessionIdentity(
            lane_id=self.lane_id,
            source=self.source.name,
            proxy_id=None if proxy_id == "direct" else proxy_id,
            proxy_url=SecretStr(proxy_url) if proxy_url else None,
            user_agent=user_agent,
            storage_state_path=str(profile_dir),
        )
        self.page.on("request", self._capture_request)
        self.page.on("response", self._capture_response)
        self.page.on("requestfailed", self._log_failed_request)
        self._capture_event.clear()
        logger.info(
            "zakup_browser_navigation_started",
            timeout_seconds=self.navigation_timeout_seconds,
            disk_cache_mb=self.disk_cache_mb,
            profile_dir=str(profile_dir),
        )
        await self.page.goto(
            f"{self.source.base_url}/#/ext?tabs=lot&adst=PUBLISHED&lst=PUBLISHED&page=1",
            wait_until="commit",
            timeout=self.navigation_timeout_seconds * 1000,
        )
        self._initial_navigation_started = True
        logger.info("zakup_browser_navigation_committed", url=self.page.url)
        await self._solve_captcha_if_present()
        cookies = await self.context.cookies()
        self.identity.cookies = {
            cookie["name"]: SecretStr(cookie["value"]) for cookie in cookies
        }
        return self.identity

    async def _capture_response(self, response) -> None:
        request_key = id(response.request)
        started_at = self._request_started_at.pop(request_key, None)
        if response.request.resource_type == "script":
            headers = await response.all_headers()
            content_length = headers.get("content-length")
            logger.info(
                "zakup_script_response",
                url=response.url,
                status=response.status,
                content_length=content_length,
                cache_control=headers.get("cache-control"),
                etag=headers.get("etag"),
                elapsed_seconds=(
                    round((datetime.now(UTC) - started_at).total_seconds(), 2)
                    if started_at
                    else None
                ),
                **self._lane_log_context(),
            )
        if "/eprocsearch/api/external/" not in response.url:
            return
        try:
            data = await response.json()
            headers = await response.all_headers()
        except (PlaywrightError, ValueError):
            return
        self._response_cache.append(
            (response.url, response.status, headers, data)
        )
        if len(self._response_cache) > 100:
            del self._response_cache[:-100]

    async def _capture_request(self, request: Request) -> None:
        if request.resource_type == "script" and not self._is_blocked_third_party(
            request.url
        ):
            self._request_started_at[id(request)] = datetime.now(UTC)
            logger.info(
                "zakup_script_request",
                url=request.url,
                **self._lane_log_context(),
            )
        if "/eprocsearch/api/external/" not in request.url:
            return
        try:
            headers_array = await request.headers_array()
        except PlaywrightError:
            return
        static: dict[str, str] = {}
        session: dict[str, SecretStr] = {}
        scoped: set[str] = set()
        for item in headers_array:
            name = item["name"]
            value = item["value"]
            lower = name.lower()
            if name.startswith(":") or lower in HOP_BY_HOP_HEADERS:
                continue
            if lower in REQUEST_SCOPED_HEADER_NAMES or any(
                pattern in lower for pattern in REQUEST_SCOPED_PATTERNS
            ):
                scoped.add(lower)
                continue
            if lower in SESSION_HEADER_NAMES or lower.startswith("x-"):
                session[name] = SecretStr(value)
            else:
                static[name] = value
        profile = RequestProfile(
            method=request.method,
            url_pattern=request.url.split("?", 1)[0],
            static_headers=static,
            session_headers=session,
            request_scoped_header_names=scoped,
            body_template=request.post_data,
            browser_only=bool(scoped),
        )
        if self.identity:
            key = request_profile_key(request.method, request.url)
            previous = self.identity.request_profiles.get(key)
            if previous:
                changed = {
                    name.lower()
                    for name, value in profile.session_headers.items()
                    if name.lower() != "cookie"
                    and (
                        old := next(
                            (
                                old_value
                                for old_name, old_value in previous.session_headers.items()
                                if old_name.lower() == name.lower()
                            ),
                            None,
                        )
                    )
                    and old.get_secret_value() != value.get_secret_value()
                }
                if changed:
                    profile.request_scoped_header_names.update(changed)
                    profile.browser_only = True
                    logger.info(
                        "zakup_dynamic_headers_detected",
                        url_pattern=profile.url_pattern,
                        header_names=sorted(changed),
                        **self._lane_log_context(),
                    )
            self.identity.request_profiles[key] = profile
            while len(self.identity.request_profiles) > 100:
                self.identity.request_profiles.pop(next(iter(self.identity.request_profiles)))
        self._capture_event.set()
        logger.debug(
            "zakup_api_profile_captured",
            method=request.method,
            url_pattern=profile.url_pattern,
            browser_only=profile.browser_only,
            **self._lane_log_context(),
        )

    async def _log_failed_request(self, request: Request) -> None:
        if request.resource_type != "script":
            return
        self._request_started_at.pop(id(request), None)
        if self._is_blocked_third_party(request.url):
            return
        logger.warning(
            "zakup_script_request_failed",
            url=request.url,
            failure=request.failure,
            **self._lane_log_context(),
        )

    @staticmethod
    def _is_blocked_third_party(url: str) -> bool:
        hostname = (urlparse(url).hostname or "").lower()
        return hostname in BLOCKED_THIRD_PARTY_HOSTS or hostname.endswith(
            ".googlevideo.com"
        )

    async def capture_profile(
        self,
        method: str | None = None,
        url: str | None = None,
        *,
        timeout_seconds: int | None = None,
    ) -> RequestProfile | None:
        timeout_seconds = timeout_seconds or self.capture_timeout_seconds
        identity = await self.start()
        target_key = request_profile_key(method, url) if method and url else None
        if target_key and target_key in identity.request_profiles:
            return identity.request_profiles[target_key]
        if not target_key and identity.request_profiles:
            return next(reversed(identity.request_profiles.values()))
        deadline = asyncio.get_running_loop().time() + timeout_seconds
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            self._capture_event.clear()
            try:
                await asyncio.wait_for(self._capture_event.wait(), timeout=remaining)
            except TimeoutError:
                return None
            assert self.identity is not None
            if target_key:
                if profile := self.identity.request_profiles.get(target_key):
                    return profile
                continue
            return next(reversed(self.identity.request_profiles.values()), None)

    def get_profile(self, method: str, url: str) -> RequestProfile | None:
        if not self.identity:
            return None
        return self.identity.request_profiles.get(request_profile_key(method, url))

    async def fetch_json(
        self,
        method: str,
        url: str,
        *,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> tuple[int, dict[str, str], Any]:
        await self.start()
        assert self.page is not None
        result = await self.page.evaluate(
            """
            async ({method, url, body, headers}) => {
                const response = await fetch(url, {
                    method,
                    headers: {'Content-Type': 'application/json', ...headers},
                    credentials: 'include',
                    body: body === null ? undefined : JSON.stringify(body),
                });
                const text = await response.text();
                let data;
                try { data = JSON.parse(text); } catch (_) { data = {raw: text}; }
                return {
                    status: response.status,
                    headers: Object.fromEntries(response.headers.entries()),
                    data,
                };
            }
            """,
            {"method": method, "url": url, "body": body, "headers": headers or {}},
        )
        return result["status"], result["headers"], result["data"]

    async def intercept_route_json(
        self,
        route_fragment: str,
        endpoint_fragment: str,
        *,
        timeout_seconds: int | None = None,
    ) -> tuple[int, dict[str, str], Any]:
        timeout_seconds = timeout_seconds or self.response_timeout_seconds
        await self.start()
        assert self.page is not None
        target_url = f"{self.source.base_url}/#{route_fragment}"
        route_page_match = re.search(r"[?&]page=(\d+)", route_fragment)
        api_page = int(route_page_match.group(1)) - 1 if route_page_match else None
        cached = self._cached_response(endpoint_fragment, api_page)
        if cached:
            return cached
        async with self.page.expect_response(
            lambda response: endpoint_fragment in response.url,
            timeout=timeout_seconds * 1000,
        ) as response_info:
            if self.page.url.split("#", 1)[0] == self.source.base_url + "/":
                await self.page.evaluate(
                    "hash => { window.location.hash = hash; }",
                    route_fragment,
                )
            else:
                await self.page.goto(
                    target_url,
                    wait_until="commit",
                    timeout=self.navigation_timeout_seconds * 1000,
                )
        response = await response_info.value
        return response.status, await response.all_headers(), await response.json()

    async def dom_discovery_json(
        self,
        route_fragment: str,
    ) -> tuple[int, dict[str, str], Any]:
        await self.start()
        assert self.page is not None
        target_url = f"{self.source.base_url}/#{route_fragment}"
        await self.page.goto(
            target_url,
            wait_until="commit",
            timeout=self.navigation_timeout_seconds * 1000,
        )
        await self._solve_captcha_if_present()
        await self.page.wait_for_timeout(1500)
        items = await self.page.locator(
            'a[href*="popup:item/"]'
        ).evaluate_all(
            """
            links => {
                const result = [];
                const seen = new Set();
                for (const link of links) {
                    const href = link.getAttribute('href') || '';
                    const match = href.match(/item\\/(\\d+)\\/(lot|advert)/);
                    if (!match || seen.has(match[1])) continue;
                    seen.add(match[1]);
                    result.push({
                        id: match[1],
                        nameRu: (link.textContent || '').trim() || null,
                        domFallback: true,
                        href,
                    });
                }
                return result;
            }
            """
        )
        if not items:
            raise RuntimeError(
                "Zakup DOM fallback found no entity links; "
                "the page structure may have changed"
            )
        return 200, {"x-parser-strategy": "dom-fallback"}, {
            "content": items,
            "totalElements": len(items),
            "domFallback": True,
        }

    def _cached_response(
        self,
        endpoint_fragment: str,
        api_page: int | None,
    ) -> tuple[int, dict[str, str], Any] | None:
        for url, status, headers, data in reversed(self._response_cache):
            if endpoint_fragment not in url:
                continue
            if api_page is not None:
                query = parse_qs(urlparse(url).query)
                if int(query.get("page", [-1])[0]) != api_page:
                    continue
            return status, headers, data
        return None

    async def _solve_captcha_if_present(self) -> None:
        assert self.page is not None
        captcha_frame = next(
            (frame for frame in self.page.frames if "recaptcha" in frame.url.lower()),
            None,
        )
        site_key_locator = self.page.locator("[data-sitekey]")
        site_key = (
            await site_key_locator.first.get_attribute("data-sitekey")
            if await site_key_locator.count()
            else None
        )
        if not captcha_frame and not site_key:
            return
        if not site_key:
            match = re.search(r"[?&]k=([^&]+)", captcha_frame.url if captcha_frame else "")
            site_key = match.group(1) if match else None
        if not site_key:
            raise RuntimeError("reCAPTCHA detected, but site key could not be extracted")
        challenge = CaptchaChallenge(
            kind=CaptchaKind.RECAPTCHA_V2,
            website_url=self.page.url,
            site_key=site_key,
            user_agent=await self.page.evaluate("navigator.userAgent"),
            cookies="; ".join(
                f"{cookie['name']}={cookie['value']}"
                for cookie in await self.context.cookies()
            )
            if self.context
            else None,
            proxy_url=(
                self.network.proxy_url.get_secret_value()
                if self.network.proxy_url
                else None
            ),
            session_lane_id=(
                self.identity.lane_id
                if self.identity
                else self.lane_id
            ),
        )
        solution = await self.captcha_solver.solve(challenge)
        if solution.token == "__browser_manual__":
            await self.page.wait_for_function(
                """
                () => Array.from(document.querySelectorAll(
                    'textarea[name="g-recaptcha-response"], #g-recaptcha-response'
                )).some(target => target.value && target.value.length > 20)
                """,
                timeout=self.response_timeout_seconds * 1000,
            )
            return
        await self.page.evaluate(
            """
            token => {
                const targets = document.querySelectorAll(
                    'textarea[name="g-recaptcha-response"], #g-recaptcha-response'
                );
                targets.forEach(target => {
                    target.value = token;
                    target.innerHTML = token;
                    target.dispatchEvent(new Event('change', {bubbles: true}));
                });
                const widget = document.querySelector('[data-callback]');
                if (widget && typeof window[widget.dataset.callback] === 'function') {
                    window[widget.dataset.callback](token);
                }
            }
            """,
            solution.token,
        )

    async def close(self) -> None:
        if self.context:
            try:
                await self.context.close()
            except Exception:
                pass
        if self.playwright:
            try:
                await self.playwright.stop()
            except Exception:
                pass
        self.context = None
        self.page = None
        self.identity = None
        self._cdp_session = None
