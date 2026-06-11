from __future__ import annotations

import asyncio
import os
import sys
import time
from collections import deque
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from urllib.parse import urlparse

import httpx

from procurement_parser.config.settings import CaptchaSettings
from procurement_parser.domain.models import (
    CaptchaChallenge,
    CaptchaKind,
    CaptchaSolution,
)
from procurement_parser.domain.ports import RuntimeStatePort
from procurement_parser.metrics import (
    CAPTCHA_COST,
    CAPTCHA_LATENCY,
    CAPTCHA_OUTCOMES,
)


class CaptchaError(RuntimeError):
    pass


class DisabledCaptchaSolver:
    interactive = False

    async def solve(self, challenge: CaptchaChallenge) -> CaptchaSolution:
        raise CaptchaError("CAPTCHA solving is disabled")


class ManualCaptchaSolver:
    interactive = True

    def __init__(
        self,
        runtime_state: RuntimeStatePort | None = None,
    ) -> None:
        self.runtime_state = runtime_state

    async def solve(self, challenge: CaptchaChallenge) -> CaptchaSolution:
        if self.runtime_state and challenge.session_lane_id:
            async with self.runtime_state.captcha_lock(
                challenge.session_lane_id
            ):
                return await self._solve_interactive(challenge)
        return await self._solve_interactive(challenge)

    async def _solve_interactive(
        self,
        challenge: CaptchaChallenge,
    ) -> CaptchaSolution:
        if os.getenv("PLAYWRIGHT_HEADLESS", "").lower() in {"1", "true", "yes"}:
            raise CaptchaError(
                "Manual CAPTCHA requires a visible browser; "
                "PLAYWRIGHT_HEADLESS is enabled"
            )
        if not sys.stdin.isatty():
            raise CaptchaError(
                "Manual CAPTCHA requires an interactive TTY and cannot run "
                "inside a detached container"
            )
        prompt = (
            f"Complete CAPTCHA in the visible browser for {challenge.website_url}, "
            "then press Enter here: "
        )
        started = time.monotonic()
        await asyncio.to_thread(input, prompt)
        solution = CaptchaSolution(
            provider="manual",
            token="__browser_manual__",
        )
        if self.runtime_state:
            await self.runtime_state.record_captcha(
                session_lane_id=challenge.session_lane_id,
                provider="manual",
                challenge_type=challenge.kind.value,
                provider_task_id=None,
                status="success",
                cost=None,
                latency_ms=round((time.monotonic() - started) * 1000),
                error_code=None,
            )
        return solution


class TwoCaptchaSolver:
    API_URL = "https://api.2captcha.com"
    interactive = False

    def __init__(
        self,
        settings: CaptchaSettings,
        runtime_state: RuntimeStatePort | None = None,
    ) -> None:
        if not settings.api_key:
            raise ValueError("TWOCAPTCHA_API_KEY is required")
        self.api_key = settings.api_key.get_secret_value()
        self.poll_interval = settings.poll_interval_seconds
        self.timeout = settings.timeout_seconds
        self.max_attempts = settings.max_attempts
        self.max_cost_per_hour = Decimal(str(settings.max_cost_per_hour))
        self.cost_events: deque[tuple[datetime, Decimal]] = deque()
        self.client = httpx.AsyncClient(timeout=30)
        self._solve_lock = asyncio.Lock()
        self.runtime_state = runtime_state

    async def _check_budget(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(hours=1)
        if self.runtime_state:
            used = await self.runtime_state.captcha_spend_since(
                provider="2captcha",
                since=cutoff,
            )
        else:
            while self.cost_events and self.cost_events[0][0] < cutoff:
                self.cost_events.popleft()
            used = sum((cost for _, cost in self.cost_events), start=Decimal("0"))
        if used >= self.max_cost_per_hour:
            raise CaptchaError("2captcha hourly cost limit reached")

    @staticmethod
    def _proxy_fields(proxy_url: str) -> dict[str, str | int]:
        parsed = urlparse(proxy_url)
        if not parsed.hostname or not parsed.port:
            raise CaptchaError("Proxy-bound CAPTCHA requires a complete proxy URL")
        proxy_type = parsed.scheme.lower()
        if proxy_type == "https":
            proxy_type = "http"
        result: dict[str, str | int] = {
            "proxyType": proxy_type,
            "proxyAddress": parsed.hostname,
            "proxyPort": parsed.port,
        }
        if parsed.username:
            result["proxyLogin"] = parsed.username
        if parsed.password:
            result["proxyPassword"] = parsed.password
        return result

    def _task(self, challenge: CaptchaChallenge) -> dict:
        common = {
            "websiteURL": challenge.website_url,
            "websiteKey": challenge.site_key,
        }
        if challenge.kind == CaptchaKind.RECAPTCHA_V2:
            task_type = "RecaptchaV2Task" if challenge.proxy_url else "RecaptchaV2TaskProxyless"
            task = {
                "type": task_type,
                **common,
                "isInvisible": challenge.invisible,
            }
            if challenge.user_agent:
                task["userAgent"] = challenge.user_agent
            if challenge.cookies:
                task["cookies"] = challenge.cookies
            if challenge.proxy_url:
                task.update(self._proxy_fields(challenge.proxy_url))
            return task
        if challenge.kind == CaptchaKind.RECAPTCHA_ENTERPRISE:
            task_type = (
                "RecaptchaV2EnterpriseTask"
                if challenge.proxy_url
                else "RecaptchaV2EnterpriseTaskProxyless"
            )
            task = {
                "type": task_type,
                **common,
                "isInvisible": challenge.invisible,
            }
        else:
            task_type = (
                "RecaptchaV3Task"
                if challenge.proxy_url
                else "RecaptchaV3TaskProxyless"
            )
            task = {
                "type": task_type,
                **common,
                "minScore": challenge.min_score,
                "pageAction": challenge.page_action,
                "isEnterprise": challenge.enterprise,
            }
        if challenge.user_agent:
            task["userAgent"] = challenge.user_agent
        if challenge.cookies:
            task["cookies"] = challenge.cookies
        if challenge.proxy_url:
            task.update(self._proxy_fields(challenge.proxy_url))
        return task

    async def solve(self, challenge: CaptchaChallenge) -> CaptchaSolution:
        async with self._solve_lock:
            if self.runtime_state and challenge.session_lane_id:
                async with self.runtime_state.captcha_lock(
                    challenge.session_lane_id
                ):
                    return await self._solve_with_retries(challenge)
            return await self._solve_with_retries(challenge)

    async def _solve_with_retries(
        self,
        challenge: CaptchaChallenge,
    ) -> CaptchaSolution:
        last_error: Exception | None = None
        for _ in range(self.max_attempts):
            await self._check_budget()
            started = time.monotonic()
            try:
                solution = await self._solve_once(challenge)
            except (CaptchaError, httpx.HTTPError) as exc:
                last_error = exc
                elapsed = time.monotonic() - started
                CAPTCHA_LATENCY.labels(
                    provider="2captcha",
                    kind=challenge.kind.value,
                    outcome="error",
                ).observe(elapsed)
                await self._record_attempt(
                    challenge,
                    status="error",
                    latency_seconds=elapsed,
                    error_code=str(exc),
                )
                continue
            elapsed = time.monotonic() - started
            CAPTCHA_LATENCY.labels(
                provider="2captcha",
                kind=challenge.kind.value,
                outcome="success",
            ).observe(elapsed)
            await self._record_attempt(
                challenge,
                status="success",
                latency_seconds=elapsed,
                solution=solution,
            )
            return solution
        assert last_error is not None
        raise last_error

    async def _record_attempt(
        self,
        challenge: CaptchaChallenge,
        *,
        status: str,
        latency_seconds: float,
        solution: CaptchaSolution | None = None,
        error_code: str | None = None,
    ) -> None:
        if not self.runtime_state:
            return
        await self.runtime_state.record_captcha(
            session_lane_id=challenge.session_lane_id,
            provider="2captcha",
            challenge_type=challenge.kind.value,
            provider_task_id=solution.task_id if solution else None,
            status=status,
            cost=solution.cost if solution else None,
            latency_ms=round(latency_seconds * 1000),
            error_code=error_code,
        )

    async def _solve_once(self, challenge: CaptchaChallenge) -> CaptchaSolution:
        try:
            create = await self.client.post(
                f"{self.API_URL}/createTask",
                json={"clientKey": self.api_key, "task": self._task(challenge)},
            )
        except Exception:
            CAPTCHA_OUTCOMES.labels(
                provider="2captcha",
                kind=challenge.kind.value,
                outcome="transport_error",
            ).inc()
            raise
        create.raise_for_status()
        payload = create.json()
        if payload.get("errorId"):
            raise CaptchaError(payload.get("errorDescription") or payload.get("errorCode"))
        task_id = str(payload["taskId"])
        started = time.monotonic()
        while time.monotonic() - started < self.timeout:
            await asyncio.sleep(self.poll_interval)
            result = await self.client.post(
                f"{self.API_URL}/getTaskResult",
                json={"clientKey": self.api_key, "taskId": task_id},
            )
            result.raise_for_status()
            data = result.json()
            if data.get("errorId"):
                raise CaptchaError(data.get("errorDescription") or data.get("errorCode"))
            if data.get("status") != "ready":
                continue
            cost = Decimal(str(data.get("cost", "0")))
            self.cost_events.append((datetime.now(UTC), cost))
            CAPTCHA_COST.labels(provider="2captcha").inc(float(cost))
            token = data.get("solution", {}).get("token") or data.get(
                "solution", {}
            ).get("gRecaptchaResponse")
            if not token:
                raise CaptchaError("2captcha returned no token")
            CAPTCHA_OUTCOMES.labels(
                provider="2captcha",
                kind=challenge.kind.value,
                outcome="success",
            ).inc()
            return CaptchaSolution(
                provider="2captcha",
                token=token,
                task_id=task_id,
                cost=cost,
            )
        CAPTCHA_OUTCOMES.labels(
            provider="2captcha",
            kind=challenge.kind.value,
            outcome="timeout",
        ).inc()
        raise CaptchaError("2captcha task timed out")

    async def close(self) -> None:
        await self.client.aclose()

class FallbackCaptchaSolver:
    def __init__(self, primary, fallback) -> None:
        self.primary = primary
        self.fallback = fallback
        self.interactive = bool(
            getattr(fallback, "interactive", False)
            and sys.stdin.isatty()
        )

    async def solve(self, challenge: CaptchaChallenge) -> CaptchaSolution:
        try:
            return await self.primary.solve(challenge)
        except CaptchaError:
            return await self.fallback.solve(challenge)

    async def close(self) -> None:
        for solver in (self.primary, self.fallback):
            close = getattr(solver, "close", None)
            if close:
                await close()


def build_captcha_solver(
    settings: CaptchaSettings,
    runtime_state: RuntimeStatePort | None = None,
):
    if settings.provider == "disabled":
        return DisabledCaptchaSolver()
    if settings.provider == "manual":
        return ManualCaptchaSolver(runtime_state)
    primary = TwoCaptchaSolver(settings, runtime_state)
    if settings.fallback_provider == "manual":
        return FallbackCaptchaSolver(
            primary,
            ManualCaptchaSolver(runtime_state),
        )
    return primary
