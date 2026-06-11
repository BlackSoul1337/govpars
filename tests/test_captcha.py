from contextlib import asynccontextmanager
from decimal import Decimal

import httpx
import pytest
from pydantic import SecretStr

from procurement_parser.config.settings import CaptchaSettings
from procurement_parser.domain.models import (
    CaptchaChallenge,
    CaptchaKind,
    CaptchaSolution,
)
from procurement_parser.infrastructure.captcha.solvers import (
    CaptchaError,
    DisabledCaptchaSolver,
    FallbackCaptchaSolver,
    ManualCaptchaSolver,
    TwoCaptchaSolver,
    build_captcha_solver,
)


class FakeRuntimeState:
    def __init__(self, spend: Decimal = Decimal("0")) -> None:
        self.spend = spend
        self.locked = []
        self.attempts = []

    @asynccontextmanager
    async def captcha_lock(self, lane_id):
        self.locked.append(lane_id)
        yield

    async def captcha_spend_since(self, *, provider, since):
        return self.spend

    async def record_captcha(self, **kwargs):
        self.attempts.append(kwargs)


def challenge(
    kind: CaptchaKind = CaptchaKind.RECAPTCHA_V2,
    **overrides,
) -> CaptchaChallenge:
    return CaptchaChallenge(
        kind=kind,
        website_url="https://zakup.sk.kz/",
        site_key="site-key",
        session_lane_id="lane-1",
        **overrides,
    )


@pytest.mark.asyncio
async def test_proxy_bound_recaptcha_v3_uses_same_proxy_and_user_agent() -> None:
    solver = TwoCaptchaSolver(
        CaptchaSettings(
            provider="2captcha",
            api_key=SecretStr("test-key"),
        )
    )
    try:
        task = solver._task(
            CaptchaChallenge(
                kind=CaptchaKind.RECAPTCHA_V3,
                website_url="https://zakup.sk.kz/",
                site_key="site-key",
                page_action="search",
                proxy_url="http://user:password@proxy.example:8000",
                user_agent="test-agent",
            )
        )
    finally:
        await solver.close()

    assert task["type"] == "RecaptchaV3Task"
    assert task["proxyAddress"] == "proxy.example"
    assert task["proxyPort"] == 8000
    assert task["proxyLogin"] == "user"
    assert task["proxyPassword"] == "password"
    assert task["userAgent"] == "test-agent"


@pytest.mark.asyncio
async def test_captcha_payload_variants_and_invalid_proxy() -> None:
    solver = TwoCaptchaSolver(
        CaptchaSettings(provider="2captcha", api_key=SecretStr("test-key"))
    )
    try:
        v2 = solver._task(
            challenge(
                invisible=True,
                cookies="session=value",
                user_agent="browser-agent",
            )
        )
        enterprise = solver._task(
            challenge(
                CaptchaKind.RECAPTCHA_ENTERPRISE,
                proxy_url="https://proxy.example:8443",
            )
        )
        with pytest.raises(CaptchaError, match="complete proxy URL"):
            solver._task(challenge(proxy_url="http://missing-port.example"))
    finally:
        await solver.close()

    assert v2["type"] == "RecaptchaV2TaskProxyless"
    assert v2["cookies"] == "session=value"
    assert enterprise["type"] == "RecaptchaV2EnterpriseTask"
    assert enterprise["proxyType"] == "http"


@pytest.mark.asyncio
async def test_two_captcha_retry_success_uses_runtime_lock_and_budget() -> None:
    runtime = FakeRuntimeState()
    solver = TwoCaptchaSolver(
        CaptchaSettings(
            provider="2captcha",
            api_key=SecretStr("test-key"),
            max_attempts=2,
            poll_interval_seconds=0,
            timeout_seconds=2,
        ),
        runtime,
    )
    calls = {"create": 0}

    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/createTask"):
            calls["create"] += 1
            if calls["create"] == 1:
                return httpx.Response(
                    200,
                    json={"errorId": 1, "errorDescription": "temporary"},
                )
            return httpx.Response(200, json={"errorId": 0, "taskId": 42})
        return httpx.Response(
            200,
            json={
                "errorId": 0,
                "status": "ready",
                "cost": "0.003",
                "solution": {"gRecaptchaResponse": "solved-token"},
            },
        )

    await solver.client.aclose()
    solver.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        solution = await solver.solve(challenge())
    finally:
        await solver.close()

    assert solution.token == "solved-token"
    assert solution.task_id == "42"
    assert solution.cost == Decimal("0.003")
    assert runtime.locked == ["lane-1"]
    assert [attempt["status"] for attempt in runtime.attempts] == [
        "error",
        "success",
    ]


@pytest.mark.asyncio
async def test_two_captcha_enforces_persisted_hourly_budget() -> None:
    runtime = FakeRuntimeState(Decimal("1"))
    solver = TwoCaptchaSolver(
        CaptchaSettings(
            provider="2captcha",
            api_key=SecretStr("test-key"),
            max_cost_per_hour=1,
        ),
        runtime,
    )
    try:
        with pytest.raises(CaptchaError, match="hourly cost limit"):
            await solver.solve(challenge())
    finally:
        await solver.close()


@pytest.mark.asyncio
async def test_manual_disabled_fallback_and_factory(monkeypatch) -> None:
    with pytest.raises(CaptchaError, match="disabled"):
        await DisabledCaptchaSolver().solve(challenge())

    monkeypatch.setenv("PLAYWRIGHT_HEADLESS", "true")
    with pytest.raises(CaptchaError, match="visible browser"):
        await ManualCaptchaSolver().solve(challenge())

    class FailingSolver:
        async def solve(self, _challenge):
            raise CaptchaError("primary failed")

    class SuccessfulSolver:
        async def solve(self, _challenge):
            return CaptchaSolution(provider="fallback", token="token")

    solution = await FallbackCaptchaSolver(
        FailingSolver(),
        SuccessfulSolver(),
    ).solve(challenge())
    assert solution.provider == "fallback"
    assert isinstance(
        build_captcha_solver(CaptchaSettings(provider="disabled")),
        DisabledCaptchaSolver,
    )
    assert isinstance(
        build_captcha_solver(CaptchaSettings(provider="manual")),
        ManualCaptchaSolver,
    )
    with pytest.raises(ValueError, match="TWOCAPTCHA_API_KEY"):
        build_captcha_solver(CaptchaSettings(provider="2captcha"))
