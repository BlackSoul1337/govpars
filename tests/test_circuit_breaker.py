from datetime import timedelta

import pytest

from procurement_parser.infrastructure.network.circuit_breaker import (
    BreakerState,
    CircuitBreaker,
    CircuitOpenError,
)


@pytest.mark.asyncio
async def test_breaker_opens_and_recovers_with_half_open_probe() -> None:
    breaker = CircuitBreaker(threshold=3, cooldown_seconds=60)

    assert not await breaker.record_status(418)
    assert not await breaker.record_status(418)
    assert await breaker.record_status(418)
    assert breaker.state == BreakerState.OPEN

    with pytest.raises(CircuitOpenError):
        await breaker.allow()

    breaker.opened_at -= timedelta(seconds=61)
    await breaker.allow()
    assert breaker.state == BreakerState.HALF_OPEN
    await breaker.record_success()
    assert breaker.state == BreakerState.CLOSED


@pytest.mark.asyncio
async def test_open_breaker_reports_availability_after_cooldown() -> None:
    breaker = CircuitBreaker(threshold=1, cooldown_seconds=60)
    await breaker.record_status(429)

    assert not await breaker.is_available()
    breaker.opened_at -= timedelta(seconds=61)
    assert await breaker.is_available()


@pytest.mark.asyncio
async def test_transport_failures_open_breaker_at_threshold() -> None:
    breaker = CircuitBreaker(threshold=2, cooldown_seconds=60)

    assert not await breaker.record_transport_failure()
    assert await breaker.record_transport_failure()
    assert breaker.state == BreakerState.OPEN
