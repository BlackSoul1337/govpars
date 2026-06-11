from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from enum import StrEnum

from procurement_parser.domain.errors import CircuitOpenError


class BreakerState(StrEnum):
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreaker:
    def __init__(
        self,
        *,
        threshold: int = 3,
        cooldown_seconds: int = 600,
        blocked_statuses: set[int] | None = None,
    ) -> None:
        self.threshold = threshold
        self.cooldown = timedelta(seconds=cooldown_seconds)
        self.blocked_statuses = blocked_statuses or {403, 418, 429}
        self.state = BreakerState.CLOSED
        self.consecutive_blocks = 0
        self.opened_at: datetime | None = None
        self._half_open_probe_active = False
        self._lock = asyncio.Lock()

    async def restore(
        self,
        *,
        state: str,
        consecutive_blocks: int,
        cooldown_until: datetime | None,
    ) -> None:
        async with self._lock:
            self.state = BreakerState(state)
            self.consecutive_blocks = consecutive_blocks
            self.opened_at = (
                cooldown_until - self.cooldown
                if cooldown_until and self.state == BreakerState.OPEN
                else None
            )
            self._half_open_probe_active = False

    @property
    def cooldown_until(self) -> datetime | None:
        if not self.opened_at or self.state != BreakerState.OPEN:
            return None
        return self.opened_at + self.cooldown

    async def allow(self) -> None:
        async with self._lock:
            if self.state == BreakerState.CLOSED:
                return
            if self.state == BreakerState.OPEN:
                if not self.opened_at or datetime.now(UTC) - self.opened_at < self.cooldown:
                    raise CircuitOpenError("Circuit breaker is in cooldown")
                self.state = BreakerState.HALF_OPEN
            if self._half_open_probe_active:
                raise CircuitOpenError("Half-open probe is already running")
            self._half_open_probe_active = True

    async def is_available(self) -> bool:
        async with self._lock:
            if self.state != BreakerState.OPEN:
                return True
            return bool(
                self.opened_at
                and datetime.now(UTC) - self.opened_at >= self.cooldown
            )

    async def record_success(self) -> None:
        async with self._lock:
            self.state = BreakerState.CLOSED
            self.consecutive_blocks = 0
            self.opened_at = None
            self._half_open_probe_active = False

    async def record_status(self, status: int) -> bool:
        if status not in self.blocked_statuses:
            await self.record_success()
            return False
        async with self._lock:
            self.consecutive_blocks += 1
            self._half_open_probe_active = False
            if (
                self.state == BreakerState.HALF_OPEN
                or self.consecutive_blocks >= self.threshold
            ):
                self.state = BreakerState.OPEN
                self.opened_at = datetime.now(UTC)
                return True
            return False

    async def record_transport_failure(self) -> bool:
        async with self._lock:
            self.consecutive_blocks += 1
            self._half_open_probe_active = False
            if (
                self.state == BreakerState.HALF_OPEN
                or self.consecutive_blocks >= self.threshold
            ):
                self.state = BreakerState.OPEN
                self.opened_at = datetime.now(UTC)
                return True
            return False
