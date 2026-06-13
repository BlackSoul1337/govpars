from __future__ import annotations


class RetriableSourceError(RuntimeError):
    """A source failure that should be returned to the durable queue."""

    def __init__(
        self,
        message: str,
        *,
        delay_seconds: int = 600,
        strategy: str = "unknown",
        http_status: int | None = None,
    ) -> None:
        super().__init__(message)
        self.delay_seconds = delay_seconds
        self.strategy = strategy
        self.http_status = http_status


class CircuitOpenError(RetriableSourceError):
    def __init__(self, message: str = "Circuit breaker is in cooldown") -> None:
        super().__init__(
            message,
            delay_seconds=600,
            strategy="circuit-breaker",
        )


class SourceBlockedError(RetriableSourceError):
    def __init__(self, status: int, strategy: str, *, source: str) -> None:
        super().__init__(
            f"{source} blocked request with HTTP {status} via {strategy}",
            delay_seconds=600,
            strategy=strategy,
            http_status=status,
        )


class LeaseLostError(RuntimeError):
    """The task lease is no longer owned by the current worker."""


def is_permanent_http_status(status: int | None) -> bool:
    """Return True only for resource states that retries cannot repair."""

    return status in {404, 410}
