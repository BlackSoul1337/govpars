from __future__ import annotations

from collections.abc import AsyncIterator, Sequence
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from procurement_parser.domain.models import (
    CaptchaChallenge,
    CaptchaSolution,
    DiscoveredEntity,
    EntityEnvelope,
    EntityIdentity,
    EntityRelation,
    EntityType,
    ExtractedBatch,
    FrontierActivity,
    FrontierTask,
    Source,
)


class SourceAdapter(Protocol):
    source: Source

    async def discover(
        self,
        entity_type: EntityType,
        *,
        page: int = 1,
        priority: int = 0,
        filters: dict | None = None,
    ) -> list[DiscoveredEntity]: ...

    async def extract(self, identity: EntityIdentity) -> ExtractedBatch: ...

    async def close(self) -> None: ...


class FrontierPort(Protocol):
    async def enqueue(self, items: Sequence[DiscoveredEntity]) -> int: ...

    async def claim(
        self,
        worker_id: str,
        *,
        source: Source | None,
        limit: int,
        lease_seconds: int,
        backfill_only: bool = False,
    ) -> list[FrontierTask]: ...

    async def complete(self, task: FrontierTask, *, content_hash: str | None) -> None: ...

    async def release(
        self,
        tasks: Sequence[FrontierTask],
        *,
        worker_id: str,
    ) -> int: ...

    async def release_by_owner(self, worker_id: str) -> int: ...

    async def extend_lease(
        self,
        task: FrontierTask,
        *,
        worker_id: str,
        lease_seconds: int,
    ) -> bool: ...

    async def activity(self, source: Source) -> FrontierActivity: ...

    async def enqueue_refresh(
        self,
        source: Source,
        *,
        policy: str,
        priority: int,
        limit: int = 10_000,
        older_than_seconds: int = 0,
        newer_than_seconds: int | None = None,
    ) -> int: ...

    async def retry(
        self,
        task: FrontierTask,
        *,
        error: str,
        delay_seconds: int,
        strategy: str = "unknown",
        http_status: int | None = None,
    ) -> None: ...

    async def fail(
        self,
        task: FrontierTask,
        *,
        error: str,
        strategy: str = "unknown",
        http_status: int | None = None,
    ) -> None: ...


class EntityRepository(Protocol):
    async def persist(
        self,
        entities: Sequence[EntityEnvelope],
        relations: Sequence[EntityRelation],
    ) -> None: ...


class SessionProvider(Protocol):
    async def acquire(self, source: Source): ...

    async def release(self, session, *, blocked: bool = False) -> None: ...


class CaptchaSolverPort(Protocol):
    async def solve(self, challenge: CaptchaChallenge) -> CaptchaSolution: ...


class CsvExportPort(Protocol):
    async def export(self, view_name: str, destination: Path) -> int: ...


class BulkReplayPort(Protocol):
    async def replay(self, rows: AsyncIterator[dict]) -> int: ...


class RuntimeStatePort(Protocol):
    async def load_lane(self, lane_id: str) -> dict[str, Any] | None: ...

    async def save_lane(
        self,
        *,
        lane_id: str,
        source: str,
        proxy_id: str | None,
        state: str,
        consecutive_blocks: int,
        cooldown_until: datetime | None,
        profile_payload: dict[str, Any] | None = None,
    ) -> None: ...

    def captcha_lock(self, lane_id: str): ...

    async def captcha_spend_since(
        self,
        *,
        provider: str,
        since: datetime,
    ) -> Decimal: ...

    async def record_captcha(
        self,
        *,
        session_lane_id: str | None,
        provider: str,
        challenge_type: str,
        provider_task_id: str | None,
        status: str,
        cost: Decimal | None,
        latency_ms: int | None,
        error_code: str | None,
    ) -> None: ...
