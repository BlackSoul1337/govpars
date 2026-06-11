from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert

from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.infrastructure.persistence.postgres.schema import (
    CaptchaChallengeRow,
    SessionLaneRow,
)


class PostgresRuntimeStateRepository:
    def __init__(self, database: Database) -> None:
        self.database = database

    async def load_lane(self, lane_id: str) -> dict[str, Any] | None:
        async with self.database.sessions() as session:
            row = (
                await session.execute(
                    select(SessionLaneRow).where(SessionLaneRow.id == lane_id)
                )
            ).scalar_one_or_none()
        if row is None:
            return None
        return {
            "lane_id": row.id,
            "source": row.source,
            "proxy_id": row.proxy_id,
            "state": row.state,
            "consecutive_blocks": row.consecutive_blocks,
            "cooldown_until": row.cooldown_until,
            "profile_payload": row.profile_payload or {},
        }

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
    ) -> None:
        statement = (
            insert(SessionLaneRow)
            .values(
                id=lane_id,
                source=source,
                proxy_id=proxy_id,
                state=state,
                consecutive_blocks=consecutive_blocks,
                cooldown_until=cooldown_until,
                profile_payload=profile_payload or {},
            )
            .on_conflict_do_update(
                index_elements=[SessionLaneRow.id],
                set_={
                    "proxy_id": proxy_id,
                    "state": state,
                    "consecutive_blocks": consecutive_blocks,
                    "cooldown_until": cooldown_until,
                    "profile_payload": profile_payload or {},
                    "updated_at": func.now(),
                },
            )
        )
        async with self.database.sessions.begin() as session:
            await session.execute(statement)

    @asynccontextmanager
    async def captcha_lock(self, lane_id: str):
        connection = await self.database.engine.connect()
        lock_key = f"captcha:{lane_id}"
        acquired = False
        try:
            while not acquired:
                acquired = bool(
                    (
                        await connection.execute(
                            text(
                                "SELECT pg_try_advisory_lock("
                                "hashtextextended(:lock_key, 0))"
                            ),
                            {"lock_key": lock_key},
                        )
                    ).scalar_one()
                )
                if not acquired:
                    await asyncio.sleep(1)
            yield
        finally:
            if acquired:
                await connection.execute(
                    text(
                        "SELECT pg_advisory_unlock("
                        "hashtextextended(:lock_key, 0))"
                    ),
                    {"lock_key": lock_key},
                )
            await connection.close()

    async def captcha_spend_since(
        self,
        *,
        provider: str,
        since: datetime,
    ) -> Decimal:
        async with self.database.sessions() as session:
            value = (
                await session.execute(
                    select(func.coalesce(func.sum(CaptchaChallengeRow.cost), 0)).where(
                        CaptchaChallengeRow.provider == provider,
                        CaptchaChallengeRow.status == "success",
                        CaptchaChallengeRow.created_at >= since,
                    )
                )
            ).scalar_one()
        return Decimal(str(value))

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
    ) -> None:
        async with self.database.sessions.begin() as session:
            session.add(
                CaptchaChallengeRow(
                    session_lane_id=session_lane_id,
                    provider=provider,
                    challenge_type=challenge_type,
                    provider_task_id=provider_task_id,
                    status=status,
                    cost=cost,
                    latency_ms=latency_ms,
                    error_code=error_code[:256] if error_code else None,
                    completed_at=func.now(),
                )
            )
