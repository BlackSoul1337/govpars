import os
from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import make_url, text

from procurement_parser.config.settings import DatabaseSettings
from procurement_parser.infrastructure.persistence.postgres.bulk_replay import (
    PostgresLotBulkReplayer,
)
from procurement_parser.infrastructure.persistence.postgres.database import Database


async def _rows(values):
    for value in values:
        yield value


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is required for PostgreSQL integration tests",
)
async def test_bulk_replay_uses_one_transaction_and_deduplicates() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    if not (make_url(database_url).database or "").endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must point to a dedicated *_test database")
    database = Database(
        DatabaseSettings(
            url=database_url,
            pool_size=3,
            max_overflow=2,
        )
    )
    replayer = PostgresLotBulkReplayer(database, chunk_size=1)
    now = datetime.now(UTC)
    base = {
        "source": "eep-mitwork",
        "source_entity_id": "bulk-replay-test",
        "canonical_url": "https://eep.mitwork.kz/ru/publics/lot/bulk-replay-test",
        "title_ru": "Первая версия",
        "title_kk": None,
        "status": "published",
        "source_payload": {"version": 1},
        "content_hash": "a" * 64,
        "fetched_at": now,
    }
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    DELETE FROM source_entities
                    WHERE source = 'eep-mitwork'
                      AND source_entity_id = 'bulk-replay-test'
                    """
                )
            )

        newer = {
            **base,
            "title_ru": "Вторая версия",
            "source_payload": {"version": 2},
            "content_hash": "b" * 64,
            "fetched_at": now + timedelta(seconds=1),
        }
        assert await replayer.replay(_rows([base, newer])) == 2
        assert await replayer.replay(_rows([newer])) == 1

        async with database.sessions() as session:
            result = (
                await session.execute(
                    text(
                        """
                        SELECT count(*) AS count, max(l.title_ru) AS title
                        FROM lots l
                        JOIN source_entities se ON se.id = l.source_entity_fk
                        WHERE se.source = 'eep-mitwork'
                          AND se.source_entity_id = 'bulk-replay-test'
                        """
                    )
                )
            ).mappings().one()
            temp_table = (
                await session.execute(
                    text("SELECT to_regclass('pg_temp.staging_lots')")
                )
            ).scalar_one()

        assert result == {"count": 1, "title": "Вторая версия"}
        assert temp_table is None
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    DELETE FROM source_entities
                    WHERE source = 'eep-mitwork'
                      AND source_entity_id = 'bulk-replay-test'
                    """
                )
            )
        await database.close()
