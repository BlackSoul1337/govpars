import hashlib
import os
from pathlib import Path

import orjson
import pytest
from sqlalchemy import make_url, text

from procurement_parser.config.settings import DatabaseSettings
from procurement_parser.domain.models import (
    DiscoveredEntity,
    EntityIdentity,
    EntityType,
    Source,
)
from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.infrastructure.persistence.postgres.repositories import (
    PostgresEntityRepository,
    PostgresFrontierRepository,
)
from procurement_parser.infrastructure.sources.eep_mitwork.parser import parse_detail

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is required for PostgreSQL integration tests",
)
async def test_postgres_queue_upsert_and_revision_cycle() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    database_name = make_url(database_url).database or ""
    if not database_name.endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must point to a dedicated *_test database")

    database = Database(DatabaseSettings(url=database_url))
    frontier = PostgresFrontierRepository(database)
    entities = PostgresEntityRepository(database, revision_mode="changes")
    identity = EntityIdentity(
        source=Source.EEP_MITWORK,
        entity_type=EntityType.LOT,
        source_entity_id="integration-651383",
        business_number="631179-ЗЦП5",
        canonical_url="https://eep.mitwork.kz/ru/publics/lot/integration-651383",
    )
    try:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    DELETE FROM source_entities
                    WHERE source = 'eep-mitwork'
                      AND source_entity_id = 'integration-651383'
                    """
                )
            )

        assert await frontier.enqueue([DiscoveredEntity(identity=identity)]) == 1
        tasks = await frontier.claim(
            "integration-worker",
            source=Source.EEP_MITWORK,
            limit=1,
            lease_seconds=60,
        )
        assert len(tasks) == 1
        await frontier.retry(
            tasks[0],
            error="integration transport failure",
            delay_seconds=0,
            strategy="integration-test",
            http_status=503,
        )
        tasks = await frontier.claim(
            "integration-worker",
            source=Source.EEP_MITWORK,
            limit=1,
            lease_seconds=60,
        )
        assert len(tasks) == 1

        html = (FIXTURES / "eep_lot.html").read_text(encoding="utf-8")
        batch = parse_detail(html, identity)
        await entities.persist(batch.entities, batch.relations)
        await entities.persist(batch.entities, batch.relations)

        changed = batch.entities[0].model_copy(deep=True)
        changed.entity.title_ru = "Измененное название"
        changed.content_hash = hashlib.sha256(
            orjson.dumps(
                changed.entity.model_dump(mode="json"),
                option=orjson.OPT_SORT_KEYS,
            )
        ).hexdigest()
        await entities.persist([changed], [])
        await frontier.complete(tasks[0], content_hash=changed.content_hash)

        async with database.sessions() as session:
            counts = (
                await session.execute(
                    text(
                        """
                        SELECT
                          (SELECT count(*) FROM crawl_frontier q
                           JOIN source_entities se ON se.id = q.source_entity_fk
                           WHERE se.source_entity_id = 'integration-651383') AS frontier,
                          (SELECT count(*) FROM crawl_task_history h
                           JOIN source_entities se ON se.id = h.source_entity_fk
                           WHERE se.source_entity_id = 'integration-651383') AS history,
                          (SELECT count(*) FROM lots l
                           JOIN source_entities se ON se.id = l.source_entity_fk
                           WHERE se.source_entity_id = 'integration-651383') AS lots,
                          (SELECT count(*) FROM entity_revisions r
                           JOIN source_entities se ON se.id = r.source_entity_fk
                           WHERE se.source_entity_id = 'integration-651383') AS revisions,
                          (SELECT count(*) FROM fetch_failures f
                           JOIN source_entities se ON se.id = f.source_entity_fk
                           WHERE se.source_entity_id = 'integration-651383') AS failures
                        """
                    )
                )
            ).mappings().one()

        assert counts == {
            "frontier": 0,
            "history": 1,
            "lots": 1,
            "revisions": 1,
            "failures": 1,
        }
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    DELETE FROM source_entities
                    WHERE source = 'eep-mitwork'
                      AND source_entity_id = 'integration-651383'
                    """
                )
            )
        await database.close()
