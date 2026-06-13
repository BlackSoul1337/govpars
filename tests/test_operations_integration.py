import csv
import os
from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy import make_url, text

from procurement_parser.application.csv_validator import validate_csv
from procurement_parser.config.settings import DatabaseSettings
from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.infrastructure.persistence.postgres.exporter import (
    PostgresCsvExporter,
)
from procurement_parser.infrastructure.persistence.postgres.maintenance import (
    PostgresMaintenance,
)
from procurement_parser.infrastructure.persistence.postgres.runtime_state import (
    PostgresRuntimeStateRepository,
)


@pytest.mark.asyncio
@pytest.mark.skipif(
    not os.getenv("TEST_DATABASE_URL"),
    reason="TEST_DATABASE_URL is required for PostgreSQL integration tests",
)
async def test_runtime_state_maintenance_and_csv_export(
    tmp_path,
    monkeypatch,
) -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    if not (make_url(database_url).database or "").endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must point to a dedicated *_test database")
    database = Database(DatabaseSettings(url=database_url))
    runtime = PostgresRuntimeStateRepository(database)
    maintenance = PostgresMaintenance(database)
    lane_id = "integration-lane"
    source_entity_id = "operations-export-test"
    snapshot_entity_id = "operations-export-snapshot-test"
    try:
        await runtime.save_lane(
            lane_id=lane_id,
            source="zakup-sk",
            proxy_id="proxy-test",
            state="open",
            consecutive_blocks=3,
            cooldown_until=datetime.now(UTC) + timedelta(minutes=5),
            profile_payload={"generation": 1},
        )
        lane = await runtime.load_lane(lane_id)
        assert lane["state"] == "open"
        assert lane["profile_payload"] == {"generation": 1}

        async with runtime.captcha_lock(lane_id):
            await runtime.record_captcha(
                session_lane_id=lane_id,
                provider="2captcha",
                challenge_type="recaptcha_v2",
                provider_task_id="task-1",
                status="success",
                cost=Decimal("0.01"),
                latency_ms=1500,
                error_code=None,
            )
        assert await runtime.captcha_spend_since(
            provider="2captcha",
            since=datetime.now(UTC) - timedelta(hours=1),
        ) >= Decimal("0.01")

        assert await maintenance.claim_scheduler_job(
            source="eep-mitwork",
            job_name="integration-test",
            interval_seconds=60,
            lease_seconds=60,
        )
        assert not await maintenance.claim_scheduler_job(
            source="eep-mitwork",
            job_name="integration-test",
            interval_seconds=60,
            lease_seconds=60,
        )
        await maintenance.complete_scheduler_job(
            source="eep-mitwork",
            job_name="integration-test",
        )

        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO source_entities (
                        source, entity_type, source_entity_id, canonical_url,
                        summary_payload, last_success_at, current_content_hash
                    )
                    VALUES (
                        'eep-mitwork', 'lot', :source_entity_id,
                        'https://eep.mitwork.kz/ru/publics/lot/test',
                        '{}'::jsonb, now(), :content_hash
                    )
                    ON CONFLICT (source, entity_type, source_entity_id)
                    DO UPDATE SET last_success_at = now()
                    """
                ),
                {
                    "source_entity_id": source_entity_id,
                    "content_hash": "c" * 64,
                },
            )
            await connection.execute(
                text(
                    """
                    INSERT INTO lots (
                        source_entity_fk, title_ru, source_payload,
                        content_hash, fetched_at, application_start_at,
                        application_end_at
                    )
                    SELECT id, '=1+1', '{}'::jsonb, :content_hash, now(),
                           '2026-06-10 10:50:00+00'::timestamptz,
                           '2026-06-16 05:00:00+00'::timestamptz
                    FROM source_entities
                    WHERE source = 'eep-mitwork'
                      AND entity_type = 'lot'
                      AND source_entity_id = :source_entity_id
                    ON CONFLICT (source_entity_fk)
                    DO UPDATE SET title_ru = EXCLUDED.title_ru
                    """
                ),
                {
                    "source_entity_id": source_entity_id,
                    "content_hash": "c" * 64,
                },
            )

        destination = tmp_path / "lots.csv"
        count = await PostgresCsvExporter(database).export(
            "lots",
            destination,
            source="eep-mitwork",
        )
        validation = validate_csv(destination, dataset="lots")
        assert count >= 1
        assert validation["valid"] is True
        with destination.open("r", encoding="utf-8-sig", newline="") as handle:
            rows = list(csv.DictReader(handle))
        exported = next(
            row for row in rows if row["source_entity_id"] == source_entity_id
        )
        assert exported["title_ru"] == "'=1+1"
        assert exported["application_start_at"] == "2026-06-10 10:50:00+00"
        assert exported["application_start_at_local"] == "2026-06-10 15:50:00"
        assert exported["application_end_at"] == "2026-06-16 05:00:00+00"
        assert exported["application_end_at_local"] == "2026-06-16 10:00:00"
        assert exported["source_timezone"] == "Asia/Almaty"
        assert list(tmp_path.glob(".*.tmp")) == []

        exporter = PostgresCsvExporter(database)
        async with database.engine.connect() as connection:
            zakup_count_before = int(
                (
                    await connection.execute(
                        text(
                            """
                            SELECT count(*)
                            FROM export_lots
                            WHERE source = 'zakup-sk'
                            """
                        )
                    )
                ).scalar_one()
            )

        original_copy = exporter._copy_export
        copy_calls = 0

        async def copy_with_concurrent_insert(
            connection,
            view_name,
            destination,
            *,
            source,
        ):
            nonlocal copy_calls
            result = await original_copy(
                connection,
                view_name,
                destination,
                source=source,
            )
            copy_calls += 1
            if copy_calls == 1:
                async with database.engine.begin() as writer:
                    await writer.execute(
                        text(
                            """
                            INSERT INTO source_entities (
                                source, entity_type, source_entity_id,
                                canonical_url, summary_payload,
                                last_success_at, current_content_hash
                            )
                            VALUES (
                                'zakup-sk', 'lot', :source_entity_id,
                                'https://zakup.sk.kz/#/ext(snapshot)',
                                '{}'::jsonb, now(), :content_hash
                            )
                            """
                        ),
                        {
                            "source_entity_id": snapshot_entity_id,
                            "content_hash": "d" * 64,
                        },
                    )
                    await writer.execute(
                        text(
                            """
                            INSERT INTO lots (
                                source_entity_fk, title_ru, source_payload,
                                content_hash, fetched_at
                            )
                            SELECT id, 'snapshot row',
                                   '{"snapshot": true}'::jsonb,
                                   :content_hash, now()
                            FROM source_entities
                            WHERE source = 'zakup-sk'
                              AND entity_type = 'lot'
                              AND source_entity_id = :source_entity_id
                            """
                        ),
                        {
                            "source_entity_id": snapshot_entity_id,
                            "content_hash": "d" * 64,
                        },
                    )
            return result

        monkeypatch.setattr(
            exporter,
            "_copy_export",
            copy_with_concurrent_insert,
        )
        split_dir = tmp_path / "snapshot"
        split_counts = await exporter.export_split("lots", split_dir)
        assert split_counts["zakup-sk"] == zakup_count_before
        with (split_dir / "lots_zakup_sk.csv").open(
            "r",
            encoding="utf-8-sig",
            newline="",
        ) as handle:
            snapshot_rows = list(csv.DictReader(handle))
        assert all(
            row["source_entity_id"] != snapshot_entity_id
            for row in snapshot_rows
        )
        async with database.engine.connect() as connection:
            zakup_count_after = int(
                (
                    await connection.execute(
                        text(
                            """
                            SELECT count(*)
                            FROM export_lots
                            WHERE source = 'zakup-sk'
                            """
                        )
                    )
                ).scalar_one()
            )
        assert zakup_count_after == zakup_count_before + 1
        stats = await maintenance.collect_queue_stats()
        assert "queue_depth" in stats
    finally:
        async with database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    DELETE FROM source_entities
                    WHERE source_entity_id IN (
                        :source_entity_id,
                        :snapshot_entity_id
                    )
                    """
                ),
                {
                    "source_entity_id": source_entity_id,
                    "snapshot_entity_id": snapshot_entity_id,
                },
            )
            await connection.execute(
                text("DELETE FROM captcha_challenges WHERE session_lane_id = :lane_id"),
                {"lane_id": lane_id},
            )
            await connection.execute(
                text("DELETE FROM session_lanes WHERE id = :lane_id"),
                {"lane_id": lane_id},
            )
            await connection.execute(
                text("DELETE FROM scheduler_job_state WHERE job_name = 'integration-test'")
            )
        await database.close()
