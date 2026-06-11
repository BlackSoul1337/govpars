from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import orjson
from sqlalchemy import text

from procurement_parser.infrastructure.persistence.postgres.database import Database

REPLAY_COLUMNS = (
    "source",
    "source_entity_id",
    "business_number",
    "canonical_url",
    "title_ru",
    "title_kk",
    "status",
    "source_payload",
    "content_hash",
    "fetched_at",
)


class PostgresLotBulkReplayer:
    """High-throughput replay path; normal scraper ingestion uses batch UPSERT."""

    def __init__(self, database: Database, *, chunk_size: int = 10_000) -> None:
        self.database = database
        self.chunk_size = chunk_size

    async def replay(self, rows: AsyncIterator[dict[str, Any]]) -> int:
        async with self.database.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    CREATE TEMP TABLE staging_lots (
                        source text NOT NULL,
                        source_entity_id text NOT NULL,
                        business_number text,
                        canonical_url text NOT NULL,
                        title_ru text,
                        title_kk text,
                        status text,
                        source_payload jsonb NOT NULL,
                        content_hash text NOT NULL,
                        fetched_at timestamptz NOT NULL
                    ) ON COMMIT DROP
                    """
                )
            )
            raw = await connection.get_raw_connection()
            driver = raw.driver_connection
            total = 0
            chunk: list[tuple] = []
            async for row in rows:
                chunk.append(self._record(row))
                if len(chunk) >= self.chunk_size:
                    await driver.copy_records_to_table(
                        "staging_lots",
                        records=chunk,
                        columns=REPLAY_COLUMNS,
                    )
                    total += len(chunk)
                    chunk.clear()
            if chunk:
                await driver.copy_records_to_table(
                    "staging_lots",
                    records=chunk,
                    columns=REPLAY_COLUMNS,
                )
                total += len(chunk)

            await connection.execute(
                text(
                    """
                INSERT INTO source_entities (
                    source, entity_type, source_entity_id, business_number,
                    canonical_url, summary_payload, first_seen_at, last_seen_at,
                    last_success_at, current_content_hash
                )
                SELECT DISTINCT ON (source, source_entity_id)
                    source, 'lot', source_entity_id, business_number,
                    canonical_url, '{}'::jsonb, fetched_at, fetched_at,
                    fetched_at, content_hash
                FROM staging_lots
                ORDER BY source, source_entity_id, fetched_at DESC
                ON CONFLICT (source, entity_type, source_entity_id)
                DO UPDATE SET
                    business_number = EXCLUDED.business_number,
                    canonical_url = EXCLUDED.canonical_url,
                    last_seen_at = EXCLUDED.last_seen_at,
                    last_success_at = EXCLUDED.last_success_at,
                    current_content_hash = EXCLUDED.current_content_hash
                """
                )
            )
            await connection.execute(
                text(
                    """
                INSERT INTO lots (
                    source_entity_fk, title_ru, title_kk, status, source_payload,
                    content_hash, fetched_at, updated_at
                )
                SELECT se.id, staged.title_ru, staged.title_kk, staged.status,
                       staged.source_payload, staged.content_hash,
                       staged.fetched_at, now()
                FROM (
                    SELECT DISTINCT ON (source, source_entity_id)
                           source, source_entity_id, title_ru, title_kk, status,
                           source_payload, content_hash, fetched_at
                    FROM staging_lots
                    ORDER BY source, source_entity_id, fetched_at DESC
                ) staged
                JOIN source_entities se
                  ON se.source = staged.source
                 AND se.entity_type = 'lot'
                 AND se.source_entity_id = staged.source_entity_id
                ON CONFLICT (source_entity_fk)
                DO UPDATE SET
                    title_ru = EXCLUDED.title_ru,
                    title_kk = EXCLUDED.title_kk,
                    status = EXCLUDED.status,
                    source_payload = EXCLUDED.source_payload,
                    content_hash = EXCLUDED.content_hash,
                    fetched_at = EXCLUDED.fetched_at,
                    updated_at = now()
                WHERE lots.content_hash IS DISTINCT FROM EXCLUDED.content_hash
                """
                )
            )
            return total

    @staticmethod
    def _record(row: dict[str, Any]) -> tuple:
        fetched_at = row["fetched_at"]
        if isinstance(fetched_at, str):
            fetched_at = datetime.fromisoformat(fetched_at)
        payload = row.get("source_payload", {})
        if not isinstance(payload, str):
            payload = orjson.dumps(payload).decode()
        return (
            row["source"],
            row["source_entity_id"],
            row.get("business_number"),
            row["canonical_url"],
            row.get("title_ru"),
            row.get("title_kk"),
            row.get("status"),
            payload,
            row["content_hash"],
            fetched_at,
        )
