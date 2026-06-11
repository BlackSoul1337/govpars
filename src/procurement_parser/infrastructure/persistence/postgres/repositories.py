from __future__ import annotations

import hashlib
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import Any

import orjson
from pydantic_core import to_jsonable_python
from sqlalchemy import delete, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from procurement_parser.domain.models import (
    DiscoveredEntity,
    EntityEnvelope,
    EntityIdentity,
    EntityRelation,
    EntityType,
    FrontierActivity,
    FrontierTask,
    Organization,
    ProcurementEntity,
    Source,
)
from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.infrastructure.persistence.postgres.schema import (
    CrawlFrontierRow,
    CrawlTaskHistoryRow,
    DeliveryPlaceRow,
    DiscoveryCheckpointRow,
    DocumentMetadataRow,
    EntityRelationRow,
    EntityRevisionRow,
    FetchFailureRow,
    LotRow,
    OrganizationRow,
    PaymentTermsRow,
    PlanItemRow,
    ProcurementNoticeRow,
    SourceEntityRow,
)


def _jsonable(value: Any) -> Any:
    return to_jsonable_python(value)


def _identity_sort_key(identity: EntityIdentity) -> tuple[str, str, str]:
    return (
        identity.source.value,
        identity.entity_type.value,
        identity.source_entity_id,
    )


class IdentityRepositoryMixin:
    async def _ensure_source_entity(
        self,
        session: AsyncSession,
        identity: EntityIdentity,
        *,
        summary_payload: dict[str, Any] | None = None,
    ) -> int:
        stmt = (
            insert(SourceEntityRow)
            .values(
                source=identity.source.value,
                entity_type=identity.entity_type.value,
                source_entity_id=identity.source_entity_id,
                business_number=identity.business_number,
                canonical_url=identity.canonical_url,
                summary_payload=summary_payload or {},
                last_seen_at=datetime.now(UTC),
            )
            .on_conflict_do_update(
                constraint="uq_source_entity_identity",
                set_={
                    "business_number": identity.business_number,
                    "canonical_url": identity.canonical_url,
                    "last_seen_at": datetime.now(UTC),
                    "summary_payload": (
                        summary_payload
                        if summary_payload is not None
                        else SourceEntityRow.summary_payload
                    ),
                },
            )
            .returning(SourceEntityRow.id)
        )
        return int((await session.execute(stmt)).scalar_one())


class PostgresFrontierRepository(IdentityRepositoryMixin):
    def __init__(self, database: Database) -> None:
        self.database = database

    async def enqueue(self, items: Sequence[DiscoveredEntity]) -> int:
        if not items:
            return 0
        inserted = 0
        async with self.database.sessions.begin() as session:
            for item in sorted(items, key=lambda value: _identity_sort_key(value.identity)):
                source_entity_fk = await self._ensure_source_entity(
                    session,
                    item.identity,
                    summary_payload=item.summary_payload,
                )
                if not item.refresh_existing:
                    already_persisted = (
                        await session.execute(
                            select(SourceEntityRow.last_success_at).where(
                                SourceEntityRow.id == source_entity_fk
                            )
                        )
                    ).scalar_one_or_none()
                    if already_persisted is not None:
                        continue
                stmt = (
                    insert(CrawlFrontierRow)
                    .values(
                        source_entity_fk=source_entity_fk,
                        task_type=item.task_type,
                        priority=item.priority,
                        available_at=item.available_at,
                        payload=item.summary_payload,
                    )
                    .on_conflict_do_update(
                        constraint="uq_frontier_entity_task",
                        set_={
                            "priority": text(
                                "GREATEST(crawl_frontier.priority, EXCLUDED.priority)"
                            ),
                            "available_at": text(
                                "LEAST(crawl_frontier.available_at, EXCLUDED.available_at)"
                            ),
                            "payload": text(
                                "crawl_frontier.payload || EXCLUDED.payload"
                            ),
                        },
                    )
                )
                await session.execute(stmt)
                inserted += 1
        return inserted

    async def claim(
        self,
        worker_id: str,
        *,
        source: Source | None,
        limit: int,
        lease_seconds: int,
        backfill_only: bool = False,
    ) -> list[FrontierTask]:
        source_filter = "AND se.source = :source" if source else ""
        backfill_filter = "AND q.priority <= 0" if backfill_only else ""
        sql = text(
            f"""
            WITH candidates AS (
                SELECT q.id
                FROM crawl_frontier q
                JOIN source_entities se ON se.id = q.source_entity_fk
                WHERE q.available_at <= now()
                  AND (q.leased_until IS NULL OR q.leased_until < now())
                  {source_filter}
                  {backfill_filter}
                ORDER BY q.priority DESC, q.available_at ASC, q.id ASC
                FOR UPDATE OF q SKIP LOCKED
                LIMIT :limit
            )
            UPDATE crawl_frontier q
            SET lease_owner = :worker_id,
                leased_until = now() + make_interval(secs => :lease_seconds),
                attempt = q.attempt + 1
            FROM candidates c, source_entities se
            WHERE q.id = c.id
              AND se.id = q.source_entity_fk
            RETURNING q.id, q.task_type, q.priority, q.attempt, q.payload,
                      se.source, se.entity_type, se.source_entity_id,
                      se.business_number, se.canonical_url
            """
        )
        params: dict[str, Any] = {
            "worker_id": worker_id,
            "lease_seconds": lease_seconds,
            "limit": limit,
        }
        if source:
            params["source"] = source.value
        async with self.database.sessions.begin() as session:
            rows = (await session.execute(sql, params)).mappings().all()
        return [
            FrontierTask(
                id=row["id"],
                identity=EntityIdentity(
                    source=Source(row["source"]),
                    entity_type=EntityType(row["entity_type"]),
                    source_entity_id=row["source_entity_id"],
                    business_number=row["business_number"],
                    canonical_url=row["canonical_url"],
                ),
                task_type=row["task_type"],
                priority=row["priority"],
                attempt=row["attempt"],
                payload=row["payload"] or {},
            )
            for row in rows
        ]

    async def complete(self, task: FrontierTask, *, content_hash: str | None) -> None:
        async with self.database.sessions.begin() as session:
            frontier = (
                await session.execute(
                    select(CrawlFrontierRow).where(CrawlFrontierRow.id == task.id)
                )
            ).scalar_one_or_none()
            if frontier is None:
                return
            session.add(
                CrawlTaskHistoryRow(
                    source_entity_fk=frontier.source_entity_fk,
                    task_type=frontier.task_type,
                    attempt=frontier.attempt,
                    outcome="success",
                    content_hash=content_hash,
                )
            )
            await session.delete(frontier)

    async def release(
        self,
        tasks: Sequence[FrontierTask],
        *,
        worker_id: str,
    ) -> int:
        task_ids = [task.id for task in tasks]
        if not task_ids:
            return 0
        async with self.database.sessions.begin() as session:
            result = await session.execute(
                CrawlFrontierRow.__table__.update()
                .where(
                    CrawlFrontierRow.id.in_(task_ids),
                    CrawlFrontierRow.lease_owner == worker_id,
                )
                .values(
                    lease_owner=None,
                    leased_until=None,
                )
            )
        return int(result.rowcount or 0)

    async def release_by_owner(self, worker_id: str) -> int:
        async with self.database.sessions.begin() as session:
            result = await session.execute(
                CrawlFrontierRow.__table__.update()
                .where(CrawlFrontierRow.lease_owner == worker_id)
                .values(
                    lease_owner=None,
                    leased_until=None,
                )
            )
        return int(result.rowcount or 0)

    async def extend_lease(
        self,
        task: FrontierTask,
        *,
        worker_id: str,
        lease_seconds: int,
    ) -> bool:
        async with self.database.sessions.begin() as session:
            result = await session.execute(
                CrawlFrontierRow.__table__.update()
                .where(
                    CrawlFrontierRow.id == task.id,
                    CrawlFrontierRow.lease_owner == worker_id,
                )
                .values(
                    leased_until=text(
                        "now() + make_interval(secs => "
                        f"{max(1, int(lease_seconds))})"
                    )
                )
            )
        return bool(result.rowcount)

    async def activity(self, source: Source) -> FrontierActivity:
        async with self.database.sessions() as session:
            row = (
                await session.execute(
                    text(
                        """
                        SELECT count(*) AS depth,
                               count(*) FILTER (
                                   WHERE q.available_at <= now()
                                     AND (
                                         q.leased_until IS NULL
                                         OR q.leased_until < now()
                                     )
                               ) AS ready,
                               count(*) FILTER (
                                   WHERE q.available_at > now()
                               ) AS delayed,
                               count(*) FILTER (
                                   WHERE q.leased_until >= now()
                               ) AS leased
                        FROM crawl_frontier q
                        JOIN source_entities se ON se.id = q.source_entity_fk
                        WHERE se.source = :source
                        """
                    ),
                    {"source": source.value},
                )
            ).mappings().one()
        return FrontierActivity.model_validate(row)

    async def enqueue_refresh(
        self,
        source: Source,
        *,
        policy: str,
        priority: int,
        limit: int = 10_000,
        older_than_seconds: int = 0,
        newer_than_seconds: int | None = None,
    ) -> int:
        status_expression = """
            COALESCE(l.status, n.status, p.status, '')
        """
        closed_pattern = (
            "closed|cancel|completed|finished|заверш|закры|отмен|"
            "не состоя|итог|аяқтал|жабыл|болма"
        )
        policies = {
            "active": f"""
                NOT ({status_expression} ~* :closed_pattern)
                AND se.last_success_at < now()
                    - make_interval(secs => :older_than_seconds)
            """,
            "recently_closed": f"""
                {status_expression} ~* :closed_pattern
                AND se.last_success_at >= now()
                    - make_interval(secs => :newer_than_seconds)
                AND se.last_success_at < now()
                    - make_interval(secs => :older_than_seconds)
            """,
            "old": """
                se.last_success_at < now()
                    - make_interval(secs => :older_than_seconds)
            """,
            "all": "TRUE",
        }
        predicate = policies.get(policy)
        if predicate is None:
            raise ValueError(f"Unsupported refresh policy: {policy}")
        sql = text(
            f"""
            WITH candidates AS (
                SELECT se.id
                FROM source_entities se
                LEFT JOIN lots l ON l.source_entity_fk = se.id
                LEFT JOIN procurement_notices n ON n.source_entity_fk = se.id
                LEFT JOIN plan_items p ON p.source_entity_fk = se.id
                WHERE se.source = :source
                  AND se.entity_type IN ('lot', 'notice', 'plan_item')
                  AND se.last_success_at IS NOT NULL
                  AND ({predicate})
                ORDER BY se.last_success_at ASC, se.id ASC
                LIMIT :limit
            )
            INSERT INTO crawl_frontier (
                source_entity_fk, task_type, priority, available_at,
                attempt, payload, created_at
            )
            SELECT id, 'detail', :priority, now(), 0, '{{}}'::jsonb, now()
            FROM candidates
            ON CONFLICT (source_entity_fk, task_type)
            DO UPDATE SET
                priority = GREATEST(crawl_frontier.priority, EXCLUDED.priority),
                available_at = LEAST(crawl_frontier.available_at, now())
            """
        )
        async with self.database.sessions.begin() as session:
            result = await session.execute(
                sql,
                {
                    "source": source.value,
                    "closed_pattern": closed_pattern,
                    "priority": priority,
                    "limit": max(1, limit),
                    "older_than_seconds": max(0, older_than_seconds),
                    "newer_than_seconds": max(
                        older_than_seconds,
                        newer_than_seconds or older_than_seconds,
                    ),
                },
            )
        return int(result.rowcount or 0)

    async def retry(
        self,
        task: FrontierTask,
        *,
        error: str,
        delay_seconds: int,
        strategy: str = "unknown",
        http_status: int | None = None,
    ) -> None:
        async with self.database.sessions.begin() as session:
            source_entity_fk = (
                await session.execute(
                    CrawlFrontierRow.__table__.update()
                    .where(CrawlFrontierRow.id == task.id)
                    .values(
                        lease_owner=None,
                        leased_until=None,
                        available_at=datetime.now(UTC)
                        + timedelta(seconds=delay_seconds),
                        last_error=error[:4000],
                    )
                    .returning(CrawlFrontierRow.source_entity_fk)
                )
            ).scalar_one_or_none()
            if source_entity_fk is not None:
                session.add(
                    FetchFailureRow(
                        source_entity_fk=source_entity_fk,
                        strategy=strategy,
                        http_status=http_status,
                        error=error[:4000],
                    )
                )

    async def fail(
        self,
        task: FrontierTask,
        *,
        error: str,
        strategy: str = "unknown",
        http_status: int | None = None,
    ) -> None:
        async with self.database.sessions.begin() as session:
            frontier = (
                await session.execute(
                    select(CrawlFrontierRow).where(CrawlFrontierRow.id == task.id)
                )
            ).scalar_one_or_none()
            if frontier is None:
                return
            session.add(
                CrawlTaskHistoryRow(
                    source_entity_fk=frontier.source_entity_fk,
                    task_type=frontier.task_type,
                    attempt=frontier.attempt,
                    outcome="failed",
                    error=error[:4000],
                )
            )
            session.add(
                FetchFailureRow(
                    source_entity_fk=frontier.source_entity_fk,
                    strategy=strategy,
                    http_status=http_status,
                    error=error[:4000],
                )
            )
            await session.delete(frontier)

    async def get_checkpoint(
        self,
        source: Source,
        entity_type: EntityType,
        *,
        scope: str = "all",
    ) -> tuple[int, bool]:
        async with self.database.sessions() as session:
            row = (
                await session.execute(
                    select(DiscoveryCheckpointRow).where(
                        DiscoveryCheckpointRow.source == source.value,
                        DiscoveryCheckpointRow.entity_type == entity_type.value,
                        DiscoveryCheckpointRow.scope == scope,
                    )
                )
            ).scalar_one_or_none()
        return (row.next_page, row.completed) if row else (1, False)

    async def set_checkpoint(
        self,
        source: Source,
        entity_type: EntityType,
        *,
        next_page: int,
        completed: bool,
        scope: str = "all",
    ) -> None:
        stmt = insert(DiscoveryCheckpointRow).values(
            source=source.value,
            entity_type=entity_type.value,
            scope=scope,
            next_page=next_page,
            completed=completed,
        )
        async with self.database.sessions.begin() as session:
            await session.execute(
                stmt.on_conflict_do_update(
                    constraint="uq_discovery_checkpoint",
                    set_={
                        "next_page": next_page,
                        "completed": completed,
                        "updated_at": datetime.now(UTC),
                    },
                )
            )


class PostgresEntityRepository(IdentityRepositoryMixin):
    def __init__(self, database: Database, *, revision_mode: str = "changes") -> None:
        self.database = database
        self.revision_mode = revision_mode

    async def persist(
        self,
        entities: Sequence[EntityEnvelope],
        relations: Sequence[EntityRelation],
    ) -> None:
        async with self.database.sessions.begin() as session:
            identities = {
                identity.stable_key: identity
                for identity in (
                    [envelope.entity.identity for envelope in entities]
                    + [relation.parent for relation in relations]
                    + [relation.child for relation in relations]
                )
            }
            for identity in sorted(identities.values(), key=_identity_sort_key):
                await self._ensure_source_entity(session, identity)

            for envelope in sorted(
                entities,
                key=lambda value: _identity_sort_key(value.entity.identity),
            ):
                await self._persist_envelope(session, envelope)
            await self._reconcile_relations(session, entities, relations)
            for relation in sorted(
                relations,
                key=lambda value: (
                    _identity_sort_key(value.parent),
                    _identity_sort_key(value.child),
                    value.relation_type.value,
                ),
            ):
                await self._persist_relation(session, relation)

    async def _reconcile_relations(
        self,
        session: AsyncSession,
        entities: Sequence[EntityEnvelope],
        relations: Sequence[EntityRelation],
    ) -> None:
        desired_by_parent: dict[str, set[tuple[str, int]]] = {}
        for relation in relations:
            child_fk = await self._ensure_source_entity(session, relation.child)
            desired_by_parent.setdefault(relation.parent.stable_key, set()).add(
                (relation.relation_type.value, child_fk)
            )

        for envelope in entities:
            parent = envelope.entity.identity
            parent_fk = await self._ensure_source_entity(session, parent)
            desired = desired_by_parent.get(parent.stable_key, set())
            existing = (
                await session.execute(
                    select(
                        EntityRelationRow.id,
                        EntityRelationRow.relation_type,
                        EntityRelationRow.child_source_entity_fk,
                    ).where(EntityRelationRow.parent_source_entity_fk == parent_fk)
                )
            ).all()
            stale_ids = [
                row.id
                for row in existing
                if (row.relation_type, row.child_source_entity_fk) not in desired
            ]
            if stale_ids:
                await session.execute(
                    delete(EntityRelationRow).where(EntityRelationRow.id.in_(stale_ids))
                )

    async def _persist_envelope(
        self,
        session: AsyncSession,
        envelope: EntityEnvelope,
    ) -> None:
        entity = envelope.entity
        identity = entity.identity
        source_entity_fk = await self._ensure_source_entity(session, identity)
        previous_hash = (
            await session.execute(
                select(SourceEntityRow.current_content_hash).where(
                    SourceEntityRow.id == source_entity_fk
                )
            )
        ).scalar_one_or_none()

        if previous_hash == envelope.content_hash:
            await session.execute(
                SourceEntityRow.__table__.update()
                .where(SourceEntityRow.id == source_entity_fk)
                .values(last_success_at=envelope.fetched_at)
            )
            return

        table = self._table_for(identity.entity_type)
        previous = (
            await session.execute(
                select(table).where(table.c.source_entity_fk == source_entity_fk)
            )
        ).mappings().first()
        if previous and self.revision_mode == "changes":
            previous_payload = {
                key: _jsonable(value)
                for key, value in previous.items()
                if key not in {"id", "source_entity_fk"}
            }
            session.add(
                EntityRevisionRow(
                    source_entity_fk=source_entity_fk,
                    content_hash=previous_hash or previous["content_hash"],
                    payload=previous_payload,
                )
            )

        if isinstance(entity, Organization):
            values = {
                "source_entity_fk": source_entity_fk,
                "name_ru": entity.name_ru,
                "name_kk": entity.name_kk,
                "bin": entity.bin,
                "address": entity.address,
                "phone": entity.phone,
                "email": entity.email,
                "source_payload": _jsonable(entity.source_payload),
                "content_hash": envelope.content_hash,
                "fetched_at": envelope.fetched_at,
            }
        else:
            values = self._procurement_values(entity, source_entity_fk, envelope)

        stmt = insert(table).values(**values)
        update_values = {
            key: getattr(stmt.excluded, key)
            for key in values
            if key != "source_entity_fk"
        }
        await session.execute(
            stmt.on_conflict_do_update(
                index_elements=[table.c.source_entity_fk],
                set_=update_values,
                where=table.c.content_hash.is_distinct_from(stmt.excluded.content_hash),
            )
        )

        await session.execute(
            SourceEntityRow.__table__.update()
            .where(SourceEntityRow.id == source_entity_fk)
            .values(
                business_number=identity.business_number,
                canonical_url=identity.canonical_url,
                current_content_hash=envelope.content_hash,
                last_success_at=envelope.fetched_at,
                last_seen_at=envelope.fetched_at,
            )
        )

        if isinstance(entity, ProcurementEntity):
            await self._replace_children(session, source_entity_fk, entity)

    @staticmethod
    def _table_for(entity_type: EntityType):
        return {
            EntityType.LOT: LotRow.__table__,
            EntityType.NOTICE: ProcurementNoticeRow.__table__,
            EntityType.PLAN_ITEM: PlanItemRow.__table__,
            EntityType.ORGANIZATION: OrganizationRow.__table__,
        }[entity_type]

    @staticmethod
    def _procurement_values(
        entity: ProcurementEntity,
        source_entity_fk: int,
        envelope: EntityEnvelope,
    ) -> dict[str, Any]:
        return {
            "source_entity_fk": source_entity_fk,
            "title_ru": entity.title_ru,
            "title_kk": entity.title_kk,
            "description_ru": entity.description_ru,
            "description_kk": entity.description_kk,
            "additional_characteristics_ru": entity.additional_characteristics_ru,
            "additional_characteristics_kk": entity.additional_characteristics_kk,
            "status": entity.status,
            "procurement_method": entity.procurement_method,
            "tru_code": entity.tru_code,
            "oktru_code": entity.oktru_code,
            "oktru_category_ru": entity.oktru_category_ru,
            "oktru_category_kk": entity.oktru_category_kk,
            "plan_row_number": entity.plan_row_number,
            "priority": entity.priority,
            "procurement_year": entity.procurement_year,
            "procurement_month": entity.procurement_month,
            "plan_item_type": entity.plan_item_type,
            "quantity": entity.quantity,
            "unit": entity.unit,
            "unit_price": entity.unit_price,
            "total_amount": entity.total_amount,
            "currency": entity.currency,
            "published_at": entity.published_at,
            "application_start_at": entity.application_start_at,
            "application_end_at": entity.application_end_at,
            "delivery_terms_ru": entity.delivery_terms_ru,
            "delivery_terms_kk": entity.delivery_terms_kk,
            "delivery_conditions_ru": entity.delivery_conditions_ru,
            "delivery_conditions_kk": entity.delivery_conditions_kk,
            "venue_ru": entity.venue_ru,
            "venue_kk": entity.venue_kk,
            "contact_email": entity.contact_email,
            "contact_phone": entity.contact_phone,
            "contact_extension": entity.contact_extension,
            "source_payload": _jsonable(entity.source_payload),
            "content_hash": envelope.content_hash,
            "fetched_at": envelope.fetched_at,
        }

    async def _replace_children(
        self,
        session: AsyncSession,
        owner_fk: int,
        entity: ProcurementEntity,
    ) -> None:
        await session.execute(
            delete(DeliveryPlaceRow).where(
                DeliveryPlaceRow.owner_source_entity_fk == owner_fk
            )
        )
        await session.execute(
            delete(DocumentMetadataRow).where(
                DocumentMetadataRow.owner_source_entity_fk == owner_fk
            )
        )
        await session.execute(
            delete(PaymentTermsRow).where(PaymentTermsRow.owner_source_entity_fk == owner_fk)
        )

        for place in entity.delivery_places:
            session.add(
                DeliveryPlaceRow(
                    owner_source_entity_fk=owner_fk,
                    source_row_id=place.source_row_id,
                    country=place.country,
                    address=place.address,
                    quantity=place.quantity,
                    incoterms=place.incoterms,
                    source_payload=_jsonable(place.source_payload),
                )
            )

        for document in entity.documents:
            identity_key = (
                document.source_document_id
                or document.document_hash
                or document.url
                or document.filename
                or hashlib.sha256(
                    orjson.dumps(document.model_dump(mode="json"))
                ).hexdigest()
            )
            session.add(
                DocumentMetadataRow(
                    owner_source_entity_fk=owner_fk,
                    identity_key=identity_key[:512],
                    source_document_id=document.source_document_id,
                    category=document.category,
                    filename=document.filename,
                    extension=document.extension,
                    url=document.url,
                    size_bytes=document.size_bytes,
                    uploaded_at=document.uploaded_at,
                    document_hash=document.document_hash,
                    declared_content_type=document.declared_content_type,
                    inferred_content_type=document.inferred_content_type,
                    response_content_type=document.response_content_type,
                    source_payload=_jsonable(document.source_payload),
                )
            )

        if entity.payment_terms:
            session.add(
                PaymentTermsRow(
                    owner_source_entity_fk=owner_fk,
                    **entity.payment_terms.model_dump(),
                )
            )

    async def _persist_relation(
        self,
        session: AsyncSession,
        relation: EntityRelation,
    ) -> None:
        parent_fk = await self._ensure_source_entity(session, relation.parent)
        child_fk = await self._ensure_source_entity(session, relation.child)
        stmt = insert(EntityRelationRow).values(
            relation_type=relation.relation_type.value,
            parent_source_entity_fk=parent_fk,
            child_source_entity_fk=child_fk,
            source_payload=_jsonable(relation.source_payload),
            last_seen_at=datetime.now(UTC),
        )
        await session.execute(
            stmt.on_conflict_do_update(
                constraint="uq_entity_relation",
                set_={
                    "source_payload": stmt.excluded.source_payload,
                    "last_seen_at": datetime.now(UTC),
                },
            )
        )
