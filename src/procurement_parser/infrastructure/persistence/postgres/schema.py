from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class SourceEntityRow(Base):
    __tablename__ = "source_entities"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "entity_type",
            "source_entity_id",
            name="uq_source_entity_identity",
        ),
        Index("ix_source_entities_last_seen", "source", "entity_type", "last_seen_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    source_entity_id: Mapped[str] = mapped_column(String(128), nullable=False)
    business_number: Mapped[str | None] = mapped_column(String(256))
    canonical_url: Mapped[str] = mapped_column(Text, nullable=False)
    summary_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    current_content_hash: Mapped[str | None] = mapped_column(String(64))


class CrawlFrontierRow(Base):
    __tablename__ = "crawl_frontier"
    __table_args__ = (
        UniqueConstraint("source_entity_fk", "task_type", name="uq_frontier_entity_task"),
        Index(
            "ix_frontier_claim",
            "priority",
            "available_at",
            "id",
            postgresql_include=["leased_until", "source_entity_fk"],
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), nullable=False
    )
    task_type: Mapped[str] = mapped_column(String(32), nullable=False, default="detail")
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    lease_owner: Mapped[str | None] = mapped_column(String(128))
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class CrawlTaskHistoryRow(Base):
    __tablename__ = "crawl_task_history"
    __table_args__ = (
        Index("ix_task_history_completed", "completed_at"),
        Index("ix_task_history_entity", "source_entity_fk"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), nullable=False
    )
    task_type: Mapped[str] = mapped_column(String(32), nullable=False)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False)
    outcome: Mapped[str] = mapped_column(String(32), nullable=False, default="success")
    error: Mapped[str | None] = mapped_column(Text)
    content_hash: Mapped[str | None] = mapped_column(String(64))
    completed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class DiscoveryCheckpointRow(Base):
    __tablename__ = "discovery_checkpoints"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "entity_type",
            "scope",
            name="uq_discovery_checkpoint",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    entity_type: Mapped[str] = mapped_column(String(32), nullable=False)
    scope: Mapped[str] = mapped_column(String(128), nullable=False, default="all")
    next_page: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    completed: Mapped[bool] = mapped_column(default=False, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class SchedulerJobStateRow(Base):
    __tablename__ = "scheduler_job_state"
    __table_args__ = (
        UniqueConstraint(
            "source",
            "job_name",
            name="uq_scheduler_job_state",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    job_name: Mapped[str] = mapped_column(String(64), nullable=False)
    last_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    leased_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class ProcurementColumns:
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    title_ru: Mapped[str | None] = mapped_column(Text)
    title_kk: Mapped[str | None] = mapped_column(Text)
    description_ru: Mapped[str | None] = mapped_column(Text)
    description_kk: Mapped[str | None] = mapped_column(Text)
    additional_characteristics_ru: Mapped[str | None] = mapped_column(Text)
    additional_characteristics_kk: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str | None] = mapped_column(String(256))
    procurement_method: Mapped[str | None] = mapped_column(Text)
    tru_code: Mapped[str | None] = mapped_column(String(128))
    oktru_code: Mapped[str | None] = mapped_column(String(128))
    oktru_category_ru: Mapped[str | None] = mapped_column(Text)
    oktru_category_kk: Mapped[str | None] = mapped_column(Text)
    plan_row_number: Mapped[str | None] = mapped_column(String(256))
    priority: Mapped[str | None] = mapped_column(Text)
    procurement_year: Mapped[int | None] = mapped_column(Integer)
    procurement_month: Mapped[str | None] = mapped_column(String(64))
    plan_item_type: Mapped[str | None] = mapped_column(Text)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    unit: Mapped[str | None] = mapped_column(String(128))
    unit_price: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    total_amount: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    currency: Mapped[str | None] = mapped_column(String(16))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    application_start_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    application_end_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    delivery_terms_ru: Mapped[str | None] = mapped_column(Text)
    delivery_terms_kk: Mapped[str | None] = mapped_column(Text)
    delivery_conditions_ru: Mapped[str | None] = mapped_column(Text)
    delivery_conditions_kk: Mapped[str | None] = mapped_column(Text)
    venue_ru: Mapped[str | None] = mapped_column(Text)
    venue_kk: Mapped[str | None] = mapped_column(Text)
    contact_email: Mapped[str | None] = mapped_column(String(320))
    contact_phone: Mapped[str | None] = mapped_column(Text)
    contact_extension: Mapped[str | None] = mapped_column(Text)
    source_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class LotRow(ProcurementColumns, Base):
    __tablename__ = "lots"


class ProcurementNoticeRow(ProcurementColumns, Base):
    __tablename__ = "procurement_notices"


class PlanItemRow(ProcurementColumns, Base):
    __tablename__ = "plan_items"


class OrganizationRow(Base):
    __tablename__ = "organizations"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    name_ru: Mapped[str | None] = mapped_column(Text)
    name_kk: Mapped[str | None] = mapped_column(Text)
    bin: Mapped[str | None] = mapped_column(String(32), index=True)
    address: Mapped[str | None] = mapped_column(Text)
    phone: Mapped[str | None] = mapped_column(Text)
    email: Mapped[str | None] = mapped_column(String(320))
    source_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    fetched_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class DeliveryPlaceRow(Base):
    __tablename__ = "delivery_places"
    __table_args__ = (
        UniqueConstraint(
            "owner_source_entity_fk",
            "source_row_id",
            name="uq_delivery_owner_row",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    owner_source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), nullable=False
    )
    source_row_id: Mapped[str | None] = mapped_column(String(128))
    country: Mapped[str | None] = mapped_column(String(128))
    address: Mapped[str | None] = mapped_column(Text)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(24, 6))
    incoterms: Mapped[str | None] = mapped_column(Text)
    source_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)


class PaymentTermsRow(Base):
    __tablename__ = "payment_terms"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    owner_source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), unique=True, nullable=False
    )
    prepayment_percent: Mapped[Decimal | None] = mapped_column(Numeric(8, 3))
    interim_percent: Mapped[Decimal | None] = mapped_column(Numeric(8, 3))
    final_percent: Mapped[Decimal | None] = mapped_column(Numeric(8, 3))
    raw_text: Mapped[str | None] = mapped_column(Text)


class DocumentMetadataRow(Base):
    __tablename__ = "document_metadata"
    __table_args__ = (
        UniqueConstraint(
            "owner_source_entity_fk",
            "identity_key",
            name="uq_document_owner_identity",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    owner_source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), nullable=False
    )
    identity_key: Mapped[str] = mapped_column(String(512), nullable=False)
    source_document_id: Mapped[str | None] = mapped_column(String(128))
    category: Mapped[str | None] = mapped_column(Text)
    filename: Mapped[str | None] = mapped_column(Text)
    extension: Mapped[str | None] = mapped_column(String(32))
    url: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    uploaded_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    document_hash: Mapped[str | None] = mapped_column(String(256))
    declared_content_type: Mapped[str | None] = mapped_column(String(256))
    inferred_content_type: Mapped[str | None] = mapped_column(String(256))
    response_content_type: Mapped[str | None] = mapped_column(String(256))
    source_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)


class EntityRelationRow(Base):
    __tablename__ = "entity_relations"
    __table_args__ = (
        UniqueConstraint(
            "relation_type",
            "parent_source_entity_fk",
            "child_source_entity_fk",
            name="uq_entity_relation",
        ),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    relation_type: Mapped[str] = mapped_column(String(64), nullable=False)
    parent_source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), nullable=False
    )
    child_source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), nullable=False
    )
    source_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class EntityRevisionRow(Base):
    __tablename__ = "entity_revisions"
    __table_args__ = (
        Index("ix_entity_revisions_entity", "source_entity_fk", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_entity_fk: Mapped[int] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE"), nullable=False
    )
    content_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class SessionLaneRow(Base):
    __tablename__ = "session_lanes"

    id: Mapped[str] = mapped_column(String(128), primary_key=True)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    proxy_id: Mapped[str | None] = mapped_column(String(128))
    state: Mapped[str] = mapped_column(String(32), nullable=False, default="closed")
    consecutive_blocks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    cooldown_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    profile_payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )


class CaptchaChallengeRow(Base):
    __tablename__ = "captcha_challenges"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    session_lane_id: Mapped[str | None] = mapped_column(
        ForeignKey("session_lanes.id", ondelete="SET NULL")
    )
    provider: Mapped[str] = mapped_column(String(64), nullable=False)
    challenge_type: Mapped[str] = mapped_column(String(64), nullable=False)
    provider_task_id: Mapped[str | None] = mapped_column(String(128))
    status: Mapped[str] = mapped_column(String(32), nullable=False)
    cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 6))
    latency_ms: Mapped[int | None] = mapped_column(Integer)
    error_code: Mapped[str | None] = mapped_column(String(256))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    completed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class FetchFailureRow(Base):
    __tablename__ = "fetch_failures"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    source_entity_fk: Mapped[int | None] = mapped_column(
        ForeignKey("source_entities.id", ondelete="CASCADE")
    )
    strategy: Mapped[str] = mapped_column(String(64), nullable=False)
    http_status: Mapped[int | None] = mapped_column(Integer)
    error: Mapped[str] = mapped_column(Text, nullable=False)
    response_excerpt: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
