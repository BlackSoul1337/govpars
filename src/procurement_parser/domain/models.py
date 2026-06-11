from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from pathlib import PurePosixPath
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, computed_field


class Source(StrEnum):
    EEP_MITWORK = "eep-mitwork"
    ZAKUP_SK = "zakup-sk"


class EntityType(StrEnum):
    LOT = "lot"
    NOTICE = "notice"
    PLAN_ITEM = "plan_item"
    ORGANIZATION = "organization"


class RelationType(StrEnum):
    PLAN_TO_NOTICE = "plan_to_notice"
    PLAN_TO_LOT = "plan_to_lot"
    NOTICE_TO_LOT = "notice_to_lot"
    CUSTOMER = "customer"
    ORGANIZER = "organizer"


class EntityIdentity(BaseModel):
    model_config = ConfigDict(frozen=True)

    source: Source
    entity_type: EntityType
    source_entity_id: str
    business_number: str | None = None
    canonical_url: str

    @computed_field
    @property
    def stable_key(self) -> str:
        return f"{self.source}:{self.entity_type}:{self.source_entity_id}"


class Organization(BaseModel):
    identity: EntityIdentity
    name_ru: str | None = None
    name_kk: str | None = None
    bin: str | None = None
    address: str | None = None
    phone: str | None = None
    email: str | None = None
    source_payload: dict[str, Any] = Field(default_factory=dict)


class DeliveryPlace(BaseModel):
    source_row_id: str | None = None
    country: str | None = None
    address: str | None = None
    quantity: Decimal | None = None
    incoterms: str | None = None
    source_payload: dict[str, Any] = Field(default_factory=dict)


class PaymentTerms(BaseModel):
    prepayment_percent: Decimal | None = None
    interim_percent: Decimal | None = None
    final_percent: Decimal | None = None
    raw_text: str | None = None


class DocumentMetadata(BaseModel):
    source_document_id: str | None = None
    category: str | None = None
    filename: str | None = None
    url: str | None = None
    size_bytes: int | None = None
    uploaded_at: datetime | None = None
    document_hash: str | None = None
    declared_content_type: str | None = None
    inferred_content_type: str | None = None
    response_content_type: str | None = None
    source_payload: dict[str, Any] = Field(default_factory=dict)

    @computed_field
    @property
    def extension(self) -> str | None:
        if not self.filename:
            return None
        suffix = PurePosixPath(self.filename).suffix.lower()
        return suffix.removeprefix(".") or None


class ProcurementEntity(BaseModel):
    identity: EntityIdentity
    title_ru: str | None = None
    title_kk: str | None = None
    description_ru: str | None = None
    description_kk: str | None = None
    additional_characteristics_ru: str | None = None
    additional_characteristics_kk: str | None = None
    status: str | None = None
    procurement_method: str | None = None
    tru_code: str | None = None
    oktru_code: str | None = None
    oktru_category_ru: str | None = None
    oktru_category_kk: str | None = None
    plan_row_number: str | None = None
    priority: str | None = None
    procurement_year: int | None = None
    procurement_month: str | None = None
    plan_item_type: str | None = None
    quantity: Decimal | None = None
    unit: str | None = None
    unit_price: Decimal | None = None
    total_amount: Decimal | None = None
    currency: str | None = None
    published_at: datetime | None = None
    application_start_at: datetime | None = None
    application_end_at: datetime | None = None
    delivery_terms_ru: str | None = None
    delivery_terms_kk: str | None = None
    delivery_conditions_ru: str | None = None
    delivery_conditions_kk: str | None = None
    venue_ru: str | None = None
    venue_kk: str | None = None
    contact_email: str | None = None
    contact_phone: str | None = None
    contact_extension: str | None = None
    customer_identity: EntityIdentity | None = None
    organizer_identity: EntityIdentity | None = None
    delivery_places: list[DeliveryPlace] = Field(default_factory=list)
    payment_terms: PaymentTerms | None = None
    documents: list[DocumentMetadata] = Field(default_factory=list)
    source_payload: dict[str, Any] = Field(default_factory=dict)


class EntityEnvelope(BaseModel):
    entity: ProcurementEntity | Organization
    fetched_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    http_status: int = 200
    response_headers: dict[str, str] = Field(default_factory=dict)
    parser_version: str = "1"
    content_hash: str


class EntityRelation(BaseModel):
    source: Source
    relation_type: RelationType
    parent: EntityIdentity
    child: EntityIdentity
    source_payload: dict[str, Any] = Field(default_factory=dict)


class DiscoveredEntity(BaseModel):
    identity: EntityIdentity
    priority: int = 0
    task_type: str = "detail"
    refresh_existing: bool = False
    available_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    summary_payload: dict[str, Any] = Field(default_factory=dict)


class ExtractedBatch(BaseModel):
    entities: list[EntityEnvelope] = Field(default_factory=list)
    relations: list[EntityRelation] = Field(default_factory=list)
    discovered: list[DiscoveredEntity] = Field(default_factory=list)


class FrontierTask(BaseModel):
    id: int
    identity: EntityIdentity
    task_type: str
    priority: int
    attempt: int
    payload: dict[str, Any] = Field(default_factory=dict)


class FrontierActivity(BaseModel):
    depth: int = 0
    ready: int = 0
    delayed: int = 0
    leased: int = 0


class CaptchaKind(StrEnum):
    RECAPTCHA_V2 = "recaptcha_v2"
    RECAPTCHA_V3 = "recaptcha_v3"
    RECAPTCHA_ENTERPRISE = "recaptcha_enterprise"


class CaptchaChallenge(BaseModel):
    kind: CaptchaKind
    website_url: str
    site_key: str
    page_action: str | None = None
    invisible: bool = False
    enterprise: bool = False
    user_agent: str | None = None
    cookies: str | None = None
    proxy_url: str | None = None
    min_score: float = 0.7
    session_lane_id: str | None = None


class CaptchaSolution(BaseModel):
    provider: str
    token: str
    task_id: str | None = None
    cost: Decimal | None = None
    solved_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
