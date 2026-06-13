from __future__ import annotations

import hashlib
import mimetypes
from collections.abc import Iterable
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from zoneinfo import ZoneInfo

import orjson

from procurement_parser.domain.models import (
    DeliveryPlace,
    DiscoveredEntity,
    DocumentMetadata,
    EntityEnvelope,
    EntityIdentity,
    EntityRelation,
    EntityType,
    ExtractedBatch,
    Organization,
    PaymentTerms,
    ProcurementEntity,
    RelationType,
    Source,
)

PARSER_VERSION = "zakup-3"
BASE_URL = "https://zakup.sk.kz"
PORTAL_TZ = ZoneInfo("Asia/Almaty")


def _pick(data: dict[str, Any], *keys: str) -> Any:
    for key in keys:
        value = data.get(key)
        if value not in (None, "", []):
            return value
    return None


def _text(value: Any, *, language: str = "Ru") -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, dict):
        return _pick(
            value,
            f"name{language}",
            f"title{language}",
            language.lower(),
            "name",
            "title",
            "code",
        )
    return str(value)


def _decimal(value: Any) -> Decimal | None:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value).replace(" ", "").replace(",", "."))
    except InvalidOperation:
        return None


def _datetime(value: Any) -> datetime | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        result = datetime.fromisoformat(text)
        return result.replace(tzinfo=result.tzinfo or PORTAL_TZ)
    except ValueError:
        return None


def _published_at(payload: dict[str, Any]) -> datetime | None:
    direct = _datetime(
        _pick(
            payload,
            "publishDate",
            "publishedDate",
            "publicationDate",
            "datePublished",
        )
    )
    if direct:
        return direct

    history = payload.get("timeHistory")
    if isinstance(history, dict):
        history = _items(history)
    if not isinstance(history, list):
        return None
    for item in history:
        if not isinstance(item, dict):
            continue
        status = str(
            _pick(
                item,
                "status",
                "advertStatus",
                "lotStatus",
                "simpleStatus",
                "event",
            )
            or ""
        ).upper()
        if "PUBLISH" not in status:
            continue
        published = _datetime(
            _pick(
                item,
                "date",
                "eventDate",
                "changeDate",
                "createdDate",
                "createdAt",
                "dateTime",
            )
        )
        if published:
            return published
    return None


def _items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("content", "items", "data", "results", "result"):
        value = payload.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        if isinstance(value, dict):
            nested = _items(value)
            if nested:
                return nested
    return []


def _source_id(data: dict[str, Any], entity_type: EntityType) -> str | None:
    keys = (
        ("id", "lotId", "lot_id", "itemId")
        if entity_type == EntityType.LOT
        else ("id", "advertId", "advert_id", "announcementId")
    )
    value = _pick(data, *keys)
    return str(value) if value is not None else None


def identity(
    data: dict[str, Any],
    entity_type: EntityType,
    *,
    fallback_id: str | None = None,
) -> EntityIdentity:
    source_id = _source_id(data, entity_type) or fallback_id
    if not source_id:
        raise ValueError(f"Zakup payload has no {entity_type} ID")
    number = _pick(
        data,
        "number",
        "lotNumber",
        "advertNumber",
        "rowNumber",
        "num",
    )
    if entity_type == EntityType.LOT:
        url = (
            f"{BASE_URL}/#/ext(popup:item/{source_id}/lot)"
            "?tabs=lot&adst=PUBLISHED&lst=PUBLISHED&page=1"
        )
    elif entity_type == EntityType.NOTICE:
        url = (
            f"{BASE_URL}/#/ext(popup:item/{source_id}/advert)"
            "?tabs=advert&tabstatus=active&page=1"
        )
    else:
        url = f"{BASE_URL}/#/ext?entity={entity_type.value}&id={source_id}"
    return EntityIdentity(
        source=Source.ZAKUP_SK,
        entity_type=entity_type,
        source_entity_id=source_id,
        business_number=str(number) if number is not None else None,
        canonical_url=url,
    )


def parse_discovery(
    payload: Any,
    entity_type: EntityType,
    *,
    priority: int,
) -> list[DiscoveredEntity]:
    result: list[DiscoveredEntity] = []
    for item in _items(payload):
        try:
            item_identity = identity(item, entity_type)
        except ValueError:
            continue
        result.append(
            DiscoveredEntity(
                identity=item_identity,
                priority=priority,
                refresh_existing=True,
                summary_payload=item,
            )
        )
    return result


def _walk_dicts(value: Any) -> Iterable[dict[str, Any]]:
    if isinstance(value, dict):
        yield value
        for nested in value.values():
            yield from _walk_dicts(nested)
    elif isinstance(value, list):
        for nested in value:
            yield from _walk_dicts(nested)


def _documents(payload: dict[str, Any]) -> list[DocumentMetadata]:
    result: list[DocumentMetadata] = []
    seen: set[str] = set()
    for item in _walk_dicts(payload):
        filename = _pick(item, "fileName", "filename", "name")
        uid = _pick(item, "uid", "fileUid", "documentId", "id")
        file_hash = _pick(item, "hash", "fileHash", "md5", "sha256")
        if not filename or not (
            _pick(item, "fileSize", "size", "contentType", "mimeType") or file_hash
        ):
            continue
        key = str(uid or file_hash or filename)
        if key in seen:
            continue
        seen.add(key)
        url = (
            f"{BASE_URL}/eprocfilestorage/open-api/files/download/{uid}"
            if uid
            else _pick(item, "url", "downloadUrl")
        )
        result.append(
            DocumentMetadata(
                source_document_id=str(uid) if uid else None,
                category=_pick(item, "category", "documentCategory", "type"),
                filename=str(filename),
                url=url,
                size_bytes=int(_pick(item, "fileSize", "size"))
                if str(_pick(item, "fileSize", "size") or "").isdigit()
                else None,
                uploaded_at=_datetime(
                    _pick(item, "uploadDate", "createdDate", "createdAt")
                ),
                document_hash=str(file_hash) if file_hash else None,
                declared_content_type=_pick(item, "contentType", "mimeType"),
                inferred_content_type=mimetypes.guess_type(str(filename))[0],
                source_payload=item,
            )
        )
    return result


def _delivery_places(payload: dict[str, Any]) -> list[DeliveryPlace]:
    candidates = _pick(
        payload,
        "deliveryPlaces",
        "deliveryPlaceList",
        "deliveryAddresses",
    )
    if isinstance(candidates, list):
        return [
            DeliveryPlace(
                source_row_id=str(_pick(item, "id", "rowId") or "") or None,
                country=_text(_pick(item, "country", "countryName")),
                address=_text(_pick(item, "address", "place", "deliveryPlace")),
                quantity=_decimal(_pick(item, "quantity", "count")),
                incoterms=_text(_pick(item, "incoterms", "deliveryCondition")),
                source_payload=item,
            )
            for item in candidates
            if isinstance(item, dict)
        ]
    direct_address = _pick(
        payload,
        "deliveryLocationRu",
        "deliveryPlaceRu",
        "deliveryAddressRu",
    )
    if not direct_address:
        return []
    return [
        DeliveryPlace(
            country=_text(_pick(payload, "deliveryCountry")),
            address=_text(direct_address),
            quantity=_decimal(_pick(payload, "count", "quantity")),
            incoterms=_text(_pick(payload, "incoterms")),
            source_payload={
                "deliveryCountry": payload.get("deliveryCountry"),
                "deliveryLocationRu": payload.get("deliveryLocationRu"),
                "deliveryLocationKk": payload.get("deliveryLocationKk"),
                "deliveryKato": payload.get("deliveryKato"),
                "incoterms": payload.get("incoterms"),
            },
        )
    ]


def _organization(
    payload: dict[str, Any],
    *keys: str,
) -> tuple[Organization | None, EntityIdentity | None]:
    value = _pick(payload, *keys)
    if not isinstance(value, dict):
        return None, None
    source_id = _pick(value, "id", "companyId", "bin", "identifier")
    if source_id is None:
        return None, None
    org_identity = EntityIdentity(
        source=Source.ZAKUP_SK,
        entity_type=EntityType.ORGANIZATION,
        source_entity_id=str(source_id),
        business_number=str(_pick(value, "bin", "identifier") or source_id),
        canonical_url=f"{BASE_URL}/#/ext?organization={source_id}",
    )
    legal_address = value.get("legalAddress")
    address = _pick(value, "address", "addressRu")
    if not address and isinstance(legal_address, dict):
        address = _pick(legal_address, "fullAddressRu")
        if not address:
            address = ", ".join(
                str(part).strip()
                for part in (
                    legal_address.get("countryRu"),
                    legal_address.get("katoNameRu"),
                    legal_address.get("street"),
                    legal_address.get("building"),
                    legal_address.get("flat"),
                )
                if part and str(part).strip()
            ) or None
    return (
        Organization(
            identity=org_identity,
            name_ru=_pick(value, "nameRu", "name", "fullNameRu"),
            name_kk=_pick(value, "nameKk", "fullNameKk"),
            bin=_pick(value, "bin", "identifier"),
            address=address,
            phone=_pick(value, "phone"),
            email=_pick(value, "email"),
            source_payload=value,
        ),
        org_identity,
    )


def _goods_description(payload: dict[str, Any], language: str) -> str | None:
    suffix = "Ru" if language == "ru" else "Kk"
    parts: list[str] = []
    for item in payload.get("goodsAttributeList") or []:
        if not isinstance(item, dict):
            continue
        value = _pick(item, f"name{suffix}", "name")
        attribute = item.get("attribute")
        label = (
            _pick(attribute, language, f"name{suffix}")
            if isinstance(attribute, dict)
            else None
        )
        if value:
            parts.append(f"{label}: {value}" if label else str(value))
    return "; ".join(parts) or None


def _payment_terms(payload: dict[str, Any]) -> PaymentTerms:
    values: dict[str, Decimal | None] = {
        "prepayment_percent": _decimal(
            _pick(
                payload,
                "prepaymentPercent",
                "advancePaymentPercent",
                "prepayment",
            )
        ),
        "interim_percent": _decimal(
            _pick(
                payload,
                "interimPaymentPercent",
                "partialPaymentPercent",
                "interimPayment",
            )
        ),
        "final_percent": _decimal(
            _pick(
                payload,
                "finalPaymentPercent",
                "finalPercent",
                "finalPayment",
            )
        ),
    }
    for item in payload.get("paymentConditionList") or []:
        if not isinstance(item, dict):
            continue
        target = {
            "PREPAY": "prepayment_percent",
            "INTERIM_PAYMENT": "interim_percent",
            "FINAL_PAYMENT": "final_percent",
        }.get(str(item.get("paymentType")))
        if target:
            values[target] = _decimal(item.get("value"))
    return PaymentTerms(
        **values,
        raw_text=_pick(payload, "paymentTerms", "paymentCondition"),
    )


def _delivery_schedule(payload: dict[str, Any], language: str) -> str | None:
    suffix = "Ru" if language == "ru" else "Kk"
    direct = _pick(
        payload,
        f"deliveryTerms{suffix}",
        f"deliveryDate{suffix}",
        f"deliveryPeriod{suffix}",
    )
    if direct:
        return str(direct)
    schedule = payload.get("schedule")
    if not isinstance(schedule, dict):
        return None
    count = _pick(schedule, "count", "daysForDelivery")
    day_type_payload = schedule.get("dayType")
    day_type = _text(day_type_payload, language=suffix)
    day_type_code = (
        str(day_type_payload.get("code", "")).upper()
        if isinstance(day_type_payload, dict)
        else ""
    )
    if count is not None:
        if language == "ru":
            qualifier_value = {
                "CALENDAR": "календарных",
                "WORKING": "рабочих",
            }.get(day_type_code, str(day_type).lower() if day_type else "")
            qualifier = f" {qualifier_value}" if qualifier_value else ""
            return (
                f"С даты подписания договора в течение "
                f"{count}{qualifier} дней"
            )
        qualifier = f" {str(day_type).lower()}" if day_type else ""
        return f"Шартқа қол қойылған күннен бастап {count}{qualifier} күн ішінде"
    month_from = schedule.get("monthFrom")
    month_to = schedule.get("monthTo")
    if month_from and month_to:
        return (
            f"С {month_from} по {month_to}"
            if language == "ru"
            else f"{month_from} бастап {month_to} дейін"
        )
    if month_to:
        return (
            f"С даты подписания договора по (включительно) {month_to}"
            if language == "ru"
            else (
                f"Шартқа қол қойылған күннен бастап "
                f"{month_to} дейін (қоса алғанда)"
            )
        )
    return None


def parse_detail(
    payload: dict[str, Any],
    entity_type: EntityType,
    *,
    source_entity_id: str,
    http_status: int = 200,
) -> ExtractedBatch:
    entity_identity = identity(payload, entity_type, fallback_id=source_entity_id)
    customer, customer_identity = _organization(payload, "customer", "customerInfo")
    organizer, organizer_identity = _organization(payload, "organizer", "organizerInfo")
    payment = _payment_terms(payload)
    procurement = ProcurementEntity(
        identity=entity_identity,
        title_ru=_pick(payload, "nameRu", "name", "titleRu", "lotNameRu"),
        title_kk=_pick(payload, "nameKk", "titleKk", "lotNameKk"),
        description_ru=_pick(
            payload,
            "descriptionRu",
            "briefDescriptionRu",
            "characteristicRu",
        )
        or _goods_description(payload, "ru"),
        description_kk=_pick(
            payload, "descriptionKk", "briefDescriptionKk", "characteristicKk"
        )
        or _goods_description(payload, "kk"),
        additional_characteristics_ru=_pick(payload, "addAttributeRu")
        or _goods_description(payload, "ru"),
        additional_characteristics_kk=_pick(payload, "addAttributeKk")
        or _goods_description(payload, "kk"),
        status=str(
            _pick(
                payload,
                "status",
                "statusName",
                "lotStatus",
                "advertStatus",
                "simpleStatus",
            )
            or ""
        )
        or None,
        procurement_method=_text(
            _pick(payload, "tenderTypeNameRu", "procurementMethod", "tenderType")
        ),
        tru_code=_pick(
            payload,
            "truCode",
            "enstruCode",
            "codeEnstru",
        ),
        oktru_code=_pick(payload, "oktruFullCode"),
        oktru_category_ru=_pick(payload, "oktruCategoryNameRu"),
        oktru_category_kk=_pick(payload, "oktruCategoryNameKk"),
        plan_row_number=_pick(payload, "lotRowNumber"),
        plan_item_type=_text(
            _pick(payload, "tenderSubjectType", "subjectType", "truType")
        ),
        priority=_text(_pick(payload, "tenderPriority")),
        quantity=_decimal(_pick(payload, "quantity", "count")),
        unit=_text(_pick(payload, "measureNameRu", "unitNameRu", "measure", "mkei")),
        unit_price=_decimal(_pick(payload, "price", "unitPrice", "priceNoNds")),
        total_amount=_decimal(
            _pick(payload, "sum", "totalSum", "sumNoNds", "sumTruNoNds")
        ),
        currency=_pick(payload, "currency", "currencyCode") or "KZT",
        published_at=_published_at(payload),
        application_start_at=_datetime(
            _pick(
                payload,
                "beginDate",
                "beginDateTime",
                "acceptanceBeginDateTime",
            )
        ),
        application_end_at=_datetime(
            _pick(
                payload,
                "endDate",
                "endDateTime",
                "acceptanceEndDateTime",
            )
        ),
        delivery_terms_ru=_delivery_schedule(payload, "ru"),
        delivery_terms_kk=_delivery_schedule(payload, "kk"),
        delivery_conditions_ru=_text(
            _pick(
                payload,
                "deliveryConditionRu",
                "deliveryConditionsRu",
                "incoterms",
            ),
            language="Ru",
        ),
        delivery_conditions_kk=_text(
            _pick(
                payload,
                "deliveryConditionKk",
                "deliveryConditionsKk",
                "incoterms",
            ),
            language="Kk",
        ),
        venue_ru=_pick(payload, "tenderLocationRu", "tenderLocation"),
        venue_kk=_pick(payload, "tenderLocationKk"),
        contact_email=_pick(payload, "email"),
        contact_phone=_pick(payload, "phone"),
        contact_extension=(
            str(payload["extensionNumber"])
            if payload.get("extensionNumber") not in (None, "")
            else None
        ),
        customer_identity=customer_identity,
        organizer_identity=organizer_identity,
        delivery_places=_delivery_places(payload),
        payment_terms=payment,
        documents=_documents(payload),
        source_payload=payload,
    )
    entities: list[EntityEnvelope] = []
    relations: list[EntityRelation] = []
    related_entity_keys: set[str] = set()
    for item in (customer, organizer):
        if not item or item.identity.stable_key in related_entity_keys:
            continue
        related_entity_keys.add(item.identity.stable_key)
        item_hash = hashlib.sha256(
            orjson.dumps(item.model_dump(mode="json"), option=orjson.OPT_SORT_KEYS)
        ).hexdigest()
        entities.append(
            EntityEnvelope(
                entity=item,
                content_hash=item_hash,
                parser_version=PARSER_VERSION,
            )
        )
    for related, relation_type in (
        (customer_identity, RelationType.CUSTOMER),
        (organizer_identity, RelationType.ORGANIZER),
    ):
        if related:
            relations.append(
                EntityRelation(
                    source=Source.ZAKUP_SK,
                    relation_type=relation_type,
                    parent=entity_identity,
                    child=related,
                )
            )
    discovered: list[DiscoveredEntity] = []
    if entity_type == EntityType.LOT and (advert_id := payload.get("advertId")):
        notice_identity = identity(
            {
                "id": advert_id,
                "number": _pick(payload, "advertNumber", "advertId"),
            },
            EntityType.NOTICE,
        )
        relations.append(
            EntityRelation(
                source=Source.ZAKUP_SK,
                relation_type=RelationType.NOTICE_TO_LOT,
                parent=notice_identity,
                child=entity_identity,
            )
        )
        discovered.append(
            DiscoveredEntity(
                identity=notice_identity,
                priority=10,
                summary_payload={
                    "nameRu": payload.get("advertNameRu"),
                    "nameKk": payload.get("advertNameKk"),
                    "status": payload.get("advertStatus"),
                },
            )
        )
    content_hash = hashlib.sha256(
        orjson.dumps(procurement.model_dump(mode="json"), option=orjson.OPT_SORT_KEYS)
    ).hexdigest()
    entities.insert(
        0,
        EntityEnvelope(
            entity=procurement,
            content_hash=content_hash,
            parser_version=PARSER_VERSION,
            http_status=http_status,
        ),
    )
    return ExtractedBatch(
        entities=entities,
        relations=relations,
        discovered=discovered,
    )


def attach_notice_lots(
    batch: ExtractedBatch,
    notice: EntityIdentity,
    payload: Any,
) -> None:
    for item in _items(payload):
        try:
            lot_identity = identity(item, EntityType.LOT)
        except ValueError:
            continue
        batch.discovered.append(
            DiscoveredEntity(
                identity=lot_identity,
                priority=10,
                summary_payload=item,
            )
        )
        batch.relations.append(
            EntityRelation(
                source=Source.ZAKUP_SK,
                relation_type=RelationType.NOTICE_TO_LOT,
                parent=notice,
                child=lot_identity,
                source_payload=item,
            )
        )
