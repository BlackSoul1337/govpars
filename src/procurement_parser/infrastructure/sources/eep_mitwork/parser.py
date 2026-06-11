from __future__ import annotations

import hashlib
import mimetypes
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import orjson
from selectolax.parser import HTMLParser, Node

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

PARSER_VERSION = "eep-2"
BASE_URL = "https://eep.mitwork.kz"
# Portal timestamps are timezone-naive and use one portal-wide Kazakhstan timezone.
# This does not filter or reinterpret the geographic region of a procurement.
PORTAL_TZ = ZoneInfo("Asia/Almaty")

ENTITY_PATHS = {
    EntityType.LOT: "lot",
    EntityType.NOTICE: "buy",
    EntityType.PLAN_ITEM: "point",
    EntityType.ORGANIZATION: "subject",
}
PATH_ENTITIES = {value: key for key, value in ENTITY_PATHS.items()}

MONTHS_RU = {
    "янв": 1,
    "фев": 2,
    "мар": 3,
    "апр": 4,
    "мая": 5,
    "май": 5,
    "июн": 6,
    "июл": 7,
    "авг": 8,
    "сен": 9,
    "окт": 10,
    "ноя": 11,
    "дек": 12,
}


def clean_text(node: Node | None) -> str | None:
    if node is None:
        return None
    for excluded in node.css("script, style"):
        excluded.decompose()
    value = " ".join(node.text(separator=" ", strip=True).replace("\xa0", " ").split())
    return value or None


def parse_decimal(value: str | None) -> Decimal | None:
    if not value:
        return None
    normalized = re.sub(r"[^\d,.\-]", "", value.replace(" ", "").replace("\xa0", ""))
    if not normalized:
        return None
    if "," in normalized and "." in normalized:
        normalized = normalized.replace(".", "").replace(",", ".")
    elif "," in normalized:
        normalized = normalized.replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    match = re.search(r"(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})", value)
    if match:
        return datetime(*map(int, match.groups()), tzinfo=PORTAL_TZ)
    match = re.search(
        r"(\d{1,2})\s+([а-яё]{3})[а-яё.]*\s+(\d{4}).*?(\d{2}):(\d{2}):(\d{2})",
        value.lower(),
    )
    if match:
        day, month_name, year, hour, minute, second = match.groups()
        month = MONTHS_RU.get(month_name)
        if month:
            return datetime(
                int(year),
                month,
                int(day),
                int(hour),
                int(minute),
                int(second),
                tzinfo=PORTAL_TZ,
            )
    return None


def parse_size(value: str | None) -> int | None:
    if not value:
        return None
    number = parse_decimal(value)
    if number is None:
        return None
    lower = value.lower()
    multiplier = 1
    if "кб" in lower or "kb" in lower:
        multiplier = 1024
    elif "мб" in lower or "mb" in lower:
        multiplier = 1024**2
    elif "гб" in lower or "gb" in lower:
        multiplier = 1024**3
    return int(number * multiplier)


def identity_from_url(url: str, *, business_number: str | None = None) -> EntityIdentity | None:
    absolute = urljoin(BASE_URL, url)
    match = re.search(r"/ru/publics/(lot|buy|point|subject)/([^/?#]+)", absolute)
    if not match:
        return None
    path_type, source_id = match.groups()
    return EntityIdentity(
        source=Source.EEP_MITWORK,
        entity_type=PATH_ENTITIES[path_type],
        source_entity_id=source_id,
        business_number=business_number,
        canonical_url=absolute,
    )


def extract_tables(tree: HTMLParser) -> list[dict[str, Any]]:
    tables: list[dict[str, Any]] = []
    for table in tree.css("table"):
        heading = None
        previous = table.parent
        while previous is not None and heading is None:
            sibling = previous.prev
            while sibling is not None:
                if sibling.tag in {"h2", "h3", "h4"}:
                    heading = clean_text(sibling)
                    break
                sibling = sibling.prev
            previous = previous.parent
        headers = [clean_text(cell) or "" for cell in table.css("thead th")]
        rows: list[dict[str, Any]] = []
        for row in table.css("tbody tr"):
            cells = row.css("td")
            if not cells:
                continue
            values = [clean_text(cell) for cell in cells]
            payload = {
                (headers[index] if index < len(headers) and headers[index] else str(index)): value
                for index, value in enumerate(values)
            }
            payload["_source_row_id"] = row.attributes.get("data-key")
            rows.append(payload)
        if rows or headers:
            tables.append({"heading": heading, "headers": headers, "rows": rows})
    return tables


def detail_fields(tree: HTMLParser) -> tuple[dict[str, str], dict[str, Node]]:
    values: dict[str, str] = {}
    value_nodes: dict[str, Node] = {}
    for table in tree.css("table.detail-view"):
        for row in table.css("tr"):
            header = row.css_first("th")
            value = row.css_first("td")
            key = clean_text(header)
            text_value = clean_text(value)
            if key and text_value:
                values.setdefault(key, text_value)
                value_nodes.setdefault(key, value)
    return values, value_nodes


def parse_list(
    html: str,
    entity_type: EntityType,
    *,
    priority: int,
) -> list[DiscoveredEntity]:
    tree = HTMLParser(html)
    singular = ENTITY_PATHS[entity_type]
    results: list[DiscoveredEntity] = []
    seen: set[str] = set()
    for row in tree.css(".grid-view tbody tr"):
        anchor = row.css_first(f'a[href*="/ru/publics/{singular}/"]')
        if anchor is None:
            continue
        headers = [
            clean_text(header) or str(index)
            for index, header in enumerate(
                row.parent.parent.css("thead th") if row.parent and row.parent.parent else []
            )
        ]
        cell_nodes = row.css("td")
        cells = [clean_text(cell) for cell in cell_nodes]
        summary = {
            (headers[index] if index < len(headers) else str(index)): value
            for index, value in enumerate(cells)
        }
        business_number = None
        for index, cell in enumerate(cell_nodes):
            if cell.css_first(f'a[href*="/ru/publics/{singular}/"]') is None:
                continue
            header = headers[index].casefold() if index < len(headers) else ""
            if any(marker in header for marker in ("номер", "№", "ид", "id")):
                business_number = clean_text(anchor)
            break
        identity = identity_from_url(
            anchor.attributes.get("href", ""),
            business_number=business_number,
        )
        if identity is None or identity.stable_key in seen:
            continue
        seen.add(identity.stable_key)
        results.append(
            DiscoveredEntity(
                identity=identity,
                priority=priority,
                refresh_existing=True,
                summary_payload=summary,
            )
        )
    return results


def _cost_fields(
    value: str | None,
) -> tuple[
    Decimal | None,
    Decimal | None,
    str | None,
    Decimal | None,
    str | None,
]:
    if not value:
        return None, None, None, None, None
    match = re.search(
        (
            r"([\d\s,.]+)\s*([A-Z]{3})\s*x\s*([\d\s,.]+)"
            r"\s+(.+?)\s*=\s*([\d\s,.]+)\s*([A-Z]{3})"
        ),
        value,
    )
    if not match:
        return None, None, None, None, None
    unit_price, currency, quantity, unit, total, _ = match.groups()
    return (
        parse_decimal(unit_price),
        parse_decimal(quantity),
        unit.strip() or None,
        parse_decimal(total),
        currency,
    )


def _payment_terms(value: str | None) -> PaymentTerms | None:
    if not value:
        return None
    percent_match = re.search(r"x\s*([\d,.]+)%", value)
    return PaymentTerms(
        prepayment_percent=parse_decimal(percent_match.group(1)) if percent_match else None,
        raw_text=value,
    )


def _delivery_places(tables: list[dict[str, Any]]) -> list[DeliveryPlace]:
    result: list[DeliveryPlace] = []
    for table in tables:
        if table["heading"] != "Места поставки":
            continue
        for row in table["rows"]:
            address = row.get("Место поставки")
            incoterms_match = re.match(r"([A-Z]{3})\s+", address or "")
            result.append(
                DeliveryPlace(
                    source_row_id=row.get("_source_row_id"),
                    country=row.get("Страна"),
                    address=address,
                    quantity=parse_decimal(row.get("Количество")),
                    incoterms=incoterms_match.group(1) if incoterms_match else None,
                    source_payload=row,
                )
            )
    return result


def _documents(tables: list[dict[str, Any]], tree: HTMLParser) -> list[DocumentMetadata]:
    result: list[DocumentMetadata] = []
    rows_by_id = {
        row.attributes.get("data-key"): row
        for row in tree.css("table tbody tr[data-key]")
        if row.attributes.get("data-key")
    }
    for table in tables:
        if table["heading"] != "Документы":
            continue
        for row in table["rows"]:
            source_id = row.get("_source_row_id")
            html_row = rows_by_id.get(source_id)
            link = html_row.css_first('a[title="Скачать"]') if html_row else None
            url = urljoin(BASE_URL, link.attributes.get("href", "")) if link else None
            filename = row.get("Наименование документа")
            guessed_type = mimetypes.guess_type(filename or "")[0]
            result.append(
                DocumentMetadata(
                    source_document_id=source_id,
                    category=row.get("Категория документа"),
                    filename=filename,
                    url=url,
                    size_bytes=parse_size(row.get("Размер")),
                    uploaded_at=parse_datetime(row.get("Дата загрузки")),
                    document_hash=row.get("Уникальный хэш"),
                    inferred_content_type=guessed_type,
                    source_payload=row,
                )
            )
    return result


def _relations_and_discovered(
    tree: HTMLParser,
    parent: EntityIdentity,
) -> tuple[list[EntityRelation], list[DiscoveredEntity]]:
    relations: list[EntityRelation] = []
    discovered: list[DiscoveredEntity] = []
    seen: set[str] = set()
    for anchor in tree.css('a[href*="/ru/publics/"]'):
        child = identity_from_url(
            anchor.attributes.get("href", ""),
            business_number=clean_text(anchor),
        )
        if child is None or child.stable_key == parent.stable_key or child.stable_key in seen:
            continue
        seen.add(child.stable_key)
        discovered.append(DiscoveredEntity(identity=child, priority=10))
        relation_type = None
        if parent.entity_type == EntityType.PLAN_ITEM and child.entity_type == EntityType.NOTICE:
            relation_type = RelationType.PLAN_TO_NOTICE
        elif parent.entity_type == EntityType.PLAN_ITEM and child.entity_type == EntityType.LOT:
            relation_type = RelationType.PLAN_TO_LOT
        elif parent.entity_type == EntityType.NOTICE and child.entity_type == EntityType.LOT:
            relation_type = RelationType.NOTICE_TO_LOT
        if relation_type:
            relations.append(
                EntityRelation(
                    source=Source.EEP_MITWORK,
                    relation_type=relation_type,
                    parent=parent,
                    child=child,
                )
            )
    return relations, discovered


def parse_detail(html: str, identity: EntityIdentity, *, http_status: int = 200) -> ExtractedBatch:
    tree = HTMLParser(html)
    fields, field_nodes = detail_fields(tree)
    tables = extract_tables(tree)
    title = clean_text(tree.css_first("h1.page-title"))
    if (
        identity.business_number
        and title
        and title.endswith(f": {identity.business_number}")
    ):
        identity = identity.model_copy(update={"business_number": None})
    relations, discovered = _relations_and_discovered(tree, identity)
    views_text = clean_text(tree.css_first(".stats")) or ""
    views_match = re.search(r"(\d+)", views_text)
    views = int(views_match.group(1)) if views_match else None

    if identity.entity_type == EntityType.ORGANIZATION:
        entity: ProcurementEntity | Organization = Organization(
            identity=identity,
            name_ru=fields.get("Наименование на русском языке") or title,
            name_kk=fields.get("Наименование на государственном языке"),
            bin=fields.get("БИН"),
            address=fields.get("Адрес"),
            phone=fields.get("Телефон"),
            email=fields.get("Электронная почта"),
            source_payload={"fields": fields, "tables": tables, "views": views},
        )
    else:
        unit_price, quantity, unit, total_amount, currency = _cost_fields(
            fields.get("Расчет полной стоимости")
        )
        customer_identity = None
        organizer_identity = None
        for label, target in (("Заказчик", "customer"), ("Организатор", "organizer")):
            node = field_nodes.get(label)
            link = node.css_first('a[href*="/ru/publics/subject/"]') if node else None
            related = identity_from_url(link.attributes.get("href", "")) if link else None
            if target == "customer":
                customer_identity = related
            else:
                organizer_identity = related

        entity = ProcurementEntity(
            identity=identity,
            title_ru=fields.get("Наименование на русском языке") or title,
            title_kk=fields.get("Наименование на государственном языке"),
            description_ru=fields.get("Описание на русском языке"),
            description_kk=fields.get("Описание на государственном языке"),
            additional_characteristics_ru=fields.get(
                "Дополнительная характеристика на русском языке"
            ),
            additional_characteristics_kk=fields.get(
                "Дополнительная характеристика на государственном языке"
            ),
            status=fields.get("Статус"),
            procurement_method=fields.get("Способ закупки"),
            tru_code=fields.get("Код КТРУ"),
            procurement_year=(
                int(fields["Год"])
                if (fields.get("Год") or "").isdigit()
                else None
            ),
            procurement_month=fields.get("Месяц"),
            plan_item_type=fields.get("Тип пункта плана"),
            quantity=quantity,
            unit=unit,
            unit_price=unit_price,
            total_amount=total_amount,
            currency=currency,
            application_start_at=parse_datetime(fields.get("Дата начала приема заявок")),
            application_end_at=parse_datetime(fields.get("Дата окончания приема заявок")),
            delivery_terms_ru=fields.get("Срок поставки на русском языке"),
            delivery_terms_kk=fields.get("Срок поставки на государственном языке"),
            customer_identity=customer_identity,
            organizer_identity=organizer_identity,
            delivery_places=_delivery_places(tables),
            payment_terms=_payment_terms(fields.get("Расчет авансового платежа")),
            documents=_documents(tables, tree),
            source_payload={
                "fields": fields,
                "tables": tables,
                "views": views,
                "page_title": title,
            },
        )
        for related, relation_type in (
            (customer_identity, RelationType.CUSTOMER),
            (organizer_identity, RelationType.ORGANIZER),
        ):
            if related:
                relations.append(
                    EntityRelation(
                        source=Source.EEP_MITWORK,
                        relation_type=relation_type,
                        parent=identity,
                        child=related,
                    )
                )

    canonical = entity.model_dump(mode="json", exclude_none=False)
    content_hash = hashlib.sha256(
        orjson.dumps(canonical, option=orjson.OPT_SORT_KEYS)
    ).hexdigest()
    envelope = EntityEnvelope(
        entity=entity,
        http_status=http_status,
        parser_version=PARSER_VERSION,
        content_hash=content_hash,
    )
    return ExtractedBatch(
        entities=[envelope],
        relations=relations,
        discovered=discovered,
    )
