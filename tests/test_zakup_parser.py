import json
from decimal import Decimal
from pathlib import Path

from procurement_parser.domain.models import EntityType
from procurement_parser.infrastructure.sources.zakup_sk.parser import parse_detail

FIXTURES = Path(__file__).parent / "fixtures"


def test_zakup_lot_preserves_payload_and_children() -> None:
    payload = json.loads((FIXTURES / "zakup_lot.json").read_text(encoding="utf-8"))
    batch = parse_detail(
        payload,
        EntityType.LOT,
        source_entity_id="4445808",
    )
    lot = batch.entities[0].entity

    assert lot.identity.source_entity_id == "4445808"
    assert lot.title_ru == "Дрель"
    assert lot.unit_price == Decimal("51500")
    assert lot.delivery_places[0].address == "г. Алматы"
    assert lot.documents[0].source_document_id == "file-1"
    assert lot.source_payload["truCode"] == "257330.930.000030"
    assert len(batch.entities) == 2  # lot + embedded customer
    assert batch.discovered[0].identity.source_entity_id == "1227366"


def test_zakup_preserves_long_source_text_fields() -> None:
    long_delivery_condition = "товар доставляется перевозчиком заказчика " * 10
    long_phone = "+7 723 229-8215 по технической спецификации " * 10
    payload = {
        "id": 4453353,
        "number": "4453353",
        "nameRu": "Тестовый лот",
        "deliveryPlaces": [
            {
                "address": "г. Атырау",
                "deliveryCondition": long_delivery_condition,
            }
        ],
        "phone": long_phone,
    }

    batch = parse_detail(payload, EntityType.LOT, source_entity_id="4453353")
    lot = batch.entities[0].entity

    assert lot.contact_phone == long_phone
    assert lot.delivery_places[0].incoterms == long_delivery_condition


def test_zakup_uses_nested_address_and_goods_attributes() -> None:
    payload = {
        "id": 10,
        "nameRu": "Ремень",
        "goodsAttributeList": [
            {
                "nameRu": "Клиновый ремень",
                "attribute": {"ru": "Прочие характеристики"},
            }
        ],
        "customer": {
            "id": 20,
            "bin": "123456789012",
            "nameRu": "Заказчик",
            "legalAddress": {
                "countryRu": "КАЗАХСТАН",
                "katoNameRu": "г. Астана",
                "street": "Кунаева",
                "building": "6",
            },
        },
    }

    batch = parse_detail(payload, EntityType.LOT, source_entity_id="10")
    lot = batch.entities[0].entity
    customer = batch.entities[1].entity

    assert lot.description_ru == "Прочие характеристики: Клиновый ремень"
    assert customer.address == "КАЗАХСТАН, г. Астана, Кунаева, 6"


def test_zakup_normalizes_schedule_contacts_and_source_specific_fields() -> None:
    payload = {
        "id": 4452839,
        "nameRu": "Пылесос",
        "briefDescriptionRu": "для сухой уборки",
        "addAttributeRu": "Дополнительная характеристика",
        "oktruFullCode": "1066-0004-0001-100042411",
        "oktruCategoryNameRu": "Пылесосы бытовые",
        "lotRowNumber": "335-1 Т",
        "tenderPriority": "HOLDING_PRODUCER",
        "tenderLocationRu": "Бостандыкский район",
        "email": "buyer@example.kz",
        "phone": "+7 700 000 00 00",
        "extensionNumber": 42,
        "incoterms": "DDP",
        "schedule": {
            "count": 30,
            "dayType": {
                "ru": "Календарные",
                "kk": "Күнтізбелік",
                "code": "CALENDAR",
            },
        },
    }

    batch = parse_detail(payload, EntityType.LOT, source_entity_id="4452839")
    lot = batch.entities[0].entity

    assert lot.description_ru == "для сухой уборки"
    assert lot.additional_characteristics_ru == "Дополнительная характеристика"
    assert lot.oktru_code == "1066-0004-0001-100042411"
    assert lot.plan_row_number == "335-1 Т"
    assert (
        lot.delivery_terms_ru
        == "С даты подписания договора в течение 30 календарных дней"
    )
    assert lot.delivery_conditions_ru == "DDP"
    assert lot.contact_extension == "42"
