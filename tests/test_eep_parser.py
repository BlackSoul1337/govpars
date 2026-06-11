from decimal import Decimal
from pathlib import Path

from procurement_parser.domain.models import EntityIdentity, EntityType, RelationType, Source
from procurement_parser.infrastructure.sources.eep_mitwork.parser import (
    parse_detail,
    parse_list,
)

FIXTURES = Path(__file__).parent / "fixtures"


def test_list_keeps_route_id_separate_from_business_number() -> None:
    html = (FIXTURES / "eep_lots.html").read_text(encoding="utf-8")
    items = parse_list(html, EntityType.LOT, priority=100)

    assert len(items) == 1
    assert items[0].identity.source_entity_id == "651383"
    assert items[0].identity.business_number == "631179-ЗЦП5"
    assert items[0].priority == 100
    assert items[0].refresh_existing is True


def test_list_does_not_treat_title_link_as_business_number() -> None:
    html = """
    <div class="grid-view"><table>
      <thead><tr><th>Наименование</th><th>Статус</th></tr></thead>
      <tbody><tr>
        <td><a href="/ru/publics/lot/651457">Полотенце</a></td>
        <td>Опубликовано</td>
      </tr></tbody>
    </table></div>
    """

    items = parse_list(html, EntityType.LOT, priority=0)

    assert items[0].identity.source_entity_id == "651457"
    assert items[0].identity.business_number is None


def test_detail_text_excludes_embedded_scripts() -> None:
    html = """
    <h1 class="page-title">Лот #1: Тест</h1>
    <table class="detail-view">
      <tr><th>Способ закупки</th><td>Запрос цен
        <script>function showLicenses() { alert('x'); }</script>
      </td></tr>
    </table>
    """
    identity = EntityIdentity(
        source=Source.EEP_MITWORK,
        entity_type=EntityType.LOT,
        source_entity_id="1",
        canonical_url="https://eep.mitwork.kz/ru/publics/lot/1",
    )

    batch = parse_detail(html, identity)

    assert batch.entities[0].entity.procurement_method == "Запрос цен"


def test_detail_clears_title_mistaken_for_business_number() -> None:
    html = """
    <h1 class="page-title">Лот #651457: Полотенце</h1>
    <table class="detail-view">
      <tr><th>Наименование на русском языке</th><td>Полотенце</td></tr>
    </table>
    """
    identity = EntityIdentity(
        source=Source.EEP_MITWORK,
        entity_type=EntityType.LOT,
        source_entity_id="651457",
        business_number="Полотенце",
        canonical_url="https://eep.mitwork.kz/ru/publics/lot/651457",
    )

    batch = parse_detail(html, identity)

    assert batch.entities[0].entity.identity.business_number is None


def test_lot_detail_extracts_normalized_and_raw_data() -> None:
    html = (FIXTURES / "eep_lot.html").read_text(encoding="utf-8")
    identity = EntityIdentity(
        source=Source.EEP_MITWORK,
        entity_type=EntityType.LOT,
        source_entity_id="651383",
        business_number="631179-ЗЦП5",
        canonical_url="https://eep.mitwork.kz/ru/publics/lot/651383",
    )
    batch = parse_detail(html, identity)
    lot = batch.entities[0].entity

    assert lot.identity.business_number == "631179-ЗЦП5"
    assert lot.title_ru == "Персональный компьютер"
    assert lot.quantity == Decimal("40")
    assert lot.unit == "Комплект"
    assert lot.unit_price == Decimal("738945.69")
    assert lot.total_amount == Decimal("29557827.60")
    assert lot.delivery_places[0].source_row_id == "1486838"
    assert lot.documents[0].document_hash == "abc123"
    assert lot.documents[0].extension == "pdf"
    assert lot.documents[0].inferred_content_type == "application/pdf"
    assert lot.source_payload["fields"]["Код КТРУ"] == "26.20.40"
    assert any(
        relation.relation_type == RelationType.CUSTOMER
        for relation in batch.relations
    )
    assert not any(
        relation.parent.entity_type == EntityType.ORGANIZATION
        for relation in batch.relations
    )
    assert any(
        item.identity.entity_type == EntityType.NOTICE
        and item.identity.source_entity_id == "195123"
        for item in batch.discovered
    )
