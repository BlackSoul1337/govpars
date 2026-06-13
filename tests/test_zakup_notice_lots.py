from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from procurement_parser.config.settings import load_settings
from procurement_parser.domain.models import (
    EntityIdentity,
    EntityType,
    ExtractedBatch,
    RelationType,
    Source,
)
from procurement_parser.infrastructure.captcha.solvers import DisabledCaptchaSolver
from procurement_parser.infrastructure.sources.zakup_sk.adapter import ZakupSkAdapter
from procurement_parser.infrastructure.sources.zakup_sk.parser import parse_detail
from procurement_parser.infrastructure.sources.zakup_sk.strategies import ApiResponse


def notice_identity() -> EntityIdentity:
    return EntityIdentity(
        source=Source.ZAKUP_SK,
        entity_type=EntityType.NOTICE,
        source_entity_id="notice-1",
        canonical_url="https://zakup.sk.kz/#/notice-1",
    )


def build_adapter() -> ZakupSkAdapter:
    settings = load_settings(
        source=Source.ZAKUP_SK,
        runtime_profile="local",
        network_profile="direct",
        captcha_profile="disabled",
        config_dir=Path("config"),
    )
    return ZakupSkAdapter(
        settings.source,
        settings.network,
        settings.captcha,
        DisabledCaptchaSolver(),
        settings.runtime,
    )


@pytest.mark.asyncio
async def test_notice_lots_are_loaded_until_reported_total() -> None:
    adapter = build_adapter()
    adapter._request = AsyncMock(
        side_effect=[
            ApiResponse(
                status=200,
                headers={},
                data={"content": [{"id": 101}], "totalElements": 2},
                strategy="curl-cffi",
            ),
            ApiResponse(
                status=200,
                headers={},
                data={"content": [{"id": 102}], "totalElements": 2},
                strategy="curl-cffi",
            ),
        ]
    )
    batch = ExtractedBatch()
    try:
        await adapter._attach_notice_lots(batch, notice_identity())
    finally:
        await adapter.close()

    assert [item.identity.source_entity_id for item in batch.discovered] == [
        "101",
        "102",
    ]
    assert [relation.relation_type for relation in batch.relations] == [
        RelationType.NOTICE_TO_LOT,
        RelationType.NOTICE_TO_LOT,
    ]
    assert adapter._request.await_count == 2


@pytest.mark.asyncio
async def test_notice_lot_failure_prevents_partial_notice_persistence() -> None:
    adapter = build_adapter()
    adapter._request = AsyncMock(
        return_value=ApiResponse(
            status=503,
            headers={},
            data={},
            strategy="curl-cffi",
        )
    )
    try:
        with pytest.raises(RuntimeError, match="Zakup HTTP 503"):
            await adapter._attach_notice_lots(ExtractedBatch(), notice_identity())
    finally:
        await adapter.close()


@pytest.mark.asyncio
async def test_notice_lots_rejects_ambiguous_empty_success_response() -> None:
    adapter = build_adapter()
    adapter._request = AsyncMock(
        return_value=ApiResponse(
            status=200,
            headers={},
            data={},
            strategy="curl-cffi",
        )
    )
    try:
        with pytest.raises(RuntimeError, match="no items or explicit total"):
            await adapter._attach_notice_lots(ExtractedBatch(), notice_identity())
    finally:
        await adapter.close()


def test_notice_deduplicates_shared_customer_and_organizer_entity() -> None:
    organization = {
        "id": 10476106,
        "bin": "040140000476",
        "nameRu": "Shared organization",
    }
    batch = parse_detail(
        {
            "id": 1229639,
            "nameRu": "USB ФЛЕШ накопитель",
            "advertStatus": "PUBLISHED",
            "customer": organization,
            "organizer": organization,
        },
        EntityType.NOTICE,
        source_entity_id="1229639",
    )

    identities = [
        envelope.entity.identity.stable_key
        for envelope in batch.entities
    ]
    assert len(identities) == len(set(identities))
    assert len(batch.entities) == 2
    assert [relation.relation_type for relation in batch.relations] == [
        RelationType.CUSTOMER,
        RelationType.ORGANIZER,
    ]
