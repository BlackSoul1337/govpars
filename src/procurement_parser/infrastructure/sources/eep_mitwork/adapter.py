from __future__ import annotations

import re
import time

from selectolax.parser import HTMLParser

from procurement_parser.config.settings import NetworkSettings, SourceSettings
from procurement_parser.domain.models import (
    DiscoveredEntity,
    EntityIdentity,
    EntityType,
    ExtractedBatch,
    Source,
)
from procurement_parser.domain.ports import RuntimeStatePort
from procurement_parser.infrastructure.sources.eep_mitwork.client import EepHttpClient
from procurement_parser.infrastructure.sources.eep_mitwork.parser import (
    ENTITY_PATHS,
    parse_detail,
    parse_list,
)
from procurement_parser.metrics import SOURCE_LATENCY, SOURCE_RESPONSES

LIST_PATHS = {
    EntityType.LOT: "/ru/publics/lots",
    EntityType.NOTICE: "/ru/publics/buys",
    EntityType.PLAN_ITEM: "/ru/publics/points",
}


class EepMitworkAdapter:
    source = Source.EEP_MITWORK

    def __init__(
        self,
        settings: SourceSettings,
        network: NetworkSettings,
        runtime_state: RuntimeStatePort | None = None,
    ) -> None:
        self.settings = settings
        self.client = EepHttpClient(settings, network, runtime_state)

    async def discover(
        self,
        entity_type: EntityType,
        *,
        page: int = 1,
        priority: int = 0,
        filters: dict | None = None,
    ) -> list[DiscoveredEntity]:
        del filters
        if entity_type == EntityType.ORGANIZATION:
            return []
        started = time.monotonic()
        response = await self.client.get(
            LIST_PATHS[entity_type],
            params={"page": page, "per-page": self.settings.per_page},
        )
        self._observe(
            response.status_code,
            time.monotonic() - started,
            response.strategy,
        )
        return parse_list(response.text, entity_type, priority=priority)

    async def probe_catalog(
        self,
        entity_type: EntityType,
        *,
        samples: int = 1,
    ) -> dict:
        if entity_type not in LIST_PATHS:
            raise ValueError(f"Unsupported EEP catalog: {entity_type.value}")
        started = time.monotonic()
        returned = 0
        total = None
        strategy = None
        sample_identities = []
        for page in range(1, samples + 1):
            response = await self.client.get(
                LIST_PATHS[entity_type],
                params={"page": page, "per-page": self.settings.per_page},
            )
            strategy = response.strategy
            discovered = parse_list(response.text, entity_type, priority=0)
            returned += len(discovered)
            sample_identities.extend(item.identity for item in discovered)
            if total is None:
                total = self._catalog_total(response.text)
        elapsed = time.monotonic() - started
        return {
            "total": total,
            "returned": returned,
            "samples": samples,
            "page_size": self.settings.per_page,
            "elapsed_seconds": elapsed,
            "strategy": strategy,
            "sample_identities": sample_identities,
        }

    @staticmethod
    def _catalog_total(html: str) -> int | None:
        text = HTMLParser(html).text(separator=" ", strip=True)
        match = re.search(r"(?:из|of)\s+([\d\s\u00a0]+)", text, re.IGNORECASE)
        return int(re.sub(r"\D", "", match.group(1))) if match else None

    async def extract(self, identity: EntityIdentity) -> ExtractedBatch:
        singular = ENTITY_PATHS[identity.entity_type]
        started = time.monotonic()
        response = await self.client.get(
            f"/ru/publics/{singular}/{identity.source_entity_id}"
        )
        self._observe(
            response.status_code,
            time.monotonic() - started,
            response.strategy,
        )
        batch = parse_detail(response.text, identity, http_status=response.status_code)
        for envelope in batch.entities:
            envelope.response_headers = dict(response.headers)
        return batch

    async def close(self) -> None:
        await self.client.close()

    @staticmethod
    def _observe(status: int, elapsed: float, strategy: str) -> None:
        SOURCE_RESPONSES.labels(
            source=Source.EEP_MITWORK.value,
            strategy=strategy,
            status=str(status),
        ).inc()
        SOURCE_LATENCY.labels(
            source=Source.EEP_MITWORK.value,
            strategy=strategy,
        ).observe(elapsed)
