import asyncio

from typer.testing import CliRunner

from procurement_parser.domain.models import EntityType, Source
from procurement_parser.entrypoints.cli import _run_discovery_catalogs, app


class FakeDiscoveryService:
    def __init__(self):
        self.active = 0
        self.max_active = 0

    async def run(self, _entity_type, **_kwargs):
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            await asyncio.sleep(0.01)
            return 1
        finally:
            self.active -= 1


async def test_eep_catalog_entrypoints_run_in_parallel() -> None:
    service = FakeDiscoveryService()

    total = await _run_discovery_catalogs(
        service,
        Source.EEP_MITWORK,
        [EntityType.LOT, EntityType.NOTICE, EntityType.PLAN_ITEM],
    )

    assert total == 3
    assert service.max_active == 3


async def test_zakup_catalog_entrypoints_remain_sequential() -> None:
    service = FakeDiscoveryService()

    total = await _run_discovery_catalogs(
        service,
        Source.ZAKUP_SK,
        [EntityType.LOT, EntityType.NOTICE],
    )

    assert total == 2
    assert service.max_active == 1


def test_cli_rejects_discovery_concurrency_outside_range() -> None:
    result = CliRunner().invoke(
        app,
        ["discover", "--discovery-concurrency", "65"],
    )

    assert result.exit_code != 0
    assert "64" in result.output
