from procurement_parser.application.pipeline import DiscoveryService
from procurement_parser.domain.models import (
    DiscoveredEntity,
    EntityIdentity,
    EntityType,
    Source,
)


class FakeDiscoveryAdapter:
    source = Source.EEP_MITWORK

    async def discover(self, entity_type, *, page, priority, filters):
        if page > 2:
            return []
        return [
            DiscoveredEntity(
                identity=EntityIdentity(
                    source=self.source,
                    entity_type=entity_type,
                    source_entity_id=str(page),
                    canonical_url=f"https://example.test/{page}",
                ),
                priority=priority,
            )
        ]


class FakeDiscoveryFrontier:
    def __init__(self):
        self.checkpoint = (1, False)
        self.saved = []

    async def get_checkpoint(self, *_args, **_kwargs):
        return self.checkpoint

    async def set_checkpoint(self, *_args, **kwargs):
        self.saved.append(kwargs)

    async def enqueue(self, items):
        return len(items)


async def test_discovery_walks_pages_and_completes_checkpoint() -> None:
    frontier = FakeDiscoveryFrontier()
    service = DiscoveryService(FakeDiscoveryAdapter(), frontier)

    total = await service.run(EntityType.LOT)

    assert total == 2
    assert frontier.saved[-1]["completed"] is True
    assert frontier.saved[-1]["next_page"] == 3


async def test_completed_full_checkpoint_is_not_repeated() -> None:
    frontier = FakeDiscoveryFrontier()
    frontier.checkpoint = (10, True)
    service = DiscoveryService(FakeDiscoveryAdapter(), frontier)

    assert await service.run(EntityType.LOT) == 0
