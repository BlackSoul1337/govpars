import asyncio

import pytest

from procurement_parser.application.pipeline import (
    DiscoveryService,
    classify_discovery_window,
)
from procurement_parser.domain.models import (
    DiscoveredEntity,
    EntityIdentity,
    EntityType,
    Source,
)


def discovered(page: int, entity_type: EntityType = EntityType.LOT):
    return [
        DiscoveredEntity(
            identity=EntityIdentity(
                source=Source.EEP_MITWORK,
                entity_type=entity_type,
                source_entity_id=f"{entity_type.value}-{page}",
                canonical_url=f"https://example.test/{entity_type.value}/{page}",
            ),
            priority=0,
        )
    ]


class FakeDiscoveryAdapter:
    source = Source.EEP_MITWORK

    def __init__(self, pages=None, *, delays=None, wait_event=None):
        self.pages = pages or {}
        self.delays = delays or {}
        self.wait_event = wait_event
        self.active = 0
        self.max_active = 0
        self.calls = []

    async def discover(self, entity_type, *, page, priority, filters):
        del priority, filters
        self.calls.append((entity_type, page))
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        try:
            if self.wait_event is not None:
                await self.wait_event.wait()
            await asyncio.sleep(self.delays.get(page, 0))
            result = self.pages.get((entity_type, page), self.pages.get(page, []))
            if isinstance(result, BaseException):
                raise result
            return result
        finally:
            self.active -= 1


class FakeDiscoveryFrontier:
    def __init__(self, adapter=None):
        self.checkpoint = (1, False)
        self.saved = []
        self.enqueue_calls = []
        self.adapter = adapter
        self.fail_enqueue = False
        self.fail_checkpoint = False
        self.active_enqueues = 0
        self.max_active_enqueues = 0

    async def get_checkpoint(self, *_args, **_kwargs):
        return self.checkpoint

    async def set_checkpoint(self, *_args, **kwargs):
        if self.fail_checkpoint:
            self.fail_checkpoint = False
            raise RuntimeError("checkpoint failed")
        self.saved.append(kwargs)
        self.checkpoint = (kwargs["next_page"], kwargs["completed"])

    async def enqueue(self, items):
        if self.fail_enqueue:
            raise RuntimeError("enqueue failed")
        self.active_enqueues += 1
        self.max_active_enqueues = max(
            self.max_active_enqueues,
            self.active_enqueues,
        )
        try:
            await asyncio.sleep(0.005)
            self.enqueue_calls.append(list(items))
            return len(items)
        finally:
            self.active_enqueues -= 1


async def test_discovery_walks_windows_and_completes_checkpoint() -> None:
    adapter = FakeDiscoveryAdapter(
        {1: discovered(1), 2: discovered(2), 3: []}
    )
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=2)

    total = await service.run(EntityType.LOT)

    assert total == 2
    assert len(frontier.enqueue_calls) == 1
    assert frontier.saved[-1]["completed"] is True
    assert frontier.saved[-1]["next_page"] == 3


async def test_completed_full_checkpoint_is_not_repeated() -> None:
    adapter = FakeDiscoveryAdapter()
    frontier = FakeDiscoveryFrontier(adapter)
    frontier.checkpoint = (10, True)
    service = DiscoveryService(adapter, frontier)

    assert await service.run(EntityType.LOT) == 0
    assert adapter.calls == []


async def test_completed_named_scope_is_not_repeated() -> None:
    adapter = FakeDiscoveryAdapter()
    frontier = FakeDiscoveryFrontier(adapter)
    frontier.checkpoint = (10, True)
    service = DiscoveryService(adapter, frontier)

    assert await service.run(EntityType.LOT, scope="weekly") == 0
    assert adapter.calls == []


def test_left_to_right_classification_ignores_tail_failure() -> None:
    analysis = classify_discovery_window(
        [41, 42, 43, 44, 45],
        [
            discovered(41),
            discovered(42),
            [],
            TimeoutError("tail"),
            discovered(45),
        ],
    )

    assert [result.page for result in analysis.meaningful_pages] == [41, 42]
    assert analysis.terminal_page == 43
    assert analysis.ignored_speculative_failures == 1
    assert analysis.failure is None


def test_error_before_terminal_fails_window() -> None:
    failure = TimeoutError("page 42")
    analysis = classify_discovery_window(
        [41, 42, 43],
        [discovered(41), failure, []],
    )

    assert analysis.failed_page == 42
    assert analysis.failure is failure
    assert analysis.terminal_page is None


def test_first_empty_page_wins_and_counts_later_empty_pages() -> None:
    analysis = classify_discovery_window(
        [1, 2, 3, 4],
        [[], [], discovered(3), []],
    )

    assert analysis.terminal_page == 1
    assert analysis.meaningful_pages == ()
    assert analysis.speculative_empty_pages == 2


async def test_data_data_empty_error_persists_only_prefix() -> None:
    adapter = FakeDiscoveryAdapter(
        {
            1: discovered(1),
            2: discovered(2),
            3: [],
            4: TimeoutError("ignored"),
        }
    )
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=4)

    assert await service.run(EntityType.LOT) == 2
    assert len(frontier.enqueue_calls) == 1
    assert [
        item.identity.source_entity_id for item in frontier.enqueue_calls[0]
    ] == ["lot-1", "lot-2"]
    assert frontier.checkpoint == (3, True)


@pytest.mark.parametrize(
    "pages",
    [
        {1: discovered(1), 2: TimeoutError("failed"), 3: []},
        {
            1: discovered(1),
            2: discovered(2),
            3: TimeoutError("failed"),
            4: discovered(4),
        },
    ],
)
async def test_failure_before_terminal_never_partially_enqueues(pages) -> None:
    adapter = FakeDiscoveryAdapter(pages)
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=4)

    with pytest.raises(TimeoutError):
        await service.run(EntityType.LOT, max_pages=4)

    assert frontier.enqueue_calls == []
    assert frontier.saved == []
    assert frontier.checkpoint == (1, False)


async def test_empty_first_page_completes_without_enqueue() -> None:
    adapter = FakeDiscoveryAdapter({1: [], 2: discovered(2)})
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=2)

    assert await service.run(EntityType.LOT) == 0
    assert frontier.enqueue_calls == []
    assert frontier.checkpoint == (1, True)


async def test_shared_semaphore_caps_all_catalogs() -> None:
    pages = {
        (entity_type, page): discovered(page, entity_type)
        for entity_type in (
            EntityType.LOT,
            EntityType.NOTICE,
            EntityType.PLAN_ITEM,
        )
        for page in (1, 2)
    }
    adapter = FakeDiscoveryAdapter(pages, delays={1: 0.02, 2: 0.02})
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=2)

    await asyncio.gather(
        *(
            service.run(entity_type, max_pages=2, resume=False)
            for entity_type in (
                EntityType.LOT,
                EntityType.NOTICE,
                EntityType.PLAN_ITEM,
            )
        )
    )

    assert adapter.max_active == 2
    assert len(frontier.enqueue_calls) == 3
    assert frontier.max_active_enqueues == 1


async def test_completion_order_does_not_change_enqueue_order() -> None:
    adapter = FakeDiscoveryAdapter(
        {1: discovered(1), 2: discovered(2), 3: discovered(3)},
        delays={1: 0.03, 2: 0.02, 3: 0},
    )
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=3)

    await service.run(EntityType.LOT, max_pages=3)

    assert [
        item.identity.source_entity_id for item in frontier.enqueue_calls[0]
    ] == ["lot-1", "lot-2", "lot-3"]


async def test_cancellation_does_not_enqueue_or_move_checkpoint() -> None:
    wait_event = asyncio.Event()
    adapter = FakeDiscoveryAdapter({1: discovered(1)}, wait_event=wait_event)
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=2)
    task = asyncio.create_task(service.run(EntityType.LOT, max_pages=2))
    await asyncio.sleep(0)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert frontier.enqueue_calls == []
    assert frontier.saved == []


async def test_window_progress_wait_does_not_change_results(monkeypatch) -> None:
    monkeypatch.setattr(
        "procurement_parser.application.pipeline.DISCOVERY_PROGRESS_INTERVAL_SECONDS",
        0.001,
    )
    adapter = FakeDiscoveryAdapter(
        {1: discovered(1), 2: discovered(2)},
        delays={1: 0.01, 2: 0.01},
    )
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=2)

    assert await service.run(EntityType.LOT, max_pages=2) == 2
    assert frontier.checkpoint == (3, False)


async def test_enqueue_failure_does_not_move_checkpoint() -> None:
    adapter = FakeDiscoveryAdapter({1: discovered(1)})
    frontier = FakeDiscoveryFrontier(adapter)
    frontier.fail_enqueue = True
    service = DiscoveryService(adapter, frontier)

    with pytest.raises(RuntimeError, match="enqueue failed"):
        await service.run(EntityType.LOT, max_pages=1)

    assert frontier.saved == []


async def test_checkpoint_failure_repeats_idempotent_window() -> None:
    adapter = FakeDiscoveryAdapter({1: discovered(1)})
    frontier = FakeDiscoveryFrontier(adapter)
    frontier.fail_checkpoint = True
    service = DiscoveryService(adapter, frontier)

    with pytest.raises(RuntimeError, match="checkpoint failed"):
        await service.run(EntityType.LOT, max_pages=1)
    assert frontier.checkpoint == (1, False)

    assert await service.run(EntityType.LOT, max_pages=1) == 1
    assert len(frontier.enqueue_calls) == 2
    assert frontier.checkpoint == (2, False)


async def test_max_pages_shrinks_last_window() -> None:
    adapter = FakeDiscoveryAdapter(
        {page: discovered(page) for page in range(1, 6)}
    )
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=4)

    await service.run(EntityType.LOT, max_pages=5)

    assert [page for _, page in adapter.calls] == [1, 2, 3, 4, 5]
    assert len(frontier.enqueue_calls) == 2
    assert frontier.checkpoint == (6, False)


async def test_resume_starts_at_checkpoint() -> None:
    adapter = FakeDiscoveryAdapter({7: discovered(7)})
    frontier = FakeDiscoveryFrontier(adapter)
    frontier.checkpoint = (7, False)
    service = DiscoveryService(adapter, frontier)

    await service.run(EntityType.LOT, max_pages=1)

    assert adapter.calls == [(EntityType.LOT, 7)]


async def test_no_resume_starts_at_page_one() -> None:
    adapter = FakeDiscoveryAdapter({1: discovered(1)})
    frontier = FakeDiscoveryFrontier(adapter)
    frontier.checkpoint = (7, False)
    service = DiscoveryService(adapter, frontier)

    await service.run(EntityType.LOT, max_pages=1, resume=False)

    assert adapter.calls == [(EntityType.LOT, 1)]


async def test_concurrency_one_preserves_sequential_behavior() -> None:
    adapter = FakeDiscoveryAdapter({1: discovered(1), 2: discovered(2), 3: []})
    frontier = FakeDiscoveryFrontier(adapter)
    service = DiscoveryService(adapter, frontier, concurrency=1)

    assert await service.run(EntityType.LOT) == 2
    assert adapter.max_active == 1
    assert len(frontier.enqueue_calls) == 2
    assert frontier.checkpoint == (3, True)


@pytest.mark.parametrize("value", [0, 65])
def test_invalid_concurrency_is_rejected(value) -> None:
    with pytest.raises(ValueError, match="between 1 and 64"):
        DiscoveryService(
            FakeDiscoveryAdapter(),
            FakeDiscoveryFrontier(),
            concurrency=value,
        )
