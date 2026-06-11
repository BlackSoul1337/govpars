from procurement_parser.application.pipeline import WorkerService
from procurement_parser.domain.models import (
    EntityIdentity,
    EntityType,
    ExtractedBatch,
    FrontierActivity,
    FrontierTask,
    Source,
)


class FakeAdapter:
    source = Source.EEP_MITWORK

    def __init__(self) -> None:
        self.on_extract = None
        self.error = None

    async def extract(self, _identity):
        if self.on_extract:
            self.on_extract()
        if self.error:
            raise self.error
        return ExtractedBatch()


class FakeFrontier:
    def __init__(self, tasks):
        self.tasks = tasks
        self.claimed = False
        self.completed = []
        self.released = []
        self.failed = []
        self.retried = []

    async def claim(self, *_args, **_kwargs):
        if self.claimed:
            return []
        self.claimed = True
        return self.tasks

    async def enqueue(self, _items):
        return 0

    async def complete(self, task, *, content_hash):
        self.completed.append((task.id, content_hash))

    async def release(self, tasks, *, worker_id):
        self.released.extend(task.id for task in tasks)
        return len(tasks)

    async def release_by_owner(self, _worker_id):
        return 0

    async def extend_lease(self, _task, *, worker_id, lease_seconds):
        return True

    async def activity(self, _source):
        return FrontierActivity()

    async def fail(self, task, **kwargs):
        self.failed.append((task.id, kwargs))

    async def retry(self, task, **kwargs):
        self.retried.append((task.id, kwargs))


class FakeEntities:
    async def persist(self, _entities, _relations):
        return None


def _task(task_id: int) -> FrontierTask:
    return FrontierTask(
        id=task_id,
        identity=EntityIdentity(
            source=Source.EEP_MITWORK,
            entity_type=EntityType.LOT,
            source_entity_id=str(task_id),
            canonical_url=f"https://eep.mitwork.kz/ru/publics/lot/{task_id}",
        ),
        task_type="detail",
        priority=0,
        attempt=1,
    )


async def test_worker_releases_unprocessed_claimed_tasks_on_shutdown() -> None:
    adapter = FakeAdapter()
    frontier = FakeFrontier([_task(1), _task(2), _task(3)])
    service = WorkerService(
        adapter=adapter,
        frontier=frontier,
        entities=FakeEntities(),
        worker_count=1,
        claim_batch_size=3,
        lease_seconds=180,
        backfill_capacity_percent=20,
        max_attempts=3,
    )
    adapter.on_extract = service.request_shutdown

    await service.run()

    assert [task_id for task_id, _ in frontier.completed] == [1]
    assert frontier.released == [2, 3]


async def test_worker_drain_exits_after_idle_grace() -> None:
    service = WorkerService(
        adapter=FakeAdapter(),
        frontier=FakeFrontier([]),
        entities=FakeEntities(),
        worker_count=1,
        claim_batch_size=1,
        lease_seconds=30,
        backfill_capacity_percent=20,
        max_attempts=3,
        idle_grace_seconds=0,
    )

    await service.run(drain=True)

    assert service.shutdown_requested.is_set()


async def test_worker_does_not_retry_missing_source_resource() -> None:
    class MissingResourceError(RuntimeError):
        strategy = "httpx"

        def __init__(self):
            self.response = type("Response", (), {"status_code": 404})()
            super().__init__("not found")

    adapter = FakeAdapter()
    adapter.error = MissingResourceError()
    frontier = FakeFrontier([])
    service = WorkerService(
        adapter=adapter,
        frontier=frontier,
        entities=FakeEntities(),
        worker_count=1,
        claim_batch_size=1,
        lease_seconds=30,
        backfill_capacity_percent=20,
        max_attempts=5,
    )

    await service._process_task(
        _task(404),
        worker_id="test-worker",
        run_id="test-run",
    )

    assert frontier.failed[0][0] == 404
    assert frontier.failed[0][1]["http_status"] == 404
    assert frontier.retried == []
