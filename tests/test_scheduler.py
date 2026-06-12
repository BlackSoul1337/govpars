from types import SimpleNamespace

import pytest

from procurement_parser.application import scheduler
from procurement_parser.domain.models import EntityType, Source


class FakeAdapter:
    def __init__(self, source):
        self.source = source

    async def discover(self, entity_type, *, page, priority, filters):
        return []


class FakeFrontier:
    def __init__(self):
        self.refreshes = []

    async def enqueue(self, _items):
        return 0

    async def get_checkpoint(self, *_args, **_kwargs):
        return 1, False

    async def set_checkpoint(self, *_args, **_kwargs):
        return None

    async def enqueue_refresh(self, source, **kwargs):
        self.refreshes.append((source, kwargs))
        return 1


class FakeContext:
    def __init__(self, source):
        self.adapter = FakeAdapter(source)
        self.frontier = FakeFrontier()
        self.database = object()
        self.closed = False

    async def close(self):
        self.closed = True


class FakeMaintenance:
    completions = []

    def __init__(self, _database):
        pass

    async def complete_scheduler_job(self, **kwargs):
        self.completions.append(kwargs)


@pytest.mark.parametrize(
    "job_name",
    [
        "incremental_lists",
        "active_entities",
        "recently_closed",
        "old_entities",
        "weekly_reconcile",
    ],
)
async def test_scheduler_executes_each_policy(monkeypatch, job_name) -> None:
    source_settings = SimpleNamespace(
        name="eep-mitwork",
        discovery=SimpleNamespace(direct=6, proxy=10),
        concurrency=SimpleNamespace(direct=6, proxy=10),
        list_refresh_seconds=300,
        active_refresh_seconds=900,
        closed_refresh_seconds=86400,
        old_refresh_seconds=604800,
        full_reconcile_seconds=604800,
        recently_closed_window_seconds=1209600,
    )
    settings = SimpleNamespace(
        source=source_settings,
        network=SimpleNamespace(kind="direct"),
    )
    context = FakeContext(Source.EEP_MITWORK)
    monkeypatch.setattr(scheduler, "load_settings", lambda **_kwargs: settings)
    monkeypatch.setattr(scheduler, "build_context", lambda _settings: context)
    monkeypatch.setattr(scheduler, "PostgresMaintenance", FakeMaintenance)

    await scheduler._execute_job(
        source=Source.EEP_MITWORK,
        runtime_profile="local",
        network_profile="direct",
        captcha_profile="disabled",
        job=scheduler.SchedulerJob(job_name, 1, 60),
    )

    assert context.closed is True
    assert FakeMaintenance.completions[-1]["error"] is None


def test_scheduler_source_entity_types_and_filters() -> None:
    assert scheduler._entity_types(Source.EEP_MITWORK) == (
        EntityType.LOT,
        EntityType.NOTICE,
        EntityType.PLAN_ITEM,
    )
    assert scheduler._entity_types(Source.ZAKUP_SK) == (
        EntityType.LOT,
        EntityType.NOTICE,
    )
    assert scheduler._incremental_filters(
        Source.EEP_MITWORK,
        EntityType.LOT,
    ) is None
    filters = scheduler._incremental_filters(
        Source.ZAKUP_SK,
        EntityType.LOT,
    )
    assert filters["lotStatus"] == "PUBLISHED"
