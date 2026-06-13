from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

import structlog

from procurement_parser.application.pipeline import DiscoveryService
from procurement_parser.config.settings import (
    configure_discovery_concurrency,
    load_settings,
)
from procurement_parser.domain.models import EntityType, Source
from procurement_parser.entrypoints.runtime import build_context
from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.infrastructure.persistence.postgres.maintenance import (
    PostgresMaintenance,
)
from procurement_parser.metrics import (
    SCHEDULER_JOB_DURATION,
    SCHEDULER_LAST_SUCCESS,
)

logger = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class SchedulerJob:
    name: str
    interval_seconds: int
    lease_seconds: int


def _jobs(settings) -> tuple[SchedulerJob, ...]:
    return (
        SchedulerJob("incremental_lists", settings.list_refresh_seconds, 15 * 60),
        SchedulerJob("active_entities", settings.active_refresh_seconds, 60 * 60),
        SchedulerJob("recently_closed", settings.closed_refresh_seconds, 2 * 60 * 60),
        SchedulerJob("old_entities", settings.old_refresh_seconds, 4 * 60 * 60),
        SchedulerJob(
            "weekly_reconcile",
            settings.full_reconcile_seconds,
            24 * 60 * 60,
        ),
    )


async def run_scheduler(
    *,
    source: Source,
    runtime_profile: str,
    network_profile: str,
    captcha_profile: str,
    discovery_concurrency_override: int | None = None,
    stop_event: asyncio.Event | None = None,
) -> None:
    stop_event = stop_event or asyncio.Event()
    settings = load_settings(
        source=source,
        runtime_profile=runtime_profile,
        network_profile=network_profile,
        captcha_profile=(
            captcha_profile
            if source == Source.ZAKUP_SK
            else "disabled"
        ),
    )
    coordinator_database = Database(settings.database)
    maintenance = PostgresMaintenance(coordinator_database)
    running: dict[str, asyncio.Task] = {}
    try:
        while not stop_event.is_set():
            for name, task in tuple(running.items()):
                if task.done():
                    await asyncio.gather(task, return_exceptions=True)
                    running.pop(name, None)

            for job in _jobs(settings.source):
                if job.name in running:
                    continue
                claimed = await maintenance.claim_scheduler_job(
                    source=source.value,
                    job_name=job.name,
                    interval_seconds=job.interval_seconds,
                    lease_seconds=job.lease_seconds,
                    start_immediately=(job.name == "incremental_lists"),
                )
                if claimed:
                    running[job.name] = asyncio.create_task(
                        _execute_job(
                            source=source,
                            runtime_profile=runtime_profile,
                            network_profile=network_profile,
                            captcha_profile=captcha_profile,
                            discovery_concurrency_override=(
                                discovery_concurrency_override
                            ),
                            job=job,
                        ),
                        name=f"scheduler-{source.value}-{job.name}",
                    )

            try:
                await asyncio.wait_for(stop_event.wait(), timeout=15)
            except TimeoutError:
                pass
    finally:
        for task in running.values():
            task.cancel()
        await asyncio.gather(*running.values(), return_exceptions=True)
        await coordinator_database.close()


async def _execute_job(
    *,
    source: Source,
    runtime_profile: str,
    network_profile: str,
    captcha_profile: str,
    job: SchedulerJob,
    discovery_concurrency_override: int | None = None,
) -> None:
    settings = load_settings(
        source=source,
        runtime_profile=runtime_profile,
        network_profile=network_profile,
        captcha_profile=(
            captcha_profile
            if source == Source.ZAKUP_SK
            else "disabled"
        ),
    )
    current_discovery_concurrency = configure_discovery_concurrency(
        settings,
        discovery_concurrency_override,
    )
    context = build_context(settings)
    maintenance = PostgresMaintenance(context.database)
    started_at = time.monotonic()
    error: str | None = None
    try:
        discovery = DiscoveryService(
            context.adapter,
            context.frontier,
            concurrency=current_discovery_concurrency,
        )
        if job.name == "incremental_lists":
            await _run_discovery_catalogs(
                discovery,
                source,
                incremental=True,
            )
        elif job.name == "active_entities":
            await context.frontier.enqueue_refresh(
                source,
                policy="active",
                priority=100,
                older_than_seconds=settings.source.active_refresh_seconds,
            )
        elif job.name == "recently_closed":
            await context.frontier.enqueue_refresh(
                source,
                policy="recently_closed",
                priority=50,
                older_than_seconds=settings.source.closed_refresh_seconds,
                newer_than_seconds=(
                    settings.source.recently_closed_window_seconds
                ),
            )
        elif job.name == "old_entities":
            await context.frontier.enqueue_refresh(
                source,
                policy="old",
                priority=10,
                older_than_seconds=settings.source.old_refresh_seconds,
            )
        elif job.name == "weekly_reconcile":
            await _run_discovery_catalogs(
                discovery,
                source,
                incremental=False,
            )
            await context.frontier.enqueue_refresh(
                source,
                policy="all",
                priority=0,
            )
        else:
            raise ValueError(f"Unknown scheduler job: {job.name}")
        SCHEDULER_LAST_SUCCESS.labels(
            source=source.value,
            job=job.name,
        ).set_to_current_time()
        logger.info(
            "scheduler_job_complete",
            source=source.value,
            job=job.name,
        )
    except asyncio.CancelledError:
        error = "scheduler job cancelled during shutdown"
        raise
    except Exception as exc:
        error = repr(exc)
        logger.exception(
            "scheduler_job_failed",
            source=source.value,
            job=job.name,
        )
    finally:
        SCHEDULER_JOB_DURATION.labels(
            source=source.value,
            job=job.name,
        ).observe(time.monotonic() - started_at)
        await maintenance.complete_scheduler_job(
            source=source.value,
            job_name=job.name,
            error=error,
        )
        await context.close()


def _entity_types(source: Source) -> tuple[EntityType, ...]:
    if source == Source.EEP_MITWORK:
        return (
            EntityType.LOT,
            EntityType.NOTICE,
            EntityType.PLAN_ITEM,
        )
    return (EntityType.LOT, EntityType.NOTICE)


def _incremental_filters(
    source: Source,
    entity_type: EntityType,
) -> dict | None:
    if source != Source.ZAKUP_SK:
        return None
    if entity_type == EntityType.LOT:
        return {
            "tenderSubjectTypes": [],
            "advertStatus": "PUBLISHED",
            "lotStatus": "PUBLISHED",
        }
    return {
        "tenderSubjectTypes": [],
        "advertStatus": "PUBLISHED",
    }


async def _run_discovery_catalogs(
    discovery: DiscoveryService,
    source: Source,
    *,
    incremental: bool,
) -> None:
    async def run(entity_type: EntityType) -> int:
        return await discovery.run(
            entity_type,
            start_page=1,
            max_pages=1 if incremental else None,
            priority=100 if incremental else 0,
            filters=(
                _incremental_filters(source, entity_type)
                if incremental
                else None
            ),
            scope="incremental" if incremental else "weekly-reconcile",
            resume=False,
        )

    if source == Source.ZAKUP_SK:
        for entity_type in _entity_types(source):
            await run(entity_type)
        return
    results = await asyncio.gather(
        *(run(entity_type) for entity_type in _entity_types(source)),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, BaseException)]
    if failures:
        raise failures[0]
