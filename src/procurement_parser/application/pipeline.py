from __future__ import annotations

import asyncio
import math
import time
import uuid
from collections.abc import Awaitable, Sequence
from dataclasses import dataclass
from typing import Any

import structlog

from procurement_parser.domain.errors import (
    LeaseLostError,
    RetriableSourceError,
    is_permanent_http_status,
)
from procurement_parser.domain.models import (
    DiscoveredEntity,
    EntityEnvelope,
    EntityType,
    FrontierTask,
)
from procurement_parser.domain.ports import EntityRepository, FrontierPort, SourceAdapter
from procurement_parser.metrics import (
    DISCOVERED_ENTITIES,
    DISCOVERY_ACTIVE_REQUESTS,
    DISCOVERY_CONFIGURED_CONCURRENCY,
    DISCOVERY_EFFECTIVE_CONCURRENCY,
    DISCOVERY_ENTITY_RATE,
    DISCOVERY_PAGES,
    DISCOVERY_WINDOW_DURATION,
    DISCOVERY_WINDOWS,
    TASK_DURATION,
    TASK_OUTCOMES,
)

logger = structlog.get_logger()
DISCOVERY_PROGRESS_INTERVAL_SECONDS = 5


async def gather_fail_fast(
    awaitables: Sequence[Awaitable[Any]],
) -> list[Any]:
    tasks = [asyncio.ensure_future(value) for value in awaitables]
    if not tasks:
        return []
    try:
        done, pending = await asyncio.wait(
            tasks,
            return_when=asyncio.FIRST_EXCEPTION,
        )
        failure: BaseException | None = None
        for task in done:
            if task.cancelled():
                failure = asyncio.CancelledError()
                break
            exception = task.exception()
            if exception is not None:
                failure = exception
                break
        if failure is not None:
            for task in pending:
                task.cancel()
            await asyncio.gather(*pending, return_exceptions=True)
            raise failure
        if pending:
            await asyncio.gather(*pending)
        return [task.result() for task in tasks]
    except BaseException:
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        raise


@dataclass(frozen=True, slots=True)
class DiscoveryPageSuccess:
    page: int
    items: list[DiscoveredEntity]


@dataclass(frozen=True, slots=True)
class DiscoveryWindowAnalysis:
    meaningful_pages: tuple[DiscoveryPageSuccess, ...]
    terminal_page: int | None
    speculative_empty_pages: int
    ignored_speculative_failures: int
    failed_page: int | None = None
    failure: BaseException | None = None


def classify_discovery_window(
    pages: Sequence[int],
    results: Sequence[list[DiscoveredEntity] | BaseException],
) -> DiscoveryWindowAnalysis:
    meaningful: list[DiscoveryPageSuccess] = []
    terminal_page: int | None = None
    speculative_empty_pages = 0
    ignored_failures = 0

    for page, result in zip(pages, results, strict=True):
        if terminal_page is not None:
            if isinstance(result, BaseException):
                ignored_failures += 1
            elif not result:
                speculative_empty_pages += 1
            continue
        if isinstance(result, BaseException):
            return DiscoveryWindowAnalysis(
                meaningful_pages=tuple(meaningful),
                terminal_page=None,
                speculative_empty_pages=0,
                ignored_speculative_failures=0,
                failed_page=page,
                failure=result,
            )
        if not result:
            terminal_page = page
            continue
        meaningful.append(DiscoveryPageSuccess(page=page, items=result))

    return DiscoveryWindowAnalysis(
        meaningful_pages=tuple(meaningful),
        terminal_page=terminal_page,
        speculative_empty_pages=speculative_empty_pages,
        ignored_speculative_failures=ignored_failures,
    )


class DiscoveryService:
    def __init__(
        self,
        adapter: SourceAdapter,
        frontier: FrontierPort,
        *,
        concurrency: int = 1,
        semaphore: asyncio.Semaphore | None = None,
    ) -> None:
        if not 1 <= concurrency <= 64:
            raise ValueError("discovery concurrency must be between 1 and 64")
        self.adapter = adapter
        self.frontier = frontier
        self.concurrency = concurrency
        self.semaphore = semaphore or asyncio.Semaphore(concurrency)
        self._persistence_lock = asyncio.Lock()
        DISCOVERY_CONFIGURED_CONCURRENCY.labels(
            source=self.adapter.source.value,
        ).set(concurrency)

    async def _fetch_page(
        self,
        entity_type: EntityType,
        *,
        page: int,
        priority: int,
        filters: dict | None,
    ) -> list[DiscoveredEntity]:
        async with self.semaphore:
            metric = DISCOVERY_ACTIVE_REQUESTS.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
            )
            metric.inc()
            try:
                return await self.adapter.discover(
                    entity_type,
                    page=page,
                    priority=priority,
                    filters=filters,
                )
            finally:
                metric.dec()

    async def _gather_window(
        self,
        page_tasks: Sequence[asyncio.Task],
        *,
        entity_type: EntityType,
        pages: Sequence[int],
        started: float,
    ) -> list[list[DiscoveredEntity] | BaseException]:
        gather_future = asyncio.gather(
            *page_tasks,
            return_exceptions=True,
        )
        try:
            while True:
                try:
                    return await asyncio.wait_for(
                        asyncio.shield(gather_future),
                        timeout=DISCOVERY_PROGRESS_INTERVAL_SECONDS,
                    )
                except TimeoutError:
                    completed = sum(task.done() for task in page_tasks)
                    logger.info(
                        "discovery_window_progress",
                        source=self.adapter.source.value,
                        entity_type=entity_type.value,
                        first_page=pages[0],
                        last_page=pages[-1],
                        completed_pages=completed,
                        pending_pages=len(page_tasks) - completed,
                        elapsed_seconds=round(time.monotonic() - started, 3),
                        effective_concurrency=len(pages),
                    )
        except asyncio.CancelledError:
            for task in page_tasks:
                task.cancel()
            await asyncio.gather(*page_tasks, return_exceptions=True)
            raise

    async def run(
        self,
        entity_type: EntityType,
        *,
        start_page: int | None = None,
        max_pages: int | None = None,
        priority: int = 0,
        filters: dict | None = None,
        scope: str = "all",
        resume: bool = True,
    ) -> int:
        get_checkpoint = getattr(self.frontier, "get_checkpoint", None)
        set_checkpoint = getattr(self.frontier, "set_checkpoint", None)
        if start_page is None and resume and get_checkpoint:
            page, completed = await get_checkpoint(
                self.adapter.source,
                entity_type,
                scope=scope,
            )
            if completed:
                return 0
        else:
            page = start_page or 1

        discovered_total = 0
        pages_processed = 0
        while max_pages is None or pages_processed < max_pages:
            remaining = (
                self.concurrency
                if max_pages is None
                else min(self.concurrency, max_pages - pages_processed)
            )
            pages = tuple(range(page, page + remaining))
            started = time.monotonic()
            DISCOVERY_EFFECTIVE_CONCURRENCY.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
            ).set(len(pages))
            DISCOVERY_PAGES.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
                kind="requested",
            ).inc(len(pages))
            logger.info(
                "discovery_window_started",
                source=self.adapter.source.value,
                entity_type=entity_type.value,
                first_page=pages[0],
                last_page=pages[-1],
                requested_pages=len(pages),
                effective_concurrency=len(pages),
            )
            page_tasks = [
                asyncio.create_task(
                    self._fetch_page(
                        entity_type,
                        page=current_page,
                        priority=priority,
                        filters=filters,
                    ),
                    name=(
                        f"discovery-{self.adapter.source.value}-"
                        f"{entity_type.value}-{current_page}"
                    ),
                )
                    for current_page in pages
            ]
            results = await self._gather_window(
                page_tasks,
                entity_type=entity_type,
                pages=pages,
                started=started,
            )
            analysis = classify_discovery_window(pages, results)
            if analysis.failure is not None:
                elapsed = time.monotonic() - started
                DISCOVERY_WINDOWS.labels(
                    source=self.adapter.source.value,
                    entity_type=entity_type.value,
                    outcome="failed",
                ).inc()
                DISCOVERY_WINDOW_DURATION.labels(
                    source=self.adapter.source.value,
                    entity_type=entity_type.value,
                    outcome="failed",
                ).observe(elapsed)
                logger.error(
                    "discovery_window_failed",
                    source=self.adapter.source.value,
                    entity_type=entity_type.value,
                    first_page=pages[0],
                    last_page=pages[-1],
                    failed_page=analysis.failed_page,
                    error_type=type(analysis.failure).__name__,
                    elapsed_seconds=round(elapsed, 6),
                    effective_concurrency=len(pages),
                )
                raise analysis.failure

            items = [
                item
                for page_result in analysis.meaningful_pages
                for item in page_result.items
            ]
            next_page = (
                analysis.terminal_page
                if analysis.terminal_page is not None
                else pages[-1] + 1
            )
            try:
                async with self._persistence_lock:
                    enqueued = await self.frontier.enqueue(items) if items else 0
                    if set_checkpoint:
                        await set_checkpoint(
                            self.adapter.source,
                            entity_type,
                            next_page=next_page,
                            completed=analysis.terminal_page is not None,
                            scope=scope,
                        )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                elapsed = time.monotonic() - started
                DISCOVERY_WINDOWS.labels(
                    source=self.adapter.source.value,
                    entity_type=entity_type.value,
                    outcome="failed",
                ).inc()
                DISCOVERY_WINDOW_DURATION.labels(
                    source=self.adapter.source.value,
                    entity_type=entity_type.value,
                    outcome="failed",
                ).observe(elapsed)
                logger.error(
                    "discovery_window_failed",
                    source=self.adapter.source.value,
                    entity_type=entity_type.value,
                    first_page=pages[0],
                    last_page=pages[-1],
                    failed_page=None,
                    error_type=type(exc).__name__,
                    elapsed_seconds=round(elapsed, 6),
                    effective_concurrency=len(pages),
                    stage="persistence",
                )
                raise
            discovered_total += enqueued
            DISCOVERED_ENTITIES.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
            ).inc(len(items))
            elapsed = time.monotonic() - started
            meaningful_count = len(analysis.meaningful_pages) + int(
                analysis.terminal_page is not None
            )
            requested_count = len(pages)
            speculative_count = requested_count - meaningful_count
            items_per_second = len(items) / elapsed if elapsed else 0.0
            DISCOVERY_WINDOWS.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
                outcome="success",
            ).inc()
            DISCOVERY_WINDOW_DURATION.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
                outcome="success",
            ).observe(elapsed)
            for kind, value in (
                ("meaningful", meaningful_count),
                ("speculative", speculative_count),
                ("speculative_empty", analysis.speculative_empty_pages),
                ("ignored_speculative_failure", analysis.ignored_speculative_failures),
            ):
                DISCOVERY_PAGES.labels(
                    source=self.adapter.source.value,
                    entity_type=entity_type.value,
                    kind=kind,
                ).inc(value)
            DISCOVERY_ENTITY_RATE.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
            ).observe(items_per_second)
            logger.info(
                "discovery_window_complete",
                source=self.adapter.source.value,
                entity_type=entity_type.value,
                first_page=pages[0],
                last_page=pages[-1],
                requested_pages=requested_count,
                meaningful_pages=meaningful_count,
                non_empty_pages=len(analysis.meaningful_pages),
                terminal_page=analysis.terminal_page,
                speculative_empty_pages=analysis.speculative_empty_pages,
                ignored_speculative_failures=(
                    analysis.ignored_speculative_failures
                ),
                items_discovered=len(items),
                items_enqueued=enqueued,
                elapsed_seconds=round(elapsed, 6),
                items_per_second=round(items_per_second, 3),
                effective_concurrency=len(pages),
            )
            pages_processed += requested_count
            page = next_page
            if analysis.terminal_page is not None:
                break
        return discovered_total


class WorkerService:
    def __init__(
        self,
        *,
        adapter: SourceAdapter,
        frontier: FrontierPort,
        entities: EntityRepository,
        worker_count: int,
        claim_batch_size: int,
        lease_seconds: int,
        backfill_capacity_percent: int,
        max_attempts: int,
        idle_grace_seconds: int = 5,
    ) -> None:
        self.adapter = adapter
        self.frontier = frontier
        self.entities = entities
        self.worker_count = worker_count
        self.claim_batch_size = claim_batch_size
        self.lease_seconds = lease_seconds
        self.backfill_workers = max(
            1,
            math.ceil(worker_count * backfill_capacity_percent / 100),
        )
        self.max_attempts = max_attempts
        self.idle_grace_seconds = max(0, idle_grace_seconds)
        self.shutdown_requested = asyncio.Event()
        self.worker_ids: set[str] = set()
        self._empty_since: float | None = None
        self._drain_lock = asyncio.Lock()

    def request_shutdown(self) -> None:
        if not self.shutdown_requested.is_set():
            logger.info("worker_shutdown_requested", source=self.adapter.source.value)
            self.shutdown_requested.set()

    async def run(self, *, once: bool = False, drain: bool = False) -> None:
        if once and drain:
            raise ValueError("once and drain modes are mutually exclusive")
        run_id = uuid.uuid4().hex
        logger.info(
            "worker_started",
            source=self.adapter.source.value,
            worker_count=self.worker_count,
            claim_batch_size=self.claim_batch_size,
            run_id=run_id,
        )
        workers = [
            asyncio.create_task(
                self._worker_loop(
                    worker_index=index,
                    run_id=run_id,
                    once=once,
                    drain=drain,
                ),
                name=f"{self.adapter.source.value}-worker-{index}",
            )
            for index in range(self.worker_count)
        ]
        try:
            await gather_fail_fast(workers)
        except BaseException:
            self.request_shutdown()
            raise
        finally:
            await asyncio.gather(
                *(
                    self.frontier.release_by_owner(worker_id)
                    for worker_id in tuple(self.worker_ids)
                ),
                return_exceptions=True,
            )
            logger.info(
                "worker_stopped",
                source=self.adapter.source.value,
                run_id=run_id,
            )

    async def _worker_loop(
        self,
        *,
        worker_index: int,
        run_id: str,
        once: bool,
        drain: bool,
    ) -> None:
        worker_id = f"{self.adapter.source.value}-{worker_index}-{uuid.uuid4().hex[:8]}"
        self.worker_ids.add(worker_id)
        is_backfill_worker = worker_index < self.backfill_workers
        try:
            while not self.shutdown_requested.is_set():
                tasks = await self.frontier.claim(
                    worker_id,
                    source=self.adapter.source,
                    limit=self.claim_batch_size,
                    lease_seconds=self.lease_seconds,
                    backfill_only=is_backfill_worker,
                )
                if not tasks and is_backfill_worker:
                    tasks = await self.frontier.claim(
                        worker_id,
                        source=self.adapter.source,
                        limit=self.claim_batch_size,
                        lease_seconds=self.lease_seconds,
                        backfill_only=False,
                    )
                if not tasks:
                    if once:
                        return
                    if drain and await self._drain_complete():
                        self.request_shutdown()
                        return
                    try:
                        await asyncio.wait_for(
                            self.shutdown_requested.wait(),
                            timeout=1,
                        )
                    except TimeoutError:
                        pass
                    continue
                async with self._drain_lock:
                    self._empty_since = None
                for index, task in enumerate(tasks):
                    if self.shutdown_requested.is_set():
                        released = await self.frontier.release(
                            tasks[index:],
                            worker_id=worker_id,
                        )
                        logger.info(
                            "worker_batch_released",
                            worker_id=worker_id,
                            released=released,
                        )
                        return
                    try:
                        await self._process_task(task, worker_id=worker_id, run_id=run_id)
                    except asyncio.CancelledError:
                        await asyncio.shield(
                            self.frontier.release(tasks[index:], worker_id=worker_id)
                        )
                        raise
                if once:
                    return
        finally:
            await asyncio.shield(self.frontier.release_by_owner(worker_id))
            self.worker_ids.discard(worker_id)

    async def _drain_complete(self) -> bool:
        activity = await self.frontier.activity(self.adapter.source)
        if activity.depth or activity.leased:
            async with self._drain_lock:
                self._empty_since = None
            return False
        now = time.monotonic()
        async with self._drain_lock:
            if self._empty_since is None:
                self._empty_since = now
                logger.info(
                    "worker_drain_idle_grace_started",
                    source=self.adapter.source.value,
                    idle_grace_seconds=self.idle_grace_seconds,
                )
                return self.idle_grace_seconds == 0
            return now - self._empty_since >= self.idle_grace_seconds

    async def _process_task(
        self,
        task: FrontierTask,
        *,
        worker_id: str,
        run_id: str,
    ) -> None:
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(
            run_id=run_id,
            task_id=task.id,
            worker_id=worker_id,
            source=task.identity.source.value,
            entity_type=task.identity.entity_type.value,
            source_entity_id=task.identity.source_entity_id,
            attempt=task.attempt,
        )
        heartbeat = asyncio.create_task(
            self._lease_heartbeat(task, worker_id=worker_id),
            name=f"lease-heartbeat-{task.id}",
        )
        started_at = time.monotonic()
        outcome = "cancelled"

        async def stop_heartbeat() -> None:
            if heartbeat.done():
                await asyncio.gather(heartbeat, return_exceptions=True)
                return
            heartbeat.cancel()
            await asyncio.gather(heartbeat, return_exceptions=True)

        try:
            batch = await self.adapter.extract(task.identity)
            self._validate_extracted_batch(batch, task)
            lease_extended = await self.frontier.extend_lease(
                task,
                worker_id=worker_id,
                lease_seconds=self.lease_seconds,
            )
            if not lease_extended:
                raise LeaseLostError(
                    f"Task {task.id} lease was lost before persistence"
                )
            await self.entities.persist(batch.entities, batch.relations)
            await self.frontier.enqueue(batch.discovered)
            content_hash = self._task_content_hash(batch.entities, task)
            await stop_heartbeat()
            await self.frontier.complete(task, content_hash=content_hash)
            TASK_OUTCOMES.labels(
                source=task.identity.source.value,
                entity_type=task.identity.entity_type.value,
                outcome="success",
            ).inc()
            outcome = "success"
            logger.info(
                "task_complete",
                entities=len(batch.entities),
                relations=len(batch.relations),
                discovered=len(batch.discovered),
            )
        except RetriableSourceError as exc:
            await stop_heartbeat()
            await self.frontier.retry(
                task,
                error=str(exc),
                delay_seconds=exc.delay_seconds,
                strategy=exc.strategy,
                http_status=exc.http_status,
            )
            TASK_OUTCOMES.labels(
                source=task.identity.source.value,
                entity_type=task.identity.entity_type.value,
                outcome="blocked",
            ).inc()
            outcome = "blocked"
            logger.warning("task_blocked", error=str(exc))
        except LeaseLostError as exc:
            await stop_heartbeat()
            TASK_OUTCOMES.labels(
                source=task.identity.source.value,
                entity_type=task.identity.entity_type.value,
                outcome="lease_lost",
            ).inc()
            outcome = "lease_lost"
            logger.warning("task_lease_lost", error=str(exc))
        except Exception as exc:
            await stop_heartbeat()
            response = getattr(exc, "response", None)
            http_status = getattr(response, "status_code", None)
            strategy = getattr(exc, "strategy", "unknown")
            if (
                is_permanent_http_status(http_status)
                or task.attempt >= self.max_attempts
            ):
                await self.frontier.fail(
                    task,
                    error=repr(exc),
                    strategy=strategy,
                    http_status=http_status,
                )
                TASK_OUTCOMES.labels(
                    source=task.identity.source.value,
                    entity_type=task.identity.entity_type.value,
                    outcome="failed",
                ).inc()
                outcome = "failed"
                logger.exception("task_failed_permanently")
                return
            delay = min(300, 5 * 2 ** max(0, task.attempt - 1))
            await self.frontier.retry(
                task,
                error=repr(exc),
                delay_seconds=delay,
                strategy=strategy,
                http_status=http_status,
            )
            TASK_OUTCOMES.labels(
                source=task.identity.source.value,
                entity_type=task.identity.entity_type.value,
                outcome="retry",
            ).inc()
            outcome = "retry"
            logger.exception("task_retry_scheduled", delay_seconds=delay)
        finally:
            await stop_heartbeat()
            TASK_DURATION.labels(
                source=task.identity.source.value,
                entity_type=task.identity.entity_type.value,
                outcome=outcome,
            ).observe(time.monotonic() - started_at)
            structlog.contextvars.clear_contextvars()

    async def _lease_heartbeat(
        self,
        task: FrontierTask,
        *,
        worker_id: str,
    ) -> None:
        interval = max(1, self.lease_seconds // 3)
        while True:
            await asyncio.sleep(interval)
            extended = await self.frontier.extend_lease(
                task,
                worker_id=worker_id,
                lease_seconds=self.lease_seconds,
            )
            if not extended:
                return

    @staticmethod
    def _validate_extracted_batch(
        batch,
        task: FrontierTask,
    ) -> None:
        entity_keys = [
            envelope.entity.identity.stable_key
            for envelope in batch.entities
        ]
        primary_count = entity_keys.count(task.identity.stable_key)
        if primary_count != 1:
            raise ValueError(
                "Extraction contract violation: expected exactly one primary "
                f"entity for {task.identity.stable_key}, got {primary_count}"
            )
        if len(entity_keys) != len(set(entity_keys)):
            raise ValueError("Extraction contract violation: duplicate entity identities")

        expected_source = task.identity.source
        if any(
            envelope.entity.identity.source != expected_source
            for envelope in batch.entities
        ):
            raise ValueError("Extraction contract violation: cross-source entity")
        if any(
            relation.source != expected_source
            or relation.parent.source != expected_source
            or relation.child.source != expected_source
            for relation in batch.relations
        ):
            raise ValueError("Extraction contract violation: cross-source relation")
        if any(
            item.identity.source != expected_source
            for item in batch.discovered
        ):
            raise ValueError("Extraction contract violation: cross-source discovery")

        primary = next(
            envelope
            for envelope in batch.entities
            if envelope.entity.identity.stable_key == task.identity.stable_key
        )
        if not primary.entity.source_payload:
            raise ValueError("Extraction contract violation: primary raw payload is empty")

    @staticmethod
    def _task_content_hash(
        envelopes: Sequence[EntityEnvelope],
        task: FrontierTask,
    ) -> str:
        for envelope in envelopes:
            if envelope.entity.identity.stable_key == task.identity.stable_key:
                return envelope.content_hash
        raise ValueError("Extraction contract violation: primary entity is missing")
