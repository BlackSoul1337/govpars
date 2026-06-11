from __future__ import annotations

import asyncio
import math
import time
import uuid
from collections.abc import Sequence

import structlog

from procurement_parser.domain.errors import (
    RetriableSourceError,
    is_permanent_http_status,
)
from procurement_parser.domain.models import EntityEnvelope, EntityType, FrontierTask
from procurement_parser.domain.ports import EntityRepository, FrontierPort, SourceAdapter
from procurement_parser.metrics import (
    DISCOVERED_ENTITIES,
    TASK_DURATION,
    TASK_OUTCOMES,
)

logger = structlog.get_logger()


class DiscoveryService:
    def __init__(self, adapter: SourceAdapter, frontier: FrontierPort) -> None:
        self.adapter = adapter
        self.frontier = frontier

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
            if completed and scope == "all":
                return 0
        else:
            page = start_page or 1

        discovered_total = 0
        pages_processed = 0
        while max_pages is None or pages_processed < max_pages:
            items = await self.adapter.discover(
                entity_type,
                page=page,
                priority=priority,
                filters=filters,
            )
            if not items:
                if set_checkpoint:
                    await set_checkpoint(
                        self.adapter.source,
                        entity_type,
                        next_page=page,
                        completed=True,
                        scope=scope,
                    )
                break
            discovered_total += await self.frontier.enqueue(items)
            DISCOVERED_ENTITIES.labels(
                source=self.adapter.source.value,
                entity_type=entity_type.value,
            ).inc(len(items))
            pages_processed += 1
            page += 1
            if set_checkpoint:
                await set_checkpoint(
                    self.adapter.source,
                    entity_type,
                    next_page=page,
                    completed=False,
                    scope=scope,
                )
            logger.info(
                "discovery_page_complete",
                source=self.adapter.source.value,
                entity_type=entity_type.value,
                page=page - 1,
                discovered=len(items),
                discovered_total=discovered_total,
            )
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
            await asyncio.gather(*workers)
        except asyncio.CancelledError:
            self.request_shutdown()
            await asyncio.gather(*workers, return_exceptions=True)
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
    def _task_content_hash(
        envelopes: Sequence[EntityEnvelope],
        task: FrontierTask,
    ) -> str | None:
        for envelope in envelopes:
            if envelope.entity.identity.stable_key == task.identity.stable_key:
                return envelope.content_hash
        return envelopes[0].content_hash if envelopes else None
