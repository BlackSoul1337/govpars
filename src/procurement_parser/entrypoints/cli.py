from __future__ import annotations

import asyncio
import json
import signal
import time
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any

import structlog
import typer

from procurement_parser.application.csv_validator import (
    validate_csv,
    validate_export_directory,
)
from procurement_parser.application.pipeline import (
    DiscoveryService,
    WorkerService,
    gather_fail_fast,
)
from procurement_parser.config.settings import (
    configure_discovery_concurrency,
    discovery_concurrency,
    load_settings,
)
from procurement_parser.domain.models import EntityType, Source
from procurement_parser.entrypoints.runtime import build_context
from procurement_parser.entrypoints.scheduler import run_scheduler
from procurement_parser.infrastructure.captcha.solvers import build_captcha_solver
from procurement_parser.infrastructure.network.proxy_pool import (
    build_proxy_pool,
    check_proxy_pool,
)
from procurement_parser.infrastructure.persistence.postgres.database import Database
from procurement_parser.infrastructure.persistence.postgres.exporter import (
    PostgresCsvExporter,
)
from procurement_parser.infrastructure.persistence.postgres.maintenance import (
    PostgresMaintenance,
)
from procurement_parser.infrastructure.sources.eep_mitwork.adapter import EepMitworkAdapter
from procurement_parser.infrastructure.sources.zakup_sk.adapter import ZakupSkAdapter
from procurement_parser.infrastructure.sources.zakup_sk.bundle_cache import (
    ZakupBundleCache,
)
from procurement_parser.metrics import run_database_sampler, start_metrics_server
from procurement_parser.observability import configure_logging

app = typer.Typer(no_args_is_help=True)
logger = structlog.get_logger()


def _install_signal_handlers(callback: Callable[[], None]) -> Callable[[], None]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    previous: dict[signal.Signals, object] = {}
    for current_signal in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(current_signal, callback)
            installed.append(current_signal)
        except (NotImplementedError, RuntimeError):
            previous[current_signal] = signal.getsignal(current_signal)

            def handler(_signum, _frame, *, notify=callback) -> None:
                loop.call_soon_threadsafe(notify)

            signal.signal(current_signal, handler)

    def remove() -> None:
        for current_signal in installed:
            loop.remove_signal_handler(current_signal)
        for current_signal, previous_handler in previous.items():
            signal.signal(current_signal, previous_handler)

    return remove


def _source(value: str) -> Source:
    aliases = {
        "eep": Source.EEP_MITWORK,
        "eep-mitwork": Source.EEP_MITWORK,
        "zakup": Source.ZAKUP_SK,
        "zakup-sk": Source.ZAKUP_SK,
    }
    try:
        return aliases[value]
    except KeyError as exc:
        raise typer.BadParameter(f"Unknown source: {value}") from exc


def _sources(value: str) -> list[Source]:
    if value == "all":
        return [Source.EEP_MITWORK, Source.ZAKUP_SK]
    return [_source(value)]


async def _run_discovery_catalogs(
    service: DiscoveryService,
    source: Source,
    entity_types: list[EntityType] | tuple[EntityType, ...],
    **run_kwargs,
) -> int:
    if source == Source.ZAKUP_SK:
        total = 0
        for entity_type in entity_types:
            total += await service.run(entity_type, **run_kwargs)
        return total

    results = await asyncio.gather(
        *(service.run(entity_type, **run_kwargs) for entity_type in entity_types),
        return_exceptions=True,
    )
    failures = [result for result in results if isinstance(result, BaseException)]
    if failures:
        raise failures[0]
    return sum(result for result in results if isinstance(result, int))


def _network_for_source(
    source: Source,
    *,
    network: str,
    eep_network: str | None,
    zakup_network: str | None,
) -> str:
    if source == Source.EEP_MITWORK:
        return eep_network or network
    return zakup_network or network


def _entity_types(value: str, source: Source) -> list[EntityType]:
    if value != "all":
        return [EntityType(value)]
    result = [EntityType.LOT, EntityType.NOTICE]
    if source == Source.EEP_MITWORK:
        result.append(EntityType.PLAN_ITEM)
    return result


def _profile_list(value: str | None, fallback: str) -> list[str]:
    if not value:
        return [fallback]
    result = list(dict.fromkeys(item.strip() for item in value.split(",") if item.strip()))
    if not result:
        raise typer.BadParameter("Profile list cannot be empty")
    return result


def _progress(message: str, *, enabled: bool) -> None:
    if enabled:
        typer.echo(f"[catalog-stats] {message}", err=True)


def _render_json_result(document: dict[str, Any], output: Path | None = None) -> str:
    payload = json.dumps(document, ensure_ascii=False, indent=2)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(f"{output.suffix}.tmp")
        temporary.write_text(f"{payload}\n", encoding="utf-8")
        temporary.replace(output)
    return payload


async def _await_with_progress(
    awaitable: Awaitable,
    *,
    label: str,
    enabled: bool,
    interval_seconds: int,
    timeout_seconds: int | None = None,
):
    started = time.monotonic()
    _progress(f"START {label}", enabled=enabled)
    task = asyncio.create_task(awaitable)
    while True:
        done, _ = await asyncio.wait({task}, timeout=interval_seconds)
        elapsed = time.monotonic() - started
        if not done:
            _progress(
                f"WAIT  {label} elapsed={elapsed:.1f}s",
                enabled=enabled,
            )
            if timeout_seconds is not None and elapsed >= timeout_seconds:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                error = TimeoutError(
                    f"Probe exceeded {timeout_seconds} seconds"
                )
                _progress(
                    f"FAIL  {label} elapsed={elapsed:.1f}s error={error}",
                    enabled=enabled,
                )
                raise error
            continue
        try:
            result = task.result()
        except Exception as exc:
            _progress(
                f"FAIL  {label} elapsed={elapsed:.1f}s "
                f"error={type(exc).__name__}: {exc}",
                enabled=enabled,
            )
            raise
        _progress(
            f"DONE  {label} elapsed={elapsed:.1f}s",
            enabled=enabled,
        )
        return result


def _duration_parts(seconds: float | None) -> dict | None:
    if seconds is None:
        return None
    rounded = round(seconds, 1)
    return {
        "seconds": rounded,
        "hours": round(rounded / 3600, 2),
        "days": round(rounded / 86400, 2),
    }


def _full_run_forecasts(results: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str, str], list[dict]] = {}
    for item in results:
        key_values = (
            item.get("source"),
            item.get("runtime"),
            item.get("network"),
        )
        if not all(isinstance(value, str) for value in key_values):
            continue
        grouped.setdefault(key_values, []).append(item)

    forecasts = []
    for (source, runtime, network), items in grouped.items():
        usable = [
            item
            for item in items
            if isinstance(item.get("estimated_discovery_seconds"), (int, float))
            and isinstance(
                (item.get("worker_probe") or {}).get("estimated_detail_seconds"),
                (int, float),
            )
        ]
        discovery_seconds = sum(
            item["estimated_discovery_seconds"] for item in usable
        )
        detail_seconds = sum(
            item["worker_probe"]["estimated_detail_seconds"] for item in usable
        )
        forecasts.append(
            {
                "source": source,
                "runtime": runtime,
                "network": network,
                "complete": len(usable) == len(items) and bool(items),
                "catalogs_measured": len(usable),
                "catalogs_expected": len(items),
                "discovery": _duration_parts(discovery_seconds if usable else None),
                "detail": _duration_parts(detail_seconds if usable else None),
                "full_cycle": _duration_parts(
                    discovery_seconds + detail_seconds if usable else None
                ),
            }
        )
    return forecasts


def _combined_forecasts(forecasts: list[dict]) -> list[dict]:
    grouped: dict[tuple[str, str], list[dict]] = {}
    for item in forecasts:
        grouped.setdefault((item["runtime"], item["network"]), []).append(item)

    combined = []
    for (runtime, network), items in grouped.items():
        durations = [
            item["full_cycle"]["seconds"]
            for item in items
            if item["complete"] and item["full_cycle"] is not None
        ]
        combined.append(
            {
                "runtime": runtime,
                "network": network,
                "complete": len(durations) == len(items) and bool(items),
                "sources_measured": len(durations),
                "sources_expected": len(items),
                "parallel_wall_clock": _duration_parts(
                    max(durations) if durations else None
                ),
                "sequential_total": _duration_parts(
                    sum(durations) if durations else None
                ),
            }
        )
    return combined


def _catalog_stats_summary(
    results: list[dict],
    forecasts: list[dict],
    combined_forecasts: list[dict],
    totals_by_source: dict[str, int],
) -> dict:
    worker_probes = [
        item["worker_probe"]
        for item in results
        if isinstance(item.get("worker_probe"), dict)
    ]
    requested = sum(item.get("requested", 0) for item in worker_probes)
    succeeded = sum(item.get("succeeded", 0) for item in worker_probes)
    complete_combined = [
        item
        for item in combined_forecasts
        if item.get("complete") and item.get("parallel_wall_clock")
    ]
    ranked = sorted(
        complete_combined,
        key=lambda item: item["parallel_wall_clock"]["seconds"],
    )
    fastest = ranked[0] if ranked else None
    slowest = ranked[-1] if ranked else None
    per_source = {}
    for source in totals_by_source:
        candidates = [
            item
            for item in forecasts
            if item.get("source") == source
            and item.get("complete")
            and item.get("full_cycle")
        ]
        if candidates:
            best = min(
                candidates,
                key=lambda item: item["full_cycle"]["seconds"],
            )
            per_source[source] = {
                "catalog_entries": totals_by_source[source],
                "fastest_measured_profile": {
                    "runtime": best["runtime"],
                    "network": best["network"],
                },
                "estimated_full_cycle": best["full_cycle"],
            }
        else:
            per_source[source] = {
                "catalog_entries": totals_by_source[source],
                "fastest_measured_profile": None,
                "estimated_full_cycle": None,
            }

    return {
        "status": (
            "complete"
            if complete_combined
            and all(item.get("complete") for item in combined_forecasts)
            else "partial"
        ),
        "workload": {
            "catalog_entries_total": sum(totals_by_source.values()),
            "by_source": per_source,
        },
        "sample_quality": {
            "catalog_measurements": len(results),
            "worker_details_requested": requested,
            "worker_details_succeeded": succeeded,
            "worker_success_rate": (
                round(succeeded / requested, 4) if requested else None
            ),
            "confidence": "low",
            "reason": (
                "The estimate extrapolates a small live sample and excludes "
                "retries, PostgreSQL writes, refreshes and reconciliation."
            ),
        },
        "time_estimate_for_both_sources": {
            "fastest_measured_profile": (
                {
                    "runtime": fastest["runtime"],
                    "network": fastest["network"],
                }
                if fastest
                else None
            ),
            "parallel_wall_clock": (
                fastest["parallel_wall_clock"] if fastest else None
            ),
            "sequential_total": fastest["sequential_total"] if fastest else None,
            "measured_range_parallel": {
                "minimum": fastest["parallel_wall_clock"] if fastest else None,
                "maximum": slowest["parallel_wall_clock"] if slowest else None,
            },
        },
        "plain_text": (
            f"Measured {sum(totals_by_source.values()):,} catalog entries. "
            f"Worker probes succeeded {succeeded}/{requested}. "
            + (
                "The fastest measured full run of both sources in parallel is "
                f"about {fastest['parallel_wall_clock']['days']} days using "
                f"{fastest['runtime']} + {fastest['network']}. "
                f"The measured parallel range is "
                f"{fastest['parallel_wall_clock']['days']}-"
                f"{slowest['parallel_wall_clock']['days']} days. "
                "Treat this as a low-confidence benchmark, not an SLA."
                if fastest and slowest
                else "A complete full-run estimate is unavailable."
            )
        ),
    }


async def _detail_probe(
    adapter,
    identities,
    *,
    limit: int,
    concurrency: int,
    progress_enabled: bool = False,
    progress_interval_seconds: int = 10,
    probe_timeout_seconds: int | None = None,
) -> dict:
    selected = identities[:limit]
    if not selected:
        return {
            "requested": 0,
            "succeeded": 0,
            "failed": 0,
            "elapsed_seconds": 0.0,
            "details_per_second": None,
            "items": [],
        }

    semaphore = asyncio.Semaphore(max(1, concurrency))

    async def extract(index, identity):
        async with semaphore:
            started = time.monotonic()
            try:
                batch = await _await_with_progress(
                    adapter.extract(identity),
                    label=(
                        f"worker {index}/{len(selected)} "
                        f"{identity.entity_type.value}:{identity.source_entity_id}"
                    ),
                    enabled=progress_enabled,
                    interval_seconds=progress_interval_seconds,
                    timeout_seconds=probe_timeout_seconds,
                )
            except Exception as exc:
                return {
                    "source_entity_id": identity.source_entity_id,
                    "success": False,
                    "elapsed_seconds": round(time.monotonic() - started, 3),
                    "error": f"{type(exc).__name__}: {exc}",
                }
            return {
                "source_entity_id": identity.source_entity_id,
                "success": True,
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "entities": len(batch.entities),
                "relations": len(batch.relations),
                "discovered": len(batch.discovered),
            }

    started = time.monotonic()
    items = await asyncio.gather(
        *(
            extract(index, identity)
            for index, identity in enumerate(selected, start=1)
        )
    )
    elapsed = time.monotonic() - started
    succeeded = sum(1 for item in items if item["success"])
    return {
        "requested": len(selected),
        "succeeded": succeeded,
        "failed": len(selected) - succeeded,
        "concurrency": max(1, concurrency),
        "elapsed_seconds": round(elapsed, 3),
        "details_per_second": round(succeeded / elapsed, 3) if elapsed else None,
        "items": items,
    }


def _settings(
    source_name: str,
    runtime: str,
    network: str,
    captcha: str,
):
    settings = load_settings(
        source=_source(source_name),
        runtime_profile=runtime,
        network_profile=network,
        captcha_profile=captcha,
    )
    configure_logging(
        settings.app.log_level,
        log_to_file=settings.app.log_to_file,
        log_dir=settings.app.log_dir,
        log_filename=settings.app.log_filename,
        log_max_bytes=settings.app.log_max_bytes,
        log_backup_count=settings.app.log_backup_count,
    )
    return settings


@app.command()
def discover(
    source: Annotated[str, typer.Option()] = "eep-mitwork",
    entity_type: Annotated[str, typer.Option()] = "all",
    pages: Annotated[int, typer.Option(help="0 means until the list is exhausted")] = 0,
    start_page: Annotated[int | None, typer.Option()] = None,
    priority: Annotated[int, typer.Option()] = 0,
    filters_json: Annotated[
        str | None,
        typer.Option(help="Source-specific discovery filters as a JSON object"),
    ] = None,
    runtime: Annotated[str, typer.Option()] = "local",
    network: Annotated[str, typer.Option()] = "direct",
    eep_network: Annotated[str | None, typer.Option()] = None,
    zakup_network: Annotated[str | None, typer.Option()] = None,
    captcha: Annotated[str, typer.Option()] = "disabled",
    resume: Annotated[bool, typer.Option()] = True,
    discovery_concurrency_override: Annotated[
        int | None,
        typer.Option(
            "--discovery-concurrency",
            min=1,
            max=64,
            help="EEP list request concurrency; Zakup always remains sequential.",
        ),
    ] = None,
) -> None:
    async def execute() -> None:
        contexts = []
        try:
            filters = json.loads(filters_json) if filters_json else None
            if filters is not None and not isinstance(filters, dict):
                raise typer.BadParameter("--filters-json must contain a JSON object")
            scope = (
                f"filters:{json.dumps(filters, sort_keys=True, ensure_ascii=True)}"
                if filters
                else "all"
            )
            jobs = []
            selected_sources = _sources(source)
            for current_source in selected_sources:
                current_network = _network_for_source(
                    current_source,
                    network=network,
                    eep_network=eep_network,
                    zakup_network=zakup_network,
                )
                current_captcha = (
                    captcha
                    if current_source == Source.ZAKUP_SK
                    else "disabled"
                )
                settings = _settings(
                    current_source.value,
                    runtime,
                    current_network,
                    current_captcha,
                )
                current_discovery_concurrency = configure_discovery_concurrency(
                    settings,
                    discovery_concurrency_override,
                )
                selected_entity_types = _entity_types(
                    entity_type,
                    current_source,
                )
                typer.echo(
                    f"{current_source.value}: discovery starting "
                    f"(types={','.join(value.value for value in selected_entity_types)}, "
                    f"pages={'until exhausted' if pages == 0 else pages}, "
                    f"concurrency={current_discovery_concurrency})"
                )
                context = build_context(settings)
                contexts.append(context)

                async def run_source(
                    *,
                    current_context=context,
                    current_source=current_source,
                    current_discovery_concurrency=current_discovery_concurrency,
                    current_entity_types=tuple(selected_entity_types),
                ) -> tuple[Source, int]:
                    service = DiscoveryService(
                        current_context.adapter,
                        current_context.frontier,
                        concurrency=current_discovery_concurrency,
                    )
                    total = await _run_discovery_catalogs(
                        service,
                        current_source,
                        current_entity_types,
                        start_page=start_page,
                        max_pages=pages or None,
                        priority=priority,
                        filters=filters,
                        scope=scope,
                        resume=resume,
                    )
                    return current_source, total

                jobs.append(run_source())
            results = await asyncio.gather(*jobs, return_exceptions=True)
            failed = False
            for result in results:
                if isinstance(result, BaseException):
                    failed = True
                    logger.exception(
                        "discovery_source_failed",
                        error=repr(result),
                    )
                    typer.echo(f"Discovery failed: {result}", err=True)
                    continue
                completed_source, total = result
                typer.echo(
                    f"{completed_source.value}: enqueued {total} entities"
                )
            if failed:
                raise typer.Exit(code=1)
        finally:
            await asyncio.gather(
                *(context.close() for context in contexts),
                return_exceptions=True,
            )

    asyncio.run(execute())


@app.command()
def worker(
    source: Annotated[str, typer.Option()] = "eep-mitwork",
    once: Annotated[bool, typer.Option()] = False,
    drain: Annotated[bool, typer.Option()] = False,
    idle_grace_seconds: Annotated[int, typer.Option(min=0)] = 5,
    runtime: Annotated[str, typer.Option()] = "local",
    network: Annotated[str, typer.Option()] = "direct",
    eep_network: Annotated[str | None, typer.Option()] = None,
    zakup_network: Annotated[str | None, typer.Option()] = None,
    captcha: Annotated[str, typer.Option()] = "disabled",
) -> None:
    if once and drain:
        raise typer.BadParameter("--once and --drain are mutually exclusive")

    async def execute() -> None:
        contexts = []
        services = []
        sampler_stop = asyncio.Event()
        sampler_task = None
        try:
            selected_sources = _sources(source)
            for current_source in selected_sources:
                current_network = _network_for_source(
                    current_source,
                    network=network,
                    eep_network=eep_network,
                    zakup_network=zakup_network,
                )
                current_captcha = (
                    captcha
                    if current_source == Source.ZAKUP_SK
                    else "disabled"
                )
                settings = _settings(
                    current_source.value,
                    runtime,
                    current_network,
                    current_captcha,
                )
                if not contexts:
                    start_metrics_server(settings.app.metrics_port)
                context = build_context(settings)
                contexts.append(context)
                services.append(
                    WorkerService(
                        adapter=context.adapter,
                        frontier=context.frontier,
                        entities=context.entities,
                        worker_count=settings.runtime.worker_count,
                        claim_batch_size=settings.runtime.claim_batch_size,
                        lease_seconds=settings.runtime.lease_seconds,
                        backfill_capacity_percent=(
                            settings.runtime.backfill_capacity_percent
                        ),
                        max_attempts=settings.source.max_attempts,
                        idle_grace_seconds=idle_grace_seconds,
                    )
                )
            sampler_task = asyncio.create_task(
                run_database_sampler(
                    PostgresMaintenance(contexts[0].database),
                    stop_event=sampler_stop,
                    interval_seconds=contexts[0].settings.runtime.metrics_sample_seconds,
                ),
                name="database-metrics-sampler",
            )

            def request_shutdown() -> None:
                for current_service in services:
                    current_service.request_shutdown()

            remove_signal_handlers = _install_signal_handlers(
                request_shutdown
            )
            try:
                results = await asyncio.gather(
                    *(
                        service.run(once=once, drain=drain)
                        for service in services
                    ),
                    return_exceptions=True,
                )
                failures = [
                    result
                    for result in results
                    if isinstance(result, BaseException)
                ]
                for failure in failures:
                    logger.error(
                        "worker_source_failed",
                        error=repr(failure),
                    )
                if failures:
                    raise typer.Exit(code=1)
            finally:
                remove_signal_handlers()
        finally:
            sampler_stop.set()
            if sampler_task:
                await asyncio.gather(sampler_task, return_exceptions=True)
            await asyncio.gather(
                *(context.close() for context in contexts),
                return_exceptions=True,
            )

    asyncio.run(execute())


@app.command()
def run(
    source: Annotated[str, typer.Option()] = "eep-mitwork",
    runtime: Annotated[str, typer.Option()] = "local",
    network: Annotated[str, typer.Option()] = "direct",
    eep_network: Annotated[str | None, typer.Option()] = None,
    zakup_network: Annotated[str | None, typer.Option()] = None,
    captcha: Annotated[str, typer.Option()] = "disabled",
    pages: Annotated[int, typer.Option(help="0 means until exhausted")] = 0,
    drain: Annotated[bool, typer.Option()] = False,
    idle_grace_seconds: Annotated[int, typer.Option(min=0)] = 5,
    discovery_concurrency_override: Annotated[
        int | None,
        typer.Option(
            "--discovery-concurrency",
            min=1,
            max=64,
            help="EEP list request concurrency; Zakup always remains sequential.",
        ),
    ] = None,
) -> None:
    async def execute() -> None:
        contexts = []
        workers = []
        sampler_stop = asyncio.Event()
        sampler_task = None
        try:
            pipelines = []
            for current_source in _sources(source):
                current_network = _network_for_source(
                    current_source,
                    network=network,
                    eep_network=eep_network,
                    zakup_network=zakup_network,
                )
                current_captcha = (
                    captcha
                    if current_source == Source.ZAKUP_SK
                    else "disabled"
                )
                settings = _settings(
                    current_source.value,
                    runtime,
                    current_network,
                    current_captcha,
                )
                current_discovery_concurrency = configure_discovery_concurrency(
                    settings,
                    discovery_concurrency_override,
                )
                if not contexts:
                    start_metrics_server(settings.app.metrics_port)
                context = build_context(settings)
                contexts.append(context)
                discovery = DiscoveryService(
                    context.adapter,
                    context.frontier,
                    concurrency=current_discovery_concurrency,
                )
                async def run_source_discovery(
                    *,
                    current_discovery=discovery,
                    current_source=current_source,
                ) -> int:
                    return await _run_discovery_catalogs(
                        current_discovery,
                        current_source,
                        _entity_types("all", current_source),
                        max_pages=pages or None,
                    )
                current_worker = WorkerService(
                    adapter=context.adapter,
                    frontier=context.frontier,
                    entities=context.entities,
                    worker_count=settings.runtime.worker_count,
                    claim_batch_size=settings.runtime.claim_batch_size,
                    lease_seconds=settings.runtime.lease_seconds,
                    backfill_capacity_percent=(
                        settings.runtime.backfill_capacity_percent
                    ),
                    max_attempts=settings.source.max_attempts,
                    idle_grace_seconds=idle_grace_seconds,
                )
                workers.append(current_worker)
                pipelines.append(
                    (
                        current_source,
                        run_source_discovery,
                        current_worker,
                    )
                )
            sampler_task = asyncio.create_task(
                run_database_sampler(
                    PostgresMaintenance(contexts[0].database),
                    stop_event=sampler_stop,
                    interval_seconds=contexts[0].settings.runtime.metrics_sample_seconds,
                ),
                name="database-metrics-sampler",
            )

            def request_shutdown() -> None:
                for current_worker in workers:
                    current_worker.request_shutdown()

            remove_signal_handlers = _install_signal_handlers(
                request_shutdown
            )
            try:
                async def run_pipeline(
                    current_source: Source,
                    current_discovery_job,
                    current_worker: WorkerService,
                ) -> list[BaseException]:
                    if drain:
                        discovery_results = await asyncio.gather(
                            current_discovery_job(),
                            return_exceptions=True,
                        )
                        failures = [
                            result
                            for result in discovery_results
                            if isinstance(result, BaseException)
                        ]
                        if failures:
                            return failures
                        worker_result = await asyncio.gather(
                            current_worker.run(drain=True),
                            return_exceptions=True,
                        )
                        return [
                            result
                            for result in worker_result
                            if isinstance(result, BaseException)
                        ]
                    try:
                        await gather_fail_fast(
                            [
                                current_discovery_job(),
                                current_worker.run(),
                            ]
                        )
                    except asyncio.CancelledError:
                        raise
                    except BaseException as exc:
                        return [exc]
                    return []

                pipeline_results = await asyncio.gather(
                    *(
                        run_pipeline(
                            current_source,
                            discovery_jobs,
                            current_worker,
                        )
                        for (
                            current_source,
                            discovery_jobs,
                            current_worker,
                        ) in pipelines
                    ),
                    return_exceptions=True,
                )
                failures = []
                for result in pipeline_results:
                    if isinstance(result, BaseException):
                        failures.append(result)
                    else:
                        failures.extend(result)
                for failure in failures:
                    logger.error(
                        "run_source_failed",
                        error=repr(failure),
                    )
                if failures:
                    raise typer.Exit(code=1)
            finally:
                remove_signal_handlers()
        finally:
            sampler_stop.set()
            if sampler_task:
                await asyncio.gather(sampler_task, return_exceptions=True)
            await asyncio.gather(
                *(context.close() for context in contexts),
                return_exceptions=True,
            )

    asyncio.run(execute())


@app.command()
def export(
    dataset: Annotated[
        str,
        typer.Option(
            help=(
                "lots, notices, plan_items, organizations, relations, "
                "delivery_places, payment_terms, documents, or all"
            )
        ),
    ] = "lots",
    output: Annotated[
        Path,
        typer.Option(help="CSV path, or output directory when --dataset all"),
    ] = Path("exports/lots.csv"),
    layout: Annotated[
        str,
        typer.Option(help="combined, split, or both"),
    ] = "combined",
    delimiter: Annotated[
        str,
        typer.Option(help="comma or semicolon; use semicolon for ru-RU Excel"),
    ] = "comma",
    excel_safe: Annotated[
        bool,
        typer.Option(
            "--excel-safe/--raw-csv",
            help=(
                "Prefix spreadsheet formula-like text with an apostrophe. "
                "Use --raw-csv only for machine-to-machine exports."
            ),
        ),
    ] = True,
) -> None:
    async def execute() -> None:
        if layout not in {"combined", "split", "both"}:
            raise typer.BadParameter("--layout must be combined, split, or both")
        delimiter_chars = {"comma": ",", "semicolon": ";"}
        if delimiter not in delimiter_chars:
            raise typer.BadParameter("--delimiter must be comma or semicolon")
        settings = _settings("eep-mitwork", "local", "direct", "disabled")
        context = build_context(settings)
        exported_files: dict[str, int] = {}
        try:
            exporter = PostgresCsvExporter(
                context.database,
                delimiter=delimiter_chars[delimiter],
                excel_safe=excel_safe,
            )
            output_dir = output if output.suffix == "" else output.parent
            if dataset == "all":
                if layout == "both":
                    results = await exporter.export_all_both(output_dir)
                    for key, count in results.items():
                        parts = key.split(":")
                        if parts[0] == "combined":
                            destination = output_dir / f"{parts[1]}.csv"
                        else:
                            source_slug = parts[2].replace("-", "_")
                            destination = (
                                output_dir / f"{parts[1]}_{source_slug}.csv"
                            )
                        exported_files[str(destination)] = count
                        typer.echo(f"Exported {count} rows to {destination}")
                elif layout == "combined":
                    results = await exporter.export_all(output_dir)
                    for name, count in results.items():
                        destination = output_dir / f"{name}.csv"
                        exported_files[str(destination)] = count
                        typer.echo(f"Exported {count} rows to {destination}")
                elif layout == "split":
                    results = await exporter.export_all_split(output_dir)
                    for key, count in results.items():
                        name, source = key.split(":", 1)
                        source_slug = source.replace("-", "_")
                        destination = output_dir / f"{name}_{source_slug}.csv"
                        exported_files[str(destination)] = count
                        typer.echo(
                            f"Exported {count} rows to "
                            f"{destination}"
                        )
            else:
                if layout == "both":
                    results = await exporter.export_both(
                        dataset,
                        output,
                        output_dir,
                    )
                    exported_files[str(output)] = results["combined"]
                    typer.echo(
                        f"Exported {results['combined']} rows to {output}"
                    )
                    for source in ("eep-mitwork", "zakup-sk"):
                        source_slug = source.replace("-", "_")
                        destination = (
                            output_dir / f"{dataset}_{source_slug}.csv"
                        )
                        count = results[f"split:{source}"]
                        exported_files[str(destination)] = count
                        typer.echo(f"Exported {count} rows to {destination}")
                elif layout == "combined":
                    count = await exporter.export(dataset, output)
                    exported_files[str(output)] = count
                    typer.echo(f"Exported {count} rows to {output}")
                elif layout == "split":
                    results = await exporter.export_split(dataset, output_dir)
                    for source, count in results.items():
                        source_slug = source.replace("-", "_")
                        destination = output_dir / f"{dataset}_{source_slug}.csv"
                        exported_files[str(destination)] = count
                        typer.echo(f"Exported {count} rows to {destination}")
            manifest_path = output_dir / "export_manifest.json"
            manifest = {
                "generated_at": datetime.now(UTC).isoformat(),
                "dataset": dataset,
                "layout": layout,
                "delimiter": delimiter,
                "excel_safe": excel_safe,
                "files": exported_files,
            }
            await asyncio.to_thread(
                _write_json_atomic,
                manifest_path,
                manifest,
            )
            typer.echo(f"Wrote export manifest to {manifest_path}")
        finally:
            await context.close()

    asyncio.run(execute())


@app.command("validate-export")
def validate_export(
    input_path: Annotated[
        Path,
        typer.Option("--input", exists=True),
    ] = Path("exports"),
) -> None:
    result = (
        validate_export_directory(input_path)
        if input_path.is_dir()
        else validate_csv(input_path)
    )
    typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
    if not result["valid"]:
        raise typer.Exit(code=1)


@app.command("catalog-stats")
def catalog_stats(
    source: Annotated[str, typer.Option()] = "all",
    samples: Annotated[int, typer.Option(min=1, max=10)] = 1,
    worker_samples: Annotated[int, typer.Option(min=0, max=20)] = 0,
    runtime: Annotated[str, typer.Option()] = "local",
    network: Annotated[str, typer.Option()] = "direct",
    runtime_profiles: Annotated[str | None, typer.Option()] = None,
    network_profiles: Annotated[str | None, typer.Option()] = None,
    eep_network: Annotated[str | None, typer.Option()] = None,
    zakup_network: Annotated[str | None, typer.Option()] = None,
    captcha: Annotated[str, typer.Option()] = "disabled",
    progress: Annotated[bool, typer.Option()] = True,
    progress_interval_seconds: Annotated[
        int,
        typer.Option(min=1, max=300),
    ] = 10,
    probe_timeout_seconds: Annotated[
        int,
        typer.Option(min=10, max=3600),
    ] = 600,
    output: Annotated[
        Path | None,
        typer.Option(help="Save the complete JSON report to this file."),
    ] = None,
) -> None:
    async def execute() -> None:
        configure_logging("WARNING", log_to_file=False)
        results = []
        _progress(
            f"matrix sources={source} samples={samples} "
            f"worker_samples={worker_samples} "
            f"runtime={runtime_profiles or runtime} "
            f"network={network_profiles or network}",
            enabled=progress,
        )
        for current_source in _sources(source):
            default_network = _network_for_source(
                current_source,
                network=network,
                eep_network=eep_network,
                zakup_network=zakup_network,
            )
            for current_runtime in _profile_list(runtime_profiles, runtime):
                for current_network in _profile_list(
                    network_profiles,
                    default_network,
                ):
                    combination = (
                        f"source={current_source.value} "
                        f"runtime={current_runtime} network={current_network}"
                    )
                    _progress(f"CONFIG {combination}", enabled=progress)
                    try:
                        settings = load_settings(
                            source=current_source,
                            runtime_profile=current_runtime,
                            network_profile=current_network,
                            captcha_profile=(
                                captcha
                                if current_source == Source.ZAKUP_SK
                                else "disabled"
                            ),
                        )
                    except Exception as exc:
                        _progress(
                            f"SKIP  {combination} error={type(exc).__name__}: {exc}",
                            enabled=progress,
                        )
                        results.append(
                            {
                                "source": current_source.value,
                                "runtime": current_runtime,
                                "network": current_network,
                                "error": f"{type(exc).__name__}: {exc}",
                            }
                        )
                        continue
                    if (
                        settings.network.kind != "direct"
                        and not settings.network.proxy_url
                        and not settings.network.proxy_pool_urls
                    ):
                        _progress(
                            f"SKIP  {combination} no proxy configured",
                            enabled=progress,
                        )
                        results.append(
                            {
                                "source": current_source.value,
                                "runtime": current_runtime,
                                "network": current_network,
                                "error": (
                                    "Network profile has no PROXY_URL or usable "
                                    "proxy pool"
                                ),
                            }
                        )
                        continue
                    solver = build_captcha_solver(settings.captcha, None)
                    adapter = (
                        EepMitworkAdapter(settings.source, settings.network)
                        if current_source == Source.EEP_MITWORK
                        else ZakupSkAdapter(
                            settings.source,
                            settings.network,
                            settings.captcha,
                            solver,
                            settings.runtime,
                        )
                    )
                    entity_types = (
                        [
                            EntityType.LOT,
                            EntityType.NOTICE,
                            EntityType.PLAN_ITEM,
                        ]
                        if current_source == Source.EEP_MITWORK
                        else [EntityType.LOT, EntityType.NOTICE]
                    )
                    try:
                        for current_type in entity_types:
                            catalog_label = (
                                f"{combination} catalog={current_type.value} "
                                f"list_pages={samples}"
                            )
                            try:
                                probe = await _await_with_progress(
                                    adapter.probe_catalog(
                                        current_type,
                                        samples=samples,
                                        concurrency=discovery_concurrency(settings),
                                    ),
                                    label=catalog_label,
                                    enabled=progress,
                                    interval_seconds=progress_interval_seconds,
                                    timeout_seconds=probe_timeout_seconds,
                                )
                            except Exception as exc:
                                results.append(
                                    {
                                        "source": current_source.value,
                                        "entity_type": current_type.value,
                                        "runtime": current_runtime,
                                        "network": current_network,
                                        "error": f"{type(exc).__name__}: {exc}",
                                    }
                                )
                                continue
                            identities = probe.pop("sample_identities")
                            elapsed = probe["elapsed_seconds"]
                            rate = probe["returned"] / elapsed if elapsed else None
                            sequential_elapsed = probe.get(
                                "sequential_elapsed_seconds",
                                elapsed,
                            )
                            sequential_rate = (
                                probe["returned"] / sequential_elapsed
                                if sequential_elapsed
                                else None
                            )
                            estimated = (
                                probe["total"] / rate
                                if probe["total"] is not None and rate
                                else None
                            )
                            sequential_estimated = (
                                probe["total"] / sequential_rate
                                if probe["total"] is not None and sequential_rate
                                else None
                            )
                            detail_result = (
                                await _detail_probe(
                                    adapter,
                                    identities,
                                    limit=worker_samples,
                                    concurrency=min(
                                        worker_samples,
                                        settings.runtime.worker_count,
                                    ),
                                    progress_enabled=progress,
                                    progress_interval_seconds=(
                                        progress_interval_seconds
                                    ),
                                    probe_timeout_seconds=probe_timeout_seconds,
                                )
                                if worker_samples
                                else None
                            )
                            if (
                                detail_result
                                and detail_result["details_per_second"]
                                and probe["total"] is not None
                            ):
                                detail_result["estimated_detail_seconds"] = round(
                                    probe["total"]
                                    / detail_result["details_per_second"],
                                    1,
                                )
                            results.append(
                                {
                                    "source": current_source.value,
                                    "entity_type": current_type.value,
                                    "runtime": current_runtime,
                                    "network": current_network,
                                    "runtime_settings": {
                                        "worker_count": settings.runtime.worker_count,
                                        "browser_lanes": settings.runtime.browser_lanes,
                                        "claim_batch_size": (
                                            settings.runtime.claim_batch_size
                                        ),
                                        "browser_timeout_seconds": (
                                            settings.runtime.browser_response_timeout_seconds
                                        ),
                                        "discovery_concurrency": (
                                            discovery_concurrency(settings)
                                        ),
                                    },
                                    "network_settings": {
                                        "kind": settings.network.kind,
                                        "max_lanes": settings.network.max_lanes,
                                        "configured_proxies": len(
                                            settings.network.proxy_pool_urls
                                        )
                                        + int(settings.network.proxy_url is not None),
                                    },
                                    "scope": "default_catalog_filter",
                                    **probe,
                                    "list_items_per_second": (
                                        round(rate, 2) if rate else None
                                    ),
                                    "estimated_discovery_seconds": (
                                        round(estimated, 1)
                                        if estimated is not None
                                        else None
                                    ),
                                    "sequential_list_items_per_second": (
                                        round(sequential_rate, 2)
                                        if sequential_rate
                                        else None
                                    ),
                                    "estimated_discovery_seconds_sequential": (
                                        round(sequential_estimated, 1)
                                        if sequential_estimated is not None
                                        else None
                                    ),
                                    "worker_probe": detail_result,
                                }
                            )
                    finally:
                        await adapter.close()
                        close = getattr(solver, "close", None)
                        if close:
                            await close()
        totals_by_source = {}
        for current_source in _sources(source):
            totals_by_type = {}
            for item in results:
                if (
                    item.get("source") == current_source.value
                    and isinstance(item.get("total"), int)
                ):
                    entity_type = item["entity_type"]
                    totals_by_type[entity_type] = max(
                        item["total"],
                        totals_by_type.get(entity_type, 0),
                    )
            totals_by_source[current_source.value] = sum(totals_by_type.values())
        forecasts = _full_run_forecasts(results)
        combined_forecasts = _combined_forecasts(forecasts)
        summary = _catalog_stats_summary(
            results,
            forecasts,
            combined_forecasts,
            totals_by_source,
        )
        document = {
            "measured_at_unix": round(time.time()),
            "catalog_entries_by_source": totals_by_source,
            "catalog_entries_total": sum(totals_by_source.values()),
            "results": results,
            "full_run_forecasts": forecasts,
            "combined_forecasts": combined_forecasts,
            "note": (
                "Counts use each portal's default catalog filter and are not a "
                "guaranteed all-status historical total. Speed measures catalog "
                "discovery. worker_probe performs extract/parse/relations without "
                "writing to PostgreSQL. Full-cycle forecasts add discovery and "
                "detail estimates; they remain approximate and exclude queue, "
                "UPSERT, retries, refresh passes and relation deduplication cost."
            ),
            "summary": summary,
        }
        payload = _render_json_result(document, output)
        if output is not None:
            _progress(
                f"Saved complete report to {output.resolve()}",
                enabled=progress,
            )
        typer.echo(payload)

    asyncio.run(execute())


@app.command("zakup-spike")
def zakup_spike(
    runtime: Annotated[str, typer.Option()] = "local",
    network: Annotated[str, typer.Option()] = "direct",
    captcha: Annotated[str, typer.Option()] = "manual",
) -> None:
    async def execute() -> None:
        settings = _settings("zakup-sk", runtime, network, captcha)
        context = build_context(settings)
        try:
            result = await context.adapter.spike()
            typer.echo(json.dumps(result, ensure_ascii=False, indent=2))
        finally:
            await context.close()

    asyncio.run(execute())


@app.command("zakup-cache")
def zakup_cache(
    runtime: Annotated[str, typer.Option()] = "slow_internet",
    network: Annotated[str, typer.Option()] = "direct",
) -> None:
    async def execute() -> None:
        settings = _settings("zakup-sk", runtime, network, "disabled")
        proxy_url = (
            settings.network.proxy_url.get_secret_value()
            if settings.network.proxy_url
            else None
        )
        result = await ZakupBundleCache(
            base_url=settings.source.base_url,
            cache_dir=Path("playwright/.cache/zakup"),
            proxy_url=proxy_url,
            timeout_seconds=settings.runtime.browser_navigation_timeout_seconds,
        ).prepare()
        if result is None:
            raise typer.Exit(code=1)
        _, path = result
        typer.echo(f"Zakup main bundle is ready: {path}")

    asyncio.run(execute())


@app.command("proxy-pool-build")
def proxy_pool_build(
    structured_json: Annotated[Path | None, typer.Option(exists=True)] = None,
    generic_json: Annotated[Path | None, typer.Option(exists=True)] = None,
    spys_text: Annotated[Path | None, typer.Option(exists=True)] = None,
    output: Annotated[Path, typer.Option()] = Path("config/proxy_pools/public_test.json"),
) -> None:
    if not any((structured_json, generic_json, spys_text)):
        raise typer.BadParameter(
            "Provide at least one proxy source file"
        )
    document = build_proxy_pool(
        structured_json=structured_json,
        generic_json=generic_json,
        spys_text=spys_text,
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(document, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    typer.echo(f"Collected {len(document['proxies'])} unique proxy candidates in {output}")


@app.command("proxy-pool-check")
def proxy_pool_check(
    pool: Annotated[Path, typer.Option(exists=True)] = Path(
        "config/proxy_pools/public_test.json"
    ),
    output: Annotated[Path, typer.Option()] = Path(
        "config/proxy_pools/public_working.json"
    ),
    target_url: Annotated[str, typer.Option()] = (
        "https://zakup.sk.kz/content/js/settings.js"
    ),
    limit: Annotated[int, typer.Option()] = 100,
    concurrency: Annotated[int, typer.Option()] = 10,
    timeout_seconds: Annotated[int, typer.Option()] = 15,
) -> None:
    async def execute() -> None:
        document = json.loads(pool.read_text(encoding="utf-8"))
        result = await check_proxy_pool(
            document,
            target_url=target_url,
            limit=limit,
            concurrency=concurrency,
            timeout_seconds=timeout_seconds,
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        typer.echo(
            f"Working proxies: {result['working_count']}/{result['tested']} in {output}"
        )

    asyncio.run(execute())


@app.command()
def scheduler(
    source: Annotated[str, typer.Option()] = "all",
    runtime: Annotated[str, typer.Option()] = "local",
    network: Annotated[
        str | None,
        typer.Option(help="Legacy default for both sources"),
    ] = None,
    eep_network: Annotated[str, typer.Option()] = "direct",
    zakup_network: Annotated[str, typer.Option()] = "direct",
    captcha: Annotated[str, typer.Option()] = "disabled",
    discovery_concurrency_override: Annotated[
        int | None,
        typer.Option(
            "--discovery-concurrency",
            min=1,
            max=64,
            help="EEP list request concurrency; Zakup always remains sequential.",
        ),
    ] = None,
) -> None:
    if network:
        eep_network = network
        zakup_network = network
    selected_sources = _sources(source)
    first_source = selected_sources[0]
    first_network = _network_for_source(
        first_source,
        network=network or "direct",
        eep_network=eep_network,
        zakup_network=zakup_network,
    )
    settings = load_settings(
        source=first_source,
        runtime_profile=runtime,
        network_profile=first_network,
        captcha_profile=(
            captcha
            if first_source == Source.ZAKUP_SK
            else "disabled"
        ),
    )
    configure_logging(
        settings.app.log_level,
        log_to_file=settings.app.log_to_file,
        log_dir=settings.app.log_dir,
        log_filename=settings.app.log_filename,
        log_max_bytes=settings.app.log_max_bytes,
        log_backup_count=settings.app.log_backup_count,
    )
    start_metrics_server(settings.app.metrics_port)

    async def execute() -> None:
        stop_event = asyncio.Event()
        remove_signal_handlers = _install_signal_handlers(stop_event.set)
        sampler_database = Database(settings.database)
        sampler_task = asyncio.create_task(
            run_database_sampler(
                PostgresMaintenance(sampler_database),
                stop_event=stop_event,
                interval_seconds=settings.runtime.metrics_sample_seconds,
            ),
            name="scheduler-database-metrics-sampler",
        )
        try:
            results = await asyncio.gather(
                *(
                    run_scheduler(
                        source=current_source,
                        runtime_profile=runtime,
                        network_profile=_network_for_source(
                            current_source,
                            network=network or "direct",
                            eep_network=eep_network,
                            zakup_network=zakup_network,
                        ),
                        captcha_profile=captcha,
                        discovery_concurrency_override=(
                            discovery_concurrency_override
                        ),
                        stop_event=stop_event,
                    )
                    for current_source in selected_sources
                ),
                return_exceptions=True,
            )
            failures = [
                result
                for result in results
                if isinstance(result, BaseException)
                and not isinstance(result, asyncio.CancelledError)
            ]
            if failures:
                for failure in failures:
                    logger.error(
                        "scheduler_source_failed",
                        error=repr(failure),
                    )
                raise typer.Exit(code=1)
        finally:
            stop_event.set()
            await asyncio.gather(sampler_task, return_exceptions=True)
            await sampler_database.close()
            remove_signal_handlers()

    asyncio.run(execute())


@app.command("db-stats")
def db_stats() -> None:
    async def execute() -> None:
        settings = _settings("eep-mitwork", "local", "direct", "disabled")
        context = build_context(settings)
        try:
            stats = await PostgresMaintenance(context.database).collect_queue_stats()
            typer.echo(json.dumps(stats, ensure_ascii=False, indent=2))
        finally:
            await context.close()

    asyncio.run(execute())


@app.command("prune-history")
def prune_history(
    days: Annotated[int | None, typer.Option()] = None,
) -> None:
    async def execute() -> None:
        settings = _settings("eep-mitwork", "local", "direct", "disabled")
        context = build_context(settings)
        try:
            deleted = await PostgresMaintenance(context.database).prune_history(
                days or settings.app.history_retention_days
            )
            typer.echo(f"Deleted {deleted} history rows")
        finally:
            await context.close()

    asyncio.run(execute())


@app.command("release-stale-leases")
def release_stale_leases(
    source: Annotated[str | None, typer.Option()] = None,
) -> None:
    async def execute() -> None:
        normalized_source = _source(source).value if source else None
        settings = _settings("eep-mitwork", "local", "direct", "disabled")
        context = build_context(settings)
        try:
            released = await PostgresMaintenance(
                context.database
            ).release_stale_leases(normalized_source)
            typer.echo(f"Released {released} stale leases")
        finally:
            await context.close()

    asyncio.run(execute())


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        temporary.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    app()
