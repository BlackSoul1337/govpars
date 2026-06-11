from __future__ import annotations

import asyncio
import json
import signal
from collections.abc import Callable
from pathlib import Path
from typing import Annotated

import structlog
import typer

from procurement_parser.application.csv_validator import (
    validate_csv,
    validate_export_directory,
)
from procurement_parser.application.factory import build_context
from procurement_parser.application.pipeline import DiscoveryService, WorkerService
from procurement_parser.application.scheduler import run_scheduler
from procurement_parser.config.settings import load_settings
from procurement_parser.domain.models import EntityType, Source
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
                context = build_context(settings)
                contexts.append(context)

                async def run_source(
                    *,
                    current_context=context,
                    current_source=current_source,
                ) -> tuple[Source, int]:
                    service = DiscoveryService(
                        current_context.adapter,
                        current_context.frontier,
                    )
                    total = 0
                    for current_type in _entity_types(entity_type, current_source):
                        total += await service.run(
                            current_type,
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
                if not contexts:
                    start_metrics_server(settings.app.metrics_port)
                context = build_context(settings)
                contexts.append(context)
                discovery = DiscoveryService(
                    context.adapter,
                    context.frontier,
                )
                source_discovery_jobs = [
                    discovery.run(
                        entity_type,
                        max_pages=pages or None,
                    )
                    for entity_type in _entity_types("all", current_source)
                ]
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
                        source_discovery_jobs,
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
                    current_discovery_jobs,
                    current_worker: WorkerService,
                ) -> list[BaseException]:
                    if drain:
                        discovery_results = await asyncio.gather(
                            *current_discovery_jobs,
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
                    results = await asyncio.gather(
                        *current_discovery_jobs,
                        current_worker.run(),
                        return_exceptions=True,
                    )
                    return [
                        result
                        for result in results
                        if isinstance(result, BaseException)
                    ]

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
) -> None:
    async def execute() -> None:
        if layout not in {"combined", "split", "both"}:
            raise typer.BadParameter("--layout must be combined, split, or both")
        delimiter_chars = {"comma": ",", "semicolon": ";"}
        if delimiter not in delimiter_chars:
            raise typer.BadParameter("--delimiter must be comma or semicolon")
        settings = _settings("eep-mitwork", "local", "direct", "disabled")
        context = build_context(settings)
        try:
            exporter = PostgresCsvExporter(
                context.database,
                delimiter=delimiter_chars[delimiter],
            )
            output_dir = output if output.suffix == "" else output.parent
            if dataset == "all":
                if layout in {"combined", "both"}:
                    results = await exporter.export_all(output_dir)
                    for name, count in results.items():
                        typer.echo(f"Exported {count} rows to {output_dir / f'{name}.csv'}")
                if layout in {"split", "both"}:
                    results = await exporter.export_all_split(output_dir)
                    for key, count in results.items():
                        name, source = key.split(":", 1)
                        source_slug = source.replace("-", "_")
                        typer.echo(
                            f"Exported {count} rows to "
                            f"{output_dir / f'{name}_{source_slug}.csv'}"
                        )
            else:
                if layout in {"combined", "both"}:
                    count = await exporter.export(dataset, output)
                    typer.echo(f"Exported {count} rows to {output}")
                if layout in {"split", "both"}:
                    results = await exporter.export_split(dataset, output_dir)
                    for source, count in results.items():
                        source_slug = source.replace("-", "_")
                        destination = output_dir / f"{dataset}_{source_slug}.csv"
                        typer.echo(f"Exported {count} rows to {destination}")
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


if __name__ == "__main__":
    app()
