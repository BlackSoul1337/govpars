from __future__ import annotations

import asyncio

import structlog
from prometheus_client import Counter, Gauge, Histogram, start_http_server

logger = structlog.get_logger()

DISCOVERED_ENTITIES = Counter(
    "procurement_discovered_entities_total",
    "Entities discovered from source lists",
    ["source", "entity_type"],
)
DISCOVERY_WINDOW_DURATION = Histogram(
    "procurement_discovery_window_duration_seconds",
    "Discovery window wall-clock duration including persistence",
    ["source", "entity_type", "outcome"],
)
DISCOVERY_PAGES = Counter(
    "procurement_discovery_pages_total",
    "Discovery pages classified by window outcome",
    ["source", "entity_type", "kind"],
)
DISCOVERY_WINDOWS = Counter(
    "procurement_discovery_windows_total",
    "Discovery window outcomes",
    ["source", "entity_type", "outcome"],
)
DISCOVERY_ENTITY_RATE = Histogram(
    "procurement_discovery_entities_per_second",
    "Entities discovered per second for successful windows",
    ["source", "entity_type"],
)
DISCOVERY_ACTIVE_REQUESTS = Gauge(
    "procurement_discovery_active_requests",
    "Active source list requests",
    ["source", "entity_type"],
)
DISCOVERY_CONFIGURED_CONCURRENCY = Gauge(
    "procurement_discovery_configured_concurrency",
    "Configured source-level discovery concurrency",
    ["source"],
)
DISCOVERY_EFFECTIVE_CONCURRENCY = Gauge(
    "procurement_discovery_effective_concurrency",
    "Pages requested in the current discovery window",
    ["source", "entity_type"],
)
TASK_OUTCOMES = Counter(
    "procurement_task_outcomes_total",
    "Worker task outcomes",
    ["source", "entity_type", "outcome"],
)
TASK_DURATION = Histogram(
    "procurement_task_duration_seconds",
    "End-to-end worker task duration",
    ["source", "entity_type", "outcome"],
)
TASK_HISTORY = Gauge(
    "procurement_task_history_total",
    "Persisted task history outcomes",
    ["source", "outcome"],
)
SOURCE_RESPONSES = Counter(
    "procurement_source_responses_total",
    "Final source response statuses by strategy",
    ["source", "strategy", "status"],
)
SOURCE_LATENCY = Histogram(
    "procurement_source_request_seconds",
    "Source request latency by strategy",
    ["source", "strategy"],
)
CAPTCHA_OUTCOMES = Counter(
    "procurement_captcha_outcomes_total",
    "CAPTCHA solver outcomes",
    ["provider", "kind", "outcome"],
)
CAPTCHA_COST = Counter(
    "procurement_captcha_cost_total",
    "Reported CAPTCHA solver cost",
    ["provider"],
)
CAPTCHA_LATENCY = Histogram(
    "procurement_captcha_latency_seconds",
    "CAPTCHA solver latency",
    ["provider", "kind", "outcome"],
)
CAPTCHA_HOURLY_SPEND = Gauge(
    "procurement_captcha_hourly_spend",
    "CAPTCHA cost persisted during the last hour",
    ["provider"],
)
QUEUE_DEPTH = Gauge(
    "procurement_queue_depth",
    "Active crawl frontier rows",
    ["source"],
)
QUEUE_READY = Gauge(
    "procurement_queue_ready",
    "Runnable crawl frontier rows",
    ["source"],
)
QUEUE_DELAYED = Gauge(
    "procurement_queue_delayed",
    "Delayed crawl frontier rows",
    ["source"],
)
QUEUE_LEASED = Gauge(
    "procurement_queue_leased",
    "Actively leased crawl frontier rows",
    ["source"],
)
QUEUE_STALE_LEASES = Gauge(
    "procurement_queue_stale_leases",
    "Expired leases that can be reclaimed",
    ["source"],
)
QUEUE_DEAD_TUPLES = Gauge(
    "procurement_queue_dead_tuples",
    "Estimated dead tuples in crawl_frontier",
)
LANE_STATE = Gauge(
    "procurement_session_lane_state",
    "Session lane state (one-hot)",
    ["source", "lane_id", "state"],
)
LANE_ROTATIONS = Counter(
    "procurement_session_lane_rotations_total",
    "Session lane replacements or provider rotations",
    ["source", "rotation_kind"],
)
SCHEDULER_JOB_DURATION = Histogram(
    "procurement_scheduler_job_duration_seconds",
    "Scheduler job duration",
    ["source", "job"],
)
SCHEDULER_LAST_SUCCESS = Gauge(
    "procurement_scheduler_last_success_timestamp_seconds",
    "Unix timestamp of the last successful scheduler job",
    ["source", "job"],
)

_started = False


def start_metrics_server(port: int) -> None:
    global _started
    if _started:
        return
    try:
        start_http_server(port)
    except OSError as exc:
        raise RuntimeError(
            f"Could not bind Prometheus metrics server to port {port}: {exc}"
        ) from exc
    _started = True


async def run_database_sampler(
    maintenance,
    *,
    stop_event,
    interval_seconds: int = 15,
) -> None:
    while not stop_event.is_set():
        try:
            await maintenance.collect_queue_stats()
        except Exception:
            logger.exception("database_metrics_sample_failed")
        try:
            await asyncio.wait_for(
                stop_event.wait(),
                timeout=interval_seconds,
            )
        except TimeoutError:
            pass
