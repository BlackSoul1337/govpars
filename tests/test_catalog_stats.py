import asyncio
import json

import pytest

from procurement_parser.domain.models import (
    EntityIdentity,
    EntityType,
    ExtractedBatch,
    Source,
)
from procurement_parser.entrypoints.cli import (
    _await_with_progress,
    _catalog_stats_summary,
    _combined_forecasts,
    _detail_probe,
    _full_run_forecasts,
    _profile_list,
    _render_json_result,
)
from procurement_parser.infrastructure.sources.eep_mitwork.adapter import (
    EepMitworkAdapter,
)
from procurement_parser.infrastructure.sources.zakup_sk.adapter import ZakupSkAdapter


def test_eep_catalog_total_accepts_grouped_digits() -> None:
    html = "<div class='summary'>Показаны записи 1-50 из 578 183.</div>"

    assert EepMitworkAdapter._catalog_total(html) == 578_183


def test_zakup_catalog_total_accepts_nested_payload() -> None:
    payload = {
        "data": {
            "content": [{"id": 1}],
            "totalElements": 6_277,
        }
    }

    assert ZakupSkAdapter._catalog_total(payload) == 6_277


def test_profile_list_deduplicates_profiles() -> None:
    assert _profile_list("local, slow_internet,local", "server") == [
        "local",
        "slow_internet",
    ]
    assert _profile_list(None, "server") == ["server"]


@pytest.mark.asyncio
async def test_detail_probe_reports_success_and_failure() -> None:
    identities = [
        EntityIdentity(
            source=Source.EEP_MITWORK,
            entity_type=EntityType.LOT,
            source_entity_id=value,
            canonical_url=f"https://example.test/lot/{value}",
        )
        for value in ("1", "2")
    ]

    class Adapter:
        async def extract(self, identity):
            if identity.source_entity_id == "2":
                raise RuntimeError("test failure")
            return ExtractedBatch()

    result = await _detail_probe(Adapter(), identities, limit=2, concurrency=1)

    assert result["requested"] == 2
    assert result["succeeded"] == 1
    assert result["failed"] == 1
    assert result["concurrency"] == 1


def test_full_run_forecast_sums_catalogs_per_profile() -> None:
    results = [
        {
            "source": "eep-mitwork",
            "runtime": "local",
            "network": "direct",
            "estimated_discovery_seconds": 100,
            "worker_probe": {"estimated_detail_seconds": 400},
        },
        {
            "source": "eep-mitwork",
            "runtime": "local",
            "network": "direct",
            "estimated_discovery_seconds": 50,
            "worker_probe": {"estimated_detail_seconds": 200},
        },
    ]

    forecast = _full_run_forecasts(results)[0]

    assert forecast["complete"] is True
    assert forecast["discovery"]["seconds"] == 150
    assert forecast["detail"]["seconds"] == 600
    assert forecast["full_cycle"]["seconds"] == 750


def test_combined_forecast_uses_max_for_parallel_sources() -> None:
    forecasts = [
        {
            "source": "eep-mitwork",
            "runtime": "local",
            "network": "direct",
            "complete": True,
            "full_cycle": {"seconds": 1000},
        },
        {
            "source": "zakup-sk",
            "runtime": "local",
            "network": "direct",
            "complete": True,
            "full_cycle": {"seconds": 300},
        },
    ]

    combined = _combined_forecasts(forecasts)[0]

    assert combined["parallel_wall_clock"]["seconds"] == 1000
    assert combined["sequential_total"]["seconds"] == 1300


@pytest.mark.asyncio
async def test_progress_wrapper_returns_result() -> None:
    async def operation():
        return 42

    assert (
        await _await_with_progress(
            operation(),
            label="test",
            enabled=False,
            interval_seconds=1,
            timeout_seconds=5,
        )
        == 42
    )


@pytest.mark.asyncio
async def test_progress_wrapper_enforces_probe_timeout() -> None:
    async def operation():
        await asyncio.sleep(2)

    with pytest.raises(TimeoutError, match="Probe exceeded"):
        await _await_with_progress(
            operation(),
            label="test-timeout",
            enabled=False,
            interval_seconds=1,
            timeout_seconds=1,
        )


def test_render_json_result_writes_complete_utf8_file(tmp_path) -> None:
    destination = tmp_path / "reports" / "catalog-stats.json"
    document = {"source": "eep-mitwork", "title": "Закупки"}

    payload = _render_json_result(document, destination)

    assert json.loads(payload) == document
    assert json.loads(destination.read_text(encoding="utf-8")) == document
    assert not destination.with_suffix(".json.tmp").exists()


def test_catalog_stats_summary_selects_fastest_complete_profile() -> None:
    results = [
        {"worker_probe": {"requested": 10, "succeeded": 9}},
        {"worker_probe": {"requested": 10, "succeeded": 10}},
    ]
    forecasts = [
        {
            "source": "eep-mitwork",
            "runtime": "local",
            "network": "direct",
            "complete": True,
            "full_cycle": {"seconds": 200, "hours": 0.06, "days": 0.0},
        }
    ]
    combined = [
        {
            "runtime": "local",
            "network": "direct",
            "complete": True,
            "parallel_wall_clock": {"seconds": 200, "hours": 0.06, "days": 0.0},
            "sequential_total": {"seconds": 250, "hours": 0.07, "days": 0.0},
        },
        {
            "runtime": "slow_internet",
            "network": "public_pool",
            "complete": True,
            "parallel_wall_clock": {"seconds": 400, "hours": 0.11, "days": 0.0},
            "sequential_total": {"seconds": 450, "hours": 0.12, "days": 0.01},
        },
    ]

    summary = _catalog_stats_summary(
        results,
        forecasts,
        combined,
        {"eep-mitwork": 100},
    )

    assert summary["status"] == "complete"
    assert summary["sample_quality"]["worker_success_rate"] == 0.95
    assert summary["time_estimate_for_both_sources"][
        "fastest_measured_profile"
    ] == {"runtime": "local", "network": "direct"}
