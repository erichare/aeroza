"""End-to-end integration tests for ``GET /v1/calibration``.

Seeds verification rows directly (the worker is exercised separately
in ``test_verify_worker.py``) and confirms the route shapes the
response correctly: camelCase keys, sample-weighted means, window /
algorithm filters.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from httpx import AsyncClient
from sqlalchemy import text

from aeroza.ingest.mrms import MrmsFile
from aeroza.ingest.mrms_store import upsert_mrms_files
from aeroza.nowcast.engine import PERSISTENCE_ALGORITHM
from aeroza.nowcast.store import upsert_nowcast
from aeroza.shared.db import Database
from aeroza.verify.metrics import DeterministicMetrics
from aeroza.verify.store import upsert_verification

pytestmark = pytest.mark.integration

ROUTE: str = "/v1/calibration"


@pytest.fixture(autouse=True)
async def _truncate_after(integration_db: Database) -> Any:
    yield
    async with integration_db.sessionmaker() as session:
        await session.execute(text("TRUNCATE TABLE mrms_files CASCADE"))
        await session.commit()


async def _seed_verification(
    integration_db: Database,
    *,
    horizon_minutes: int,
    metrics: DeterministicMetrics,
    algorithm: str = PERSISTENCE_ALGORITHM,
    valid_at: datetime | None = None,
) -> uuid.UUID:
    """Insert source file + nowcast + verification rows. Returns the id of
    the verification.

    The keys carry a per-call random suffix so multiple seeds in the
    same (algorithm, horizon) bucket don't collide on the
    ``uq_nowcast_verifications_nowcast_observation`` constraint.
    Tests that want sample-weighted aggregation over multiple
    verifications rely on this — fixed keys would silently upsert
    the second seed over the first.
    """
    valid_at = valid_at or datetime(2026, 5, 1, 12, 30, tzinfo=UTC)
    src_at = valid_at - timedelta(minutes=horizon_minutes)
    suffix = uuid.uuid4().hex[:6]
    obs_key = f"obs-{horizon_minutes}-{algorithm}-{suffix}"
    src_key = f"src-{horizon_minutes}-{algorithm}-{suffix}"

    async with integration_db.sessionmaker() as session:
        await upsert_mrms_files(
            session,
            (
                MrmsFile(
                    key=src_key,
                    product="MergedReflectivityComposite",
                    level="00.50",
                    valid_at=src_at,
                    size_bytes=1,
                    etag="e",
                ),
                MrmsFile(
                    key=obs_key,
                    product="MergedReflectivityComposite",
                    level="00.50",
                    valid_at=valid_at,
                    size_bytes=1,
                    etag="e",
                ),
            ),
        )
        await session.commit()

    async with integration_db.sessionmaker() as session:
        nowcast = await upsert_nowcast(
            session,
            source_file_key=src_key,
            product="MergedReflectivityComposite",
            level="00.50",
            algorithm=algorithm,
            horizon_minutes=horizon_minutes,
            valid_at=valid_at,
            zarr_uri=f"/tmp/{src_key}-{horizon_minutes}m.zarr",
            variable="reflectivity",
            dims=("latitude", "longitude"),
            shape=(3, 3),
            dtype="float32",
            nbytes=36,
        )
        verification = await upsert_verification(
            session,
            nowcast_id=nowcast.id,
            observation_file_key=obs_key,
            product="MergedReflectivityComposite",
            level="00.50",
            algorithm=algorithm,
            horizon_minutes=horizon_minutes,
            valid_at=valid_at,
            metrics=metrics,
        )
        await session.commit()
    return verification.id


async def test_calibration_empty_envelope_when_no_data(
    api_client: AsyncClient,
) -> None:
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "Calibration"
    assert "generatedAt" in body
    assert body["windowHours"] == 24
    assert body["items"] == []


async def test_calibration_returns_one_row_per_algorithm_horizon(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed_verification(
        integration_db,
        horizon_minutes=10,
        metrics=DeterministicMetrics(mae=1.0, bias=0.5, rmse=1.2, sample_count=100),
    )
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=3.0, bias=-1.0, rmse=4.0, sample_count=100),
    )

    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 2
    by_h = {item["forecastHorizonMinutes"]: item for item in items}
    # Sample-weighted but only one verification per bucket → equals raw values.
    assert by_h[10]["maeMean"] == pytest.approx(1.0)
    assert by_h[10]["biasMean"] == pytest.approx(0.5)
    assert by_h[10]["rmseMean"] == pytest.approx(1.2)
    assert by_h[10]["sampleCount"] == 100
    assert by_h[10]["verificationCount"] == 1
    assert by_h[30]["maeMean"] == pytest.approx(3.0)


async def test_calibration_filters_by_algorithm(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=1.0, bias=0.0, rmse=1.0, sample_count=100),
        algorithm="persistence",
    )
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=0.5, bias=0.0, rmse=0.5, sample_count=100),
        algorithm="pretend-pysteps",
    )

    only_persistence = await api_client.get(ROUTE, params={"algorithm": "persistence"})
    assert only_persistence.status_code == 200
    items = only_persistence.json()["items"]
    assert {item["algorithm"] for item in items} == {"persistence"}


async def test_calibration_window_excludes_old_verifications(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """A 1-hour window must not surface verifications older than 1 hour.

    The aggregator filters on ``verified_at`` (when we scored), not
    ``valid_at`` (the forecast moment). Backdating ``verified_at`` via
    raw SQL is the cleanest way to test this without sleeping.
    """
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=10.0, bias=10.0, rmse=10.0, sample_count=100),
    )
    # Backdate ``verified_at`` 2 days into the past.
    async with integration_db.sessionmaker() as session:
        await session.execute(
            text("UPDATE nowcast_verifications SET verified_at = NOW() - INTERVAL '2 days'")
        )
        await session.commit()

    response = await api_client.get(ROUTE, params={"windowHours": 1})
    assert response.status_code == 200
    assert response.json()["items"] == []


async def test_calibration_camelcase_only_on_wire(
    api_client: AsyncClient, integration_db: Database
) -> None:
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=1.0, bias=0.5, rmse=1.0, sample_count=10),
    )
    response = await api_client.get(ROUTE)
    body = response.json()
    item = body["items"][0]
    for snake_key in (
        "forecast_horizon_minutes",
        "verification_count",
        "sample_count",
        "mae_mean",
        "bias_mean",
        "rmse_mean",
    ):
        assert snake_key not in item
    for camel_key in (
        "forecastHorizonMinutes",
        "verificationCount",
        "sampleCount",
        "maeMean",
        "biasMean",
        "rmseMean",
    ):
        assert camel_key in item


async def test_calibration_validates_window_hours_range(
    api_client: AsyncClient,
) -> None:
    response = await api_client.get(ROUTE, params={"windowHours": 0})
    assert response.status_code == 422
    response = await api_client.get(ROUTE, params={"windowHours": 100_000})
    assert response.status_code == 422


async def test_calibration_aggregates_pod_far_csi_from_contingency_table(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """Two verifications with the same threshold sum to the right
    POD/FAR/CSI on the wire — averaging ratios would be wrong, but
    summing the contingency table and computing on read is correct."""
    # Row 1: 8 hits, 2 misses, 0 false alarms, 90 correct negatives.
    #        POD=0.8, FAR=0.0, CSI=0.8.
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(
            mae=2.0,
            bias=0.0,
            rmse=2.0,
            sample_count=100,
            threshold_dbz=35.0,
            hits=8,
            misses=2,
            false_alarms=0,
            correct_negatives=90,
        ),
    )
    # Row 2: 10 hits, 0 misses, 5 false alarms, 85 correct negatives.
    #        POD=1.0, FAR=0.333, CSI=0.667.
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(
            mae=4.0,
            bias=0.0,
            rmse=4.0,
            sample_count=100,
            threshold_dbz=35.0,
            hits=10,
            misses=0,
            false_alarms=5,
            correct_negatives=85,
        ),
    )

    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    item = items[0]
    # Sums: 18 hits / 2 misses / 5 false alarms.
    # POD = 18 / (18 + 2) = 0.9
    # FAR = 5 / (18 + 5) ≈ 0.2174
    # CSI = 18 / (18 + 2 + 5) = 0.72
    assert item["thresholdDbz"] == pytest.approx(35.0)
    assert item["pod"] == pytest.approx(18 / 20)
    assert item["far"] == pytest.approx(5 / 23)
    assert item["csi"] == pytest.approx(18 / 25)


async def test_calibration_emits_null_pod_when_no_categorical_metrics(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """Old-style verification rows (threshold not set) round-trip
    cleanly: pod/far/csi/thresholdDbz are null, mae/bias/rmse work
    as before."""
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(
            mae=1.0,
            bias=0.0,
            rmse=1.0,
            sample_count=10,
            # threshold_dbz=None on purpose — emulates a row that
            # predates the migration / was scored without a threshold.
        ),
    )
    response = await api_client.get(ROUTE)
    item = response.json()["items"][0]
    assert item["thresholdDbz"] is None
    assert item["pod"] is None
    assert item["far"] is None
    assert item["csi"] is None
    # Continuous metrics still populated.
    assert item["maeMean"] == pytest.approx(1.0)


async def test_calibration_emits_null_threshold_when_rows_disagree(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """If two rows in the same bucket scored at different thresholds,
    the route can't safely surface one — null is the honest answer."""
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(
            mae=1.0,
            bias=0.0,
            rmse=1.0,
            sample_count=10,
            threshold_dbz=35.0,
            hits=1,
            misses=0,
            false_alarms=0,
            correct_negatives=9,
        ),
    )
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(
            mae=1.0,
            bias=0.0,
            rmse=1.0,
            sample_count=10,
            threshold_dbz=40.0,
            hits=2,
            misses=0,
            false_alarms=0,
            correct_negatives=8,
        ),
    )
    response = await api_client.get(ROUTE)
    item = response.json()["items"][0]
    assert item["thresholdDbz"] is None
    # POD/FAR/CSI still computable from summed counts (3 hits, 0 misses, 0 FA).
    assert item["pod"] == pytest.approx(1.0)
