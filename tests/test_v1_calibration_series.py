"""End-to-end integration tests for ``GET /v1/calibration/series``.

Seeds verification rows at known timestamps, then asserts the time-
bucketed wire shape comes back grouped per (algorithm, horizon) with
points sorted oldest → newest.
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

ROUTE: str = "/v1/calibration/series"


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
    verified_at: datetime | None = None,
) -> uuid.UUID:
    """Insert source + nowcast + verification rows. Optionally backdate
    `verified_at` so a single test can manufacture multiple time buckets
    without sleeping."""
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
        if verified_at is not None:
            await session.execute(
                text("UPDATE nowcast_verifications SET verified_at = :v WHERE id = :id"),
                {"v": verified_at, "id": verification.id},
            )
        await session.commit()
    return verification.id


async def test_series_empty_envelope_when_no_data(
    api_client: AsyncClient,
) -> None:
    response = await api_client.get(ROUTE)
    assert response.status_code == 200
    body = response.json()
    assert body["type"] == "CalibrationSeries"
    assert body["windowHours"] == 24
    assert body["bucketSeconds"] == 3600
    assert body["items"] == []


async def test_series_groups_by_algorithm_horizon_with_sorted_points(
    api_client: AsyncClient, integration_db: Database
) -> None:
    # Two buckets of "now" data: 2h ago and 30m ago.
    now = datetime.now(UTC)
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=2.0, bias=0.0, rmse=2.0, sample_count=100),
        verified_at=now - timedelta(hours=2),
    )
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=1.0, bias=0.0, rmse=1.0, sample_count=100),
        verified_at=now - timedelta(minutes=30),
    )

    response = await api_client.get(ROUTE, params={"windowHours": 24})
    assert response.status_code == 200
    items = response.json()["items"]
    assert len(items) == 1
    series = items[0]
    assert series["algorithm"] == PERSISTENCE_ALGORITHM
    assert series["forecastHorizonMinutes"] == 30
    points = series["points"]
    assert len(points) == 2
    # Oldest → newest order; the first bucket (2h-ago) carries the
    # higher MAE.
    assert points[0]["maeMean"] == pytest.approx(2.0)
    assert points[1]["maeMean"] == pytest.approx(1.0)
    assert points[0]["bucketStart"] < points[1]["bucketStart"]


async def test_series_separates_algorithms(
    api_client: AsyncClient, integration_db: Database
) -> None:
    now = datetime.now(UTC)
    for algo, mae in (("persistence", 1.0), ("pretend-pysteps", 0.5)):
        await _seed_verification(
            integration_db,
            horizon_minutes=30,
            metrics=DeterministicMetrics(mae=mae, bias=0.0, rmse=mae, sample_count=100),
            algorithm=algo,
            verified_at=now - timedelta(minutes=15),
        )
    response = await api_client.get(ROUTE)
    items = response.json()["items"]
    assert {item["algorithm"] for item in items} == {"persistence", "pretend-pysteps"}


async def test_series_filters_window(api_client: AsyncClient, integration_db: Database) -> None:
    now = datetime.now(UTC)
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=10.0, bias=0.0, rmse=10.0, sample_count=100),
        verified_at=now - timedelta(days=2),
    )
    response = await api_client.get(ROUTE, params={"windowHours": 1})
    assert response.status_code == 200
    assert response.json()["items"] == []


async def test_series_buckets_use_requested_width(
    api_client: AsyncClient, integration_db: Database
) -> None:
    """Two rows in the same hour collapse to one point at hourly width;
    they should split when we ask for a 5-minute bucket."""
    now = datetime.now(UTC)
    base = now - timedelta(minutes=30)
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=2.0, bias=0.0, rmse=2.0, sample_count=100),
        verified_at=base,
    )
    await _seed_verification(
        integration_db,
        horizon_minutes=30,
        metrics=DeterministicMetrics(mae=4.0, bias=0.0, rmse=4.0, sample_count=100),
        verified_at=base + timedelta(minutes=10),
    )

    hourly = await api_client.get(ROUTE, params={"windowHours": 24, "bucketSeconds": 3600})
    five_min = await api_client.get(ROUTE, params={"windowHours": 24, "bucketSeconds": 300})
    assert len(hourly.json()["items"][0]["points"]) == 1
    assert len(five_min.json()["items"][0]["points"]) == 2


async def test_series_validates_bucket_seconds_range(
    api_client: AsyncClient,
) -> None:
    too_small = await api_client.get(ROUTE, params={"bucketSeconds": 1})
    assert too_small.status_code == 422
    too_big = await api_client.get(ROUTE, params={"bucketSeconds": 10**7})
    assert too_big.status_code == 422


async def test_series_camelcase_only_on_wire(
    api_client: AsyncClient, integration_db: Database
) -> None:
    now = datetime.now(UTC)
    await _seed_verification(
        integration_db,
        horizon_minutes=10,
        metrics=DeterministicMetrics(mae=1.0, bias=0.5, rmse=1.0, sample_count=10),
        verified_at=now - timedelta(minutes=5),
    )
    response = await api_client.get(ROUTE)
    body = response.json()
    item = body["items"][0]
    point = item["points"][0]
    for snake_key in (
        "forecast_horizon_minutes",
        "verification_count",
        "sample_count",
        "mae_mean",
        "bias_mean",
        "rmse_mean",
        "bucket_start",
    ):
        assert snake_key not in {**item, **point}
    for camel_key in ("forecastHorizonMinutes",):
        assert camel_key in item
    for camel_key in (
        "bucketStart",
        "verificationCount",
        "sampleCount",
        "maeMean",
        "biasMean",
        "rmseMean",
    ):
        assert camel_key in point
