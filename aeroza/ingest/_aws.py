"""AWS / S3 client factories for ingest sources.

NOAA's open-data buckets (``noaa-mrms-pds``, ``noaa-nexrad-level2``,
``noaa-goes16``, …) accept anonymous reads, so ingest workers don't need
AWS credentials. The factory below uses ``signature_version=UNSIGNED`` so a
plain ``s3.list_objects_v2`` works against any of those buckets without
configuration. Workers that target our *own* buckets (the eventual Zarr
mirror) can construct a separate signed client at the call site.
"""

from __future__ import annotations

from typing import Any

import boto3
from botocore import UNSIGNED
from botocore.config import Config

DEFAULT_AWS_REGION: str = "us-east-1"


def open_data_s3_client(*, region: str = DEFAULT_AWS_REGION) -> Any:
    """Return a boto3 S3 client configured for anonymous reads."""
    return boto3.client(
        "s3",
        region_name=region,
        config=Config(signature_version=UNSIGNED),
    )
