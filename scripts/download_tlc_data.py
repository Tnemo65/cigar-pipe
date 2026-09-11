"""Download TLC yellow-taxi Parquet files and reference data to GCS.

Injectable Uploader for testability — production uses Google Cloud Storage,
tests pass a stub that records calls without network/GCS dependencies.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Callable

import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.common import paths
from src.ingestion.raw_landing import LandingResult, land_bytes

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

Uploader = Callable[[bytes, str], LandingResult]


def _default_uploader(content: bytes, gcs_uri: str) -> LandingResult:
    from src.ingestion.raw_landing import gcs_immutable_uploader

    result = land_bytes(content, gcs_uri, gcs_immutable_uploader)
    state = "already landed" if result.already_landed else "uploaded"
    print(f"  {state} {gcs_uri} checksum={result.checksum}")
    return result


def download_month(year: int, month: int, config: dict, uploader: Uploader = _default_uploader) -> str:
    filename = f"yellow_tripdata_{year:04d}-{month:02d}.parquet"
    url = f"{TLC_BASE}/{filename}"
    print(f"  downloading {url}")
    response = requests.get(url, timeout=120)
    if response.status_code != 200:
        raise RuntimeError(f"failed to download {url}: HTTP {response.status_code}")
    gcs_uri = f"{paths.raw_yellow_path(config).rstrip('/')}/{filename}"
    uploader(response.content, gcs_uri)
    return gcs_uri


def download_zone_lookup(config: dict, uploader: Uploader = _default_uploader) -> str:
    print(f"  downloading {ZONE_LOOKUP_URL}")
    response = requests.get(ZONE_LOOKUP_URL, timeout=30)
    if response.status_code != 200:
        raise RuntimeError(f"failed to download {ZONE_LOOKUP_URL}: HTTP {response.status_code}")
    gcs_uri = f"{paths.raw_ref_path(config).rstrip('/')}/taxi_zone_lookup.csv"
    uploader(response.content, gcs_uri)
    return gcs_uri


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download TLC data and zone lookup to GCS")
    p.add_argument("--config", default="configs/config.yaml", help="Path to config.yaml")
    p.add_argument("--year", type=int, default=2024)
    p.add_argument("--start-month", type=int, default=1, dest="start_month")
    p.add_argument("--end-month", type=int, default=2, dest="end_month")
    p.add_argument("--skip-zone", action="store_true", dest="skip_zone")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    cfg = paths.load_config(args.config)
    if not args.skip_zone:
        download_zone_lookup(cfg)
    for m in range(args.start_month, args.end_month + 1):
        print(download_month(args.year, m, cfg))
    print("Done.")
