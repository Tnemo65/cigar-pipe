"""Download TLC yellow-taxi Parquet files and upload to GCS.

Injectable Uploader for testability — production uses GCSUploader,
tests pass a stub that records calls without touching GCS.
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from typing import Protocol

import requests

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


class Uploader(Protocol):
    def upload(self, data: bytes, destination: str) -> None: ...


@dataclass
class GCSUploader:
    bucket: str

    def upload(self, data: bytes, destination: str) -> None:
        from google.cloud import storage  # lazy import — not needed for tests

        client = storage.Client()
        blob = client.bucket(self.bucket).blob(destination)
        blob.upload_from_string(data, content_type="application/octet-stream")
        print(f"  uploaded gs://{self.bucket}/{destination}")


def _tlc_url(year: int, month: int) -> str:
    return f"{TLC_BASE}/yellow_tripdata_{year}-{month:02d}.parquet"


def download_and_upload(year: int, month: int, uploader: Uploader) -> None:
    url = _tlc_url(year, month)
    print(f"  downloading {url}")
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    destination = f"raw/yellow/year={year}/month={month:02d}/yellow_tripdata_{year}-{month:02d}.parquet"
    uploader.upload(resp.content, destination)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Download TLC data to GCS")
    p.add_argument("--bucket", required=True, help="GCS bucket name (without gs://)")
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--start-month", type=int, default=1, dest="start_month")
    p.add_argument("--end-month", type=int, default=12, dest="end_month")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    uploader = GCSUploader(bucket=args.bucket)
    for month in range(args.start_month, args.end_month + 1):
        download_and_upload(args.year, month, uploader)
    print("Done.")
