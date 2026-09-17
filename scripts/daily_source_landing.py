"""Discover and land the newest published TLC monthly snapshot."""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from datetime import date
from pathlib import Path
from typing import Callable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import requests

from src.ingestion.raw_landing import LandingResult, land_bytes

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTH_PATTERN = re.compile(r"yellow_tripdata_(\d{4})-(\d{2})\.parquet$")


def candidate_months(today: date, lookback_months: int = 2) -> list[str]:
    """Return the latest published candidates, newest first."""
    result = []
    year, month = today.year, today.month
    for _ in range(lookback_months + 1):
        result.append(f"{year:04d}-{month:02d}-01")
        month -= 1
        if month == 0:
            year, month = year - 1, 12
    return result


def discover_latest(
    today: date,
    session=requests,
    lookback_months: int = 2,
) -> tuple[str, str] | None:
    """Return (month, URL) for the newest available TLC file."""
    for month in candidate_months(today, lookback_months):
        filename = f"yellow_tripdata_{month[:7]}.parquet"
        url = f"{TLC_BASE}/{filename}"
        response = session.head(url, timeout=(15, 30), allow_redirects=True)
        if response.status_code == 200:
            return month, url
        if response.status_code not in (404, 403):
            response.raise_for_status()
    return None


def download_and_land(
    month: str,
    url: str,
    bucket: str,
    *,
    session=requests,
    uploader: Callable[[bytes, str], LandingResult],
) -> dict:
    response = session.get(url, stream=True, timeout=(15, 300))
    if response.status_code == 404:
        return {"status": "NO_DATA", "month": month, "url": url}
    response.raise_for_status()
    digest = hashlib.sha256()
    chunks = []
    for chunk in response.iter_content(1024 * 1024):
        if chunk:
            digest.update(chunk)
            chunks.append(chunk)
    content = b"".join(chunks)
    checksum = digest.hexdigest()
    filename = f"yellow_tripdata_{month[:7]}.parquet"
    uri = (
        f"gs://{bucket}/prod/raw/yellow/source_month={month}/"
        f"snapshot={checksum}/{filename}"
    )
    result = uploader(content, uri)
    return {
        "status": "LANDED" if not result.already_landed else "ALREADY_LANDED",
        "month": month,
        "url": url,
        "uri": uri,
        "checksum": checksum,
        "generation": result.generation,
        "bytes_received": len(content),
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--output", default="artifacts/source-landing.json")
    parser.add_argument("--lookback-months", type=int, default=2)
    args = parser.parse_args()
    discovered = discover_latest(date.today(), lookback_months=args.lookback_months)
    if discovered is None:
        result = {"status": "NO_DATA", "checked_at": date.today().isoformat()}
    else:
        month, url = discovered
        from src.ingestion.raw_landing import gcs_immutable_uploader

        result = download_and_land(
            month,
            url,
            args.bucket,
            uploader=lambda content, uri: land_bytes(content, uri, gcs_immutable_uploader),
        )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
