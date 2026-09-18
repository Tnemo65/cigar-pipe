"""Prepare a daily source result without shell heredoc parsing."""
from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.daily_source_landing import discover_latest, download_and_land


def verify_gcs_object(uri: str, project: str, runner=subprocess.run) -> None:
    gcloud = shutil.which("gcloud") or shutil.which("gcloud.cmd") or "gcloud.cmd"
    result = runner(
        [gcloud, "storage", "ls", uri, "--project", project],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0 or not result.stdout.strip():
        raise RuntimeError(f"Landed source object is unavailable: {uri}")


def prepare_source(
    bucket: str,
    output: str,
    source_uri_override: str = "",
    lookback_months: int = 2,
    project: str = "",
    runner=subprocess.run,
) -> dict:
    if source_uri_override.strip():
        value = source_uri_override.strip()
        month, separator, uri = value.partition("=")
        if not separator or not month or not uri.startswith("gs://"):
            raise ValueError("source_uri_override must be MONTH=gs://...")
        verify_gcs_object(uri, project, runner)
        result = {"status": "LANDED_OVERRIDE", "month": month, "uri": uri}
    else:
        discovered = discover_latest(__import__("datetime").date.today(), lookback_months=lookback_months)
        if discovered is None:
            result = {"status": "NO_DATA"}
        else:
            month, url = discovered
            from src.ingestion.raw_landing import gcs_immutable_uploader, land_bytes

            result = download_and_land(
                month,
                url,
                bucket,
                uploader=lambda content, uri: land_bytes(content, uri, gcs_immutable_uploader),
            )
    destination = Path(output)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(json.dumps(result, indent=2), encoding="utf-8")
    if result["status"] != "NO_DATA":
        Path(destination.parent / "source-month").write_text(result["month"], encoding="utf-8")
        Path(destination.parent / "source-uri").write_text(
            f"{result['month']}={result['uri']}", encoding="utf-8"
        )
    else:
        Path(destination.parent / "no-data").touch()
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--project", default=os.environ.get("BUNDLE_VAR_gcp_project", ""))
    parser.add_argument("--source-uri-override", default=os.environ.get("SOURCE_URI_OVERRIDE", ""))
    parser.add_argument("--lookback-months", type=int, default=2)
    parser.add_argument("--output", default="artifacts/source-landing.json")
    args = parser.parse_args()
    print(json.dumps(prepare_source(
        args.bucket,
        args.output,
        args.source_uri_override,
        args.lookback_months,
        args.project,
    ), indent=2))


if __name__ == "__main__":
    main()
