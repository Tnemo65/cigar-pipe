"""Create a serving handoff for a publisher outside Databricks Serverless."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path


def handoff_path(config: dict, pipeline_run_id: str) -> str:
    return (
        f"gs://{config['gcp']['bucket']}/{config['environment']}/"
        f"serving_handoff/{pipeline_run_id}/manifest.json"
    )


def build_handoff(config: dict, payload: dict, spark=None) -> dict:
    months = payload.get("months", [])
    if not months:
        raise ValueError("Serving handoff requires affected months")
    if not payload.get("gold_metrics"):
        raise ValueError("Serving handoff requires Gold metrics")
    return {
        "pipeline_run_id": config["pipeline_run_id"],
        "environment": config["environment"],
        "project_id": config["gcp"]["project_id"],
        "dataset": config["bigquery"]["dataset"],
        "bucket": config["gcp"]["bucket"],
        "months": sorted(months),
        "snapshots": payload.get("snapshots", []),
        "gold_metrics": payload["gold_metrics"],
        "export_prefix": f"gs://{config['gcp']['bucket']}/{config['environment']}/gold_export/",
        "status": "READY_FOR_SERVING",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def write_handoff(config: dict, payload: dict, writer=None) -> dict:
    handoff = build_handoff(config, payload)
    uri = handoff_path(config, config["pipeline_run_id"])
    if writer is None:
        from google.cloud import storage

        bucket = storage.Client(project=config["gcp"]["project_id"]).bucket(config["gcp"]["bucket"])
        blob = bucket.blob(
            f"{config['environment']}/serving_handoff/"
            f"{config['pipeline_run_id']}/manifest.json"
        )
        blob.upload_from_string(
            json.dumps(handoff, sort_keys=True),
            content_type="application/json",
            if_generation_match=0,
        )
    else:
        writer(uri, handoff)
    return {**payload, "serving_handoff_uri": uri, "serving_status": handoff["status"]}


if __name__ == "__main__":
    raise SystemExit("Run serving publication through scripts/publish_bigquery.py")
