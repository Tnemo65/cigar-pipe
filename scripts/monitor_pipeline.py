"""Freshness, no-data and reconciliation gate; failures trigger Job alerts."""
import sys
from datetime import date, datetime, timezone
from pathlib import Path

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[1] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from pyspark.sql import SparkSession, functions as F
from src.common.alerting import alert_fingerprint
from src.common.run_state import RunState
from src.common.runtime import configure_spark, runtime_config
from src.ingestion.source_landing import expected_months


def check_monitoring(payload, config, published, no_data_runs):
    settings = config["monitoring"]
    if payload["status"] == "NO_DATA" and no_data_runs >= settings["max_no_data_runs"]:
        fingerprint = alert_fingerprint(config["environment"], "NO_DATA", config["processing_date"])
        raise RuntimeError(f"NO_DATA run limit exceeded; alert_fingerprint={fingerprint}")
    available = set(published) | {s["source_month"] for s in payload.get("snapshots", [])}
    # Latest may still be unpublished, but cannot remain stale indefinitely.
    if not available:
        raise RuntimeError("No published source data exists")
    today = date.fromisoformat(config["processing_date"])
    newest = max(date.fromisoformat(m) for m in available)
    if not config.get("start_month") and (today - newest).days > settings["max_source_age_days"]:
        fingerprint = alert_fingerprint(config["environment"], "SOURCE_FRESHNESS", str(newest))
        raise RuntimeError(f"Source freshness SLA exceeded; alert_fingerprint={fingerprint}")
    expected = expected_months(config)
    if any(m not in available for m in expected[:-1]):
        fingerprint = alert_fingerprint(config["environment"], "SOURCE_COMPLETENESS", config["processing_date"])
        raise RuntimeError(f"Source completeness gap; alert_fingerprint={fingerprint}")
    for snapshot in payload.get("snapshots", []):
        if (
            snapshot.get("rows_received") is not None
            and snapshot["rows_received"] < settings["min_snapshot_rows"]
        ):
            raise RuntimeError(f"Source row-count alert: {snapshot['source_month']}")
    if payload["status"] == "SUCCESS":
        def keyed(metrics):
            return {(m["mart"],m["month"]):(m["rows"],m["trips"]) for m in metrics}
        gold, serving = payload.get("gold_metrics", []), payload.get("serving_metrics", [])
        expected_count = len(payload["snapshots"]) * 3
        if (
            payload.get("serving_status") != "READY_FOR_SERVING"
            or len(gold) != expected_count
            or len(payload.get("serving_handoff", {}).get("exports", [])) != expected_count
            or len(keyed(gold)) != expected_count
        ):
            raise RuntimeError("Missing serving handoff or Gold reconciliation mismatch")
        if serving and (len(serving) != expected_count or keyed(gold) != keyed(serving)):
            raise RuntimeError("Serving reconciliation mismatch")



def run(spark, config):
    state = RunState(spark, config)
    payload = state.read("export_bigquery")
    try:
        recent = (spark.table(state.table).filter((F.col("task_name") == "source_landing") &
                  F.col("status").isin("SUCCESS", "NO_DATA"))
                  .orderBy(F.col("updated_at").desc()).limit(config["monitoring"]["max_no_data_runs"]).collect())
        consecutive = 0
        for row in recent:
            if row.status != "NO_DATA":
                break
            consecutive += 1
        check_monitoring(payload, config, state.published_snapshots(), consecutive)
        state.write("monitor", payload["status"], {**payload, "checked_at": datetime.now(timezone.utc).isoformat()})
        return payload
    except Exception as error:
        state.write("monitor", "FAILED", {**payload, "error": str(error)[:4000]})
        raise


if __name__ == "__main__":
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
