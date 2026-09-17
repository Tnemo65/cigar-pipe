# src/transform/run_dq_gate.py
import sys
from pathlib import Path

from pyspark.sql import SparkSession

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from src.common import paths


def table_names(config: dict | None = None) -> tuple[str, str]:
    return (
        paths.catalog_table("silver", "trips_clean", config),
        paths.catalog_table("silver", "trips_quarantine", config),
    )


def check_snapshot_metrics(metrics, snapshots, threshold):
    if not 0 <= threshold <= 1:
        raise ValueError("DQ threshold must be between zero and one")
    expected = {s["snapshot_id"]: s for s in snapshots}
    unknown_expected_rows = any(s.get("rows_received") is None for s in snapshots)
    if not metrics or len(metrics) != len(expected) or {m["source_snapshot_id"] for m in metrics} != set(expected):
        raise RuntimeError("Missing or duplicate source snapshot metrics")
    for metric in metrics:
        total = metric["rows_in"]
        bad = metric["raw_rows_quarantined"]
        expected_rows = expected[metric["source_snapshot_id"]].get("rows_received")
        if total <= 0 or (expected_rows is not None and total != expected_rows):
            raise RuntimeError("Source row count reconciliation failed")
        if any(metric[k] < 0 for k in ("rows_out", "rows_quarantined", "rows_deduplicated", "raw_rows_quarantined")):
            raise RuntimeError("Negative row counts")
        if total != metric["rows_out"] + metric["rows_quarantined"] + metric["rows_deduplicated"] or bad > total:
            raise RuntimeError("Silver row count reconciliation failed")
        rate = bad / total
        if rate > threshold:
            raise RuntimeError(f"quarantine rate {rate:.4f} exceeds threshold {threshold:.4f} for snapshot {metric['source_snapshot_id']}")


def run(spark, config):
    from src.common.run_state import execute_task
    def action(payload):
        metrics = payload.get("metrics") or []
        if metrics and all(metric.get("rows_in", 0) == 0 for metric in metrics):
            return {**payload, "status": "NO_DATA", "reason": "snapshot_already_processed"}
        check_snapshot_metrics(metrics, payload["snapshots"], config["thresholds"]["quarantine_rate_max"])

        minimum = config.get("monitoring", {}).get("min_snapshot_rows", 100)
        if any(
            s.get("rows_received") is not None and s["rows_received"] < minimum
            for s in payload["snapshots"]
        ):

            raise RuntimeError("Source row-count alert: snapshot below minimum")
        return payload
    return execute_task(spark, config, "dq_gate", "transform_silver", action)


if __name__ == "__main__":
    from src.common.runtime import configure_spark, runtime_config
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
