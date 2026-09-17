"""Durable task state. Missing/failed state is never interpreted as no data."""
import json
import time
from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from src.common import paths

STATE_SCHEMA = "pipeline_run_id STRING, task_name STRING, status STRING, payload STRING, updated_at TIMESTAMP"


def validate_state(status, payload):
    if status not in {"RUNNING", "SUCCESS", "NO_DATA", "FAILED"}:
        raise ValueError(f"Invalid status {status}")
    if status == "SUCCESS" and not payload.get("snapshots"):
        raise ValueError("SUCCESS requires source snapshots")
    if status == "NO_DATA":
        snapshots = payload.get("snapshots") or []
        metrics = payload.get("metrics") or []
        replay = payload.get("reason") == "snapshot_already_processed"
        if snapshots and not replay:
            raise ValueError("NO_DATA cannot contain unprocessed work")
        if replay and any(metric.get("rows_in", 0) != 0 for metric in metrics):
            raise ValueError("Replay NO_DATA requires zero input rows")


class RunState:
    def __init__(self, spark, config):
        self.spark = spark
        self.run_id = config["pipeline_run_id"]
        self.table = paths.catalog_table("reference", "pipeline_run_state", config)
        spark.sql(f"CREATE TABLE IF NOT EXISTS {self.table} ({STATE_SCHEMA}) USING DELTA")

    def write(self, task, status, payload):
        validate_state(status, payload)
        row = self.spark.createDataFrame(
            [(self.run_id, task, status, json.dumps(payload, sort_keys=True), datetime.now(timezone.utc))],
            STATE_SCHEMA,
        )
        (DeltaTable.forName(self.spark, self.table).alias("t")
         .merge(row.alias("s"), "t.pipeline_run_id=s.pipeline_run_id AND t.task_name=s.task_name")
         .whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())

    def read(self, task, required=True):
        rows = (self.spark.table(self.table)
                .filter((F.col("pipeline_run_id") == self.run_id) & (F.col("task_name") == task))
                .limit(2).collect())
        if not rows and not required:
            return None
        if len(rows) != 1:
            raise RuntimeError(f"Missing or duplicate durable state for {self.run_id}/{task}")
        result = {**json.loads(rows[0].payload), "status": rows[0].status}
        if required and result["status"] not in ("SUCCESS", "NO_DATA"):
            raise RuntimeError(f"Upstream {task} is {result['status']}")
        validate_state(result["status"], result)
        if required and result["status"] == "SUCCESS" and task in {"transform_silver", "dq_gate", "aggregate_gold", "export_bigquery"}:
            expected = sorted(s["source_month"] for s in result["snapshots"])
            if not result.get("months") or sorted(result["months"]) != expected or len(set(expected)) != len(expected):
                raise RuntimeError(f"Missing or inconsistent affected-month state: {task}")
        return result

    def published_snapshots(self):
        rows = (self.spark.table(self.table)
                .filter((F.col("task_name") == "monitor") & (F.col("status") == "SUCCESS"))
                .orderBy(F.col("updated_at").desc()).select("payload").toLocalIterator())
        latest = {}
        for row in rows:
            for snapshot in json.loads(row.payload)["snapshots"]:
                latest.setdefault(snapshot["source_month"], snapshot["snapshot_id"])
        return latest

    def dirty_months(self):
        """A failed attempt may have mutated Silver/Gold; force reconciliation."""
        events = (self.spark.table(self.table)
                  .filter(F.col("task_name").isin("source_landing", "monitor") & (F.col("status") == "SUCCESS"))
                  .orderBy(F.col("updated_at").desc()).toLocalIterator())
        latest = {}
        for row in events:
            for snapshot in json.loads(row.payload).get("snapshots", []):
                latest.setdefault(snapshot["source_month"], row.task_name)
        return {month for month, task in latest.items() if task == "source_landing"}

    def ensure_not_superseded(self, payload):
        source = (self.spark.table(self.table)
                  .filter((F.col("pipeline_run_id") == self.run_id) & (F.col("task_name") == "source_landing"))
                  .first())
        if source is None:
            raise RuntimeError("Missing source plan")
        months = {s["source_month"] for s in payload.get("snapshots", [])}
        newer = (self.spark.table(self.table)
                 .filter((F.col("task_name") == "source_landing") & (F.col("status") == "SUCCESS") &
                         (F.col("updated_at") > source.updated_at) & (F.col("pipeline_run_id") != self.run_id))
                 .select("payload").toLocalIterator())
        if any(months & {s["source_month"] for s in json.loads(r.payload).get("snapshots", [])} for r in newer):
            raise RuntimeError("Run superseded by a newer source plan; start a new backfill instead of repairing this run")


def execute_task(spark, config, task, upstream, action):
    state = RunState(spark, config)
    payload = state.read(upstream) if upstream else {}
    started = time.monotonic()

    def with_cost_context(result):
        return {
            **result,
            "cost_context": {
                "environment": config["environment"],
                "task": task,
                "duration_ms": round((time.monotonic() - started) * 1000),
                "backfill": bool(config.get("start_month")),
            },
        }

    if payload.get("status") == "NO_DATA":
        result = with_cost_context(payload)
        state.write(task, "NO_DATA", result)
        return result
    if upstream:
        state.ensure_not_superseded(payload)
    state.write(task, "RUNNING", payload)
    try:
        result = with_cost_context(action(payload))
        state.write(task, result.get("status", "SUCCESS"), result)
        return result
    except Exception as error:
        state.write(task, "FAILED", with_cost_context({**payload, "error": str(error)[:4000]}))
        raise

