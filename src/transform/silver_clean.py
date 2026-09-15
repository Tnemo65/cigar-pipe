"""Silver cleaning: compute trip_id, apply validity rules, cast money, MERGE.

process_batch  — pure transform; returns (silver_valid, quarantine, touched_months)
write_batch    — idempotent DeltaTable MERGE into trips_clean and trips_quarantine
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession, Window, functions as F

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
from delta.tables import DeltaTable

from src.common import paths
from src.common.schemas import MONEY_COLUMNS_SILVER, SILVER_RENAME
from src.transform.validity_rules import (
    flag_implausible_trips,
    flag_unresolved_references,
    with_trip_id,
)


class SilverWriter(Protocol):
    def merge(self, valid_df: DataFrame, target_table: str) -> None: ...
    def append_quarantine(self, quar_df: DataFrame, target_table: str) -> None: ...


class _DefaultSilverWriter:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    def merge(self, valid_df: DataFrame, target_table: str) -> None:
        if not self.spark.catalog.tableExists(target_table):
            valid_df.write.format("delta").mode("append").partitionBy("pickup_month").saveAsTable(target_table)
            return
        target = DeltaTable.forName(self.spark, target_table)
        (
            target.alias("t")
            .merge(valid_df.alias("s"), "t.trip_id = s.trip_id")
            .whenNotMatchedInsertAll()
            .execute()
        )

    def append_quarantine(self, quar_df: DataFrame, target_table: str) -> None:
        if not quar_df.take(1):
            return
        if not self.spark.catalog.tableExists(target_table):
            quar_df.write.format("delta").mode("append").saveAsTable(target_table)
            return

        target = DeltaTable.forName(self.spark, target_table)
        (
            target.alias("t")
            .merge(quar_df.alias("s"), "t.trip_id = s.trip_id")
            .whenNotMatchedInsertAll()
            .execute()
        )


def process_batch(
    bronze_batch: DataFrame,
    dim_zone: DataFrame,
    dim_rate_code: DataFrame,
    dim_payment_type: DataFrame,
    checked: DataFrame | None = None,
) -> tuple[DataFrame, DataFrame, list]:
    """design.md §12.4 silver_clean.py: compute trip_id, apply rules 1+2,
    split valid/invalid, rename+cast the valid side to the Silver schema,
    return (silver_valid, quarantine, distinct touched pickup_months)."""
    if checked is None:
        checked = classify_batch(bronze_batch, dim_zone, dim_rate_code, dim_payment_type)

    valid = checked.filter(F.col("reason_code").isNull())
    invalid = checked.filter(F.col("reason_code").isNotNull())

    # A single microbatch may contain duplicate deliveries. Keep one row per
    # stable source event/business key before the first table write or MERGE.
    # source lineage identifies delivery provenance; trip_id identifies the
    # canonical business entity and therefore remains the Silver dedup key.
    def deterministic_unique(frame, keys):
        order = [F.col(c).desc_nulls_last() for c in ("_source_object_version", "_ingested_at", "_source_file") if c in frame.columns]
        window = Window.partitionBy(*keys).orderBy(*(order or [F.col("trip_id")]))
        return frame.withColumn("_rank", F.row_number().over(window)).filter("_rank = 1").drop("_rank")
    valid = deterministic_unique(valid, ["trip_id"])
    invalid = deterministic_unique(invalid, ["trip_id", "reason_code"])

    silver_cols = [
        F.col(bronze_col).alias(silver_col)
        for bronze_col, silver_col in SILVER_RENAME.items()
        if bronze_col in bronze_batch.columns
    ]
    # Source-object lineage remains in Bronze. Keep the established Silver
    # schema stable until an explicit migration is deployed for new metadata.
    passthrough = [c for c in ("_source_file", "_ingested_at", "_source_object_id", "_source_object_version",
                   "source_month", "source_snapshot_id", "source_record_id", "trip_business_key", "pipeline_run_id", "batch_id") if c in checked.columns]

    silver_valid = valid.select("trip_id", *silver_cols, *passthrough)
    for timestamp in ("pickup_at", "dropoff_at"):
        if timestamp in silver_valid.columns:
            silver_valid = silver_valid.withColumn(timestamp, F.col(timestamp).cast("timestamp_ntz"))
    for money_col in MONEY_COLUMNS_SILVER:
        if money_col in silver_valid.columns:
            silver_valid = silver_valid.withColumn(money_col, F.col(money_col).cast("decimal(10,2)"))
    if "pickup_at" in silver_valid.columns:
        silver_valid = silver_valid.withColumn(
            "pickup_month", F.trunc(F.col("pickup_at"), "month")
        )

    quarantine = invalid.withColumn(
        "quarantined_at", F.current_timestamp()
    )

    touched_months: list = []
    if "pickup_month" in silver_valid.columns:
        touched_months = [
            row.pickup_month
            for row in silver_valid.select("pickup_month").distinct().collect()
        ]

    return silver_valid, quarantine, touched_months


def classify_batch(batch, dim_zone, dim_rate_code, dim_payment_type):
    return (batch.transform(with_trip_id).transform(flag_implausible_trips)
            .transform(lambda d: flag_unresolved_references(d, dim_zone, dim_rate_code, dim_payment_type)))


def write_batch(
    spark: SparkSession,
    silver_valid: DataFrame,
    quarantine: DataFrame,
    writer: SilverWriter | None = None,
    config: dict | None = None,
) -> None:
    """design.md §11 layer 2: cross-run MERGE anti-join on trip_id, not
    dropDuplicates() scoped to the micro-batch."""
    w = writer or _DefaultSilverWriter(spark)
    target_clean = paths.catalog_table("silver", "trips_clean", config)
    target_quar = paths.catalog_table("silver", "trips_quarantine", config)
    w.merge(silver_valid, target_clean)
    w.append_quarantine(quarantine, target_quar)


def replace_snapshot(spark, frame, table, month):
    """Replace all records of the source month, including removed/corrected trips."""
    from src.common.runtime import month_start
    month_start(month)
    if frame.filter(F.col("source_month").isNull() | (F.col("source_month") != month)).take(1):
        raise ValueError("Snapshot contains rows outside its source month")
    if not spark.catalog.tableExists(table):
        frame.write.format("delta").partitionBy("source_month").saveAsTable(table)
        return
    if "source_month" not in spark.table(table).columns:
        raise RuntimeError("Legacy Silver schema: migrate to the isolated v2 catalog first")
    match = f"t.source_month = DATE '{month}' AND t.trip_id = s.trip_id"
    if "reason_code" in frame.columns:
        match += " AND t.reason_code = s.reason_code"
    (DeltaTable.forName(spark, table).alias("t").merge(frame.alias("s"), match)
     .whenMatchedUpdateAll().whenNotMatchedInsertAll()
     .whenNotMatchedBySourceDelete(f"t.source_month = DATE '{month}'").execute())


def run(spark, config):
    from src.common.reference import load_references
    from src.common.run_state import execute_task

    def action(payload):
        references = load_references(spark, config)
        metrics = []
        for snapshot in payload["snapshots"]:
            month = snapshot["source_month"]
            batch = (spark.table(paths.catalog_table("bronze", "trips_raw", config))
                     .filter(F.col("_source_file") == snapshot["uri"])
                     .withColumn("source_month", F.lit(month).cast("date"))
                     .withColumn("source_snapshot_id", F.lit(snapshot["snapshot_id"]))
                     .withColumn("pipeline_run_id", F.lit(config["pipeline_run_id"]))
                     .withColumn("batch_id", F.lit(snapshot["snapshot_id"]))
                     .withColumn("_source_object_version", F.lit(snapshot["generation"])))
            valid = quarantine = checked = None
            try:
                rows_in = batch.count()
                expected_rows = snapshot.get("rows_received")
                if expected_rows is not None and rows_in != expected_rows:
                    raise RuntimeError(f"Bronze/source reconciliation failed for {month}: {rows_in} != {expected_rows}")
                checked = classify_batch(batch, *references)
                valid, quarantine, _ = process_batch(batch, *references, checked=checked)

                rows_valid, rows_quarantine = valid.count(), quarantine.count()
                raw_bad = checked.filter(F.col("reason_code").isNotNull()).count()
                replace_snapshot(spark, valid, paths.catalog_table("silver", "trips_clean", config), month)
                replace_snapshot(spark, quarantine, paths.catalog_table("silver", "trips_quarantine", config), month)
                for table, expected in (("trips_clean", rows_valid), ("trips_quarantine", rows_quarantine)):
                    stored = spark.table(paths.catalog_table("silver", table, config)).filter(F.col("source_month") == month)
                    if stored.count() != expected or stored.filter(F.col("source_snapshot_id") != snapshot["snapshot_id"]).take(1):
                        raise RuntimeError(f"Silver stored snapshot reconciliation failed: {table}/{month}")
                metrics.append(dict(source_month=month, source_snapshot_id=snapshot["snapshot_id"],
                                    batch_id=snapshot["snapshot_id"], rows_in=rows_in,
                                    rows_out=rows_valid, rows_quarantined=rows_quarantine,
                                    raw_rows_quarantined=raw_bad,
                                    rows_deduplicated=rows_in-rows_valid-rows_quarantine))
            finally:
                # Serverless Spark Connect does not support DataFrame.persist.
                # The bounded monthly batch is recomputed per action instead.
                pass

        return {**payload, "metrics": metrics, "months": [s["source_month"] for s in payload["snapshots"]]}
    return execute_task(spark, config, "transform_silver", "ingest_bronze", action)


if __name__ == "__main__":
    from src.common.runtime import configure_spark, runtime_config
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
