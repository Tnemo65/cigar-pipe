"""Silver cleaning: compute trip_id, apply validity rules, cast money, MERGE.

process_batch  — pure transform; returns (silver_valid, quarantine, touched_months)
write_batch    — DeltaTable MERGE into trips_clean + append to trips_quarantine
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Protocol

from pyspark.sql import DataFrame, SparkSession, functions as F

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
        try:
            target = DeltaTable.forName(self.spark, target_table)
            (
                target.alias("t")
                .merge(valid_df.alias("s"), "t.trip_id = s.trip_id")
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception:
            valid_df.write.format("delta").mode("append").partitionBy("pickup_month").saveAsTable(target_table)

    def append_quarantine(self, quar_df: DataFrame, target_table: str) -> None:
        quar_df.write.format("delta").mode("append").saveAsTable(target_table)


def process_batch(
    bronze_batch: DataFrame,
    dim_zone: DataFrame,
    dim_rate_code: DataFrame,
    dim_payment_type: DataFrame,
) -> tuple[DataFrame, DataFrame, list]:
    """design.md §12.4 silver_clean.py: compute trip_id, apply rules 1+2,
    split valid/invalid, rename+cast the valid side to the Silver schema,
    return (silver_valid, quarantine, distinct touched pickup_months)."""
    checked = (
        bronze_batch.transform(with_trip_id)
        .transform(flag_implausible_trips)
        .transform(lambda d: flag_unresolved_references(d, dim_zone, dim_rate_code, dim_payment_type))
    )

    valid = checked.filter(F.col("reason_code").isNull())
    invalid = checked.filter(F.col("reason_code").isNotNull())

    silver_cols = [
        F.col(bronze_col).alias(silver_col)
        for bronze_col, silver_col in SILVER_RENAME.items()
        if bronze_col in bronze_batch.columns
    ]
    passthrough = [c for c in ("_source_file", "_ingested_at") if c in bronze_batch.columns]

    silver_valid = valid.select("trip_id", *silver_cols, *passthrough)
    for money_col in MONEY_COLUMNS_SILVER:
        if money_col in silver_valid.columns:
            silver_valid = silver_valid.withColumn(money_col, F.col(money_col).cast("decimal(10,2)"))
    if "pickup_at" in silver_valid.columns:
        silver_valid = silver_valid.withColumn(
            "pickup_month", F.trunc(F.col("pickup_at"), "month")
        )

    cols_to_drop = [c for c in ("_rescued_data",) if c in invalid.columns]
    quarantine = invalid.drop(*cols_to_drop).withColumn(
        "quarantined_at", F.current_timestamp()
    )

    touched_months: list = []
    if "pickup_month" in silver_valid.columns:
        touched_months = [
            row.pickup_month
            for row in silver_valid.select("pickup_month").distinct().collect()
        ]

    return silver_valid, quarantine, touched_months


def write_batch(
    spark: SparkSession,
    silver_valid: DataFrame,
    quarantine: DataFrame,
    writer: SilverWriter | None = None,
) -> None:
    """design.md §11 layer 2: cross-run MERGE anti-join on trip_id, not
    dropDuplicates() scoped to the micro-batch."""
    w = writer or _DefaultSilverWriter(spark)
    target_clean = paths.catalog_table("silver", "trips_clean")
    target_quar = paths.catalog_table("silver", "trips_quarantine")
    w.merge(silver_valid, target_clean)
    w.append_quarantine(quarantine, target_quar)


if __name__ == "__main__":
    from datetime import datetime
    from src.common.run_log import log_run

    started_at = datetime.now()
    status = "SUCCESS"
    total_in = 0
    total_valid = 0
    total_quar = 0

    cfg = paths.load_config()
    spark = SparkSession.builder.getOrCreate()

    try:
        dim_zone_df = spark.table(paths.catalog_table("reference", "dim_zone"))
        dim_rate_df = spark.table(paths.catalog_table("reference", "dim_rate_code"))
        dim_pay_df = spark.table(paths.catalog_table("reference", "dim_payment_type"))

        all_months: set = set()

        def _foreach_batch(batch_df: DataFrame, batch_id: int) -> None:
            global total_in, total_valid, total_quar
            batch_count = batch_df.count()
            s_valid, s_quarantine, months = process_batch(
                batch_df, dim_zone_df, dim_rate_df, dim_pay_df
            )
            v_count = s_valid.count()
            q_count = s_quarantine.count()
            write_batch(spark, s_valid, s_quarantine)
            all_months.update(months)
            total_in += batch_count
            total_valid += v_count
            total_quar += q_count

        bronze_stream = spark.readStream.table(paths.catalog_table("bronze", "trips_raw"))
        query = (
            bronze_stream.writeStream.foreachBatch(_foreach_batch)
            .option("checkpointLocation", paths.checkpoint_path("silver", cfg))
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

        try:
            from pyspark.dbutils import DBUtils  # type: ignore

            dbutils = DBUtils(spark)
            dbutils.jobs.taskValues.set(
                key="touched_months", value=[str(m)[:10] for m in all_months]
            )
            print(f"Set taskValues touched_months={[str(m)[:10] for m in all_months]}")
        except Exception as e:
            print(f"touched_months={sorted(all_months)} (dbutils taskValues skipped: {e})")
    except Exception:
        status = "FAILED"
        raise
    finally:
        try:
            log_run(
                spark,
                task_name="transform_silver",
                rows_in=total_in,
                rows_out=total_valid,
                rows_quarantined=total_quar,
                status=status,
                started_at=started_at,
                ended_at=datetime.now(),
            )
        except Exception as log_error:
            print(f"run_log write failed (non-fatal): {log_error}")
