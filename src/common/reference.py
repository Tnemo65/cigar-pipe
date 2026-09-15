"""Reference contracts, checked before joins can multiply or discard trips."""
from pyspark.sql import functions as F

from src.common import paths

DIMENSIONS = (("dim_zone", "location_id", ("borough", "zone", "is_sentinel")),
              ("dim_rate_code", "rate_code_id", ("is_flat_fare",)),
              ("dim_payment_type", "payment_type_id", ("payment_type_name", "tip_is_recorded")))


def validate_dimension(df, key, required=(), max_age_days=None):
    if not df.take(1):
        raise RuntimeError(f"Empty reference dimension: {key}")
    for column in (key, *required):
        if column not in df.columns or df.filter(F.col(column).isNull()).take(1):
            raise RuntimeError(f"Missing/null reference field: {column}")
    if df.groupBy(key).count().filter(F.col("count") > 1).take(1):
        raise RuntimeError(f"Duplicate reference key: {key}")
    if max_age_days is not None:
        if "_loaded_at" not in df.columns:
            raise RuntimeError(f"Reference freshness metadata missing: {key}")
        if df.filter(F.col("_loaded_at").isNull() |
                     (F.col("_loaded_at") < F.current_timestamp() - F.expr(f"INTERVAL {int(max_age_days)} DAYS")) |
                     (F.col("_loaded_at") > F.current_timestamp())).take(1):
            raise RuntimeError(f"Stale reference dimension: {key}")


def load_references(spark, config):
    frames = []
    for table, key, required in DIMENSIONS:
        df = spark.table(paths.catalog_table("reference", table, config))
        validate_dimension(df, key, required, config.get("reference", {}).get("max_age_days", 90))
        frames.append(df)
    return frames
