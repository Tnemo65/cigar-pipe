"""Load reference dimension tables into Unity Catalog.

Runs dim_rate_code.sql and dim_payment_type.sql via Databricks SQL, then
loads dim_zone from the TLC taxi zone lookup CSV.

LocationIDs 264 and 265 are sentinel rows (Unknown/NV) — flagged with
is_sentinel=True so validity rules can distinguish them from real boroughs.
"""
from __future__ import annotations

import argparse
import io
import sys

import requests

TLC_ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
SENTINEL_IDS = {264, 265}


def _spark():
    from pyspark.sql import SparkSession
    return SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()


def load_reference_sql(sql_path: str) -> None:
    spark = _spark()
    sql_text = open(sql_path).read()
    for stmt in sql_text.split(";"):
        stmt = stmt.strip()
        if stmt:
            spark.sql(stmt)


def load_dim_zone() -> None:
    spark = _spark()
    resp = requests.get(TLC_ZONE_URL, timeout=30)
    resp.raise_for_status()

    rows = []
    for line in resp.text.splitlines()[1:]:  # skip header
        parts = line.split(",", 3)
        if len(parts) < 4:
            continue
        location_id = int(parts[0])
        borough = parts[1].strip('"')
        zone = parts[2].strip('"')
        service_zone = parts[3].strip('"')
        rows.append((location_id, borough, zone, service_zone, location_id in SENTINEL_IDS))

    df = spark.createDataFrame(
        rows,
        schema="location_id INT, borough STRING, zone STRING, service_zone STRING, is_sentinel BOOLEAN",
    )
    df.write.format("delta").mode("overwrite").saveAsTable(
        "taxi_lakehouse.reference.dim_zone"
    )
    print(f"  loaded {len(rows)} zone rows ({len(SENTINEL_IDS)} sentinels)")


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Load reference tables into Unity Catalog")
    p.add_argument("--sql-dir", default="sql/reference", dest="sql_dir")
    p.add_argument("--skip-zone", action="store_true", dest="skip_zone")
    return p.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    import os
    for fname in ("dim_rate_code.sql", "dim_payment_type.sql"):
        path = os.path.join(args.sql_dir, fname)
        print(f"Loading {path}")
        load_reference_sql(path)
    if not args.skip_zone:
        print("Loading dim_zone from TLC CSV")
        load_dim_zone()
    print("Done.")
