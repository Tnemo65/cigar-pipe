"""Monthly source acquisition, immutable snapshots and persistent manifests."""
import hashlib
import json
import re
import shutil
import sys
import tempfile
from datetime import date, datetime, timezone
from pathlib import Path

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[2] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from src.common import paths
from src.common.run_state import RunState
from src.common.runtime import configure_spark, month_start, runtime_config
from src.ingestion.raw_landing import land_file

TLC_BASE = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def months_between(start, end):
    current = date.fromisoformat(month_start(start))
    stop = date.fromisoformat(month_start(end))
    if current > stop:
        raise ValueError("Reversed month range")
    while current <= stop:
        yield current.isoformat()
        current = date(current.year + (current.month == 12), current.month % 12 + 1, 1)


def expected_months(config):
    if config.get("start_month"):
        return list(months_between(config["start_month"], config["end_month"]))
    today = date.fromisoformat(config["processing_date"])
    index = today.year * 12 + today.month - 1
    lag = int(config["ingestion"].get("publication_lag_months", 2))
    lookback = int(config["ingestion"].get("late_arrival_months", 2))
    if lag < 1 or lookback < 1:
        raise ValueError("Publication lag and lookback must be positive")
    def at(i):
        return date(i // 12, i % 12 + 1, 1).isoformat()
    return list(months_between(at(index-lag-lookback+1), at(index-lag)))


def http_session():
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(
        total=4, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",), respect_retry_after_header=True,
    )))
    return session


REQUIRED_COLUMNS = {
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "fare_amount",
    "trip_distance",
    "total_amount",
}


def _snapshot_from_landed_gcs(month, source_uri, config):
    """Register an immutable object; Auto Loader performs the data read."""
    match = re.search(r"/snapshot=([a-f0-9]{64})/", source_uri)
    if not match:
        raise RuntimeError("Landed source URI must contain a SHA-256 snapshot path")
    checksum = match.group(1)
    return {
        "source_month": month,
        "snapshot_id": checksum,
        "uri": source_uri,
        "checksum": checksum,
        "generation": "external-landing",
        "rows_received": None,
        "bytes_received": None,
        "source_url": source_uri,
        "source_version": checksum,
    }


def fetch_snapshot(month, config, session, uploader=land_file):
    filename = f"yellow_tripdata_{month[:7]}.parquet"
    source_uri = config.get("source_uris", {}).get(month)
    url = source_uri or f"{TLC_BASE}/{filename}"
    if source_uri and source_uri.startswith("gs://"):
        return _snapshot_from_landed_gcs(month, source_uri, config)

    with tempfile.TemporaryDirectory(prefix="tlc_source_") as temp:
        file = Path(temp) / filename
        source_headers = {}
        if source_uri and source_uri.startswith("file://"):
            shutil.copyfile(source_uri.removeprefix("file://"), file)
        else:
            with session.get(url, stream=True, timeout=(15, 120)) as response:
                if response.status_code == 404:
                    return None
                response.raise_for_status()
                source_headers = dict(response.headers)
                with file.open("wb") as handle:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)
                expected_length = response.headers.get("Content-Length")
                if expected_length and file.stat().st_size != int(expected_length):
                    raise RuntimeError(f"Incomplete download: {url}")

        digest = hashlib.sha256()
        size = 0
        with file.open("rb") as handle:
            with pq.ParquetFile(handle) as parquet:
                missing = REQUIRED_COLUMNS - set(parquet.schema_arrow.names)
                if missing:
                    raise RuntimeError(f"Missing required Parquet columns: {sorted(missing)}")
                row_count = parquet.metadata.num_rows
                if row_count <= 0:
                    raise RuntimeError(f"Empty source snapshot: {url}")
            with file.open("rb") as binary:
                for chunk in iter(lambda: binary.read(1024 * 1024), b""):
                    digest.update(chunk)
                    size += len(chunk)
        if not size or row_count <= 0:
            raise RuntimeError(f"Empty or corrupt Parquet snapshot: {url}")

        checksum = digest.hexdigest()
        uri = f"{paths.raw_yellow_path(config)}source_month={month}/snapshot={checksum}/{filename}"
        result = uploader(str(file), uri, checksum)
        if result.checksum != checksum or not result.generation:
            raise RuntimeError("Landing did not confirm checksum/generation")
        return dict(
            source_month=month,
            snapshot_id=checksum,
            uri=uri,
            checksum=checksum,
            generation=result.generation,
            rows_received=row_count,
            bytes_received=size,
            source_url=url,
            source_version=source_headers.get("ETag", checksum),
        )


def write_manifest(spark, config, snapshot):
    from delta.tables import DeltaTable
    table = paths.catalog_table("reference", "ingestion_manifest", config)
    schema = ("pipeline_run_id STRING, source_month DATE, source_snapshot_id STRING, "
              "source_url STRING, object_uri STRING, object_name STRING, object_checksum STRING, "
              "object_generation STRING, source_version STRING, rows_received BIGINT, "
              "bytes_received BIGINT, status STRING, first_seen_at TIMESTAMP, "
              "completed_at TIMESTAMP, metadata_json STRING")
    spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({schema}) USING DELTA")
    now = datetime.now(timezone.utc)
    row = spark.createDataFrame([(
        config["pipeline_run_id"], date.fromisoformat(snapshot["source_month"]), snapshot["snapshot_id"],
        snapshot.get("source_url"), snapshot["uri"], snapshot["uri"].rsplit("/", 1)[-1],
        snapshot["checksum"], snapshot["generation"], snapshot.get("source_version"),
        snapshot["rows_received"], snapshot.get("bytes_received"), "LANDED", now, now,
        json.dumps(snapshot, sort_keys=True),
    )], schema)
    (DeltaTable.forName(spark, table).alias("t").merge(
        row.alias("s"),
        "t.pipeline_run_id=s.pipeline_run_id AND "
        "t.source_snapshot_id=s.source_snapshot_id AND "
        "t.object_checksum=s.object_checksum",
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())



def run(spark, config, session=None, fetcher=fetch_snapshot):
    state = RunState(spark, config)
    prior = state.read("source_landing", required=False)
    if prior and prior["status"] in ("SUCCESS", "NO_DATA"):
        requested_uris = config.get("source_uris", {})
        prior_uris = {
            snapshot["source_month"]: snapshot.get("uri")
            for snapshot in prior.get("snapshots", [])
        }
        same_snapshot_plan = not requested_uris or all(
            prior_uris.get(month) == uri for month, uri in requested_uris.items()
        )
        if same_snapshot_plan:
            return prior
        # A new URI for an existing month is an explicit correction/re-publish.
        # Continue ingestion so Bronze/Silver can replace the month snapshot.
    state.write("source_landing", "RUNNING", {})
    session = session or http_session()
    try:
        published = state.published_snapshots()
        dirty = state.dirty_months()
        snapshots, unavailable = [], []
        expected = expected_months(config)
        for month in expected:
            snapshot = fetcher(month, config, session)
            if snapshot is None:
                # Only the latest expected month may be temporarily unpublished.
                if not config.get("allow_unpublished", False) or month != expected[-1] or month in published:
                    raise RuntimeError(f"Required source month is unavailable: {month}")
                unavailable.append(month)
                continue
            write_manifest(spark, config, snapshot)
            if config.get("start_month") or month in dirty or published.get(month) != snapshot["snapshot_id"]:
                snapshots.append(snapshot)
        known_rows = [
            s["rows_received"]
            for s in snapshots
            if s.get("rows_received") is not None
        ]
        payload = dict(
            snapshots=snapshots,
            unavailable_months=unavailable,
            expected_months=expected,
            rows_in=sum(known_rows) if known_rows else None,
            observed_at=datetime.now(timezone.utc).isoformat(),
        )

        status = "SUCCESS" if snapshots else "NO_DATA"

        state.write("source_landing", status, payload)
        return {"status": status, **payload}
    except Exception as error:
        state.write("source_landing", "FAILED", {"error": str(error)[:4000]})
        raise
    finally:
        session.close()


if __name__ == "__main__":
    from pyspark.sql import SparkSession
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
