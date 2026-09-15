from datetime import date, datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pyspark.sql import functions as F

from src.common.reference import validate_dimension
from src.common.run_state import RunState, execute_task, validate_state
from src.common.runtime import runtime_config
from src.ingestion.source_landing import expected_months, months_between
from src.transform.run_dq_gate import check_snapshot_metrics
from src.transform.silver_clean import process_batch, replace_snapshot
from src.transform.validity_rules import flag_implausible_trips, with_trip_id


def test_environment_runtime_is_explicit_and_isolated():
    from src.common.paths import checkpoint_path, raw_yellow_path, schema_location_path
    configs = []
    for env in ("dev", "staging", "prod"):
        configs.append(runtime_config(["--environment", env, "--catalog", "taxi_"+env,
          "--bucket", "taxi-lake-"+env, "--project", "test-project", "--dataset", "taxi_"+env,
          "--pipeline-run-id", "run-123"]))
    for fn in (raw_yellow_path, lambda c: checkpoint_path("bronze", c), lambda c: schema_location_path("bronze", c)):
        assert len({fn(c) for c in configs}) == 3
        assert all('/'+c['environment']+'/' in fn(c) for c in configs)
    with pytest.raises(ValueError, match="suffix"):
        runtime_config(["--environment","prod","--catalog","taxi_dev","--bucket","lake-prod",
                        "--project","test-project","--dataset","taxi_prod","--pipeline-run-id","1"])


def test_source_month_ranges_cross_year_and_validate():
    assert list(months_between("2023-12-01", "2024-02-01")) == ["2023-12-01", "2024-01-01", "2024-02-01"]
    assert expected_months({"processing_date":"2024-02-15", "ingestion":{"publication_lag_months":2,"late_arrival_months":2}}) == ["2023-11-01","2023-12-01"]
    with pytest.raises(ValueError):
        list(months_between("2024-02-01", "2024-01-01"))


def test_no_data_is_distinct_from_missing_or_failed_state():
    with pytest.raises(ValueError):
        validate_state("SUCCESS", {"snapshots":[]})
    with patch("src.common.run_state.RunState") as state:
        state.return_value.read.side_effect = RuntimeError("Missing state")
        action = MagicMock()
        with pytest.raises(RuntimeError, match="Missing"):
            execute_task(MagicMock(), {"environment": "dev"}, "gold", "silver", action)
        action.assert_not_called()
        state.return_value.read.side_effect = None
        state.return_value.read.return_value = {"status":"NO_DATA","snapshots":[]}
        execute_task(MagicMock(), {"environment": "dev"}, "gold", "silver", action)
        action.assert_not_called()
        assert state.return_value.write.call_args.args[1] == "NO_DATA"


def test_dq_counts_null_timestamp_quarantine_and_does_not_dilute_small_snapshots():
    snapshot = {"snapshot_id":"a", "rows_received":2}
    metric = dict(source_snapshot_id="a", rows_in=2, rows_out=1, rows_quarantined=1,
                  raw_rows_quarantined=1, rows_deduplicated=0)
    with pytest.raises(RuntimeError, match="quarantine rate"):
        check_snapshot_metrics([metric], [snapshot], .1)
    check_snapshot_metrics([metric], [snapshot], .5)
    with pytest.raises(RuntimeError, match="Missing"):
        check_snapshot_metrics([], [snapshot], .5)
    with pytest.raises(RuntimeError, match="reconciliation"):
        check_snapshot_metrics([{**metric, "rows_deduplicated":1}], [snapshot], .5)


def test_required_fields_and_rescue_and_nonfinite_values(spark):
    schema = "tpep_pickup_datetime TIMESTAMP, tpep_dropoff_datetime TIMESTAMP, fare_amount DOUBLE, trip_distance DOUBLE, total_amount DOUBLE, _rescued_data STRING"
    base = [datetime(2024,1,1,8), datetime(2024,1,1,9), 12., 2., 15., None]
    rows = [base]
    for i in range(5):
        row = base.copy(); row[i] = None; rows.append(row)
    row = base.copy(); row[-1] = '{"new_field":1}'; rows.append(row)
    row = base.copy(); row[2] = float('nan'); rows.append(row)
    row = base.copy(); row[4] = float('inf'); rows.append(row)
    reasons = [r.reason_code for r in flag_implausible_trips(spark.createDataFrame(rows, schema)).collect()]
    assert reasons == [None] + ["MISSING_REQUIRED_FIELD"]*5 + ["SCHEMA_RESCUED_DATA", "INVALID_NUMERIC_VALUE", "INVALID_NUMERIC_VALUE"]


def test_reference_duplicates_and_stale_metadata_fail(spark):
    with pytest.raises(RuntimeError, match="Duplicate"):
        validate_dimension(spark.createDataFrame([(1,), (1,)], "location_id INT"), "location_id")
    with pytest.raises(RuntimeError, match="freshness"):
        validate_dimension(spark.createDataFrame([(1,)], "location_id INT"), "location_id", max_age_days=90)
    stale = spark.createDataFrame([(1, datetime(2000,1,1))], "location_id INT, _loaded_at TIMESTAMP")
    with pytest.raises(RuntimeError, match="Stale"):
        validate_dimension(stale, "location_id", max_age_days=90)


def test_snapshot_replacement_removes_corrections_deleted_trips_and_empty_month(spark, tmp_delta_path):
    table = "snapshot_replace_test"
    old = spark.createDataFrame([(date(2024,1,1), "old", 10), (date(2024,1,1), "deleted", 20), (date(2024,2,1), "untouched", 30)],
                               "source_month DATE, trip_id STRING, amount INT")
    old.write.format("delta").partitionBy("source_month").save(tmp_delta_path)
    location = tmp_delta_path.replace('\\', '/')
    spark.sql(f"CREATE TABLE {table} USING DELTA LOCATION '{location}'")
    try:
        correction = spark.createDataFrame([(date(2024,1,1), "corrected", 15)], old.schema)
        for _ in range(2):
            replace_snapshot(spark, correction, table, "2024-01-01")
        assert sorted(r.amount for r in spark.table(table).collect()) == [15,30]
        replace_snapshot(spark, correction.limit(0), table, "2024-01-01")
        assert [r.amount for r in spark.table(table).collect()] == [30]
    finally:
        spark.sql(f"DROP TABLE {table}")


def test_ntz_preserves_ny_wall_clock_across_dst_and_month_boundary(spark):
    df = spark.sql("SELECT timestamp_ntz'2024-03-10 01:59:00' pickup, timestamp_ntz'2024-03-10 03:01:00' dropoff UNION ALL SELECT timestamp_ntz'2024-11-03 01:30:00', timestamp_ntz'2024-11-03 02:30:00' UNION ALL SELECT timestamp_ntz'2024-02-29 23:59:00', timestamp_ntz'2024-03-01 00:01:00'")
    try:
        for zone in ("UTC", "America/New_York", "Asia/Ho_Chi_Minh"):
            spark.conf.set("spark.sql.session.timeZone", zone)
            rows = df.select(F.hour('pickup').alias('hour'), F.trunc('pickup','month').alias('month')).collect()
            assert [r.hour for r in rows] == [1,1,23]
            assert rows[2].month == date(2024,2,1)
    finally:
        spark.conf.set("spark.sql.session.timeZone", "UTC")


def test_bundle_all_tasks_receive_runtime_and_explicit_dependencies():
    config = yaml.safe_load(Path('jobs/databricks.yml').read_text())
    job = config['resources']['jobs']['taxi_pipeline']
    tasks = job['tasks']
    assert tasks[0]['task_key'] == 'source_landing'
    assert tasks[-1]['task_key'] == 'monitor'
    assert job['max_concurrent_runs'] == 1
    required = {
        '--environment', '--catalog', '--bucket', '--project', '--dataset',
        '--pipeline-run-id', '--start-month', '--end-month', '--processing-date',
        '--allow-unpublished',
    }
    for i, task in enumerate(tasks):
        params = task['spark_python_task']['parameters']
        assert required <= set(params)
        assert params.count('--pipeline-run-id') == 1
        if i:
            assert task['depends_on'] == [{'task_key':tasks[i-1]['task_key']}]
    for key in ('bucket','catalog','dataset'):
        assert len({target['variables'][key] for target in config['targets'].values()}) == 3
