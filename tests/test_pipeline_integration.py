"""Local integration uses real Spark/Delta and real runtime entry functions.

Only source HTTP/GCS, Auto Loader discovery and BigQuery transport are injected.
"""
from datetime import datetime
from unittest.mock import MagicMock
from uuid import uuid4

from pyspark.sql import functions as F

from src.common import paths
from src.common.run_state import RunState
from src.common.schemas import BRONZE_SCHEMA
from src.ingestion import source_landing, bronze_autoloader
from src.transform import silver_clean, run_dq_gate, run_gold_sql
from src.export import export_bigquery
from scripts.reference_preflight import run as preflight
from scripts.monitor_pipeline import run as monitor


def test_source_to_gold_retry_correction_and_durable_recovery(spark, monkeypatch):
    cfg = {"pipeline_run_id":"integration-1", "environment":"staging", "databricks":{"catalog":"taxi_lakehouse_staging"},
           "gcp":{"bucket":"lake-staging"}, "start_month":"2024-01-01", "end_month":"2024-01-01",
           "processing_date":"2024-04-01", "allow_unpublished":False,
           "thresholds":{"quarantine_rate_max":.4}, "reference":{"max_age_days":90},
           "monitoring":{"max_no_data_runs":40,"max_source_age_days":100,"min_snapshot_rows":1}}
    prefix = "it_" + uuid4().hex[:8] + "_"
    monkeypatch.setattr(
        paths,
        "catalog_table",
        lambda schema, table, config=None: f"{prefix}{schema}.{table}",
    )
    sql = spark.sql
    def local_sql(statement, *args, **kwargs):
        statement = statement.replace('taxi_lakehouse_staging.', prefix)
        for schema in ('bronze','silver','gold','reference'):
                statement = statement.replace('spark_catalog.'+schema+'.',prefix+schema+'.')

        return sql(statement,*args,**kwargs)
    monkeypatch.setattr(spark,'sql',local_sql)
    for schema in ('bronze','silver','gold','reference'):
        sql('CREATE DATABASE '+prefix+schema)
    created = []
    def save(frame,schema,table):
        name = paths.catalog_table(schema,table,cfg)
        frame.write.format('delta').mode('overwrite').saveAsTable(name)
        created.append(name)
    def published(_spark, _cfg, payload):
        exports = [
            {"mart": metric["mart"], "month": metric["month"], "uri": "gs://test/export"}
            for metric in payload["gold_metrics"]
        ]
        return {
            **payload,
            "serving_status": "READY_FOR_SERVING",
            "serving_handoff": {"exports": exports},
        }

    try:
        for name, rows, schema in [
          ('dim_zone',[(4,'Manhattan','Zone',False)],'location_id INT, borough STRING, zone STRING, is_sentinel BOOLEAN'),
          ('dim_rate_code',[(1,'Standard',False)],'rate_code_id INT, rate_code_name STRING, is_flat_fare BOOLEAN'),
          ('dim_payment_type',[(1,'Credit card',True)],'payment_type_id INT, payment_type_name STRING, tip_is_recorded BOOLEAN')]:
            save(spark.createDataFrame(rows,schema).withColumn('_loaded_at',F.current_timestamp()),'reference',name)
        base = {f.name:None for f in BRONZE_SCHEMA}
        base.update(VendorID=1,tpep_pickup_datetime=datetime(2024,1,1,8),tpep_dropoff_datetime=datetime(2024,1,1,9),
                    passenger_count=1,trip_distance=2.,RatecodeID=1,PULocationID=4,DOLocationID=4,payment_type=1,
                    fare_amount=10.,tip_amount=2.,total_amount=12.)
        snapshot = dict(source_month='2024-01-01',snapshot_id='a'*64,uri='gs://lake-staging/staging/raw/a.parquet',
                        checksum='a'*64,generation='1',rows_received=3)
        source_landing.run(spark,cfg,MagicMock(),lambda *args:snapshot)
        preflight(spark,cfg)
        data = [base,{**base,'tpep_pickup_datetime':datetime(2024,1,1,7)}, {**base,'tpep_pickup_datetime':None}]
        bronze = spark.createDataFrame(data,BRONZE_SCHEMA).withColumn('_source_file',F.lit(snapshot['uri'])).withColumn('_ingested_at',F.current_timestamp())
        save(bronze,'bronze','trips_raw')
        monkeypatch.setattr(bronze_autoloader,'build_bronze_stream',lambda *args:MagicMock())
        bronze_autoloader.run(spark,cfg)
        first = silver_clean.run(spark,cfg)
        assert first['metrics'][0]['raw_rows_quarantined'] == 1
        # Simulate failure AFTER Silver committed but BEFORE downstream ran.
        state = RunState(spark,cfg)
        state.write('transform_silver','FAILED',first)
        try:
            state.read('transform_silver')
            raise AssertionError('FAILED state must win over stale status in its payload')
        except RuntimeError:
            pass
        silver_clean.run(spark,cfg)
        run_dq_gate.run(spark,cfg)
        run_gold_sql.run(spark,cfg)
        export_bigquery.run(spark,cfg,published)
        monitor(spark,cfg)
        clean = paths.catalog_table('silver','trips_clean',cfg)
        assert spark.table(clean).count() == 2
        assert RunState(spark,cfg).read('monitor')['status'] == 'SUCCESS'
        # New monthly correction removes one trip and changes the remaining fare.
        cfg = {**cfg,'pipeline_run_id':'integration-2'}
        corrected = {**snapshot,'snapshot_id':'b'*64,'checksum':'b'*64,'generation':'2','uri':snapshot['uri'].replace('a.parquet','b.parquet'),'rows_received':1}
        source_landing.run(spark,cfg,MagicMock(),lambda *args:corrected)
        preflight(spark,cfg)
        (spark.createDataFrame([{**base,'fare_amount':20.,'total_amount':22.}],BRONZE_SCHEMA)
         .withColumn('_source_file',F.lit(corrected['uri'])).withColumn('_ingested_at',F.current_timestamp())
         .write.format('delta').mode('append').saveAsTable(paths.catalog_table('bronze','trips_raw',cfg)))
        bronze_autoloader.run(spark,cfg)
        silver_clean.run(spark,cfg)
        run_dq_gate.run(spark,cfg)
        run_gold_sql.run(spark,cfg)
        export_bigquery.run(spark,cfg,published)
        monitor(spark,cfg)
        assert spark.table(clean).count() == 1
        assert spark.table(clean).first().total_amount == 22
        assert spark.table(paths.catalog_table('silver','trips_quarantine',cfg)).count() == 0
    finally:
        for schema in ('bronze','silver','gold','reference'):
            sql('DROP DATABASE IF EXISTS '+prefix+schema+' CASCADE')
