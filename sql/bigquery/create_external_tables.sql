-- sql/bigquery/create_external_tables.sql
-- Zero-ETL / BigLake External Tables over GCS Gold Mart Parquet exports.
-- Automatically reflects new month partitions written to GCS without manual bq load.

-- 1. revenue_by_zone_hour
CREATE OR REPLACE EXTERNAL TABLE `taxi-data-engineer.taxi_analytics.revenue_by_zone_hour`
WITH PARTITION COLUMNS (
  pickup_month INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-data-engineer-taxi-lake/gold_export/revenue_by_zone_hour/*'],
  hive_partition_uri_prefix = 'gs://taxi-data-engineer-taxi-lake/gold_export/revenue_by_zone_hour'
);

-- 2. fare_integrity_daily
CREATE OR REPLACE EXTERNAL TABLE `taxi-data-engineer.taxi_analytics.fare_integrity_daily`
WITH PARTITION COLUMNS (
  pickup_month INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-data-engineer-taxi-lake/gold_export/fare_integrity_daily/*'],
  hive_partition_uri_prefix = 'gs://taxi-data-engineer-taxi-lake/gold_export/fare_integrity_daily'
);

-- 3. payment_mix_monthly
CREATE OR REPLACE EXTERNAL TABLE `taxi-data-engineer.taxi_analytics.payment_mix_monthly`
WITH PARTITION COLUMNS (
  pickup_month INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://taxi-data-engineer-taxi-lake/gold_export/payment_mix_monthly/*'],
  hive_partition_uri_prefix = 'gs://taxi-data-engineer-taxi-lake/gold_export/payment_mix_monthly'
);
