"""Explicit runtime configuration for every scheduled task."""
import argparse
import re
from datetime import date

from src.common import paths


def month_start(value: str) -> str:
    parsed = date.fromisoformat(value)
    if parsed.day != 1 or parsed.isoformat() != value:
        raise ValueError(f"Expected first-of-month YYYY-MM-01: {value}")
    return value


def runtime_config(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="configs/config.yaml")
    for name in ("environment", "catalog", "bucket", "project", "dataset", "pipeline-run-id"):
        parser.add_argument(f"--{name}", required=True)
    parser.add_argument("--start-month", default="")
    parser.add_argument("--end-month", default="")
    parser.add_argument("--processing-date", default="")
    parser.add_argument("--allow-unpublished", choices=("true", "false"), default="true")
    parser.add_argument("--zone-csv", default="")
    parser.add_argument("--source-uri", action="append", default=[], metavar="MONTH=URI")
    args = parser.parse_args(argv)
    if args.environment not in ("dev", "staging", "prod"):
        raise ValueError("Unknown environment")
    for value in (args.catalog, args.dataset):
        paths.identifier(value)
        if not value.endswith(f"_{args.environment}"):
            raise ValueError("Catalog and dataset must end with the environment suffix")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,220}[a-z0-9]", args.bucket):
        raise ValueError("Invalid bucket")
    if not args.bucket.endswith(f"-{args.environment}"):
        raise ValueError("Bucket must end with the environment suffix")
    if not re.fullmatch(r"[a-z][a-z0-9-]{4,28}[a-z0-9]", args.project):
        raise ValueError("Invalid project")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", args.pipeline_run_id):
        raise ValueError("Invalid pipeline run ID")
    if bool(args.start_month) != bool(args.end_month):
        raise ValueError("Backfill requires both start-month and end-month")
    if args.start_month:
        month_start(args.start_month)
        month_start(args.end_month)
        if args.start_month > args.end_month:
            raise ValueError("Backfill range is reversed")
        start, end = date.fromisoformat(args.start_month), date.fromisoformat(args.end_month)
        if (end.year-start.year)*12+end.month-start.month >= 12:
            raise ValueError("Limit each backfill to 12 months")
    cfg = paths.load_config(args.config)
    cfg.update(environment=args.environment, pipeline_run_id=args.pipeline_run_id,
               start_month=args.start_month, end_month=args.end_month,
               processing_date=args.processing_date or date.today().isoformat(),
               allow_unpublished=args.allow_unpublished == "true")
    date.fromisoformat(cfg["processing_date"])
    cfg["gcp"].update(bucket=args.bucket, project_id=args.project)
    cfg["databricks"]["catalog"] = args.catalog
    cfg["bigquery"]["dataset"] = args.dataset
    cfg["zone_csv"] = args.zone_csv
    cfg["source_uris"] = {}
    for mapping in args.source_uri:
        for item in mapping.split(";"):
            month, separator, uri = item.partition("=")
            if not separator or not uri or month != month_start(month):
                raise ValueError("source-uri must use MONTH=URI with a first-of-month MONTH")
            cfg["source_uris"][month] = uri
    if len(cfg["source_uris"]) == 1 and "=" in args.source_uri[0]:
        cfg["source_uris"] = dict(cfg["source_uris"])
    if args.start_month:
        cfg["allow_unpublished"] = False
    return cfg


def configure_spark(spark):
    # TLC Parquet encodes local wall-clock values. UTC prevents an implicit
    # session offset on legacy timestamp readers; Silver stores TIMESTAMP_NTZ.
    spark.conf.set("spark.sql.session.timeZone", "UTC")
