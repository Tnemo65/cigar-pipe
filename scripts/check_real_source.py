"""Measure required-field/business-rule outcomes on an actual local TLC file.

This is a source validation check, not a cloud performance benchmark.
"""
import argparse
import hashlib
import json
import os
import sys
import time
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F
from src.common.runtime import configure_spark, month_start
from src.common.schemas import BRONZE_SCHEMA
from src.transform.validity_rules import flag_implausible_trips


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--file',required=True)
    parser.add_argument('--source-month',required=True)
    parser.add_argument('--output',default='artifacts/real-source-check.json')
    args=parser.parse_args()
    month_start(args.source_month)
    os.environ['PYSPARK_PYTHON']=sys.executable
    spark=SparkSession.builder.master('local[2]').appName('real-tlc-source-check').getOrCreate()
    configure_spark(spark)
    started=time.monotonic()
    try:
        frame=spark.read.schema(BRONZE_SCHEMA).parquet(args.file).withColumn('source_month',F.lit(args.source_month).cast('date'))
        counts={r.reason_code or 'VALID_BUSINESS_RULES':r['count'] for r in flag_implausible_trips(frame).groupBy('reason_code').count().collect()}
        with open(args.file,'rb') as file:
            checksum=hashlib.file_digest(file,'sha256').hexdigest()
        report=dict(source_month=args.source_month,checksum=checksum,rows=sum(counts.values()),
                    business_rule_counts=counts,elapsed_seconds=round(time.monotonic()-started,3),
                    scope='Actual Parquet decode and business-rule validation; excludes reference joins, Delta writes, Auto Loader and BigQuery')
        output=Path(args.output);output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(report,indent=2),encoding='utf-8')
        print(json.dumps(report,indent=2))
    finally:
        spark.stop()


if __name__=='__main__':
    main()
