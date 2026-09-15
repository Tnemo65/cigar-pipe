"""Fail before ingestion if reference dimensions are unsafe to join."""
import sys
from pathlib import Path

_file = globals().get("__file__") or globals().get("filename") or (sys.argv[0] if sys.argv else None)
_root = Path(_file).resolve().parents[1] if _file else Path.cwd()
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from pyspark.sql import SparkSession
from src.common.reference import load_references
from src.common.run_state import execute_task
from src.common.runtime import configure_spark, runtime_config


def run(spark, config):
    def action(payload):
        load_references(spark, config)
        return payload
    return execute_task(spark, config, "reference_preflight", "source_landing", action)


if __name__ == "__main__":
    cfg = runtime_config()
    spark = SparkSession.builder.getOrCreate()
    configure_spark(spark)
    run(spark, cfg)
