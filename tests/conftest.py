import os
import shutil
import sys
import tempfile
from pathlib import Path

# Sanitize Java & Hadoop environment on Windows
java_home = os.environ.get("JAVA_HOME", "")
if os.path.isdir(java_home):
    os.environ["JAVA_HOME"] = java_home
    if f"{java_home}\\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{java_home}\\bin;" + os.environ.get("PATH", "")

hadoop_home = r"C:\hadoop"
if os.path.isdir(hadoop_home):
    os.environ["HADOOP_HOME"] = hadoop_home
    if f"{hadoop_home}\\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = f"{hadoop_home}\\bin;" + os.environ.get("PATH", "")

# Point workers to virtualenv Python interpreter
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    if not shutil.which("java") and not Path(os.environ.get("JAVA_HOME", ""), "bin", "java.exe").is_file():
        pytest.fail("Java 17 is required; set JAVA_HOME before running Spark tests")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("taxi-lakehouse-tests")
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield session
    session.stop()


@pytest.fixture()
def tmp_delta_path():
    """A throwaway local directory for a Delta table, cleaned up after the test."""
    path = Path(tempfile.mkdtemp(prefix="taxi_delta_"))
    yield str(path)
    shutil.rmtree(path, ignore_errors=True)
