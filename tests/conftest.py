import sys
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))


@pytest.fixture(scope="session")
def spark():
    s = (SparkSession.builder.master("local[2]").appName("nzelec-tests")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.ui.enabled", "false")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.ansi.enabled", "true")          # match Databricks serverless
         .getOrCreate())
    yield s
    s.stop()
