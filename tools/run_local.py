"""Run the whole pipeline on a laptop with open-source Spark and Delta Lake.

Used to test the code before it runs on Databricks. Auto Loader is Databricks-only,
so bronze is loaded here with a plain batch read; silver and gold run the exact
same functions as the notebooks.

    pip install -r requirements-dev.txt
    python tools/run_local.py /path/to/nz-electricity-data
"""
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402

from nzelec.config import Config  # noqa: E402
from nzelec.io import read_prices_batch  # noqa: E402
from nzelec.pipeline import build_gold, quality_report, refresh_reference, update_silver_prices  # noqa: E402


def main(data_dir: str, warehouse: str = "/tmp/nzelec-warehouse") -> None:
    builder = (SparkSession.builder.master("local[*]").appName("nzelec-local")
               .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
               .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
               .config("spark.sql.warehouse.dir", warehouse)
               .config("spark.sql.ansi.enabled", "true")
               .config("spark.sql.sources.default", "delta")      # as on Databricks
               .config("spark.driver.memory", "8g")
               .config("spark.ui.enabled", "false"))
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    cfg = Config(catalog="spark_catalog", raw_path=str(Path(data_dir).resolve()))
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {cfg.catalog}.{cfg.schema}")

    t = time.time()
    if not spark.catalog.tableExists(cfg.table("bronze_prices")):
        read_prices_batch(spark, f"{cfg.raw}/prices/").write.saveAsTable(cfg.table("bronze_prices"))
    print(f"bronze  {time.time() - t:6.0f}s"); t = time.time()
    refresh_reference(spark, cfg)
    update_silver_prices(spark, cfg, clustered=False)
    print(f"silver  {time.time() - t:6.0f}s"); t = time.time()
    build_gold(spark, cfg, recent_since="2025-09-01")
    print(f"gold    {time.time() - t:6.0f}s")
    quality_report(spark, cfg).show(vertical=True, truncate=False)


if __name__ == "__main__":
    main(sys.argv[1])
