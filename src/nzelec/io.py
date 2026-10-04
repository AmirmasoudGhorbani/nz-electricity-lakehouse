"""Reading the landing zone into bronze tables.

Prices arrive as one gzipped CSV per month. On Databricks they're ingested with
Auto Loader, which remembers which files it has already loaded, so dropping a new
month into the volume and re-running the job loads only that file. Locally (for
tests) the same files are read in one batch.
"""
import re

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import Config

PRICE_COLUMNS = "TradingDate STRING, TradingPeriod STRING, PointOfConnection STRING, DollarsPerMegawattHour STRING"


def clean_column(name: str) -> str:
    """'Active storage (Mm³)' -> 'active_storage_mm3' (Delta forbids spaces and brackets)."""
    prefix = "_" if name.startswith("_") else ""      # keep lineage columns like _source_file
    name = name.replace("³", "3")
    return prefix + re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def with_clean_columns(df: DataFrame) -> DataFrame:
    return df.toDF(*[clean_column(c) for c in df.columns])


def with_lineage(df: DataFrame) -> DataFrame:
    """Record where each row came from and when it was loaded."""
    return (df.withColumn("_source_file", F.col("_metadata.file_path"))
              .withColumn("_ingested_at", F.current_timestamp()))


def ingest_prices_autoloader(spark: SparkSession, cfg: Config) -> None:
    """Incrementally load new monthly price files into bronze_prices (Databricks only)."""
    stream = (spark.readStream.format("cloudFiles")
              .option("cloudFiles.format", "csv")
              .option("cloudFiles.schemaLocation", f"{cfg.checkpoints}/bronze_prices_schema")
              .option("header", "true")
              .schema(PRICE_COLUMNS)
              .load(f"{cfg.raw}/prices/"))
    (with_lineage(stream).writeStream
     .option("checkpointLocation", f"{cfg.checkpoints}/bronze_prices")
     .trigger(availableNow=True)
     .toTable(cfg.table("bronze_prices"))
     .awaitTermination())


def read_prices_batch(spark: SparkSession, path: str) -> DataFrame:
    """Read price files in one go (local runs and tests)."""
    return with_lineage(spark.read.option("header", "true").schema(PRICE_COLUMNS).csv(path))


def read_reference_csv(spark: SparkSession, path: str) -> DataFrame:
    """Small reference files: read as strings, clean the column names, keep lineage."""
    return with_clean_columns(with_lineage(spark.read.option("header", "true").csv(path)))
