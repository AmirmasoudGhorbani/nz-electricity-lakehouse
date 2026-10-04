"""The pipeline steps, shared by the Databricks notebooks and the local runner.

    bronze  raw rows exactly as delivered, plus where and when each row was loaded
    silver  typed, validated, de-duplicated; failures go to a quarantine table
    gold    analysis-ready tables for the dashboard
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import transforms as T
from .config import Config
from .io import read_reference_csv

SILVER_PRICES_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    trading_date DATE, trading_period INT, poc STRING, price_nzd_mwh DOUBLE,
    source_file STRING, ingested_at TIMESTAMP,
    interval_start_utc TIMESTAMP, interval_start_nz TIMESTAMP_NTZ
) USING DELTA {clustering}
COMMENT 'Half-hourly final wholesale prices by node: typed, validated and de-duplicated'
"""

QUARANTINE_DDL = """
CREATE TABLE IF NOT EXISTS {table} (
    trading_date DATE, trading_period INT, poc STRING, price_nzd_mwh DOUBLE,
    source_file STRING, ingested_at TIMESTAMP, failed_rules ARRAY<STRING>
) USING DELTA
COMMENT 'Price rows that failed a data-quality rule, with the rules they broke'
"""


def refresh_reference(spark: SparkSession, cfg: Config) -> None:
    """Grid points and hydro storage are small, so they're reloaded in full each run."""
    grid = read_reference_csv(spark, f"{cfg.raw}/reference/")
    grid.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("bronze_grid_points"))
    T.silver_grid_points(grid).write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(cfg.table("silver_grid_points"))

    hydro = read_reference_csv(spark, f"{cfg.raw}/hydro/*_Storage_*.csv")
    lakes = read_reference_csv(spark, f"{cfg.raw}/hydro/FileIndex_Storage.csv")
    hydro.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table("bronze_hydro_storage"))
    T.silver_hydro(hydro, lakes).write.mode("overwrite").option("overwriteSchema", "true") \
        .saveAsTable(cfg.table("silver_hydro_storage"))


def update_silver_prices(spark: SparkSession, cfg: Config, clustered: bool = True) -> None:
    """Process only the bronze rows that arrived since the last run, and MERGE them
    into silver, so re-running is safe and revised prices replace old ones."""
    silver, quarantine = cfg.table("silver_prices"), cfg.table("silver_prices_quarantine")
    spark.sql(SILVER_PRICES_DDL.format(table=silver,
                                       clustering="CLUSTER BY (trading_date, poc)" if clustered else ""))
    spark.sql(QUARANTINE_DDL.format(table=quarantine))

    def upsert(batch: DataFrame, batch_id: int) -> None:
        clean, bad = T.silver_prices(batch)
        bad.select("trading_date", "trading_period", "poc", "price_nzd_mwh", "source_file",
                   "ingested_at", "failed_rules").write.mode("append").saveAsTable(quarantine)
        clean.createOrReplaceTempView("price_updates")
        batch.sparkSession.sql(f"""
            MERGE INTO {silver} t
            USING price_updates s
            ON  t.trading_date = s.trading_date AND t.trading_period = s.trading_period AND t.poc = s.poc
            WHEN MATCHED AND s.ingested_at > t.ingested_at THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)

    (spark.readStream.option("maxFilesPerTrigger", 200).table(cfg.table("bronze_prices"))
     .writeStream.foreachBatch(upsert)
     .option("checkpointLocation", f"{cfg.checkpoints}/silver_prices")
     .trigger(availableNow=True)
     .start().awaitTermination())


def build_gold(spark: SparkSession, cfg: Config, recent_since: str) -> None:
    """Rebuild the gold tables from silver (they're small, so a full rebuild is simplest)."""
    prices = spark.table(cfg.table("silver_prices"))
    grid = spark.table(cfg.table("silver_grid_points"))
    hydro = spark.table(cfg.table("silver_hydro_storage"))

    def save(df: DataFrame, name: str, comment: str) -> None:
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(cfg.table(name))
        spark.sql(f"COMMENT ON TABLE {cfg.table(name)} IS '{comment}'")

    daily = T.gold_daily_reference(prices)
    save(daily, "gold_daily_reference_prices", "Daily price summary at Otahuhu and Benmore")
    monthly = T.gold_monthly_reference(spark.table(cfg.table("gold_daily_reference_prices")))
    save(monthly, "gold_monthly_prices", "Monthly average prices at both reference nodes and the island spread")
    save(T.gold_hourly_profile(prices, recent_since), "gold_hourly_profile",
         f"Average Otahuhu price by NZ hour and season since {recent_since}")
    hydro_nat = T.gold_hydro_national(hydro)
    save(hydro_nat, "gold_hydro_national", "National hydro storage per day and as a percentage of normal")
    save(T.gold_monthly_price_vs_storage(spark.table(cfg.table("gold_monthly_prices")),
                                         spark.table(cfg.table("gold_hydro_national"))),
         "gold_price_vs_storage", "Monthly price alongside hydro storage relative to normal")
    save(T.gold_node_premium(prices, grid, recent_since), "gold_node_premium",
         f"Average price at each node relative to Otahuhu since {recent_since}")


def quality_report(spark: SparkSession, cfg: Config) -> DataFrame:
    """Row counts and checks worth looking at after every run. The market's first
    week (1-7 October 1996) is known to be incomplete, so it's excluded from the
    trading-period check."""
    p = cfg.table("silver_prices")
    return spark.sql(f"""
        SELECT
          (SELECT count(*) FROM {cfg.table('bronze_prices')})            AS bronze_rows,
          (SELECT count(*) FROM {p})                                     AS silver_rows,
          (SELECT count(*) FROM {cfg.table('silver_prices_quarantine')}) AS quarantined_rows,
          (SELECT min(trading_date) FROM {p})                            AS first_day,
          (SELECT max(trading_date) FROM {p})                            AS last_day,
          (SELECT count(DISTINCT poc) FROM {p})                          AS nodes,
          (SELECT count(*) FROM (SELECT trading_date, count(DISTINCT trading_period) n
                                 FROM {p} WHERE poc = 'OTA2201' GROUP BY 1)
           WHERE n NOT IN (46, 48, 50)
             AND trading_date >= '1996-10-08')                          AS days_with_odd_period_counts
    """)
