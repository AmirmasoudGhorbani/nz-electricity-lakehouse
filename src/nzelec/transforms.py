"""Transformations for the bronze -> silver -> gold layers.

Every function takes and returns a DataFrame, with no I/O, so the same code runs
in the Databricks notebooks and in the local unit tests (tests/).
"""
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .config import REFERENCE_NODES, SPIKE_THRESHOLD

NZ_TZ = "Pacific/Auckland"


def safe_cast(col: str, to: str):
    """Cast that returns NULL for malformed input instead of failing the whole job
    (Spark 4 and Databricks serverless run in strict ANSI mode); the quality rules
    then quarantine those rows."""
    return F.expr(f"try_cast(`{col}` AS {to})")


# ---------------------------------------------------------------- silver: prices

def type_prices(bronze: DataFrame) -> DataFrame:
    """Cast the raw CSV strings and name the columns."""
    return bronze.select(
        safe_cast("TradingDate", "DATE").alias("trading_date"),
        safe_cast("TradingPeriod", "INT").alias("trading_period"),
        F.trim("PointOfConnection").alias("poc"),
        safe_cast("DollarsPerMegawattHour", "DOUBLE").alias("price_nzd_mwh"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )


def add_interval_times(df: DataFrame) -> DataFrame:
    """Turn (trading date, trading period) into real timestamps.

    A trading day has 48 half-hour periods, but 46 when daylight saving starts and
    50 when it ends. Counting periods from local midnight converted to UTC gets
    both right: period 5 on the spring-forward day starts at 03:00 NZDT, not 02:00.
    """
    midnight_utc = F.to_utc_timestamp(F.to_timestamp("trading_date"), NZ_TZ)
    start_utc = F.timestamp_seconds(F.unix_seconds(midnight_utc) + (F.col("trading_period") - 1) * 1800)
    return (df
            .withColumn("interval_start_utc", start_utc)
            .withColumn("interval_start_nz", F.from_utc_timestamp(start_utc, NZ_TZ).cast("timestamp_ntz")))


# Each rule is a SQL condition every valid row must satisfy
PRICE_RULES = {
    "date_present": "trading_date IS NOT NULL",
    "period_in_range": "trading_period BETWEEN 1 AND 50",
    "poc_format": "poc RLIKE '^[A-Z]{3}[0-9]{4}$'",
    "price_present": "price_nzd_mwh IS NOT NULL",
    # +/-100,000 fills whole days at 103 nodes in 1998-2006: a placeholder, not a price
    "not_placeholder": "price_nzd_mwh IS NULL OR abs(price_nzd_mwh) <> 100000",
    "price_plausible": "price_nzd_mwh IS NULL OR abs(price_nzd_mwh) = 100000 OR price_nzd_mwh BETWEEN -10000 AND 50000",
}


def split_by_rules(df: DataFrame, rules: dict) -> tuple:
    """Return (valid, quarantined); quarantined rows carry the names of the rules they broke."""
    failed = F.array_compact(F.array(*[
        F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in rules.items()
    ]))
    checked = df.withColumn("failed_rules", failed)
    valid = checked.where(F.size("failed_rules") == 0).drop("failed_rules")
    quarantined = checked.where(F.size("failed_rules") > 0)
    return valid, quarantined


def latest_per_key(df: DataFrame, keys: list, order_col: str) -> DataFrame:
    """Keep one row per key: the most recently ingested (prices can be revised)."""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")


def silver_prices(bronze: DataFrame) -> tuple:
    """Bronze rows -> (clean, de-duplicated prices with timestamps, quarantined rows)."""
    valid, quarantined = split_by_rules(type_prices(bronze), PRICE_RULES)
    clean = latest_per_key(valid, ["trading_date", "trading_period", "poc"], "ingested_at")
    return add_interval_times(clean), quarantined


# ---------------------------------------------------------------- silver: reference data

def silver_grid_points(bronze: DataFrame) -> DataFrame:
    """One row per point of connection (POC), with its island, zone and region.

    The source table lists every network supply point, so a POC appears many
    times; prefer current rows, then the most common description.
    """
    rows = bronze.select(
        F.trim("poc_code").alias("poc"),
        safe_cast("current_flag", "INT").alias("is_current"),
        F.col("island"),
        F.col("zone"),
        F.col("network_reporting_region").alias("region"),
        F.col("description"),
        safe_cast("nztm_easting", "DOUBLE").alias("nztm_easting"),
        safe_cast("nztm_northing", "DOUBLE").alias("nztm_northing"),
    ).where(F.col("poc").isNotNull())
    w = Window.partitionBy("poc").orderBy(F.col("is_current").desc_nulls_last(),
                                          F.col("nztm_easting").isNull(), "region")
    return (rows.withColumn("_rn", F.row_number().over(w)).where("_rn = 1")
            .drop("_rn", "is_current"))


def silver_hydro(bronze: DataFrame, lakes: DataFrame) -> DataFrame:
    """Daily lake storage, one row per lake per day, with the lake's name and island."""
    code = F.regexp_extract("_source_file", r"_([A-Z]{3})_Storage_", 1)
    daily = bronze.select(
        safe_cast("date", "DATE").alias("date"),
        code.alias("site_code"),
        safe_cast("lake_level_m", "DOUBLE").alias("lake_level_m"),
        safe_cast("active_storage_mm3", "DOUBLE").alias("storage_mm3"),
        safe_cast("qualitycode", "INT").alias("quality_code"),
    ).where("date IS NOT NULL AND storage_mm3 IS NOT NULL")
    daily = daily.groupBy("date", "site_code").agg(
        F.avg("lake_level_m").alias("lake_level_m"), F.avg("storage_mm3").alias("storage_mm3"),
        F.max("quality_code").alias("quality_code"))
    meta = lakes.select(F.col("sitecode").alias("site_code"), F.col("description").alias("lake"),
                        F.col("plantgroup").alias("scheme"), F.col("island"))
    return daily.join(meta, "site_code", "left")


# ---------------------------------------------------------------- gold

def gold_daily_reference(prices: DataFrame) -> DataFrame:
    """Daily price summary at the two benchmark nodes (Otahuhu and Benmore)."""
    ref = prices.where(F.col("poc").isin(list(REFERENCE_NODES)))
    return (ref.groupBy("trading_date", "poc").agg(
                F.avg("price_nzd_mwh").alias("avg_price"),
                F.max("price_nzd_mwh").alias("max_price"),
                F.percentile_approx("price_nzd_mwh", 0.95).alias("p95_price"),
                F.sum(F.when(F.col("price_nzd_mwh") > SPIKE_THRESHOLD, 1).otherwise(0)).alias("spike_periods"),
                F.count("*").alias("periods"))
            .withColumn("node", F.create_map(*[x for k, v in REFERENCE_NODES.items() for x in (F.lit(k), F.lit(v))])[F.col("poc")]))


def gold_monthly_reference(daily: DataFrame) -> DataFrame:
    """Monthly averages at both benchmark nodes side by side, plus the island spread."""
    m = (daily.withColumn("month", F.trunc("trading_date", "month"))
         .groupBy("month").pivot("poc", list(REFERENCE_NODES))
         .agg(F.avg("avg_price"), F.sum("spike_periods")))
    rename = {f"{k}_{agg}": f"{k.lower()}_{name}" for k in REFERENCE_NODES
              for agg, name in (("avg(avg_price)", "price"), ("sum(spike_periods)", "spikes"))}
    m = reduce(lambda acc, kv: acc.withColumnRenamed(*kv), rename.items(), m)
    return m.withColumn("north_south_spread", F.col("ota2201_price") - F.col("ben2201_price")).orderBy("month")


def gold_hourly_profile(prices: DataFrame, since: str) -> DataFrame:
    """Average price by NZ hour of day and season at Otahuhu: when is power dearest?"""
    df = prices.where((F.col("poc") == "OTA2201") & (F.col("trading_date") >= since))
    season = (F.when(F.month("trading_date").isin(6, 7, 8), "Winter")
              .when(F.month("trading_date").isin(12, 1, 2), "Summer")
              .when(F.month("trading_date").isin(3, 4, 5), "Autumn").otherwise("Spring"))
    return (df.withColumn("season", season).withColumn("hour", F.hour("interval_start_nz"))
            .groupBy("season", "hour").agg(F.avg("price_nzd_mwh").alias("avg_price"))
            .orderBy("season", "hour"))


def gold_hydro_national(hydro: DataFrame) -> DataFrame:
    """National hydro storage per day (days with all lakes reported), and how it
    compares with the average for that time of year."""
    n_lakes = hydro.select("site_code").distinct().count()
    daily = (hydro.groupBy("date").agg(F.sum("storage_mm3").alias("storage_mm3"),
                                       F.count("*").alias("lakes"))
             .where(F.col("lakes") == n_lakes).drop("lakes")
             .withColumn("doy", F.dayofyear("date")))
    norm = daily.groupBy("doy").agg(F.avg("storage_mm3").alias("mean_for_doy"))
    return (daily.join(norm, "doy")
            .withColumn("pct_of_mean", F.round(100 * F.col("storage_mm3") / F.col("mean_for_doy"), 1))
            .drop("doy").orderBy("date"))


def gold_monthly_price_vs_storage(monthly: DataFrame, hydro_national: DataFrame) -> DataFrame:
    """Join monthly prices to monthly average storage (% of normal for the time of year)."""
    h = (hydro_national.withColumn("month", F.trunc("date", "month"))
         .groupBy("month").agg(F.avg("pct_of_mean").alias("storage_pct_of_mean")))
    return monthly.join(h, "month", "inner").orderBy("month")


def gold_node_premium(prices: DataFrame, grid_points: DataFrame, since: str) -> DataFrame:
    """Each node's average price over a period relative to Otahuhu, with its location.
    Differences come from transmission losses and constraints."""
    p = prices.where(F.col("trading_date") >= since)
    ota = (p.where(F.col("poc") == "OTA2201")
           .select("trading_date", "trading_period", F.col("price_nzd_mwh").alias("ota_price")))
    return (p.join(ota, ["trading_date", "trading_period"])
            .groupBy("poc").agg(F.avg("price_nzd_mwh").alias("avg_price"),
                                F.avg(F.col("price_nzd_mwh") - F.col("ota_price")).alias("premium_vs_ota"),
                                F.count("*").alias("periods"))
            .join(grid_points, "poc", "left")
            .orderBy(F.col("premium_vs_ota").desc()))
