import datetime as dt

from pyspark.sql import functions as F

from nzelec.io import clean_column
from nzelec.transforms import (PRICE_RULES, add_interval_times, latest_per_key, silver_grid_points,
                               split_by_rules, type_prices)


def bronze_rows(spark, rows):
    cols = ["TradingDate", "TradingPeriod", "PointOfConnection", "DollarsPerMegawattHour", "_source_file", "_ingested_at"]
    return spark.createDataFrame(rows, cols)


def interval(spark, date, period):
    df = spark.createDataFrame([(dt.date.fromisoformat(date), period)], ["trading_date", "trading_period"])
    r = add_interval_times(df).first()
    return r.interval_start_utc, r.interval_start_nz


def test_normal_day_starts_at_local_midnight(spark):
    utc, nz = interval(spark, "2025-07-01", 1)
    assert nz == dt.datetime(2025, 7, 1, 0, 0)
    assert utc == dt.datetime(2025, 6, 30, 12, 0)          # NZST is UTC+12


def test_last_period_of_normal_day(spark):
    _, nz = interval(spark, "2025-07-01", 48)
    assert nz == dt.datetime(2025, 7, 1, 23, 30)


def test_daylight_saving_starts_46_periods(spark):
    # 28 Sep 2025: clocks jump from 02:00 to 03:00, so period 5 starts at 03:00
    _, p4 = interval(spark, "2025-09-28", 4)
    _, p5 = interval(spark, "2025-09-28", 5)
    _, p46 = interval(spark, "2025-09-28", 46)
    assert p4 == dt.datetime(2025, 9, 28, 1, 30)
    assert p5 == dt.datetime(2025, 9, 28, 3, 0)
    assert p46 == dt.datetime(2025, 9, 28, 23, 30)


def test_daylight_saving_ends_50_periods(spark):
    # 6 Apr 2025: 03:00 becomes 02:00 again, so the 02:00-03:00 hour happens twice
    utc5, nz5 = interval(spark, "2025-04-06", 5)
    utc7, nz7 = interval(spark, "2025-04-06", 7)
    _, p50 = interval(spark, "2025-04-06", 50)
    assert nz5 == nz7 == dt.datetime(2025, 4, 6, 2, 0)       # same wall clock...
    assert (utc7 - utc5) == dt.timedelta(hours=1)            # ...an hour apart in real time
    assert p50 == dt.datetime(2025, 4, 6, 23, 30)


def test_quality_rules_quarantine_bad_rows(spark):
    now = dt.datetime(2026, 1, 1)
    df = type_prices(bronze_rows(spark, [
        ("2025-07-01", "1", "OTA2201", "120.5", "f", now),      # fine
        ("2025-07-01", "51", "OTA2201", "120.5", "f", now),     # period out of range
        ("2025-07-01", "2", "otahuhu", "120.5", "f", now),      # bad node code
        ("2025-07-01", "3", "OTA2201", "", "f", now),           # missing price
        ("not a date", "4", "OTA2201", "99", "f", now),         # bad date
        ("2025-07-01", "5", "KPA1101", "100000", "f", now),     # placeholder value
        ("2025-07-01", "6", "OTA2201", "60000", "f", now),      # implausibly high
        ("2025-07-01", "7", "OTA2201", "-25.5", "f", now),      # negative prices do happen
    ]))
    valid, bad = split_by_rules(df, PRICE_RULES)
    assert valid.count() == 2
    reasons = {r.trading_period: set(r.failed_rules) for r in bad.collect()}
    assert reasons[51] == {"period_in_range"}
    assert reasons[2] == {"poc_format"}
    assert reasons[3] == {"price_present"}
    assert reasons[4] == {"date_present"}
    assert reasons[5] == {"not_placeholder"}
    assert reasons[6] == {"price_plausible"}


def test_latest_ingest_wins_on_duplicates(spark):
    df = spark.createDataFrame([
        (dt.date(2025, 7, 1), 1, "OTA2201", 100.0, dt.datetime(2026, 1, 1)),
        (dt.date(2025, 7, 1), 1, "OTA2201", 105.0, dt.datetime(2026, 2, 1)),   # revised
        (dt.date(2025, 7, 1), 2, "OTA2201", 90.0, dt.datetime(2026, 1, 1)),
    ], ["trading_date", "trading_period", "poc", "price_nzd_mwh", "ingested_at"])
    out = latest_per_key(df, ["trading_date", "trading_period", "poc"], "ingested_at")
    assert out.count() == 2
    assert out.where("trading_period = 1").first().price_nzd_mwh == 105.0


def test_grid_points_one_row_per_poc_prefers_current(spark):
    cols = ["poc_code", "current_flag", "island", "zone", "network_reporting_region", "description",
            "nztm_easting", "nztm_northing"]
    df = spark.createDataFrame([
        ("OTA2201", "0", "NI", "UNI", "Old region", "old", None, None),
        ("OTA2201", "1", "NI", "UNI", "Auckland", "OTAHUHU", "1766000", "5910000"),
        ("BEN2201", "1", "SI", "LSI", "Waitaki", "BENMORE", "1420000", "5070000"),
    ], cols)
    out = {r.poc: r for r in silver_grid_points(df).collect()}
    assert len(out) == 2
    assert out["OTA2201"].region == "Auckland"


def test_clean_column_names():
    assert clean_column("Active storage (Mm³)") == "active_storage_mm3"
    assert clean_column("Lake level (m)") == "lake_level_m"
    assert clean_column("_source_file") == "_source_file"
    assert clean_column("POC code") == "poc_code"
