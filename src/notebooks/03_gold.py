# Databricks notebook source
# MAGIC %md
# MAGIC # 3 · Gold: analysis-ready tables
# MAGIC | Table | What it holds |
# MAGIC |---|---|
# MAGIC | `gold_daily_reference_prices` | Daily average, peak, 95th percentile and spike count at Otahuhu (Auckland) and Benmore (South Island) |
# MAGIC | `gold_monthly_prices` | Monthly prices at both nodes and the North–South spread |
# MAGIC | `gold_hourly_profile` | Average price by hour of day and season |
# MAGIC | `gold_hydro_national` | National hydro storage per day, and as % of normal for the time of year |
# MAGIC | `gold_price_vs_storage` | Monthly price next to hydro storage |
# MAGIC | `gold_node_premium` | Each node's price relative to Otahuhu, with location |

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath(".."))

from nzelec.config import Config
from nzelec.pipeline import build_gold, quality_report

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nz_electricity")
dbutils.widgets.text("recent_since", "2025-09-01")
cfg = Config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))

build_gold(spark, cfg, recent_since=dbutils.widgets.get("recent_since"))

# COMMAND ----------

# MAGIC %md ### Run report

# COMMAND ----------

report = quality_report(spark, cfg).first()
display(quality_report(spark, cfg))
assert report.silver_rows > 0, "silver_prices is empty"
assert report.days_with_odd_period_counts == 0, "some days don't have 46, 48 or 50 trading periods at Otahuhu"
