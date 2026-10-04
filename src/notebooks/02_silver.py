# Databricks notebook source
# MAGIC %md
# MAGIC # 2 · Silver: clean, validate, de-duplicate
# MAGIC - **Prices:** typed, checked against data-quality rules, de-duplicated, and given real
# MAGIC   timestamps (a trading day has 46 or 50 half-hours when daylight saving changes).
# MAGIC   Only bronze rows that arrived since the last run are processed, and they're `MERGE`d in,
# MAGIC   so re-running is safe. Rows that fail a rule go to `silver_prices_quarantine`.
# MAGIC - **Reference data:** grid points and hydro storage are small and reloaded in full.

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath(".."))

from nzelec.config import Config
from nzelec.pipeline import refresh_reference, update_silver_prices
from nzelec.transforms import PRICE_RULES

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nz_electricity")
cfg = Config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))
print("Data-quality rules:")
for name, rule in PRICE_RULES.items():
    print(f"  {name:16s} {rule}")

# COMMAND ----------

refresh_reference(spark, cfg)
update_silver_prices(spark, cfg)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT failed_rules, count(*) AS rows, min(trading_date) AS first, max(trading_date) AS last
# MAGIC FROM workspace.nz_electricity.silver_prices_quarantine GROUP BY 1 ORDER BY 2 DESC
