# Databricks notebook source
# MAGIC %md
# MAGIC # 1 · Bronze: load raw files
# MAGIC **Prices** are loaded with **Auto Loader**, which keeps track of which files it has already
# MAGIC ingested. Upload a new month to `raw/prices/`, re-run the job, and only that file is read.
# MAGIC Every row keeps its source file and load time (lineage).

# COMMAND ----------

import os, sys
sys.path.append(os.path.abspath(".."))          # make src/nzelec importable

from nzelec.config import Config
from nzelec.io import ingest_prices_autoloader

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nz_electricity")
cfg = Config(catalog=dbutils.widgets.get("catalog"), schema=dbutils.widgets.get("schema"))

# COMMAND ----------

before = spark.table(cfg.table("bronze_prices")).count() if spark.catalog.tableExists(cfg.table("bronze_prices")) else 0
ingest_prices_autoloader(spark, cfg)
after = spark.table(cfg.table("bronze_prices")).count()
print(f"bronze_prices: {before:,} -> {after:,} rows (+{after - before:,})")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Files loaded so far
# MAGIC SELECT regexp_extract(_source_file, '([0-9]{6})_FinalEnergyPrices', 1) AS month, count(*) AS rows
# MAGIC FROM workspace.nz_electricity.bronze_prices GROUP BY 1 ORDER BY 1 DESC LIMIT 12
