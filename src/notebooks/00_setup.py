# Databricks notebook source
# MAGIC %md
# MAGIC # 0 · Setup
# MAGIC Creates the schema and the landing-zone volume. Run once; it's safe to run again.
# MAGIC
# MAGIC Files go into the volume like this:
# MAGIC ```
# MAGIC /Volumes/workspace/nz_electricity/raw/
# MAGIC ├── prices/      one gzipped CSV per month (YYYYMM_FinalEnergyPrices.csv.gz)
# MAGIC ├── hydro/       daily storage per hydro lake
# MAGIC └── reference/   NetworkSupplyPointsTable.csv
# MAGIC ```

# COMMAND ----------

dbutils.widgets.text("catalog", "workspace")
dbutils.widgets.text("schema", "nz_electricity")
catalog, schema = dbutils.widgets.get("catalog"), dbutils.widgets.get("schema")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT 'NZ wholesale electricity lakehouse'")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.raw COMMENT 'Landing zone for raw files'")
for d in ["prices", "hydro", "reference", "_checkpoints"]:
    dbutils.fs.mkdirs(f"/Volumes/{catalog}/{schema}/raw/{d}")

# COMMAND ----------

# What's in the landing zone?
for d in ["prices", "hydro", "reference"]:
    files = dbutils.fs.ls(f"/Volumes/{catalog}/{schema}/raw/{d}")
    print(f"{d:10s} {len(files):4d} files  {sum(f.size for f in files) / 1e6:8.1f} MB")
