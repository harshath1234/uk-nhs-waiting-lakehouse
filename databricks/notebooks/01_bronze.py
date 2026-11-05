# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze: landed files → typed raw Delta tables
# MAGIC Reads what ADF landed (`landing/<dataset>/period=YYYY-MM/published=YYYY-MM/<file>`),
# MAGIC unzips RTT, validates headers against the schema checked on 5 Nov 2026, and appends
# MAGIC **only files not ingested before** (idempotent re-runs). Lineage: `_source_file`, `_published`, `_ingest_ts`.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))  # bundle syncs databricks/src next to the notebooks

from pyspark.sql import functions as F

from nhs_lakehouse.bronze import extract_zips, read_ae, read_ods_etr, read_rtt

dbutils.widgets.text("catalog", "nhs")
dbutils.widgets.text("landing", "/Volumes/nhs/raw/landing")  # UC volume over ADLS (or a managed copy)
dbutils.widgets.text("staging", "/Volumes/nhs/raw/staging")
catalog = dbutils.widgets.get("catalog")
landing = dbutils.widgets.get("landing")
staging = dbutils.widgets.get("staging")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")

# COMMAND ----------
new_csvs = extract_zips(f"{landing}/rtt", f"{staging}/rtt")
print(f"unzipped {len(new_csvs)} new RTT files")


# COMMAND ----------
def append_new_files(df, table: str) -> int:
    """Append rows from files this table has not seen. Returns rows written."""
    full = f"{catalog}.bronze.{table}"
    if spark.catalog.tableExists(full):
        seen = spark.table(full).select("_source_file").distinct()
        df = df.join(seen, "_source_file", "left_anti")
    n = df.count()
    if n:
        df.write.format("delta").mode("append").option("mergeSchema", "false").saveAsTable(full)
    print(f"{full}: +{n:,} rows")
    return n


results = {
    "rtt": append_new_files(read_rtt(spark, f"{staging}/rtt"), "rtt"),
    "ae": append_new_files(read_ae(spark, f"{landing}/ae"), "ae"),
    "ods_etr": append_new_files(read_ods_etr(spark, f"{landing}/ods/etr"), "ods_etr"),
}

# COMMAND ----------
log = spark.createDataFrame([(k, v) for k, v in results.items()], "table string, rows_added long").withColumn(
    "run_ts", F.current_timestamp()
)
log.write.format("delta").mode("append").saveAsTable(f"{catalog}.bronze._load_log")
display(log)
