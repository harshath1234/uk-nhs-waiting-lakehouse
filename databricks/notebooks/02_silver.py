# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver: rules, quality gate, Delta MERGE
# MAGIC 1. Build silver DataFrames from bronze (latest publication of each month wins).
# MAGIC 2. Run the data-quality rules; any `fail` stops the job, `warn` is recorded.
# MAGIC 3. MERGE into silver tables, replacing whole months that were re-published.
# MAGIC 4. Log row counts before/after, then OPTIMIZE + Z-ORDER.

# COMMAND ----------
import os
import sys

sys.path.append(os.path.abspath("../src"))

from pyspark.sql import functions as F

from nhs_lakehouse import quality
from nhs_lakehouse.schemas import AE_COLUMNS
from nhs_lakehouse.silver import (
    AE_KEYS,
    RTT_BAND_KEYS,
    RTT_KEYS,
    ae_performance,
    dim_wait_band,
    latest_version,
    merge_month_replace_sql,
    ods_trusts,
    rtt_pathways,
    rtt_wait_bands,
)

dbutils.widgets.text("catalog", "nhs")
catalog = dbutils.widgets.get("catalog")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
b = lambda t: spark.table(f"{catalog}.bronze.{t}")  # noqa: E731

# COMMAND ----------
rtt_b, ae_b, etr_b = b("rtt"), b("ae"), b("ods_etr")
pathways = rtt_pathways(rtt_b)
bands = rtt_wait_bands(rtt_b)
ae = ae_performance(ae_b)
trusts = ods_trusts(etr_b)

# COMMAND ----------
# MAGIC %md ## Quality gate

# COMMAND ----------
latest_rtt = latest_version(rtt_b, ["period_month"], ["_published"])  # check the version we serve
results = [
    quality.rtt_bands_sum_to_total(latest_rtt),
    quality.rtt_specialties_sum_to_c999(latest_rtt),
    quality.unique_grain(pathways, RTT_KEYS, "rtt_pathway"),
    quality.unique_grain(bands, RTT_BAND_KEYS, "rtt_wait_band"),
    quality.non_negative(bands, ["patients"], "rtt_wait_band"),
    quality.month_completeness(pathways, "provider_code", "rtt"),
    quality.ae_rows_sum_to_total_row(ae_b, [d for _, d in AE_COLUMNS[4:]]),
    quality.unique_grain(ae, AE_KEYS, "ae"),
    quality.month_completeness(ae, "org_code", "ae"),
]
dq = spark.createDataFrame([r.as_row() for r in results]).withColumn("run_ts", F.current_timestamp())
dq.write.format("delta").mode("append").saveAsTable(f"{catalog}.silver._dq_results")
display(dq)
quality.run_all(results)  # raises on any 'fail'

# COMMAND ----------
# MAGIC %md ## Unmatched codes (reported, never dropped)

# COMMAND ----------
um = (
    quality.unmatched_codes(pathways, "provider_code", trusts)
    .withColumn("source", F.lit("rtt"))
    .unionByName(quality.unmatched_codes(ae, "org_code", trusts).withColumn("source", F.lit("ae")))
    .withColumn("run_ts", F.current_timestamp())
)
um.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.unmatched_codes")
print(f"unmatched codes: {um.count()} (mostly independent-sector providers and walk-in centres)")


# COMMAND ----------
def merge(df, table: str, keys: list[str]) -> None:
    full = f"{catalog}.silver.{table}"
    cols = [c for c in df.columns if c not in keys]
    if not spark.catalog.tableExists(full):
        df.write.format("delta").saveAsTable(full)
        print(f"{full}: created with {df.count():,} rows")
        return
    before = spark.table(full).count()
    periods = [str(r[0]) for r in df.select("period_month").distinct().collect()]
    df.createOrReplaceTempView("batch")
    spark.sql(merge_month_replace_sql(full, "batch", keys, cols, periods))
    after = spark.table(full).count()
    print(f"{full}: {before:,} -> {after:,} rows ({len(periods)} months in batch)")


merge(pathways, "rtt_pathway", RTT_KEYS)
merge(bands, "rtt_wait_band", RTT_BAND_KEYS)
merge(ae, "ae_performance", AE_KEYS)
trusts.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.silver.ods_trust")
spark.createDataFrame(dim_wait_band()).write.format("delta").mode("overwrite").saveAsTable(
    f"{catalog}.silver.dim_wait_band"
)

# COMMAND ----------
spark.sql(f"OPTIMIZE {catalog}.silver.rtt_wait_band ZORDER BY (provider_code, wait_band)")
spark.sql(f"OPTIMIZE {catalog}.silver.rtt_pathway ZORDER BY (provider_code)")
