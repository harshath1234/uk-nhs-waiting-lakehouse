"""Silver: cleaned, conformed, one version per month (latest publication wins).

Rules come from inspecting the real files (docs/source_quirks.md):
- RTT ``C_999`` rows are subtotals and are removed (kept only for reconciliation).
- Blank numeric cells mean zero.
- Only ``Total All`` is filled for every part, so it is the pathway measure.
- A&E has a ``TOTAL`` row and trailing spaces in ``Parent Org``.
- Region and ICB come from ODS, never from the source files' parent columns.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schemas import (
    AE_ATTENDANCE_COLS,
    AE_COLUMNS,
    AE_OVER4H_COLS,
    AE_TOTAL_MARKER,
    N_BANDS,
    NHS_REGIONS,
    RTT_PARTS_WITH_BANDS,
    RTT_SUBTOTAL_TF,
    band_column,
)

RTT_KEYS = ["period_month", "provider_code", "commissioner_code", "part_type", "treatment_function_code"]
RTT_BAND_KEYS = RTT_KEYS + ["wait_band"]
AE_KEYS = ["period_month", "org_code"]


def blank_to_int(col: str) -> F.Column:
    """'' or null -> 0. Anything non-numeric becomes null so the quality rules catch it."""
    c = F.trim(F.col(col))
    return F.when(c.isNull() | (c == ""), F.lit(0)).otherwise(c.cast("long")).alias(col)


def latest_version(df: DataFrame, period_cols: list[str], order_cols: list[str]) -> DataFrame:
    """Keep only rows from the newest publication of each period.

    A revision replaces the *whole* month (rows can disappear between versions), so we
    rank publications per period rather than per row.
    """
    pubs = df.select(*period_cols, *order_cols).distinct()
    w = Window.partitionBy(*period_cols).orderBy(*[F.col(c).desc() for c in order_cols])
    newest = pubs.withColumn("_rank", F.row_number().over(w)).filter("_rank = 1").drop("_rank")
    # null-safe join: an unrevised file has _revised_on = null, and null = null is not true
    cols = period_cols + order_cols
    cond = [df[c].eqNullSafe(newest[c]) for c in cols]
    return df.join(newest, cond, "left_semi")


# ---------------------------------------------------------------------- RTT
def rtt_pathways(bronze: DataFrame) -> DataFrame:
    """One row per source row (excluding C_999): the pathway count for every part."""
    df = latest_version(bronze, ["period_month"], ["_published"])
    return df.filter(F.col("treatment_function_code") != RTT_SUBTOTAL_TF).select(
        *RTT_KEYS,
        "provider_name",
        "commissioner_name",
        "part_description",
        "treatment_function_name",
        blank_to_int("total_all"),
        blank_to_int("unknown_clock_start"),
        "_published",
        "_source_file",
    )


def rtt_wait_bands(bronze: DataFrame) -> DataFrame:
    """Unpivot the 105 weekly columns into one row per non-zero band (the brief's fact grain)."""
    df = latest_version(bronze, ["period_month"], ["_published"])
    df = df.filter(
        (F.col("treatment_function_code") != RTT_SUBTOTAL_TF) & F.col("part_type").isin(*RTT_PARTS_WITH_BANDS)
    )
    bands = [band_column(i) for i in range(N_BANDS)]
    typed = df.select(*RTT_KEYS, "_published", *[blank_to_int(b) for b in bands])
    long = typed.unpivot(RTT_KEYS + ["_published"], bands, "band_col", "patients")
    return (
        long.filter(F.col("patients") != 0)
        .withColumn("wait_band", F.regexp_extract("band_col", r"wb_(\d+)", 1).cast("int"))
        .drop("band_col")
        .select(*RTT_BAND_KEYS, "patients", "_published")
    )


def dim_wait_band() -> list[dict]:
    """Static dimension rows: band 0 = >0-1 weeks ... band 104 = >104 weeks."""
    rows = []
    for i in range(N_BANDS):
        upper = None if i == 104 else i + 1
        rows.append(
            {
                "wait_band": i,
                "label": f">{i}-{upper} weeks" if upper else ">104 weeks",
                "lower_weeks": i,
                "upper_weeks": upper,
                "is_within_18": i < 18,
                "is_over_52": i >= 52,
                "is_over_65": i >= 65,
                "is_over_78": i >= 78,
            }
        )
    return rows


# ---------------------------------------------------------------------- A&E
def ae_performance(bronze: DataFrame) -> DataFrame:
    df = bronze.filter(F.col("period_label") != AE_TOTAL_MARKER)
    df = latest_version(df, ["period_label"], ["_published", "_revised_on"])
    numeric = [dst for _, dst in AE_COLUMNS[4:]]
    from .bronze import period_to_month

    out = df.select(
        period_to_month("period_label").alias("period_month"),
        F.trim("org_code").alias("org_code"),
        F.trim("org_name").alias("org_name"),
        F.trim("parent_org").alias("parent_org"),
        *[blank_to_int(c) for c in numeric],
        "_published",
        "_source_file",
    )
    att = sum((F.col(c) for c in AE_ATTENDANCE_COLS), F.lit(0))
    over = sum((F.col(c) for c in AE_OVER4H_COLS), F.lit(0))
    return (
        out.withColumn("attendances", att)
        .withColumn("over_4h", over)
        .withColumn("within_4h", att - over)
        .withColumn("attendances_type1", F.col("att_type1") + F.col("att_booked_type1"))
        .withColumn(
            "within_4h_type1",
            F.col("att_type1")
            + F.col("att_booked_type1")
            - F.col("over4h_type1")
            - F.col("over4h_booked_type1"),
        )
        .withColumn(
            "emergency_admissions",
            F.col("emerg_adm_type1")
            + F.col("emerg_adm_type2")
            + F.col("emerg_adm_other")
            + F.col("emerg_adm_not_via_ae"),
        )
    )


# ---------------------------------------------------------------------- ODS
def ods_trusts(bronze: DataFrame) -> DataFrame:
    """Trust reference data from the latest etr snapshot. Address and phone columns dropped."""
    latest = bronze.agg(F.max("snapshot_date").alias("snapshot_date"))
    df = bronze.join(latest, "snapshot_date")
    region = F.create_map(*[x for k, v in NHS_REGIONS.items() for x in (F.lit(k), F.lit(v))])
    return df.select(
        F.trim("org_code").alias("org_code"),
        F.trim("name").alias("org_name"),
        F.lit("NHS trust").alias("org_type"),
        F.nullif(F.trim("national_grouping"), F.lit("")).alias("region_code"),
        F.element_at(region, F.trim("national_grouping")).alias("region_name"),
        F.nullif(F.trim("high_level_health_geography"), F.lit("")).alias("icb_code"),
        F.to_date("open_date", "yyyyMMdd").alias("open_date"),
        F.to_date(F.nullif(F.trim("close_date"), F.lit("")), "yyyyMMdd").alias("close_date"),
        "snapshot_date",
    ).withColumn("is_open", F.col("close_date").isNull())


# ---------------------------------------------------------------------- Delta MERGE
def merge_month_replace_sql(
    target: str,
    source_view: str,
    keys: list[str],
    cols: list[str],
    periods: list[str],
    period_col: str = "period_month",
) -> str:
    """MERGE that replaces every month present in the source batch.

    - matched rows are updated, new rows inserted;
    - rows of those months that vanished from the new publication are deleted;
    - months not in the batch are untouched.
    Time travel on the target then shows the month before and after a revision.
    ``periods`` are inlined as literals because Delta does not allow subqueries in the
    NOT MATCHED BY SOURCE condition.
    """
    if not periods:
        raise ValueError("no periods in batch")
    period_list = ", ".join(f"DATE'{p}'" for p in sorted(periods))
    on = " AND ".join(f"t.{k} <=> s.{k}" for k in keys)
    sets = ", ".join(f"t.{c} = s.{c}" for c in cols)
    ins_cols = ", ".join(keys + cols)
    ins_vals = ", ".join(f"s.{c}" for c in keys + cols)
    return (
        f"MERGE INTO {target} AS t\n"
        f"USING {source_view} AS s\n"
        f"ON {on}\n"
        f"WHEN MATCHED AND s._published >= t._published THEN UPDATE SET {sets}\n"
        f"WHEN NOT MATCHED THEN INSERT ({ins_cols}) VALUES ({ins_vals})\n"
        f"WHEN NOT MATCHED BY SOURCE AND t.{period_col} IN ({period_list})\n"
        f"  THEN DELETE"
    )
