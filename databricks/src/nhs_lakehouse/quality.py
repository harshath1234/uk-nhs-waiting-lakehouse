"""Data-quality rules (brief section 8, adjusted for what the real files contain).

Each rule returns a ``RuleResult``. ``fail`` stops the job; ``warn`` is logged and reported
(e.g. trusts that did not submit that month), matching the Grid Carbon pipeline's
"known issue" pattern.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from .schemas import N_BANDS, RTT_PARTS_WITH_BANDS, RTT_SUBTOTAL_TF, band_column
from .silver import RTT_KEYS, blank_to_int


@dataclass
class RuleResult:
    rule: str
    status: str  # pass | warn | fail
    detail: str
    sample: list = field(default_factory=list)

    def as_row(self) -> dict:
        return {
            "rule": self.rule,
            "status": self.status,
            "detail": self.detail,
            "sample": str(self.sample[:5]),
        }


def _result(rule: str, bad: DataFrame, ok_msg: str, bad_msg: str, severity: str = "fail") -> RuleResult:
    n = bad.count()
    if n == 0:
        return RuleResult(rule, "pass", ok_msg)
    return RuleResult(rule, severity, bad_msg.format(n=n), [r.asDict() for r in bad.limit(5).collect()])


def unique_grain(df: DataFrame, keys: list[str], name: str) -> RuleResult:
    dupes = df.groupBy(*keys).count().filter("count > 1")
    return _result(
        f"unique_grain:{name}", dupes, f"{name}: grain is unique", f"{name}: {{n}} duplicated keys"
    )


def non_negative(df: DataFrame, cols: list[str], name: str) -> RuleResult:
    cond = None
    for c in cols:
        part = F.col(c).isNull() | (F.col(c) < 0)
        cond = part if cond is None else cond | part
    return _result(
        f"non_negative:{name}",
        df.filter(cond),
        f"{name}: no negative or non-numeric counts",
        f"{name}: {{n}} rows with negative or non-numeric counts",
    )


def rtt_bands_sum_to_total(bronze: DataFrame) -> RuleResult:
    """Bands + unknown clock start = Total All (Part_2 leaves 'Total' blank, so we use Total All)."""
    df = bronze.filter(F.col("part_type").isin(*RTT_PARTS_WITH_BANDS))
    bands = sum((blank_to_int(band_column(i)) for i in range(N_BANDS)), F.lit(0))
    bad = (
        df.withColumn("_bands", bands)
        .withColumn("_unknown", blank_to_int("unknown_clock_start"))
        .withColumn("_total_all", blank_to_int("total_all"))
        .filter(F.col("_bands") + F.col("_unknown") != F.col("_total_all"))
        .select(*RTT_KEYS, "_bands", "_unknown", "_total_all")
    )
    return _result(
        "rtt_bands_sum_to_total",
        bad,
        "every row: weekly bands + unknown = Total All",
        "{n} rows where weekly bands + unknown != Total All",
    )


def rtt_specialties_sum_to_c999(bronze: DataFrame) -> RuleResult:
    """The file's own C_999 subtotal must equal the sum of the specialty rows."""
    tot = blank_to_int("total_all")
    keys = ["period_month", "_published", "provider_code", "commissioner_code", "part_type"]
    spec = (
        bronze.filter(F.col("treatment_function_code") != RTT_SUBTOTAL_TF)
        .groupBy(*keys)
        .agg(F.sum(tot).alias("specialties"))
    )
    sub = (
        bronze.filter(F.col("treatment_function_code") == RTT_SUBTOTAL_TF)
        .groupBy(*keys)
        .agg(F.sum(tot).alias("c999"))
    )
    bad = spec.join(sub, keys, "full_outer").filter(
        F.coalesce("specialties", F.lit(0)) != F.coalesce("c999", F.lit(0))
    )
    return _result(
        "rtt_specialties_sum_to_c999",
        bad,
        "specialty rows reconcile to the C_999 subtotal",
        "{n} provider x commissioner x part groups do not reconcile to C_999",
    )


def ae_rows_sum_to_total_row(bronze: DataFrame, numeric_cols: list[str]) -> RuleResult:
    """Provider rows must add up to the TOTAL row in the same file."""
    tot = bronze.filter(F.col("period_label") == "TOTAL")
    rows = bronze.filter(F.col("period_label") != "TOTAL")
    sums = rows.groupBy("_source_file").agg(*[F.sum(blank_to_int(c)).alias(c) for c in numeric_cols])
    totals = tot.select("_source_file", *[blank_to_int(c) for c in numeric_cols])
    joined = sums.alias("s").join(totals.alias("t"), "_source_file", "full_outer")
    cond = None
    for c in numeric_cols:
        part = ~F.col(f"s.{c}").eqNullSafe(F.col(f"t.{c}"))
        cond = part if cond is None else cond | part
    bad = joined.filter(cond).select("_source_file")
    return _result(
        "ae_rows_sum_to_total_row",
        bad,
        "provider rows reconcile to the TOTAL row",
        "{n} files where provider rows do not match the TOTAL row",
    )


def month_completeness(
    df: DataFrame, provider_col: str, name: str, tolerance: float = 0.05, lookback: int = 6
) -> RuleResult:
    """Warn when a month has fewer providers than the median of the previous months by > tolerance."""
    counts = [
        (r["period_month"], r["n"])
        for r in df.groupBy("period_month")
        .agg(F.countDistinct(provider_col).alias("n"))
        .orderBy("period_month")
        .collect()
    ]
    flagged = []
    for i, (month, n) in enumerate(counts):
        prev = sorted(c for _, c in counts[max(0, i - lookback) : i])
        if not prev:
            continue
        median = prev[len(prev) // 2]
        if n < median * (1 - tolerance):
            flagged.append({"period_month": str(month), "providers": n, "expected_about": median})
    if not flagged:
        return RuleResult(f"month_completeness:{name}", "pass", f"{name}: provider counts stable")
    return RuleResult(
        f"month_completeness:{name}",
        "warn",
        f"{name}: {len(flagged)} months with >{tolerance:.0%} fewer providers",
        flagged,
    )


def unmatched_codes(fact: DataFrame, code_col: str, dim: DataFrame, dim_col: str = "org_code") -> DataFrame:
    """Codes in the fact with no match in the dimension: routed to a table, never dropped."""
    return (
        fact.select(F.col(code_col).alias("code"))
        .distinct()
        .join(dim.select(F.col(dim_col).alias("code")), "code", "left_anti")
    )


def freshness(df: DataFrame, latest_published_period: str) -> RuleResult:
    """Latest loaded month must be no older than the latest NHS publication minus one month."""
    loaded = df.agg(F.max("period_month").alias("m")).first()["m"]
    y, m = (int(x) for x in latest_published_period.split("-"))
    y, m = (y - 1, 12) if m == 1 else (y, m - 1)
    floor = f"{y}-{m:02d}-01"
    status = "pass" if loaded is not None and str(loaded) >= floor else "fail"
    return RuleResult("freshness", status, f"latest loaded {loaded}; required >= {floor}")


def run_all(results: list[RuleResult]) -> list[dict]:
    rows = [r.as_row() for r in results]
    failed = [r for r in results if r.status == "fail"]
    if failed:
        raise AssertionError("quality rules failed: " + "; ".join(f"{r.rule}: {r.detail}" for r in failed))
    return rows
