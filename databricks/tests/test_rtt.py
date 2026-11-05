import datetime as dt
import zipfile

import pytest
from pyspark.sql import functions as F

from conftest import rtt_row, standard_rtt_rows, with_c999, write_rtt
from nhs_lakehouse import quality
from nhs_lakehouse.bronze import SchemaDriftError, extract_zips, read_rtt
from nhs_lakehouse.silver import (
    RTT_BAND_KEYS,
    RTT_KEYS,
    dim_wait_band,
    merge_month_replace_sql,
    rtt_pathways,
    rtt_wait_bands,
)

AUG = dt.date(2026, 8, 1)


@pytest.fixture
def aug_bronze(spark, tmp_path):
    p = tmp_path / "rtt/period=2026-08/published=2026-10/20260831-RTT-August-2026-full-extract.csv"
    write_rtt(p, standard_rtt_rows())
    return read_rtt(spark, str(tmp_path / "rtt"))


def test_bronze_lineage_and_period(aug_bronze):
    r = aug_bronze.select("period_month", "_published", "_source_file").first()
    assert r.period_month == AUG
    assert r._published == "2026-10"
    assert r._source_file.endswith("full-extract.csv")


def test_period_label_case_does_not_matter(spark, tmp_path):
    write_rtt(
        tmp_path / "rtt/period=2021-04/published=2022-01/x.csv",
        with_c999([rtt_row("RTT-APRIL-2021", "RHQ", "QF7", "Part_2", "C_100", {0: 1})]),
    )
    assert read_rtt(spark, str(tmp_path / "rtt")).first().period_month == dt.date(2021, 4, 1)


def test_header_drift_is_rejected(spark, tmp_path):
    p = write_rtt(tmp_path / "rtt/period=2026-08/published=2026-10/x.csv", standard_rtt_rows())
    text = p.read_text(encoding="latin-1").replace("Total All", "Grand Total", 1)
    p.write_text(text, encoding="latin-1")
    with pytest.raises(SchemaDriftError):
        read_rtt(spark, str(tmp_path / "rtt"))


def test_c999_subtotals_are_removed_so_nothing_double_counts(aug_bronze):
    pathways = rtt_pathways(aug_bronze)
    assert pathways.filter("treatment_function_code = 'C_999'").count() == 0
    inc = pathways.filter("part_type = 'Part_2'").agg(F.sum("total_all")).first()[0]
    raw_all = aug_bronze.filter("part_type = 'Part_2'").agg(F.sum(F.col("total_all").cast("long"))).first()[0]
    assert inc == 33  # 20 + 5 + 6 + 2
    assert raw_all == 2 * inc  # the trap: summing the raw file doubles the waiting list


def test_part3_new_periods_kept_in_pathways(aug_bronze):
    p3 = rtt_pathways(aug_bronze).filter("part_type = 'Part_3'").first()
    assert p3.total_all == 40


def test_unpivot_to_wait_bands(aug_bronze):
    bands = rtt_wait_bands(aug_bronze)
    assert bands.filter("patients = 0").count() == 0  # blank cells dropped
    part2 = bands.filter("part_type = 'Part_2'")
    assert part2.agg(F.sum("patients")).first()[0] == 33
    assert part2.filter("wait_band = 104").count() == 1  # >104 weeks band survives
    assert bands.filter("part_type = 'Part_3'").count() == 0  # Part_3 has no bands
    assert quality.unique_grain(bands, RTT_BAND_KEYS, "rtt_wait_band").status == "pass"


def test_within_18_weeks_from_bands(aug_bronze):
    bands = rtt_wait_bands(aug_bronze).filter("part_type = 'Part_2'")
    within = bands.filter("wait_band < 18").agg(F.sum("patients")).first()[0]
    assert within == 10 + 5 + 4 + 6 + 1  # bands 0, 17, 1, 2, 0


def test_quality_rules_pass_on_clean_file(aug_bronze):
    assert quality.rtt_bands_sum_to_total(aug_bronze).status == "pass"
    assert quality.rtt_specialties_sum_to_c999(aug_bronze).status == "pass"
    assert quality.unique_grain(rtt_pathways(aug_bronze), RTT_KEYS, "rtt_pathway").status == "pass"


def test_quality_rules_catch_broken_rows(spark, tmp_path):
    rows = standard_rtt_rows()
    rows[0]["Total All"] = "999"  # bands no longer add up, and C_999 no longer reconciles
    write_rtt(tmp_path / "rtt/period=2026-08/published=2026-10/x.csv", rows)
    b = read_rtt(spark, str(tmp_path / "rtt"))
    assert quality.rtt_bands_sum_to_total(b).status == "fail"
    assert quality.rtt_specialties_sum_to_c999(b).status == "fail"
    with pytest.raises(AssertionError):
        quality.run_all([quality.rtt_bands_sum_to_total(b)])


def test_revised_publication_replaces_the_whole_month(spark, tmp_path):
    original = standard_rtt_rows()
    revised = with_c999([rtt_row("RTT-August-2026", "RHQ", "QF7", "Part_2", "C_100", {0: 12})])
    write_rtt(tmp_path / "rtt/period=2026-08/published=2026-10/orig.csv", original)
    write_rtt(tmp_path / "rtt/period=2026-08/published=2027-04/revised.csv", revised)
    p = rtt_pathways(read_rtt(spark, str(tmp_path / "rtt")))
    assert {r._published for r in p.collect()} == {"2027-04"}
    assert p.count() == 1  # rows missing from the revision disappear too


def test_extract_zips_is_idempotent_and_keeps_partitions(tmp_path):
    landing = tmp_path / "landing"
    zdir = landing / "rtt/period=2026-08/published=2026-10"
    zdir.mkdir(parents=True)
    with zipfile.ZipFile(zdir / "Full-CSV-data-file-Aug26-ZIP-3M-vjgRdG.zip", "w") as zf:
        zf.writestr("20260831-RTT-August-2026-full-extract.csv", "a,b\n1,2\n")
        zf.writestr("../evil.csv", "x")  # path traversal attempt is flattened
    first = extract_zips(str(landing), str(tmp_path / "staging"))
    assert len(first) == 2
    assert (
        tmp_path / "staging/rtt/period=2026-08/published=2026-10/20260831-RTT-August-2026-full-extract.csv"
    ).exists()
    assert not (tmp_path / "staging/rtt/period=2026-08/evil.csv").exists()
    assert extract_zips(str(landing), str(tmp_path / "staging")) == []


def test_wait_band_dimension():
    rows = dim_wait_band()
    assert len(rows) == 105
    assert rows[17]["is_within_18"] and not rows[18]["is_within_18"]
    assert rows[52]["is_over_52"] and not rows[51]["is_over_52"]
    assert rows[104]["label"] == ">104 weeks" and rows[104]["upper_weeks"] is None


def test_merge_sql_replaces_only_batch_months():
    sql = merge_month_replace_sql(
        "silver.rtt_pathway", "batch", RTT_KEYS, ["total_all", "_published"], ["2026-08-01"]
    )
    assert "WHEN NOT MATCHED BY SOURCE AND t.period_month IN (DATE'2026-08-01')" in sql
    assert "s._published >= t._published" in sql
    with pytest.raises(ValueError):
        merge_month_replace_sql("t", "s", RTT_KEYS, ["x"], [])


def test_month_completeness_warns_on_missing_trusts(spark):
    rows = [(dt.date(2026, m, 1), f"P{i}") for m in range(1, 7) for i in range(20)]
    rows += [(dt.date(2026, 7, 1), f"P{i}") for i in range(17)]  # three trusts did not submit
    df = spark.createDataFrame(rows, "period_month date, provider_code string")
    r = quality.month_completeness(df, "provider_code", "rtt")
    assert r.status == "warn" and r.sample[0]["providers"] == 17
