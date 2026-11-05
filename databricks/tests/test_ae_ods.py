import datetime as dt

from pyspark.sql import functions as F

from conftest import etr_row, standard_ae_rows, write_ae, write_csv
from nhs_lakehouse import quality
from nhs_lakehouse.bronze import read_ae, read_ods_etr
from nhs_lakehouse.schemas import AE_COLUMNS
from nhs_lakehouse.silver import AE_KEYS, ae_performance, ods_trusts

NUMERIC = [dst for _, dst in AE_COLUMNS[4:]]


def ae_bronze(
    spark, tmp_path, rows=None, name="September-2026-CSV-Re72Eh.csv", period="2026-09", published="2026-10"
):
    write_ae(tmp_path / f"ae/period={period}/published={published}/{name}", rows or standard_ae_rows())
    return read_ae(spark, str(tmp_path / "ae"))


def test_total_row_dropped_and_names_trimmed(spark, tmp_path):
    s = ae_performance(ae_bronze(spark, tmp_path))
    assert s.count() == 3
    assert s.filter("org_code = 'TOTAL'").count() == 0
    assert {r.parent_org for r in s.collect()} == {
        "NHS ENGLAND NORTH EAST AND YORKSHIRE",
        "NHS ENGLAND SOUTH WEST",
        "NHS ENGLAND SOUTH EAST",
    }
    assert s.first().period_month == dt.date(2026, 9, 1)
    assert quality.unique_grain(s, AE_KEYS, "ae").status == "pass"


def test_four_hour_measures(spark, tmp_path):
    s = ae_performance(ae_bronze(spark, tmp_path))
    rhq = s.filter("org_code = 'RHQ'").first()
    assert rhq.attendances == 160 and rhq.over_4h == 37 and rhq.within_4h == 123
    assert rhq.attendances_type1 == 110 and rhq.within_4h_type1 == 110 - 30 - 2
    assert rhq.emergency_admissions == 63  # 40 + 0 + 3 + 20
    assert rhq.dta_4_to_12h == 8 and rhq.dta_12h_plus == 1  # typo'd source header mapped


def test_total_row_reconciliation(spark, tmp_path):
    b = ae_bronze(spark, tmp_path)
    assert quality.ae_rows_sum_to_total_row(b, NUMERIC).status == "pass"
    rows = standard_ae_rows()
    rows[0][4] = "101"  # provider row edited, TOTAL row not
    b2 = ae_bronze(spark, tmp_path / "broken", rows)
    assert quality.ae_rows_sum_to_total_row(b2, NUMERIC).status == "fail"


def test_revised_ae_file_wins(spark, tmp_path):
    rows = standard_ae_rows("MSitAE-MARCH-2026")
    write_ae(tmp_path / "ae/period=2026-03/published=2026-04/March-2026-CSV-Xy12Ab.csv", rows)
    revised = [r[:] for r in rows]
    revised[0][4] = "150"
    write_ae(tmp_path / "ae/period=2026-03/published=2026-05/March-2026-revised-140526-AbC123.csv", revised)
    b = read_ae(spark, str(tmp_path / "ae"))
    assert b.filter(F.col("_revised_on").isNotNull()).first()._revised_on == dt.date(2026, 5, 14)
    s = ae_performance(b)
    assert s.count() == 3
    assert s.filter("org_code = 'RHQ'").first().att_type1 == 150


def test_ods_headerless_etr(spark, tmp_path):
    rows = [
        etr_row("RHQ", "SHEFFIELD TEACHING HOSPITALS NHS FOUNDATION TRUST", "Y63", "QF7", "20010401"),
        etr_row("RA9", "TORBAY AND SOUTH DEVON NHS FOUNDATION TRUST", "Y58", "QJK", "19910401"),
        etr_row("R1C", "SOLENT NHS TRUST", "Y59", "QRL", "20110401", "20240930"),
        etr_row("X0X", "ODD TRUST WITH NO REGION", "", "", "20200101"),
    ]
    write_csv(tmp_path / "ods/etr/snapshot=2026-11-05/etr.csv", None, rows)
    b = read_ods_etr(spark, str(tmp_path / "ods/etr"))
    assert b.first().snapshot_date == dt.date(2026, 11, 5)
    t = {r.org_code: r for r in ods_trusts(b).collect()}
    assert t["RHQ"].region_name == "North East and Yorkshire" and t["RHQ"].icb_code == "QF7"
    assert t["R1C"].close_date == dt.date(2024, 9, 30) and not t["R1C"].is_open
    assert t["X0X"].region_code is None
    assert "contact_telephone" not in ods_trusts(b).columns  # personal-ish data dropped in silver


def test_unmatched_codes_are_reported_not_dropped(spark, tmp_path):
    s = ae_performance(ae_bronze(spark, tmp_path))
    dim = spark.createDataFrame([("RHQ",), ("RA9",)], "org_code string")
    um = quality.unmatched_codes(s, "org_code", dim)
    assert [r.code for r in um.collect()] == ["NTV0B"]  # walk-in centre, not an NHS trust
    assert s.count() == 3


def test_freshness(spark):
    df = spark.createDataFrame([(dt.date(2026, 8, 1),)], "period_month date")
    assert quality.freshness(df, "2026-09").status == "pass"
    assert quality.freshness(df, "2026-11").status == "fail"
