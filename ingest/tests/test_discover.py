from datetime import date
from pathlib import Path

from nhs_ingest.discover import (
    build_manifest,
    financial_year,
    financial_years,
    ods_entries,
    parse_ae_page,
    parse_rtt_page,
    select,
)

FIX = Path(__file__).parent / "fixtures"
RTT_HTML = (FIX / "rtt_page_snippet.html").read_text()
AE_HTML = (FIX / "ae_page_snippet.html").read_text()


def by_period(entries):
    out = {}
    for e in entries:
        out.setdefault(e.period, []).append(e)
    return out


def test_financial_year_boundaries():
    assert financial_year(date(2026, 4, 1)) == "2026-27"
    assert financial_year(date(2026, 3, 31)) == "2025-26"
    assert financial_years(date(2021, 4, 1), date(2023, 4, 1)) == ["2021-22", "2022-23", "2023-24"]


def test_rtt_only_full_csv_zips_and_duplicates_collapsed():
    entries = parse_rtt_page(RTT_HTML)
    # Aug26 appears twice on the page (latest + month section): one entry. XLSX/PDF ignored.
    assert sorted(e.period for e in entries) == [
        "2019-08",
        "2021-04",
        "2021-04",
        "2022-03",
        "2026-07",
        "2026-08",
    ]
    assert all(e.url.endswith(".zip") for e in entries)


def test_rtt_publication_folder_is_not_the_data_month():
    aug = by_period(parse_rtt_page(RTT_HTML))["2026-08"][0]
    assert aug.published == "2026-10"
    assert aug.sink_path == "rtt/period=2026-08/published=2026-10/Full-CSV-data-file-Aug26-ZIP-3M-vjgRdG.zip"
    assert aug.base_url == "https://www.england.nhs.uk/"
    assert aug.relative_url.startswith("statistics/wp-content/uploads/sites/2/2026/10/")


def test_rtt_revised_flag_before_or_after_size():
    p = by_period(parse_rtt_page(RTT_HTML))
    assert p["2022-03"][0].is_revised
    assert p["2019-08"][0].is_revised  # "(ZIP, 3493K) revised"
    assert not p["2026-08"][0].is_revised


def test_latest_publication_wins_when_selecting_latest_only():
    entries = parse_rtt_page(RTT_HTML)
    all_versions = select(entries, "2021-04")
    assert len([e for e in all_versions if e.period == "2021-04"]) == 2  # landing keeps both
    latest = select(entries, "2021-04", latest_only=True)
    apr = [e for e in latest if e.period == "2021-04"]
    assert len(apr) == 1 and apr[0].is_revised and apr[0].published == "2022-01"
    assert "2019-08" not in {e.period for e in latest}  # before start


def test_ae_parsing_months_revisions_and_dates():
    p = by_period(parse_ae_page(AE_HTML))
    assert set(p) == {"2026-09", "2026-03", "2021-04"}  # XLS and PDF ignored
    mar = {e.is_revised: e for e in p["2026-03"]}
    assert mar[True].revised_on == "2026-05-14"  # "revised 14.05.26)" with unbalanced bracket
    assert p["2021-04"][0].revised_on == "2022-05-12"  # "(Revised 12.05.2022)"
    latest = select(parse_ae_page(AE_HTML), "2021-04", latest_only=True)
    assert [e for e in latest if e.period == "2026-03"][0].is_revised


def test_ods_snapshots_land_in_dated_folders():
    e = ods_entries(date(2026, 11, 5), reports=("etr",))[0]
    assert e.sink_path == "ods/etr/snapshot=2026-11-05/etr.csv"
    assert e.relative_url == "api/getReport?report=etr"
    assert e.base_url == "https://www.odsdatasearchandexport.nhs.uk/"


def test_build_manifest_survives_a_missing_year_page():
    def fake_fetch(url):
        if "2026-27" in url:
            raise RuntimeError("404")
        return RTT_HTML if "rtt-data" in url else AE_HTML

    m = build_manifest("2026-03", None, date(2026, 11, 5), latest_only=True, fetcher=fake_fetch)
    datasets = {e.dataset for e in m}
    assert {"rtt", "ae", "ods_etr"} <= datasets
