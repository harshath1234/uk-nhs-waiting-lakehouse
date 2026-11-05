"""Synthetic fixtures with the exact headers and quirks of the real NHS files.

No real data is committed: generators build small files that reproduce the traps found on
5 Nov 2026 (C_999 subtotal rows, blank Total for Part_2, A&E TOTAL row, headerless ODS).
"""

from __future__ import annotations

import csv
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from nhs_lakehouse.schemas import (  # noqa: E402
    AE_COLUMNS,
    N_BANDS,
    ODS_ETR_COLUMNS,
    RTT_COLUMNS,
)


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    from pyspark.sql import SparkSession

    s = (
        SparkSession.builder.master("local[1]")
        .appName("nhs-lakehouse-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    s.sparkContext.setLogLevel("ERROR")
    yield s
    s.stop()


def rtt_row(period, provider, commissioner, part, tf, bands=None, unknown=0, total_all=None):
    """One source row. bands: {band_index: patients}."""
    bands = bands or {}
    band_sum = sum(bands.values())
    row = {src: "" for src, _ in RTT_COLUMNS}
    row.update(
        {
            "Period": period,
            "Provider Parent Org Code": "QF7",
            "Provider Parent Name": "NHS SOUTH YORKSHIRE INTEGRATED CARE BOARD",
            "Provider Org Code": provider,
            "Provider Org Name": f"PROVIDER {provider}",
            "Commissioner Parent Org Code": "QF7",
            "Commissioner Parent Name": "NHS SOUTH YORKSHIRE INTEGRATED CARE BOARD",
            "Commissioner Org Code": commissioner,
            "Commissioner Org Name": f"COMMISSIONER {commissioner}",
            "RTT Part Type": part,
            "RTT Part Description": part,
            "Treatment Function Code": tf,
            "Treatment Function Name": "Total" if tf == "C_999" else f"TF {tf}",
        }
    )
    if part == "Part_3":  # new periods: only Total All, no bands
        row["Total All"] = str(total_all if total_all is not None else 0)
        return row
    for i in range(N_BANDS):
        v = bands.get(i, 0)
        row[RTT_COLUMNS[13 + i][0]] = str(v) if v else ""  # real files leave zeros blank
    if part in ("Part_1A", "Part_1B"):
        row["Total"] = str(band_sum)
        row["Patients with unknown clock start date"] = str(unknown) if unknown else ""
    # Part_2 / Part_2A: Total and unknown stay blank, only Total All is filled (as in the real file)
    row["Total All"] = str(total_all if total_all is not None else band_sum + unknown)
    return row


def with_c999(rows):
    """Append the file's own C_999 subtotal rows (sum of specialties per provider x commissioner x part)."""
    out = list(rows)
    groups: dict = {}
    for r in rows:
        key = (r["Period"], r["Provider Org Code"], r["Commissioner Org Code"], r["RTT Part Type"])
        g = groups.setdefault(key, {"bands": {}, "unknown": 0, "total": 0})
        for i in range(N_BANDS):
            v = r[RTT_COLUMNS[13 + i][0]]
            if v:
                g["bands"][i] = g["bands"].get(i, 0) + int(v)
        u = r["Patients with unknown clock start date"]
        g["unknown"] += int(u) if u else 0
        g["total"] += int(r["Total All"] or 0)
    for (period, prov, comm, part), g in groups.items():
        out.append(rtt_row(period, prov, comm, part, "C_999", g["bands"], g["unknown"], g["total"]))
    return out


def write_csv(path: Path, header: list[str] | None, rows: list[list[str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="latin-1") as f:
        w = csv.writer(f)
        if header:
            w.writerow(header)
        w.writerows(rows)
    return path


def write_rtt(path: Path, rows: list[dict]) -> Path:
    header = [src for src, _ in RTT_COLUMNS]
    return write_csv(path, header, [[r[h] for h in header] for r in rows])


def standard_rtt_rows(period="RTT-August-2026"):
    rows = [
        rtt_row(period, "RHQ", "QF7", "Part_2", "C_100", {0: 10, 17: 5, 20: 3, 60: 2}),
        rtt_row(period, "RHQ", "QF7", "Part_2", "C_110", {1: 4, 52: 1}),
        rtt_row(period, "RHQ", "03N", "Part_2", "C_100", {2: 6}),
        rtt_row(period, "RHQ", "QF7", "Part_2A", "C_100", {0: 3, 30: 1}),
        rtt_row(period, "RHQ", "QF7", "Part_1A", "C_100", {5: 7}, unknown=2),
        rtt_row(period, "RHQ", "QF7", "Part_1B", "C_110", {3: 9}),
        rtt_row(period, "RHQ", "QF7", "Part_3", "C_100", total_all=40),
        rtt_row(period, "RA9", "QJK", "Part_2", "C_100", {0: 1, 104: 1}),
    ]
    return with_c999(rows)


def ae_row(period, code, parent, name, values):
    return [period, code, parent, name] + [str(v) for v in values]


def standard_ae_rows(period="MSitAE-SEPTEMBER-2026"):
    a = [100, 0, 50, 10, 0, 0, 30, 0, 5, 2, 0, 0, 8, 1, 40, 0, 3, 20]
    b = [200, 5, 0, 0, 0, 0, 70, 1, 0, 0, 0, 0, 15, 4, 80, 2, 0, 30]
    c = [0, 0, 2584, 0, 0, 0, 0, 0, 273, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    total = [x + y + z for x, y, z in zip(a, b, c, strict=True)]
    return [
        ae_row(period, "RHQ", "NHS ENGLAND NORTH EAST AND YORKSHIRE ", "SHEFFIELD TEACHING", a),
        ae_row(period, "RA9", "NHS ENGLAND SOUTH WEST ", "TORBAY AND SOUTH DEVON", b),
        ae_row(period, "NTV0B", "NHS ENGLAND SOUTH EAST ", "ASHFORD WALK-IN-CENTRE", c),
        ae_row("TOTAL", "TOTAL", "TOTAL", "TOTAL", total),
    ]


def write_ae(path: Path, rows) -> Path:
    return write_csv(path, [src for src, _ in AE_COLUMNS], rows)


def etr_row(code, name, region, icb, open_date, close_date=""):
    row = [""] * len(ODS_ETR_COLUMNS)
    row[0], row[1], row[2], row[3] = code, name, region, icb
    row[4], row[9], row[10], row[11] = "1 HOSPITAL ROAD", "S1 1AA", open_date, close_date
    row[17], row[21], row[23] = "0114 000 0000", "1", "D"
    return row
