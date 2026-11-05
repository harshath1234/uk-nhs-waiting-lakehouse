"""Discover NHS England download links and write a landing manifest.

Why this exists (see docs/adr/0001-manifest-driven-landing.md):
NHS England file URLs cannot be computed from the month. Recent uploads carry a
random suffix (e.g. ``...-Aug26-ZIP-3M-vjgRdG.zip``), sit in the folder of the
*upload* month rather than the data month, and revised files get new links.
So we read the published year pages, parse the link text, and hand Azure Data
Factory a manifest of exactly what to copy and where to put it.

Usage:
    python -m nhs_ingest.discover --start 2021-04 --out config/manifest.json
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from dataclasses import asdict, dataclass
from datetime import date
from urllib.parse import urlsplit

from bs4 import BeautifulSoup

RTT_PAGE = "https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-{fy}/"
AE_PAGE = (
    "https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/"
    "ae-attendances-and-emergency-admissions-{fy}/"
)
ODS_REPORT = "https://www.odsdatasearchandexport.nhs.uk/api/getReport?report={report}"
USER_AGENT = "uk-nhs-waiting-lakehouse/0.1 (portfolio data pipeline)"

MONTHS = {
    m: i
    for i, m in enumerate(
        ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"], start=1
    )
}
FULL_MONTHS = {
    m: i
    for i, m in enumerate(
        [
            "january",
            "february",
            "march",
            "april",
            "may",
            "june",
            "july",
            "august",
            "september",
            "october",
            "november",
            "december",
        ],
        start=1,
    )
}

# "Full CSV data file Aug26 (ZIP, 3M)" / "Full CSV data file Apr21 revised (ZIP, 3587K)"
# Older pages also put "revised" after the size: "Full CSV data file Aug19 (ZIP, 3493K) revised"
RTT_TEXT = re.compile(r"full csv data file\s+([a-z]{3})(\d{2})\b(.*)", re.I)
# "Monthly A&E September 2026 (CSV, 28KB)" / "Monthly A&E April 2021 (Revised 12.05.2022) (CSV,30K)"
# "Monthly A&E March 2026 revised 14.05.26) (CSV, 29KB)"  <- unbalanced bracket is real
AE_TEXT = re.compile(r"monthly a&e\s+([a-z]+)\s+(\d{4})\b(.*)", re.I)
REVISED_DATE = re.compile(r"revised\s+(\d{1,2})\.(\d{1,2})\.(\d{2,4})", re.I)
UPLOAD_FOLDER = re.compile(r"/uploads/sites/\d+/(\d{4})/(\d{2})/")


@dataclass(frozen=True)
class ManifestEntry:
    dataset: str  # rtt | ae | ods_<report>
    period: str  # data month YYYY-MM (snapshot date for ODS)
    url: str
    base_url: str  # split for the ADF HTTP linked service
    relative_url: str
    file_name: str
    is_revised: bool
    revised_on: str | None  # YYYY-MM-DD when the link text gives a date
    published: str  # upload folder YYYY-MM: when NHS England published this version
    sink_path: str  # path inside the ADLS 'landing' container
    link_text: str

    @property
    def version_key(self) -> tuple:
        """Higher sorts later. Latest publication wins."""
        return (self.published, self.revised_on or "", self.is_revised)


def financial_year(d: date) -> str:
    """2026-08 -> '2026-27'; 2026-02 -> '2025-26'."""
    start = d.year if d.month >= 4 else d.year - 1
    return f"{start}-{str(start + 1)[-2:]}"


def financial_years(start: date, end: date) -> list[str]:
    years: list[str] = []
    y, m = start.year, start.month
    while (y, m) <= (end.year, end.month):
        fy = financial_year(date(y, m, 1))
        if fy not in years:
            years.append(fy)
        m += 1
        if m == 13:
            y, m = y + 1, 1
    return years


def _parse_revised_date(text: str) -> str | None:
    m = REVISED_DATE.search(text)
    if not m:
        return None
    d, mo, y = (int(x) for x in m.groups())
    if y < 100:
        y += 2000
    return date(y, mo, d).isoformat()


def _published(url: str) -> str:
    m = UPLOAD_FOLDER.search(url)
    return f"{m.group(1)}-{m.group(2)}" if m else "unknown"


def _split(url: str) -> tuple[str, str, str]:
    parts = urlsplit(url)
    base = f"{parts.scheme}://{parts.netloc}/"
    rel = parts.path.lstrip("/") + (f"?{parts.query}" if parts.query else "")
    return base, rel, parts.path.rsplit("/", 1)[-1]


def _entry(
    dataset: str,
    period: str,
    url: str,
    text: str,
    revised: bool,
    revised_on: str | None,
    ext_name: str | None = None,
) -> ManifestEntry:
    base, rel, name = _split(url)
    name = ext_name or name
    published = _published(url)
    return ManifestEntry(
        dataset=dataset,
        period=period,
        url=url,
        base_url=base,
        relative_url=rel,
        file_name=name,
        is_revised=revised,
        revised_on=revised_on,
        published=published,
        sink_path=f"{dataset}/period={period}/published={published}/{name}",
        link_text=" ".join(text.split()),
    )


def parse_rtt_page(html: str) -> list[ManifestEntry]:
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, ManifestEntry] = {}
    for a in soup.find_all("a", href=True):
        text = " ".join(a.get_text(" ").split())
        m = RTT_TEXT.search(text)
        if not m or not a["href"].lower().endswith(".zip"):
            continue
        mon, yy, rest = m.groups()
        if mon.lower() not in MONTHS:
            continue
        period = f"20{yy}-{MONTHS[mon.lower()]:02d}"
        revised = "revised" in rest.lower()
        out[a["href"]] = _entry("rtt", period, a["href"], text, revised, _parse_revised_date(rest))
    return list(out.values())


def parse_ae_page(html: str) -> list[ManifestEntry]:
    soup = BeautifulSoup(html, "html.parser")
    out: dict[str, ManifestEntry] = {}
    for a in soup.find_all("a", href=True):
        text = " ".join(a.get_text(" ").split())
        m = AE_TEXT.search(text)
        if not m or not a["href"].lower().endswith(".csv"):
            continue
        month, year, rest = m.groups()
        if month.lower() not in FULL_MONTHS:
            continue
        period = f"{year}-{FULL_MONTHS[month.lower()]:02d}"
        revised = "revised" in rest.lower()
        out[a["href"]] = _entry("ae", period, a["href"], text, revised, _parse_revised_date(rest))
    return list(out.values())


def ods_entries(snapshot: date, reports: tuple[str, ...] = ("etr", "eauth", "succ")) -> list[ManifestEntry]:
    """ODS reports have a fixed URL and change nightly, so we land a dated snapshot."""
    out = []
    for r in reports:
        url = ODS_REPORT.format(report=r)
        base, rel, _ = _split(url)
        day = snapshot.isoformat()
        out.append(
            ManifestEntry(
                dataset=f"ods_{r}",
                period=day,
                url=url,
                base_url=base,
                relative_url=rel,
                file_name=f"{r}.csv",
                is_revised=False,
                revised_on=None,
                published=day[:7],
                sink_path=f"ods/{r}/snapshot={day}/{r}.csv",
                link_text=f"ODS predefined report {r}",
            )
        )
    return out


def select(
    entries: list[ManifestEntry], start: str, end: str | None = None, latest_only: bool = False
) -> list[ManifestEntry]:
    """Keep periods in [start, end]. latest_only keeps the newest version per dataset+period.

    The default keeps every version: landing is append-only, and silver resolves
    revisions with a MERGE (latest publication wins).
    """
    kept = [e for e in entries if e.period >= start and (end is None or e.period <= end)]
    if latest_only:
        best: dict[tuple[str, str], ManifestEntry] = {}
        for e in kept:
            k = (e.dataset, e.period)
            if k not in best or e.version_key > best[k].version_key:
                best[k] = e
        kept = list(best.values())
    return sorted(kept, key=lambda e: (e.dataset, e.period, e.version_key))


def fetch(url: str) -> str:  # pragma: no cover - network
    import requests

    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=60)
    r.raise_for_status()
    return r.text


def build_manifest(
    start: str, end: str | None, today: date, latest_only: bool, fetcher=fetch
) -> list[ManifestEntry]:
    y, m = (int(x) for x in start.split("-"))
    entries: list[ManifestEntry] = []
    for fy in financial_years(date(y, m, 1), today):
        for template, parser in ((RTT_PAGE, parse_rtt_page), (AE_PAGE, parse_ae_page)):
            url = template.format(fy=fy)
            try:
                entries += parser(fetcher(url))
            except Exception as exc:  # a future year page may not exist yet
                print(f"warn: could not read {url}: {exc}", file=sys.stderr)
    return select(entries, start, end, latest_only) + ods_entries(today)


def main(argv: list[str] | None = None) -> int:  # pragma: no cover - CLI
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--start", default="2021-04", help="first data month, YYYY-MM")
    p.add_argument("--end", default=None, help="last data month, YYYY-MM (default: latest)")
    p.add_argument("--latest-only", action="store_true", help="one version per month")
    p.add_argument("--out", default="config/manifest.json")
    a = p.parse_args(argv)
    entries = build_manifest(a.start, a.end, date.today(), a.latest_only)
    with open(a.out, "w", encoding="utf-8") as f:
        json.dump([asdict(e) for e in entries], f, indent=2)
    counts: dict[str, int] = {}
    for e in entries:
        counts[e.dataset] = counts.get(e.dataset, 0) + 1
    print(f"wrote {len(entries)} entries to {a.out}: {counts}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
