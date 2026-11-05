# Source quirks: NHS Waiting Times Lakehouse

_Checked 5 Nov 2026 by opening the real files in the browser (read in memory; nothing saved). Files checked:_
- _RTT full extract: Aug 2026 and Apr 2021 (revised). Mar 2022 (revised): file list only._
- _A&E monthly CSV: Sep 2026 and Apr 2021 (revised)._
- _ODS `etr` (NHS trusts) report as of 5 Nov 2026._

_Items still marked **[VERIFY]** were not checked._

---

## 1. RTT waiting times (main fact)

**Year pages:** `https://www.england.nhs.uk/statistics/statistical-work-areas/rtt-waiting-times/rtt-data-YYYY-YY/`
**Latest month:** August 2026 (published 8 Oct 2026). Apr 2021 → Aug 2026 = 65 months.

### File and URL facts
| Fact | Detail |
|---|---|
| ZIP contents | One CSV per ZIP, e.g. `20260831-RTT-August-2026-full-extract.csv` (revised: `…-full-extract-revised.csv`) |
| Size | Aug26: 3.4 MB zipped → 80 MB CSV, **177,770 rows**. Apr21 revised: 3.6 MB → 105 MB, 160,251 rows |
| Volume estimate | ~65 months × ~170k wide rows ≈ 11M rows; × 105 bands ≈ 1bn cells before dropping blanks. Spark is justified (quote real counts only after the run) |
| URL pattern (recent) | `wp-content/uploads/sites/2/<upload yyyy>/<upload mm>/Full-CSV-data-file-<Mon><YY>-ZIP-<size>-<6-char code>.zip`. The code is shared with that month's press notice (Aug26 = `vjgRdG`) |
| URL pattern (older) | No random code, e.g. `…/2022/01/Full-CSV-data-file-Apr21-revised-ZIP-3587K.zip` |
| Upload folder ≠ data month | Aug26 sits in `2026/10`; all 2021-22 revised files sit in `2022/01` or `2023/03` |
| **Size in link text can be wrong** | "Mar22 revised (ZIP, 111805K)": the ZIP is really 4.5 MB; 112 MB is the *unzipped* size. Never use the link-text size for validation; record actual bytes |

**→ Design:** URLs can't be computed. A discovery step scrapes the year pages for `Full CSV data file` links and writes a manifest:
- month
- revised flag
- URL
- upload folder (as publication date)

ADF loops over the manifest.

### Schema (stable since 2021)
- **121 columns, identical names and order** in Apr21 and Aug26. One schema fits all.
- 13 descriptive columns:
  - Period
  - Provider Parent Org Code / Name
  - Provider Org Code / Name
  - Commissioner Parent Org Code / Name
  - Commissioner Org Code / Name
  - RTT Part Type
  - RTT Part Description
  - Treatment Function Code / Name
- **105 weekly band columns:** `Gt 00 To 01 Weeks SUM 1` … `Gt 103 To 104 Weeks SUM 1`, `Gt 104 Weeks SUM 1`.
- 3 totals: `Total`, `Patients with unknown clock start date`, `Total All`.
- `Period` is text, e.g. `RTT-August-2026`, `RTT-APRIL-2021`. Case differs between years → parse case-insensitively.

### Five pathway types in one file
| RTT Part Type | Description | Rows (Aug26) |
|---|---|---|
| Part_1A | Completed pathways, admitted | 18,184 |
| Part_1B | Completed pathways, non-admitted | 30,243 |
| Part_2 | **Incomplete pathways** (the waiting list) | 63,444 |
| Part_2A | Incomplete pathways with decision to admit | 30,224 |
| Part_3 | New RTT periods (clock starts) | 35,675 |

### Traps found
1. **`C_999 Total` rows double-count.** Treatment function `C_999` is a subtotal row inside the same file.
   - Summing all Part_2 `Total All` gives 14,378,426. That is exactly 2 × 7,189,213.
   - Silver must drop `C_999`, or keep it only for a reconciliation test (sum of the specialties must equal C_999).
2. **Totals are filled differently by part.**
   - Part_2 and Part_2A: `Total` and `unknown clock start` are **blank**; only `Total All` is filled.
   - Part_1A/1B: `Total` is filled.
   - Part_3: no bands at all, only `Total All`.
   - Use `Total All` as the measure everywhere; don't test "bands = Total" for Part_2.
3. **Blanks mean zero, not missing.** About 5.9M blank numeric cells in Aug26 and no non-numeric values. Cast blank → 0 in silver, and record that choice in the data dictionary.
4. **Parent columns change meaning over time.**
   - Apr21: parents are STPs/ICSs (e.g. "LANCASHIRE AND SOUTH CUMBRIA ICS").
   - Aug26: parents are ICBs (e.g. "NHS SOUTH YORKSHIRE INTEGRATED CARE BOARD"), with 36 provider parents.
   - Commissioner parent is sometimes blank.
   - **Don't take region or ICB from the file**; take it from ODS.
5. **Commissioners switch from CCGs to ICB/sub-ICB codes** (July 2022). Aug26 has 129 commissioner codes.
6. **Providers include non-trusts.** Aug26 has 533 provider codes, but ODS lists about 200 open NHS trusts. The rest are mostly independent-sector providers, which aren't in `etr`.
   - **[VERIFY]** Which ODS report covers them (`ephp` / `enonnhs`); otherwise route them to `unmatched_codes`.
7. **Missing trusts each month** are named in the press notice (Aug26: four acute trusts, incl. RA9, RK9, RNS). The file simply has no rows for them.

### Sanity check (Aug 2026, C_999 rows only, excluding estimates for missing trusts)
These were computed in the browser and are not yet pipeline output.
- Incomplete pathways: **7,189,213**
- Within 18 weeks: **65.2%**
- Over 52 weeks: 112,958 · over 65 weeks: 12,488 · over 78 weeks: 2,748
- Treatment functions: 23 specialties plus the total

Re-check these against the Aug26 press notice's "not including estimates" table in the pipeline's reconciliation test.

---

## 2. A&E attendances and emergency admissions (second fact)

**Year pages:** `.../ae-waiting-times-and-activity/ae-attendances-and-emergency-admissions-YYYY-YY/`
**Latest month:** September 2026.

| Fact | Detail |
|---|---|
| URL (recent) | `…/2026/10/September-2026-CSV-Re72Eh.csv`: random code again |
| URL (revised) | `…/2022/05/April-2021-revised-120522-ABC456.csv`. Link text: "(Revised 12.05.2022)" → parse as publication version |
| Size | About 28 KB, **192 rows** (Sep26) / 211 (Apr21) |
| Schema | **22 columns, identical in 2021 and 2026** |
| Columns | Period, Org Code, Parent Org, Org name, then attendances (Type 1 / Type 2 / Other, each also as Booked Appointments), over-4-hours (same six), 4–12h and 12+h waits from decision to admit, emergency admissions (via A&E by type, plus other) |

### Traps found
1. **A `TOTAL` row at the end** (all four text columns = "TOTAL"). Drop it from the fact, but use it as a free reconciliation test: column sums equal it exactly (checked for Sep26).
2. **`Parent Org` is the NHS England region** (7 regions), with **trailing spaces** (e.g. `"NHS ENGLAND SOUTH EAST "`). Trim it.
3. **Includes walk-in centres and independent providers** (e.g. code NTV0B, Ashford Walk-in Centre). Only some rows are NHS trusts.
4. **Header typo:** `Patients who have waited 4-12 hs from DTA to admission`. Map it to a clean name in bronze → silver.
5. **4-hour % definition:** (all attendances − all over 4h) ÷ all attendances, *including booked appointments*.
   - Sep26 check: **74.4%** all types, **60.6%** Type 1 (Type 1 including booked Type 1 appointments).
   - Emergency admissions waiting 12+ hours from decision to admit: 50,582.
   - Not pipeline output yet.
6. A&E alone does **not** justify Spark; say so in the README.

---

## 3. Organisation reference data (ODS)

**Stable download URL (no scraping needed):** `https://www.odsdatasearchandexport.nhs.uk/api/getReport?report=etr`. Also works for `ets`, `eccg`, `eauth`, `succ`.

| Fact | Detail |
|---|---|
| Format | CSV, **no header row**, **27 columns**, 274 rows (5 Nov 2026) |
| Columns used | 0 code · 1 name · 2 national grouping = **region code** (Y56 London, Y58 South West, Y59 South East, Y60 Midlands, Y61 East of England, Y62 North West, Y63 North East and Yorkshire) · 3 **ICB code** (e.g. QF7) · 10 open date · 11 close date (yyyymmdd) |
| Column positions | Defined in the report specification (Reference Data Catalogue) → hard-code a schema with names in bronze |
| Closed trusts | **72 closed trusts are included** with close dates (e.g. Solent NHS Trust, closed 30 Sep 2024) |
| New trusts | e.g. North London NHS FT opened 1 Nov 2024 (merger); "The Online NHS Trust" opened 1 Jun 2026 |
| Personal data | Columns include addresses and phone numbers → select only the columns above in silver |

### What this means for SCD2
- **Good:** open/close dates give a trust's lifetime, so mergers show as one code closing and another opening.
- **Gap:** region and ICB are **current values only**; there's no history of a trust moving between ICBs.
  - For history, use the ORD API relationships or the `succ` report (**[VERIFY]**).
  - Then let the dbt snapshot track changes from go-live.
  - Document this in an ADR.
- Cross-check: RHQ (Sheffield) → Y63 / QF7 and RA9 (Torbay) → Y58 / QJK look right.

---

## 4. Found while building (5 Nov 2026)

- **Landing folders look like Hive partitions** (`period=…/published=…`). Spark's default partition discovery turned them into columns, and `period` clashed with the file's own `Period` column (Spark is case-insensitive). Fixed by reading with `recursiveFileLookup` and taking lineage from `_metadata.file_path` (ADR 0004).
- **Null revision dates broke the "latest version" join.** Unrevised A&E files have no revision date, and `null = null` is not true in SQL, so every unrevised row disappeared. Fixed with a null-safe join; a test covers it.

## 5. Changes to the brief

| Brief says | Change |
|---|---|
| ADF ForEach over months, Copy HTTP → ADLS | Discovery step builds a manifest (RTT + A&E); ForEach over manifest rows. ODS uses a fixed URL |
| Fact grain includes wait band | Unpivot 105 band columns; drop blank/zero cells; **exclude `C_999`** |
| "Revised months replace earlier ones" | Version = upload folder + "revised" flag (RTT) or revised date (A&E); MERGE keeps the latest |
| `dim_organisation` SCD2 from ODS snapshots | Lifetime from etr open/close dates; relationship history from ORD/`succ`; dbt snapshot from go-live |
| Region from source files | Region and ICB come only from ODS (RTT parents change meaning; A&E names have trailing spaces) |
| Unknown codes → `unmatched_codes` | Expect many (independent sector in RTT, walk-in centres in A&E); report the count, don't fail |
| Reconciliation tests | Add: specialty sum = `C_999` row (RTT); provider sum = `TOTAL` row (A&E) |

## 6. Still to verify
- [ ] ODS report covering independent-sector providers (`ephp` / `enonnhs`), and match rate against RTT provider codes.
- [ ] ORD API or `succ` for trust → ICB relationship history.
- [ ] Aug26 press notice "not including estimates" figures vs 7,189,213 and 65.2%.
- [ ] Spot-check one month from each financial year 2022-23 to 2025-26 for schema drift (2021 and 2026 already match).
