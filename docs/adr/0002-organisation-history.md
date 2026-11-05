# ADR 0002: Organisation history without ODS monthly snapshots

**Status:** accepted for milestone 2; history backfill to be confirmed (5 Nov 2026)

## Context
- The brief assumed monthly ODS snapshot files to drive an SCD Type 2 `dim_organisation`.
- ODS replaced its legacy CSV files with Data Search and Export reports. These update nightly and no snapshot or amendment files are published.
- A dbt snapshot only records changes from the day it first runs, so it cannot reconstruct 2021 to 2026.

## What the `etr` report gives us (checked 5 Nov 2026)
- 274 trusts, **including 72 closed trusts with open and close dates**.
- Region (national grouping) and ICB code are the **current** values only.

## Decision
1. Land a dated `etr` snapshot on every run (ADR 0001), so history accumulates from go-live.
2. Build `silver.ods_trust` from the latest snapshot: code, name, region, ICB, open date, close date. Drop address and phone columns.
3. Validity of a *trust* comes from open and close dates, so mergers appear as one code closing and another opening.
4. Validity of a trust's *ICB/region* is "current only" until backfilled from the ORD API relationships or the `succ` report. This is still to be verified.
5. The dbt snapshot in Snowflake (milestone 5) tracks changes from go-live.

## Consequences
- Region-level reporting uses today's region for every historical month. The 7 NHS England regions are the same throughout April 2021 onwards, so region totals are only affected by trusts that moved region (to verify); ICB-level history before go-live is approximate (ICBs replaced CCGs in July 2022, and some ICBs merged since). The README must say so.
- Region and ICB never come from the source files' parent columns. RTT parents were STPs in 2021 and ICBs in 2026, and A&E parent names have trailing spaces.
