# ADR 0001: Manifest-driven landing instead of computed URLs

**Status:** accepted (5 Nov 2026)

## Context
The brief planned an ADF `ForEach` over months that builds each download URL from the month.
Checking the real NHS England pages showed this cannot work:
- Recent uploads carry a random six-character code (`...-Aug26-ZIP-3M-vjgRdG.zip`), shared with that month's press notice.
- Files sit in the folder of the **upload** month (`2026/10` for August 2026 data), and revisions are re-uploaded much later (all 2021-22 revisions sit in `2022/01` or `2023/03`).
- Revised files get new link text ("revised", "revised v2", "(Revised 12.05.2022)").

## Decision
- A small Python step (`ingest/src/nhs_ingest/discover.py`) reads the RTT and A&E year pages and writes `config/manifest.json`.
- Each entry has: dataset, data month, URL (split into base and relative), revised flag, revised date, publication folder, and the sink path.
- ADF reads the manifest with a Lookup, loops with ForEach, skips files already landed (GetMetadata `exists`), and copies the rest byte-for-byte.
- Landing path: `landing/<dataset>/period=YYYY-MM/published=YYYY-MM/<original file name>`. Each publication of a month lands in its own folder, so landing is append-only and immutable.
- ODS reports have a fixed URL but change nightly, so they land as dated snapshots: `ods/<report>/snapshot=YYYY-MM-DD/`.

## Consequences
- The pipeline depends on link text staying parseable. A test fixture holds every link-text variant seen so far, including an unbalanced bracket in "revised 14.05.26)".
- Discovery runs outside ADF for now: locally, then `az storage blob upload`. A scheduled GitHub Actions job using OIDC is on the roadmap.
- Link-text sizes are never used for validation. "Mar22 revised (ZIP, 111805K)" is really a 4.5 MB ZIP.
