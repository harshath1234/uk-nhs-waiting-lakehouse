# NHS Waiting Times Lakehouse

**Azure Data Factory → ADLS Gen2 → Databricks (Delta) → Snowflake + dbt → Power BI**, on NHS England's monthly Referral to Treatment (RTT) and A&E statistics, April 2021 onwards.

> **Status (5 Nov 2026): milestone 1–2 code written and unit-tested; nothing deployed yet.**
> No figures in this README come from a pipeline run. The "Key findings" section stays empty until real runs exist.

## Why this project
The consultancy pattern *land in Azure → engineer in Databricks → serve from Snowflake → report in Power BI*, built end to end on public NHS data (Open Government Licence). The data is messy in realistic ways, documented in [`docs/source_quirks.md`](docs/source_quirks.md):
- **Subtotal rows** in the RTT file double the waiting list if summed naively (14.4M instead of 7.19M for August 2026).
- **105 weekly wait-band columns** need unpivoting.
- **Revised months** are re-published under new URLs, months or years later.
- **File URLs** carry random codes and can't be predicted.
- **A&E files** end with a TOTAL row, and region names carry trailing spaces.
- **Organisation reference data** is a nightly-changing report with no history files.

## Architecture
```
NHS England pages ──► nhs_ingest.discover ──► config/manifest.json
                                                   │ (uploaded to ADLS config/)
                                                   ▼
NHS England files ──► ADF pl_land_nhs (Lookup → ForEach → GetMetadata → Copy) ──► ADLS landing/
                       managed identity · monthly trigger · skips files already landed
                                                   │
                                                   ▼
Databricks job  01_bronze (unzip, header check, append new files) → 02_silver (quality gate, MERGE)
                                                   │
                                                   ▼  milestone 3+
                gold (Delta) → Parquet → Snowflake (dbt marts, SCD2 snapshot) → Power BI (DAX, RLS)
```

## What's built (code, tested locally)
| Area | What | Where |
|---|---|---|
| Discovery | Parses RTT and A&E year pages into a manifest: data month, publication folder, revised flag and date, sink path | `ingest/src/nhs_ingest/discover.py` |
| Infrastructure | Terraform (azurerm): resource group, ADLS Gen2 (HNS), containers, Key Vault (RBAC), Data Factory (managed identity), role assignments, **£5 budget alert** | `infra/terraform/azure/` |
| Landing | ADF pipeline as native JSON, deployed by Terraform: manifest Lookup, parallel ForEach, skip-if-landed, HTTP → ADLS binary copy, monthly trigger | `adf/`, `infra/terraform/azure/adf.tf` |
| Bronze | ZIP extraction (path-traversal safe, idempotent), strict header check (121 RTT / 22 A&E / 27 ODS columns), lineage columns | `databricks/src/nhs_lakehouse/bronze.py` |
| Silver | Drop `C_999` subtotals; blanks → 0; unpivot to the wait-band grain; latest publication wins per month; A&E TOTAL row removed and names trimmed; ODS trusts with region and ICB | `databricks/src/nhs_lakehouse/silver.py` |
| Revisions | Delta `MERGE` that replaces whole re-published months (including rows that vanished); time-travel audit notebook | `silver.merge_month_replace_sql`, `notebooks/03_audit_revision.sql` |
| Quality | Grain uniqueness, non-negative counts, bands + unknown = Total All, specialties = `C_999`, A&E rows = TOTAL row, month completeness (warn), unmatched codes table, freshness | `databricks/src/nhs_lakehouse/quality.py` |
| Orchestration | Databricks Asset Bundle job: bronze → silver | `databricks/databricks.yml` |
| Tests | 33 pytest tests (local SparkSession on synthetic files that copy the real files' quirks), ADF/manifest contract tests | `ingest/tests`, `databricks/tests` |
| CI | ruff, pytest with Java, `terraform validate` | `.github/workflows/ci.yml` |

## Roadmap (not built yet)
- [ ] **Milestone 1 run:** `terraform apply`, land 36+ months, screenshots, budget alert confirmed.
- [ ] **Milestone 2 run:** Databricks workspace, Unity Catalog, first bronze/silver job run, revision audit.
- [ ] Milestone 3: gold tables, Parquet export, Workflow job screenshot.
- [ ] Milestone 4: Snowflake (roles, warehouse, resource monitor, storage integration, COPY INTO).
- [ ] Milestone 5: dbt-snowflake marts, SCD2 snapshot, 25+ tests, docs site, `dbt build` in CI.
- [ ] Milestone 6: Power BI model, 10+ DAX measures, RLS by region, 3 report pages.
- [ ] Scheduled discovery in GitHub Actions (OIDC to Azure).

## Key findings
_Filled from real pipeline output only._

## Run it
See [`docs/runbook.md`](docs/runbook.md). Locally:
```bash
pip install -e ".[dev]"   # Java 17+ needed for the Spark tests
pytest -q
python -m nhs_ingest.discover --start 2021-04 --out config/manifest.json
```

## Docs
- [Source quirks](docs/source_quirks.md): what the real files contain, and the traps
- ADRs:
  - [manifest-driven landing](docs/adr/0001-manifest-driven-landing.md)
  - [organisation history](docs/adr/0002-organisation-history.md)
  - [Databricks storage access](docs/adr/0003-databricks-storage-access.md)
  - [no partition discovery](docs/adr/0004-no-partition-discovery.md)
- [Runbook](docs/runbook.md) · [Cost note](docs/cost.md)

## Data and licence
- Data: © NHS England, Open Government Licence v3.0. No data files are committed; tests use synthetic files.
- Code: MIT.
