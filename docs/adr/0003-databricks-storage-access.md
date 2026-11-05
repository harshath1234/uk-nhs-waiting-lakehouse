# ADR 0003: How Databricks reads the landing zone

**Status:** proposed. Decide when the Databricks workspace exists (milestone 2).

## Options
| Option | How | Trade-off |
|---|---|---|
| A. Azure Databricks trial | Unity Catalog storage credential (access connector, managed identity) → external location on `abfss://landing@<account>.dfs.core.windows.net/` → external volume `nhs.raw.landing` | Reads ADLS natively, the "real" consultancy pattern; uses Azure credit, so small clusters and auto-termination are needed |
| B. Databricks Free Edition | Copy landed files into a managed volume `nhs.raw.landing` (Databricks CLI `fs cp`, or a notebook that reads them over HTTPS with a SAS token) | Free, but the external-storage step is not shown; must be stated in the README |

## Notes
- The notebooks only need a path (`landing` and `staging` widgets), so either option works without code changes.
- RTT ZIPs are unpacked with Python `zipfile`, which needs a file-system path. UC volumes provide one (`/Volumes/...`); a raw `abfss://` URI does not.
