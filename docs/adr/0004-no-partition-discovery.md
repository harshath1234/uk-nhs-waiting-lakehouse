# ADR 0004: Read landed files without Spark partition discovery

**Status:** accepted (5 Nov 2026). Found by the unit tests.

## Context
Landing folders look like Hive partitions (`period=2026-09/published=2026-10/`). By default Spark turns those folder names into columns. Spark column names are case-insensitive, so the inferred `period` column clashed with the files' own `Period` column. The header check then failed with `missing=['Period'] extra=['period', 'published']`.

## Decision
- Read every source with `recursiveFileLookup=true`, which switches partition discovery off.
- Take lineage from `_metadata.file_path` instead, and extract `_published` with a regex.
- If the bronze step moves to Auto Loader, set `cloudFiles.partitionColumns = ""` for the same reason.

## Consequences
- The header check stays strict: the 121 RTT columns and 22 A&E columns must match exactly, otherwise `SchemaDriftError`.
- Lineage columns are explicit and tested.
