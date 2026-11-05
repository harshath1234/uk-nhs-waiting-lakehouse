# Cost note

_No resources deployed yet. Fill this in with real figures from Cost Management after each milestone._

| Item | Expected scale | Guardrail |
|---|---|---|
| ADLS Gen2 (LRS) | ~65 RTT ZIPs × ~3–4 MB, ~130 A&E CSVs × ~30 KB, daily ODS snapshots: well under 1 GB | 7-day soft delete only |
| Data Factory | One pipeline run per month, a few hundred activity runs (one Copy per new file) | Trigger off until a manual run succeeds |
| Key Vault | Standard tier, a handful of secrets later | — |
| Databricks | Free Edition (serverless) or trial with auto-termination | See ADR 0003 |
| Snowflake (milestone 4) | XS warehouse, auto-suspend 60 s, resource monitor | Trial credit |
| **Budget** | **£5/month on the resource group**, email at 50% actual and 100% forecast | `azurerm_consumption_budget_resource_group` |

## What was left running
_To fill in at the end: either "destroyed on <date>" or what remains and its monthly cost._
