# Runbook

Run order end to end: **discover → upload manifest → ADF lands files → Databricks bronze → silver** (→ gold/export, Snowflake + dbt and Power BI arrive in later milestones).

Commands are for Windows PowerShell; the repo root is the working folder.

## 0. One-time setup
```powershell
py -3.12 -m venv .venv
.\.venv\Scripts\pip install -e ".[dev]"
.\.venv\Scripts\python -m pytest            # needs Java 17+ for the Spark tests
winget install Microsoft.AzureCLI
winget install Hashicorp.Terraform
az login
```

## 1. Infrastructure (milestone 1)
```powershell
cd infra\terraform\azure
copy terraform.tfvars.example terraform.tfvars   # fill subscription_id, alert_emails
terraform init
terraform plan -out tfplan
terraform apply tfplan
terraform output
```
Creates: resource group, ADLS Gen2 (`landing`, `gold`, `config` containers), Key Vault (RBAC), Data Factory with a managed identity and access to storage, a £5 monthly budget with email alerts, and the ADF pipeline and trigger (trigger **off**).

> Role assignments can take a few minutes to apply. If the upload in step 2 says "not authorised", wait 5 minutes and retry.

## 2. Build and upload the manifest
```powershell
cd ..\..\..
.\.venv\Scripts\python -m nhs_ingest.discover --start 2021-04 --out config\manifest.json
# prints e.g. "wrote N entries ...: {'rtt': .., 'ae': .., 'ods_etr': 1, ...}"
# then run the upload command printed by `terraform output upload_manifest_command`
```
- Default: every published version of every month (revisions included). Landing is append-only; silver keeps the latest.
- Smaller first run: `--start 2026-06` (3 months), then widen.

## 3. Land the files (ADF)
- Azure portal → Data Factory → Launch studio → Author → `pl_land_nhs` → **Debug**, or **Add trigger → Trigger now**.
- Check: Monitor shows `ReadManifest` succeeded; `LandEachFile` iterations either copied or skipped.
- Check: Storage browser → `landing/rtt/period=2026-08/published=2026-10/…zip`.
- Re-run it: every iteration should skip (already landed). That proves idempotency; screenshot it.
- Then set `trigger_activated = true` in `terraform.tfvars` and `terraform apply` to run it monthly (15th, 06:00 UK).

Screenshots for the repo (`adf/screenshots/`): pipeline canvas, successful run, re-run skipping, landing folder tree, budget alert.

## 4. Databricks (milestone 2)
Pick the storage option in `docs/adr/0003-databricks-storage-access.md`, then:
```powershell
winget install Databricks.DatabricksCLI   # the new CLI; bundles don't work with the old pip 'databricks-cli'
databricks auth login --host <workspace-url>
cd databricks
databricks bundle deploy -t dev
databricks bundle run nhs_medallion -t dev
```
- `01_bronze` unzips RTT, checks headers, appends only new files, writes `bronze._load_log`.
- `02_silver` runs the quality gate (`silver._dq_results`; a failure stops the job), writes `silver.unmatched_codes`, MERGEs silver, then OPTIMIZE/Z-ORDER.
- After a revised month lands: run `03_audit_revision.sql` (time travel before vs after).

## 5. Tear down
```powershell
cd infra\terraform\azure
terraform destroy
```
Or keep only the storage account and note it in `docs/cost.md`.

## Troubleshooting
| Symptom | Cause | Fix |
|---|---|---|
| `SchemaDriftError: rtt: header changed` | NHS England changed the CSV layout | Compare with `docs/source_quirks.md`, update `schemas.py`, add a test |
| ADF Lookup fails "file not found" | manifest not uploaded | Step 2 |
| ADF Copy 403 on sink | role assignment not propagated | Wait 5 minutes and re-run |
| Quality gate `rtt_specialties_sum_to_c999` fails | source file inconsistent or a new subtotal code | Inspect the sample rows in `silver._dq_results` |
| `month_completeness` warn | a trust did not submit that month | Expected: check the month's press notice, which names missing trusts |
