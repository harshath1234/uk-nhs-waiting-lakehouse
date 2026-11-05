output "resource_group" {
  value = azurerm_resource_group.rg.name
}

output "storage_account" {
  value = azurerm_storage_account.lake.name
}

output "adls_dfs_endpoint" {
  value = azurerm_storage_account.lake.primary_dfs_endpoint
}

output "data_factory" {
  value = azurerm_data_factory.adf.name
}

output "key_vault" {
  value = azurerm_key_vault.kv.name
}

output "upload_manifest_command" {
  description = "Run after `python -m nhs_ingest.discover` to hand ADF the manifest."
  value       = "az storage blob upload --auth-mode login --account-name ${azurerm_storage_account.lake.name} --container-name config --name manifest.json --file config/manifest.json --overwrite"
}
