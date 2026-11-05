# Deploys the ADF objects kept as native JSON in /adf, so the repo has one source of truth
# that also reads like an ADF export.

locals {
  adf_dir      = "${path.module}/../../../adf"
  ls_http      = jsondecode(file("${local.adf_dir}/linkedService/ls_http_public.json")).properties
  ds_http      = jsondecode(file("${local.adf_dir}/dataset/ds_http_binary.json")).properties
  ds_adls      = jsondecode(file("${local.adf_dir}/dataset/ds_adls_binary.json")).properties
  ds_manifest  = jsondecode(file("${local.adf_dir}/dataset/ds_manifest_json.json")).properties
  pl_land      = jsondecode(file("${local.adf_dir}/pipeline/pl_land_nhs.json")).properties
  ds_param_map = { for k, v in local.ds_http.parameters : k => "" }
}

resource "azurerm_data_factory_linked_service_data_lake_storage_gen2" "adls" {
  name                 = "ls_adls"
  data_factory_id      = azurerm_data_factory.adf.id
  url                  = azurerm_storage_account.lake.primary_dfs_endpoint
  use_managed_identity = true
}

resource "azurerm_data_factory_linked_custom_service" "http" {
  name                 = "ls_http_public"
  data_factory_id      = azurerm_data_factory.adf.id
  type                 = local.ls_http.type
  description          = local.ls_http.description
  type_properties_json = jsonencode(local.ls_http.typeProperties)
  parameters           = { for k, v in local.ls_http.parameters : k => v.defaultValue }
}

resource "azurerm_data_factory_custom_dataset" "http_binary" {
  name                 = "ds_http_binary"
  data_factory_id      = azurerm_data_factory.adf.id
  type                 = local.ds_http.type
  description          = local.ds_http.description
  type_properties_json = jsonencode(local.ds_http.typeProperties)
  parameters           = local.ds_param_map

  linked_service {
    name       = azurerm_data_factory_linked_custom_service.http.name
    parameters = { baseUrl = "@dataset().baseUrl" }
  }
}

resource "azurerm_data_factory_custom_dataset" "adls_binary" {
  name                 = "ds_adls_binary"
  data_factory_id      = azurerm_data_factory.adf.id
  type                 = local.ds_adls.type
  description          = local.ds_adls.description
  type_properties_json = jsonencode(local.ds_adls.typeProperties)
  parameters           = { for k, v in local.ds_adls.parameters : k => "" }

  linked_service {
    name = azurerm_data_factory_linked_service_data_lake_storage_gen2.adls.name
  }
}

resource "azurerm_data_factory_custom_dataset" "manifest" {
  name                 = "ds_manifest_json"
  data_factory_id      = azurerm_data_factory.adf.id
  type                 = local.ds_manifest.type
  description          = local.ds_manifest.description
  type_properties_json = jsonencode(local.ds_manifest.typeProperties)

  linked_service {
    name = azurerm_data_factory_linked_service_data_lake_storage_gen2.adls.name
  }
}

resource "azurerm_data_factory_pipeline" "land" {
  name            = "pl_land_nhs"
  data_factory_id = azurerm_data_factory.adf.id
  description     = local.pl_land.description
  annotations     = local.pl_land.annotations
  activities_json = jsonencode(local.pl_land.activities)

  depends_on = [
    azurerm_data_factory_custom_dataset.http_binary,
    azurerm_data_factory_custom_dataset.adls_binary,
    azurerm_data_factory_custom_dataset.manifest,
  ]
}

# NHS England publishes on the second Thursday of the month, so the 15th is always after it.
resource "azurerm_data_factory_trigger_schedule" "monthly" {
  name            = "tr_monthly_15th"
  data_factory_id = azurerm_data_factory.adf.id
  pipeline_name   = azurerm_data_factory_pipeline.land.name
  frequency       = "Month"
  interval        = 1
  time_zone       = "GMT Standard Time"
  activated       = var.trigger_activated

  schedule {
    days_of_month = [15]
    hours         = [6]
    minutes       = [0]
  }
}
