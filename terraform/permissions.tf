# Permissions resources for the steady-state PatPub deployment.

resource "azurerm_role_assignment" "searcher_acr_pull" {
  scope                = azurerm_container_registry.patpub.id
  role_definition_name = "AcrPull"
  principal_id         = azurerm_user_assigned_identity.searcher.principal_id
  count                = var.compute_enabled ? 1 : 0
}

resource "azurerm_role_assignment" "searcher_corpus_read" {
  for_each             = var.compute_enabled ? toset(["releases", "source", "metadata"]) : toset([])
  scope                = azurerm_storage_container.downstream[each.value].id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.searcher.principal_id
}

resource "azurerm_role_assignment" "searcher_quickwit_read" {
  count                = var.compute_enabled ? 1 : 0
  scope                = azurerm_storage_container.quickwit.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_user_assigned_identity.searcher.principal_id
}

locals {
  downstream_containers = toset(["raw", "source", "metadata", "releases"])
  worker_identities     = { ingestion = azurerm_user_assigned_identity.ingestion.principal_id }
  blob_grants = {
    for container in local.downstream_containers : "ingestion_${container}" => {
      identity = "ingestion", container = container, role = "Storage Blob Data Contributor"
    }
  }
}

resource "azurerm_role_assignment" "blob" {
  for_each             = local.blob_grants
  scope                = azurerm_storage_container.downstream[each.value.container].id
  role_definition_name = each.value.role
  principal_id         = local.worker_identities[each.value.identity]
}

resource "azurerm_role_assignment" "quickwit_blob" {
  for_each             = local.worker_identities
  scope                = azurerm_storage_container.quickwit.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "acr_pull" {
  for_each             = local.worker_identities
  scope                = azurerm_container_registry.patpub.id
  role_definition_name = "AcrPull"
  principal_id         = each.value
}

resource "azurerm_role_assignment" "ingestion_key_vault" {
  scope                = azurerm_key_vault.uspto.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.ingestion.principal_id
}

resource "azurerm_role_definition" "serving_controller" {
  name        = "${var.name_prefix}-serving-controller-${local.suffix}"
  scope       = azurerm_resource_group.patpub.id
  description = "Stop and restart the single Quickwit serving app during file-metastore writes"
  permissions {
    actions = ["Microsoft.App/containerApps/read", "Microsoft.App/containerApps/start/action", "Microsoft.App/containerApps/stop/action"]
  }
  assignable_scopes = [azurerm_resource_group.patpub.id]
}

resource "azurerm_role_assignment" "serving_controller" {
  count              = var.compute_enabled ? 1 : 0
  scope              = azurerm_container_app.searcher[0].id
  role_definition_id = azurerm_role_definition.serving_controller.role_definition_resource_id
  principal_id       = azurerm_user_assigned_identity.ingestion.principal_id
}

resource "azurerm_role_assignment" "deployment_vault_admin" {
  scope                = azurerm_key_vault.uspto.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Preserve existing grants when consolidating the controller into the worker.
moved {
  from = azurerm_role_assignment.blob["orchestrator_source"]
  to   = azurerm_role_assignment.blob["ingestion_source"]
}
moved {
  from = azurerm_role_assignment.blob["orchestrator_releases"]
  to   = azurerm_role_assignment.blob["ingestion_releases"]
}
moved {
  from = azurerm_role_assignment.acr_pull["orchestrator"]
  to   = azurerm_role_assignment.acr_pull["ingestion"]
}
moved {
  from = azurerm_role_assignment.quickwit_blob["orchestrator"]
  to   = azurerm_role_assignment.quickwit_blob["ingestion"]
}
