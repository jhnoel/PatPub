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
  worker_identities = {
    fetch        = azurerm_user_assigned_identity.fetch.principal_id
    renderer     = azurerm_user_assigned_identity.renderer.principal_id
    metadata     = azurerm_user_assigned_identity.metadata.principal_id
    indexer      = azurerm_user_assigned_identity.indexer.principal_id
    orchestrator = azurerm_user_assigned_identity.orchestrator.principal_id
  }
  blob_grants = {
    fetch_raw             = { identity = "fetch", container = "raw", role = "Storage Blob Data Contributor" }
    fetch_releases        = { identity = "fetch", container = "releases", role = "Storage Blob Data Contributor" }
    renderer_raw          = { identity = "renderer", container = "raw", role = "Storage Blob Data Reader" }
    renderer_source       = { identity = "renderer", container = "source", role = "Storage Blob Data Contributor" }
    renderer_releases     = { identity = "renderer", container = "releases", role = "Storage Blob Data Contributor" }
    metadata_raw          = { identity = "metadata", container = "raw", role = "Storage Blob Data Reader" }
    metadata_metadata     = { identity = "metadata", container = "metadata", role = "Storage Blob Data Contributor" }
    metadata_releases     = { identity = "metadata", container = "releases", role = "Storage Blob Data Contributor" }
    indexer_raw           = { identity = "indexer", container = "raw", role = "Storage Blob Data Reader" }
    indexer_releases      = { identity = "indexer", container = "releases", role = "Storage Blob Data Contributor" }
    orchestrator_raw      = { identity = "orchestrator", container = "raw", role = "Storage Blob Data Reader" }
    orchestrator_source   = { identity = "orchestrator", container = "source", role = "Storage Blob Data Contributor" }
    orchestrator_metadata = { identity = "orchestrator", container = "metadata", role = "Storage Blob Data Reader" }
    orchestrator_releases = { identity = "orchestrator", container = "releases", role = "Storage Blob Data Contributor" }
  }
}

resource "azurerm_role_assignment" "blob" {
  for_each             = local.blob_grants
  scope                = azurerm_storage_container.downstream[each.value.container].id
  role_definition_name = each.value.role
  principal_id         = local.worker_identities[each.value.identity]
}

resource "azurerm_role_assignment" "quickwit_blob" {
  for_each = {
    indexer      = azurerm_user_assigned_identity.indexer.principal_id
    orchestrator = azurerm_user_assigned_identity.orchestrator.principal_id
  }
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

resource "azurerm_role_assignment" "orchestrator_queue" {
  scope                = azurerm_storage_queue.pipeline_events.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_user_assigned_identity.orchestrator.principal_id
}

resource "azurerm_role_assignment" "fetch_queue" {
  scope                = azurerm_storage_queue.pipeline_events.id
  role_definition_name = "Storage Queue Data Contributor"
  principal_id         = azurerm_user_assigned_identity.fetch.principal_id
}

resource "azurerm_role_assignment" "fetch_key_vault" {
  scope                = azurerm_key_vault.uspto.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.fetch.principal_id
}

resource "azurerm_role_definition" "job_dispatcher" {
  name        = "${var.name_prefix}-job-dispatcher-${local.suffix}"
  scope       = azurerm_resource_group.patpub.id
  description = "Start and observe downstream Container Apps Jobs"
  permissions {
    actions = ["Microsoft.App/jobs/read", "Microsoft.App/jobs/start/action", "Microsoft.App/jobs/executions/read"]
  }
  assignable_scopes = [azurerm_resource_group.patpub.id]
}

resource "azurerm_role_assignment" "job_dispatcher" {
  for_each = var.compute_enabled ? {
    renderer = azurerm_container_app_job.renderer[0].id
    metadata = azurerm_container_app_job.metadata[0].id
    indexer  = azurerm_container_app_job.indexer[0].id
  } : {}
  scope              = each.value
  role_definition_id = azurerm_role_definition.job_dispatcher.role_definition_resource_id
  principal_id       = azurerm_user_assigned_identity.orchestrator.principal_id
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
  principal_id       = azurerm_user_assigned_identity.orchestrator.principal_id
}

resource "azurerm_role_assignment" "deployment_vault_admin" {
  scope                = azurerm_key_vault.uspto.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}
