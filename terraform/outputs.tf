output "resource_group_name" {
  description = "Resource group owned by this PatPub deployment."
  value       = azurerm_resource_group.patpub.name
}

output "uspto_key_vault_name" {
  value = azurerm_key_vault.uspto.name
}

output "uspto_api_key_secret_uri" {
  value = local.uspto_secret_uri
}

output "searcher_url" {
  description = "PatPub search API URL when compute_enabled is true."
  value       = var.compute_enabled ? "https://${azurerm_container_app.searcher[0].ingress[0].fqdn}" : null
}

output "storage_account_name" {
  description = "Storage account containing PatPub source, metadata, and release data."
  value       = azurerm_storage_account.patpub.name
}

output "quickwit_storage_account_name" {
  description = "Storage account dedicated to Quickwit index data."
  value       = azurerm_storage_account.quickwit.name
}

output "quickwit_container" {
  value = azurerm_storage_container.quickwit.name
}

output "publication_containers" {
  value = { for name, container in azurerm_storage_container.downstream : name => container.name }
}

output "acr_login_server" {
  value = azurerm_container_registry.patpub.login_server
}

output "pipeline_identity_client_ids" {
  value = {
    fetch        = azurerm_user_assigned_identity.fetch.client_id
    renderer     = azurerm_user_assigned_identity.renderer.client_id
    metadata     = azurerm_user_assigned_identity.metadata.client_id
    indexer      = azurerm_user_assigned_identity.indexer.client_id
    orchestrator = azurerm_user_assigned_identity.orchestrator.client_id
  }
}

output "pipeline_job_resource_ids" {
  value = var.compute_enabled ? {
    fetch        = azurerm_container_app_job.fetch[0].id
    renderer     = azurerm_container_app_job.renderer[0].id
    metadata     = azurerm_container_app_job.metadata[0].id
    indexer      = azurerm_container_app_job.indexer[0].id
    orchestrator = azurerm_container_app_job.orchestrator[0].id
  } : {}
}
