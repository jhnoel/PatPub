# One resumable worker performs both backfill and steady-state ingestion.
resource "azurerm_container_app_job" "ingestion" {
  count                        = var.compute_enabled ? 1 : 0
  name                         = "${var.name_prefix}-ingestion"
  resource_group_name          = azurerm_resource_group.patpub.name
  location                     = azurerm_resource_group.patpub.location
  container_app_environment_id = azurerm_container_app_environment.patpub.id
  replica_retry_limit          = 0
  replica_timeout_in_seconds   = 43200
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.ingestion.id]
  }
  registry {
    server   = azurerm_container_registry.patpub.login_server
    identity = azurerm_user_assigned_identity.ingestion.id
  }
  secret {
    name                = "uspto-api-key"
    key_vault_secret_id = local.uspto_secret_uri
    identity            = azurerm_user_assigned_identity.ingestion.id
  }
  secret {
    name  = "quickwit-storage-key"
    value = azurerm_storage_account.quickwit.primary_access_key
  }
  dynamic "secret" {
    for_each = var.router_bearer_token == "" ? [] : [var.router_bearer_token]
    content {
      name  = "candidate-canary-token"
      value = secret.value
    }
  }
  dynamic "schedule_trigger_config" {
    for_each = var.ingestion_enabled ? [1] : []
    content {
      cron_expression          = "0 * * * *"
      parallelism              = 1
      replica_completion_count = 1
    }
  }
  dynamic "manual_trigger_config" {
    for_each = var.ingestion_enabled ? [] : [1]
    content {
      parallelism              = 1
      replica_completion_count = 1
    }
  }
  template {
    container {
      name   = "ingestion"
      image  = var.ingestion_image
      cpu    = var.ingestion_cpu
      memory = var.ingestion_memory
      dynamic "env" {
        for_each = {
          AZURE_STORAGE_ACCOUNT     = azurerm_storage_account.patpub.name
          AZURE_CLIENT_ID           = azurerm_user_assigned_identity.ingestion.client_id
          QW_AZURE_STORAGE_ACCOUNT  = azurerm_storage_account.quickwit.name
          QW_METASTORE_URI          = "azure://quickwit/${var.index_metastore_prefix}"
          QW_ENABLE_INGEST_V2       = "false"
          INDEX_METASTORE_PREFIX    = var.index_metastore_prefix
          PATENT_INDEX_ID           = var.patent_index_id
          INGESTION_CONFIG_JSON     = var.ingestion_config_json
          SERVING_APP_RESOURCE_ID   = azurerm_container_app.searcher[0].id
          SERVING_HEALTH_URL        = "https://${azurerm_container_app.searcher[0].ingress[0].fqdn}/readyz"
          CANDIDATE_CANARY_URL      = var.candidate_canary_url != "" ? var.candidate_canary_url : "https://${azurerm_container_app.searcher[0].ingress[0].fqdn}/api/v1/{index_id}/search"
          CANDIDATE_CANARY_QUERY    = var.candidate_canary_query
          CANDIDATE_PUBLICATION_UID = var.candidate_publication_uid
          CACHE_IMMUTABLE_JSON      = "true"
        }
        content {
          name  = env.key
          value = env.value
        }
      }
      env {
        name        = "USPTO_API_KEY"
        secret_name = "uspto-api-key"
      }
      env {
        name        = "QW_AZURE_STORAGE_ACCESS_KEY"
        secret_name = "quickwit-storage-key"
      }
      dynamic "env" {
        for_each = var.router_bearer_token == "" ? [] : [1]
        content {
          name        = "CANDIDATE_CANARY_TOKEN"
          secret_name = "candidate-canary-token"
        }
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.ingestion_image != ""
      error_message = "ingestion_image is required when compute_enabled=true."
    }
  }
  depends_on = [azurerm_role_assignment.blob, azurerm_role_assignment.acr_pull,
  azurerm_role_assignment.ingestion_key_vault, azurerm_role_assignment.serving_controller]
}
