# Ingestion resources for the steady-state PatPub deployment.

resource "azurerm_container_app_job" "fetch" {
  count                        = var.compute_enabled ? 1 : 0
  name                         = "${var.name_prefix}-fetch"
  resource_group_name          = azurerm_resource_group.patpub.name
  location                     = azurerm_resource_group.patpub.location
  container_app_environment_id = azurerm_container_app_environment.patpub.id
  replica_retry_limit          = 0
  replica_timeout_in_seconds   = 3600
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.fetch.id]
  }
  registry {
    server   = azurerm_container_registry.patpub.login_server
    identity = azurerm_user_assigned_identity.fetch.id
  }
  secret {
    name                = "uspto-api-key"
    key_vault_secret_id = local.uspto_secret_uri
    identity            = azurerm_user_assigned_identity.fetch.id
  }
  dynamic "schedule_trigger_config" {
    for_each = var.ingestion_enabled ? [1] : []
    content {
      cron_expression          = "0 12 * * *"
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
      name   = "fetch"
      image  = var.fetch_image
      cpu    = 2
      memory = "4Gi"
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.patpub.name
      }
      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.fetch.client_id
      }
      env {
        name  = "QW_AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.quickwit.name
      }
      env {
        name  = "PIPELINE_EVENT_QUEUE"
        value = azurerm_storage_queue.pipeline_events.name
      }
      env {
        name  = "MAX_ARCHIVES_PER_RUN"
        value = "2"
      }
      env {
        name        = "USPTO_API_KEY"
        secret_name = "uspto-api-key"
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.fetch_image != ""
      error_message = "fetch_image is required when compute_enabled=true."
    }
  }
  depends_on = [azurerm_role_assignment.blob, azurerm_role_assignment.acr_pull, azurerm_role_assignment.fetch_queue, azurerm_role_assignment.fetch_key_vault]
}

resource "azurerm_container_app_job" "renderer" {
  count                        = var.compute_enabled ? 1 : 0
  name                         = "${var.name_prefix}-renderer"
  resource_group_name          = azurerm_resource_group.patpub.name
  location                     = azurerm_resource_group.patpub.location
  container_app_environment_id = azurerm_container_app_environment.patpub.id
  replica_retry_limit          = 0
  replica_timeout_in_seconds   = 7200
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.renderer.id]
  }
  registry {
    server   = azurerm_container_registry.patpub.login_server
    identity = azurerm_user_assigned_identity.renderer.id
  }
  manual_trigger_config {
    parallelism              = 1
    replica_completion_count = 1
  }
  template {
    container {
      name   = "renderer"
      image  = var.renderer_image
      cpu    = 2
      memory = "4Gi"
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.patpub.name
      }
      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.renderer.client_id
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.renderer_image != ""
      error_message = "renderer_image is required when compute_enabled=true."
    }
  }
  depends_on = [azurerm_role_assignment.blob, azurerm_role_assignment.acr_pull]
}

resource "azurerm_container_app_job" "metadata" {
  count                        = var.compute_enabled ? 1 : 0
  name                         = "${var.name_prefix}-metadata"
  resource_group_name          = azurerm_resource_group.patpub.name
  location                     = azurerm_resource_group.patpub.location
  container_app_environment_id = azurerm_container_app_environment.patpub.id
  replica_retry_limit          = 0
  replica_timeout_in_seconds   = 7200
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.metadata.id]
  }
  registry {
    server   = azurerm_container_registry.patpub.login_server
    identity = azurerm_user_assigned_identity.metadata.id
  }
  manual_trigger_config {
    parallelism              = 1
    replica_completion_count = 1
  }
  template {
    container {
      name   = "metadata"
      image  = var.metadata_image
      cpu    = 2
      memory = "4Gi"
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.patpub.name
      }
      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.metadata.client_id
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.metadata_image != ""
      error_message = "metadata_image is required when compute_enabled=true."
    }
  }
  depends_on = [azurerm_role_assignment.blob, azurerm_role_assignment.acr_pull]
}

resource "azurerm_container_app_job" "indexer" {
  count                        = var.compute_enabled ? 1 : 0
  name                         = "${var.name_prefix}-indexer"
  resource_group_name          = azurerm_resource_group.patpub.name
  location                     = azurerm_resource_group.patpub.location
  container_app_environment_id = azurerm_container_app_environment.patpub.id
  replica_retry_limit          = 0
  replica_timeout_in_seconds   = 14400
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.indexer.id]
  }
  registry {
    server   = azurerm_container_registry.patpub.login_server
    identity = azurerm_user_assigned_identity.indexer.id
  }
  secret {
    name  = "quickwit-storage-key"
    value = azurerm_storage_account.quickwit.primary_access_key
  }
  manual_trigger_config {
    parallelism              = 1
    replica_completion_count = 1
  }
  template {
    container {
      name   = "indexer"
      image  = var.indexer_image
      cpu    = var.indexer_cpu
      memory = var.indexer_memory
      env {
        name  = "REUSE_VERIFIED_ARTIFACT_HASHES"
        value = "true"
      }
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.patpub.name
      }
      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.indexer.client_id
      }
      env {
        name  = "QW_AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.quickwit.name
      }
      env {
        name        = "QW_AZURE_STORAGE_ACCESS_KEY"
        secret_name = "quickwit-storage-key"
      }
      env {
        name  = "QW_METASTORE_URI"
        value = "azure://quickwit/${var.index_metastore_prefix}"
      }
      env {
        name  = "QW_ENABLE_INGEST_V2"
        value = "false"
      }
      env {
        name  = "INDEX_METASTORE_PREFIX"
        value = var.index_metastore_prefix
      }
      env {
        name  = "PATENT_INDEX_ID"
        value = var.patent_index_id
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.indexer_image != ""
      error_message = "indexer_image is required when compute_enabled=true."
    }
  }
  depends_on = [azurerm_role_assignment.blob, azurerm_role_assignment.acr_pull]
}

resource "azurerm_container_app_job" "orchestrator" {
  count                        = var.compute_enabled ? 1 : 0
  name                         = "${var.name_prefix}-orchestrator"
  resource_group_name          = azurerm_resource_group.patpub.name
  location                     = azurerm_resource_group.patpub.location
  container_app_environment_id = azurerm_container_app_environment.patpub.id
  replica_retry_limit          = 0
  replica_timeout_in_seconds   = 18000
  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.orchestrator.id]
  }
  registry {
    server   = azurerm_container_registry.patpub.login_server
    identity = azurerm_user_assigned_identity.orchestrator.id
  }
  dynamic "secret" {
    for_each = var.router_bearer_token == "" ? [] : [var.router_bearer_token]
    content {
      name  = "candidate-canary-token"
      value = secret.value
    }
  }
  dynamic "event_trigger_config" {
    for_each = var.ingestion_enabled ? [1] : []
    content {
      parallelism              = 1
      replica_completion_count = 1
      scale {
        max_executions              = 1
        min_executions              = 0
        polling_interval_in_seconds = 30
        rules {
          name             = "pipeline-events"
          custom_rule_type = "azure-queue"
          identity_id      = azurerm_user_assigned_identity.orchestrator.id
          metadata         = { accountName = azurerm_storage_account.patpub.name, queueName = azurerm_storage_queue.pipeline_events.name, queueLength = "1" }
        }
      }
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
      name   = "orchestrator"
      image  = var.orchestration_image
      cpu    = 2
      memory = "4Gi"
      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.patpub.name
      }
      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.orchestrator.client_id
      }
      env {
        name  = "PIPELINE_EVENT_QUEUE"
        value = azurerm_storage_queue.pipeline_events.name
      }
      env {
        name  = "WORKER_RESULT_TIMEOUT_SECONDS"
        value = "14400"
      }
      env {
        name  = "STOP_SERVING_AFTER_RELEASE"
        value = "false"
      }
      env {
        name  = "ARTIFACT_VERSION_CACHE_KEY"
        value = "migration/v1/artifact-version-capture.json"
      }
      env {
        name  = "BATCH_ETAG_VALIDATION"
        value = "true"
      }
      env {
        name  = "CACHE_IMMUTABLE_JSON"
        value = "true"
      }
      env {
        name  = "RENDER_JOB_RESOURCE_ID"
        value = azurerm_container_app_job.renderer[0].id
      }
      env {
        name  = "QW_AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.quickwit.name
      }
      env {
        name  = "METADATA_JOB_RESOURCE_ID"
        value = azurerm_container_app_job.metadata[0].id
      }
      env {
        name  = "INDEX_JOB_RESOURCE_ID"
        value = azurerm_container_app_job.indexer[0].id
      }
      env {
        name  = "SERVING_APP_RESOURCE_ID"
        value = azurerm_container_app.searcher[0].id
      }
      env {
        name  = "SERVING_HEALTH_URL"
        value = "https://${azurerm_container_app.searcher[0].ingress[0].fqdn}/readyz"
      }
      env {
        name  = "PATENT_INDEX_ID"
        value = var.patent_index_id
      }
      env {
        name  = "CANDIDATE_CANARY_URL"
        value = var.candidate_canary_url != "" ? var.candidate_canary_url : "https://${azurerm_container_app.searcher[0].ingress[0].fqdn}/api/v1/{index_id}/search"
      }
      dynamic "env" {
        for_each = var.router_bearer_token == "" ? [] : [1]
        content {
          name        = "CANDIDATE_CANARY_TOKEN"
          secret_name = "candidate-canary-token"
        }
      }
      env {
        name  = "CANDIDATE_CANARY_QUERY"
        value = var.candidate_canary_query
      }
      env {
        name  = "CANDIDATE_PUBLICATION_UID"
        value = var.candidate_publication_uid
      }
    }
  }
  lifecycle {
    precondition {
      condition     = var.orchestration_image != ""
      error_message = "orchestration_image is required when compute_enabled=true."
    }
  }
  depends_on = [azurerm_role_assignment.orchestrator_queue, azurerm_role_assignment.job_dispatcher, azurerm_role_assignment.serving_controller]
}
