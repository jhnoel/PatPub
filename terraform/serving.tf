# Serving resources for the steady-state PatPub deployment.

resource "azurerm_container_app" "searcher" {
  count                        = var.compute_enabled ? 1 : 0
  name                         = local.app_name
  container_app_environment_id = azurerm_container_app_environment.patpub.id
  resource_group_name          = azurerm_resource_group.patpub.name
  revision_mode                = "Single"

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.searcher.id]
  }

  secret {
    name  = "azure-storage-access-key"
    value = azurerm_storage_account.quickwit.primary_access_key
  }

  dynamic "secret" {
    for_each = var.router_bearer_token == "" ? [] : [var.router_bearer_token]
    content {
      name  = "router-bearer-token"
      value = secret.value
    }
  }

  dynamic "secret" {
    for_each = var.patpub_mcp_key == "" ? [] : [var.patpub_mcp_key]
    content {
      name  = "patpub-mcp-key"
      value = secret.value
    }
  }

  registry {
    server   = azurerm_container_registry.patpub.login_server
    identity = azurerm_user_assigned_identity.searcher.id
  }

  ingress {
    external_enabled = var.external_ingress_enabled
    target_port      = 8080
    transport        = "auto"

    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  template {
    # Scale-to-zero is a deployment requirement. Local caches are disposable;
    # durable index, source and release objects remain in Blob Storage.
    min_replicas                = 0
    max_replicas                = 1
    cooldown_period_in_seconds  = 300
    polling_interval_in_seconds = 30

    container {
      name   = "quickwit"
      image  = local.image
      cpu    = var.container_cpu
      memory = var.container_memory

      startup_probe {
        transport               = "HTTP"
        port                    = 8080
        path                    = "/readyz"
        interval_seconds        = 5
        timeout                 = 6
        failure_count_threshold = 120
      }

      readiness_probe {
        transport               = "HTTP"
        port                    = 8080
        path                    = "/readyz"
        interval_seconds        = 10
        timeout                 = 6
        failure_count_threshold = 3
      }

      liveness_probe {
        transport               = "HTTP"
        port                    = 8080
        path                    = "/readyz"
        interval_seconds        = 30
        timeout                 = 6
        failure_count_threshold = 3
      }

      env {
        name  = "AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.patpub.name
      }

      env {
        name  = "AZURE_CLIENT_ID"
        value = azurerm_user_assigned_identity.searcher.client_id
      }

      env {
        name  = "QW_AZURE_STORAGE_ACCOUNT"
        value = azurerm_storage_account.quickwit.name
      }

      env {
        name        = "QW_AZURE_STORAGE_ACCESS_KEY"
        secret_name = "azure-storage-access-key"
      }

      env {
        name  = "QW_NODE_ID"
        value = "aca-searcher"
      }

      env {
        name  = "QW_INDEX_ROOT_URI"
        value = "azure://${azurerm_storage_container.quickwit.name}/indexes"
      }

      env {
        name  = "QW_DATA_DIR"
        value = "/tmp/qwdata"
      }

      env {
        name  = "QW_METASTORE_URI"
        value = "azure://quickwit/${var.index_metastore_prefix}"
      }

      env {
        name  = "QUICKWIT_URL"
        value = "http://127.0.0.1:7280"
      }

      env {
        name  = "SOURCE_CONTAINER"
        value = "source"
      }

      dynamic "env" {
        for_each = var.router_bearer_token == "" ? [] : [1]
        content {
          name        = "ROUTER_BEARER_TOKEN"
          secret_name = "router-bearer-token"
        }
      }

      dynamic "env" {
        for_each = var.patpub_mcp_key == "" ? [] : [1]
        content {
          name        = "PATPUB_MCP_KEY"
          secret_name = "patpub-mcp-key"
        }
      }

      env {
        name  = "PATPUB_MCP_ALLOWED_ORIGINS"
        value = var.patpub_mcp_allowed_origins
      }
    }

    http_scale_rule {
      name                = "http"
      concurrent_requests = var.scale_concurrent_requests
    }
  }

  tags = {
    purpose = "patpub"
  }

  lifecycle {
    precondition {
      condition     = var.container_image != ""
      error_message = "container_image must reference the combined serving image when compute_enabled=true."
    }
    precondition {
      condition     = !var.external_ingress_enabled || var.router_bearer_token != ""
      error_message = "Public serving ingress requires router_bearer_token."
    }
    precondition {
      condition     = !var.external_ingress_enabled || var.patpub_mcp_key != ""
      error_message = "Public serving ingress requires patpub_mcp_key for the MCP route."
    }
  }
}
