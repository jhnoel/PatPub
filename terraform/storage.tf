# Storage resources for the steady-state PatPub deployment.

resource "azurerm_storage_account" "patpub" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.patpub.name
  location                 = azurerm_resource_group.patpub.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"

  min_tls_version = "TLS1_2"
  # Terraform's storage data-plane resources use the account key unless the
  # provider is explicitly configured for Azure AD storage authentication.
  # The key is never supplied to an application workload.
  shared_access_key_enabled       = true
  public_network_access_enabled   = true
  allow_nested_items_to_be_public = false

  blob_properties {
    versioning_enabled            = true
    change_feed_enabled           = true
    change_feed_retention_in_days = 30

    delete_retention_policy {
      days = 14
    }

    container_delete_retention_policy {
      days = 14
    }
  }

  tags = {
    purpose = "patpub"
  }
}

resource "azurerm_storage_container" "quickwit" {
  name                  = "quickwit"
  storage_account_id    = azurerm_storage_account.quickwit.id
  container_access_type = "private"
}

resource "azurerm_storage_account" "quickwit" {
  name                            = substr("${var.name_prefix}qw${local.suffix}", 0, 24)
  resource_group_name             = azurerm_resource_group.patpub.name
  location                        = azurerm_resource_group.patpub.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  min_tls_version                 = "TLS1_2"
  shared_access_key_enabled       = true
  public_network_access_enabled   = true
  allow_nested_items_to_be_public = false
  blob_properties {
    versioning_enabled = true
    delete_retention_policy { days = 14 }
    container_delete_retention_policy { days = 14 }
  }
}

resource "azurerm_storage_container" "downstream" {
  for_each              = local.downstream_containers
  name                  = each.value
  storage_account_id    = azurerm_storage_account.patpub.id
  container_access_type = "private"
}

resource "azurerm_storage_queue" "pipeline_events" {
  name               = var.orchestration_queue_name
  storage_account_id = azurerm_storage_account.patpub.id
}

resource "azurerm_storage_container" "terraform_state" {
  name                  = "terraform-state"
  storage_account_id    = azurerm_storage_account.patpub.id
  container_access_type = "private"
  lifecycle {
    prevent_destroy = true
  }
}
