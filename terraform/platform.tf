# Platform resources for the steady-state PatPub deployment.

locals {
  suffix               = var.resource_suffix
  storage_account_name = substr("${var.name_prefix}${local.suffix}", 0, 24)
  registry_name        = substr("${var.name_prefix}acr${local.suffix}", 0, 50)
  vault_name           = substr("${var.name_prefix}kv${local.suffix}", 0, 24)
  uspto_secret_uri     = "${trimsuffix(azurerm_key_vault.uspto.vault_uri, "/")}/secrets/${var.uspto_api_key_secret_name}"
  app_name             = "${var.name_prefix}-searcher"
  image                = var.container_image
}

resource "azurerm_resource_group" "patpub" {
  name     = var.resource_group_name
  location = var.location

  tags = {
    purpose = "patpub"
    managed = "terraform"
  }
}

resource "azurerm_key_vault" "uspto" {
  name                       = local.vault_name
  resource_group_name        = azurerm_resource_group.patpub.name
  location                   = azurerm_resource_group.patpub.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  rbac_authorization_enabled = true
  purge_protection_enabled   = true
  soft_delete_retention_days = 90

  tags = {
    purpose = "patpub"
  }
}

resource "azurerm_consumption_budget_resource_group" "monthly" {
  name              = "${var.name_prefix}-monthly-budget"
  resource_group_id = azurerm_resource_group.patpub.id
  amount            = var.monthly_budget_usd
  time_grain        = "Monthly"
  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00'Z'", timestamp())
  }
  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      enabled        = true
      threshold      = notification.value
      operator       = "GreaterThanOrEqualTo"
      threshold_type = "Actual"
      contact_roles  = ["Owner"]
    }
  }
  lifecycle {
    ignore_changes = [time_period]
  }
}

resource "azurerm_container_registry" "patpub" {
  name                = local.registry_name
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
  sku                 = "Basic"
  # Pulls use the Container App's system identity.  The registry admin
  # account is deliberately disabled so an image credential cannot leak into
  # a job environment or Terraform state.
  admin_enabled = false

  tags = {
    purpose = "patpub"
  }
}

resource "azurerm_log_analytics_workspace" "patpub" {
  name                = "${var.name_prefix}-logs-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
  sku                 = "PerGB2018"
  retention_in_days   = 30
  daily_quota_gb      = 0.1

  tags = {
    purpose = "patpub"
  }
}

resource "azurerm_container_app_environment" "patpub" {
  name                       = "${var.name_prefix}-env"
  resource_group_name        = azurerm_resource_group.patpub.name
  location                   = azurerm_resource_group.patpub.location
  log_analytics_workspace_id = azurerm_log_analytics_workspace.patpub.id

  tags = {
    purpose = "patpub"
  }
}
