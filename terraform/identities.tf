# Identities resources for the steady-state PatPub deployment.

data "azurerm_client_config" "current" {}


resource "azurerm_user_assigned_identity" "searcher" {
  name                = "${var.name_prefix}-searcher-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}

resource "azurerm_user_assigned_identity" "ingestion" {
  # Keep the existing controller identity when upgrading an installed deployment.
  name                = "${var.name_prefix}-orchestrator-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}

moved {
  from = azurerm_user_assigned_identity.orchestrator
  to   = azurerm_user_assigned_identity.ingestion
}
