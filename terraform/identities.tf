# Identities resources for the steady-state PatPub deployment.

data "azurerm_client_config" "current" {}


resource "azurerm_user_assigned_identity" "searcher" {
  name                = "${var.name_prefix}-searcher-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}

resource "azurerm_user_assigned_identity" "renderer" {
  name                = "${var.name_prefix}-renderer-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}

resource "azurerm_user_assigned_identity" "fetch" {
  name                = "${var.name_prefix}-fetch-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}

resource "azurerm_user_assigned_identity" "metadata" {
  name                = "${var.name_prefix}-metadata-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}

resource "azurerm_user_assigned_identity" "indexer" {
  name                = "${var.name_prefix}-indexer-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}

resource "azurerm_user_assigned_identity" "orchestrator" {
  name                = "${var.name_prefix}-orchestrator-${local.suffix}"
  resource_group_name = azurerm_resource_group.patpub.name
  location            = azurerm_resource_group.patpub.location
}
