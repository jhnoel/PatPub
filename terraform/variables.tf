variable "subscription_id" {
  description = "Azure subscription used for this PatPub deployment."
  type        = string
}

variable "location" {
  description = "Azure region for PatPub resources."
  type        = string
  default     = "eastus2"
}

variable "resource_group_name" {
  description = "Dedicated resource group owned by this PatPub deployment."
  type        = string
  default     = "patpub-production"
}

variable "name_prefix" {
  description = "Lowercase alphanumeric prefix for Azure resources."
  type        = string
  default     = "patpub"
  validation {
    condition     = can(regex("^[a-z0-9]{3,13}$", var.name_prefix))
    error_message = "name_prefix must contain 3 to 13 lowercase letters or digits."
  }
}

variable "resource_suffix" {
  description = "Unique lowercase alphanumeric suffix for globally named Azure resources. Choose a new value for a new deployment."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9]{5,10}$", var.resource_suffix))
    error_message = "resource_suffix must contain 5 to 10 lowercase letters or digits."
  }
}

variable "compute_enabled" {
  description = "Create Container Apps and Jobs when true. False retains storage and identities while pausing compute."
  type        = bool
  default     = false
}

variable "ingestion_enabled" {
  description = "Run scheduled fetch and queue-triggered orchestration. False creates both jobs with manual triggers. Changing this replaces those jobs; switch only while they are idle."
  type        = bool
  default     = false
}

variable "monthly_budget_usd" {
  description = "Advisory monthly budget for the resource group. Azure budgets do not stop resource usage."
  type        = number
  default     = 20
  validation {
    condition     = var.monthly_budget_usd > 0
    error_message = "monthly_budget_usd must be positive."
  }
}

variable "fetch_image" {
  description = "Immutable image digest reference for the scheduled USPTO publication fetch job."
  type        = string
  default     = ""
}

variable "renderer_image" {
  description = "Immutable image digest reference for the publication renderer."
  type        = string
  default     = ""
}

variable "metadata_image" {
  description = "Immutable image digest reference for the publication metadata projection job."
  type        = string
  default     = ""
}

variable "indexer_image" {
  description = "Immutable image digest reference for the Quickwit indexer."
  type        = string
  default     = ""
}

variable "indexer_cpu" {
  type    = number
  default = 2
}

variable "indexer_memory" {
  type    = string
  default = "4Gi"
}

variable "orchestration_image" {
  description = "Immutable image digest reference for the release orchestration job."
  type        = string
  default     = ""
}

variable "orchestration_queue_name" {
  description = "Azure Queue that carries immutable pipeline event references."
  type        = string
  default     = "pipeline-events"
}

variable "patent_index_id" {
  description = "Durable publication index used by ingestion and serving releases."
  type        = string
  default     = "patents"
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9_-]{2,100}$", var.patent_index_id))
    error_message = "patent_index_id must be a valid Quickwit index ID."
  }
}

variable "index_metastore_prefix" {
  description = "Durable Quickwit metastore namespace in the project's Quickwit container."
  type        = string
  default     = "metastores/production-v1"
  validation {
    condition     = can(regex("^metastores/[a-zA-Z0-9_-]+$", var.index_metastore_prefix))
    error_message = "index_metastore_prefix must be metastores/<namespace>."
  }
}

variable "container_image" {
  description = "Immutable image digest for the combined Quickwit and PatPub search-router service."
  type        = string
  default     = ""
}

variable "container_cpu" {
  type    = number
  default = 0.5
}

variable "container_memory" {
  type    = string
  default = "1Gi"
}

variable "scale_concurrent_requests" {
  type    = number
  default = 1
}

variable "external_ingress_enabled" {
  description = "Expose the search service publicly. Keep false unless protected by an authenticated gateway."
  type        = bool
  default     = false
}

variable "uspto_api_key_secret_name" {
  description = "Name of the USPTO API key secret restored into this project's Key Vault outside Terraform."
  type        = string
  default     = "uspto-odp-api-key"
}

variable "candidate_canary_url" {
  description = "Optional candidate search endpoint template used by the release canary."
  type        = string
  default     = ""
}

variable "candidate_canary_query" {
  description = "JSON query sent to the candidate search endpoint."
  type        = string
  default     = ""
}

variable "candidate_publication_uid" {
  description = "Publication UID expected in the candidate canary response."
  type        = string
  default     = ""
}

variable "router_bearer_token" {
  description = "Optional service-to-service bearer token for the router."
  type        = string
  sensitive   = true
  default     = ""
}

variable "patpub_mcp_key" {
  description = "Function-key-style shared key required by the PatPub MCP route."
  type        = string
  sensitive   = true
  default     = ""
}

variable "patpub_mcp_allowed_origins" {
  description = "Optional comma-separated browser Origins allowed to call the PatPub MCP route."
  type        = string
  default     = ""
}
