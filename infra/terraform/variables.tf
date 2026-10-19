variable "name" {
  description = "Short name used in every resource name (lowercase letters and digits)."
  type        = string
  default     = "planttelem"
  validation {
    condition     = can(regex("^[a-z0-9]{3,12}$", var.name))
    error_message = "Use 3-12 lowercase letters or digits."
  }
}

variable "environment" {
  description = "dev, test or prod."
  type        = string
  default     = "dev"
}

variable "location" {
  description = "EU region, so plant data stays in the EU (GDPR, data sovereignty)."
  type        = string
  default     = "germanywestcentral"
  validation {
    condition     = contains(["germanywestcentral", "westeurope", "northeurope", "francecentral", "swedencentral"], var.location)
    error_message = "Plant data must stay in an EU region."
  }
}

variable "image" {
  description = "Container image for the processor and the API."
  type        = string
  default     = "ghcr.io/manojrevenkar12-web/plant-telemetry-dataops:latest"
}

variable "telemetry_partitions" {
  description = "Event Hub partitions for telemetry; sets the processor's maximum parallelism."
  type        = number
  default     = 6
}

variable "api_keys_yaml" {
  description = "API key file (hashed keys, roles, plant scopes). Stored in Key Vault, mounted into the API."
  type        = string
  sensitive   = true
}

variable "openlineage_url" {
  description = "OpenLineage endpoint (Marquez, or a collector that forwards to Microsoft Purview). Empty = file only."
  type        = string
  default     = ""
}

variable "public_network_access" {
  description = "Allow public endpoints. Set false in prod and add private endpoints for Event Hubs, storage and Key Vault."
  type        = bool
  default     = true
}

variable "tags" {
  type    = map(string)
  default = {}
}
