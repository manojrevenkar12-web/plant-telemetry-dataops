terraform {
  required_version = ">= 1.6"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
  # Remote state in a separate, locked-down storage account:
  # backend "azurerm" {}  (configured with -backend-config in the deployment pipeline)
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy = false
    }
  }
  # Shared-key access is disabled on the lake account, so the provider talks to storage with Entra ID.
  storage_use_azuread = true
}
