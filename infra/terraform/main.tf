data "azurerm_client_config" "current" {}

locals {
  prefix = "${var.name}-${var.environment}"
  flat   = "${var.name}${var.environment}"
  tags   = merge({ project = "plant-telemetry-dataops", environment = var.environment, data_classification = "confidential-ot" }, var.tags)
}

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.prefix}"
  location = var.location
  tags     = local.tags
}

resource "azurerm_log_analytics_workspace" "main" {
  name                = "log-${local.prefix}"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 30
  tags                = local.tags
}

# ---------------------------------------------------------------- identity used by the pipeline (no secrets for storage)

resource "azurerm_user_assigned_identity" "pipeline" {
  name                = "id-${local.prefix}-pipeline"
  location            = azurerm_resource_group.main.location
  resource_group_name = azurerm_resource_group.main.name
  tags                = local.tags
}

# ---------------------------------------------------------------- Event Hubs with the Kafka endpoint

resource "azurerm_eventhub_namespace" "main" {
  name                          = "evhns-${local.prefix}"
  location                      = azurerm_resource_group.main.location
  resource_group_name           = azurerm_resource_group.main.name
  sku                           = "Standard" # Standard and above expose the Kafka endpoint
  capacity                      = 1
  auto_inflate_enabled          = true
  maximum_throughput_units      = 4
  minimum_tls_version           = "1.2"
  public_network_access_enabled = var.public_network_access
  tags                          = local.tags
}

resource "azurerm_eventhub" "telemetry" {
  name              = "plant.telemetry.v2"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = var.telemetry_partitions
  message_retention = 7 # days: the replay window if the lake must be rebuilt from the bus
}

resource "azurerm_eventhub" "dlq" {
  name              = "plant.telemetry.dlq"
  namespace_id      = azurerm_eventhub_namespace.main.id
  partition_count   = 2
  message_retention = 7
}

resource "azurerm_eventhub_consumer_group" "processor" {
  for_each            = { telemetry = azurerm_eventhub.telemetry.name, dlq = azurerm_eventhub.dlq.name }
  name                = "stream-processor"
  namespace_name      = azurerm_eventhub_namespace.main.name
  eventhub_name       = each.value
  resource_group_name = azurerm_resource_group.main.name
}

# Least privilege: plant gateways can only send, the processor can only listen.
resource "azurerm_eventhub_namespace_authorization_rule" "gateway_send" {
  name                = "plant-gateways-send"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  send                = true
  listen              = false
  manage              = false
}

resource "azurerm_eventhub_namespace_authorization_rule" "processor_listen" {
  name                = "stream-processor-listen"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  send                = false
  listen              = true
  manage              = false
}

# ---------------------------------------------------------------- ADLS Gen2 lake (OneLake shortcuts can point here)

resource "azurerm_storage_account" "lake" {
  name                              = substr("st${local.flat}lake", 0, 24)
  location                          = azurerm_resource_group.main.location
  resource_group_name               = azurerm_resource_group.main.name
  account_tier                      = "Standard"
  account_replication_type          = "ZRS"
  account_kind                      = "StorageV2"
  is_hns_enabled                    = true # hierarchical namespace = ADLS Gen2
  min_tls_version                   = "TLS1_2"
  https_traffic_only_enabled        = true
  shared_access_key_enabled         = false # Entra ID only: no account keys to leak
  allow_nested_items_to_be_public   = false
  infrastructure_encryption_enabled = true # double encryption at rest
  public_network_access_enabled     = var.public_network_access
  tags                              = local.tags

  blob_properties {
    delete_retention_policy {
      days = 14
    }
    container_delete_retention_policy {
      days = 14
    }
  }
}

resource "azurerm_storage_container" "lake" {
  name                  = "lake" # bronze/, silver/, gold/ and ops/ are folders inside it
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_role_assignment" "pipeline_lake" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.pipeline.principal_id
}

# ---------------------------------------------------------------- Key Vault for the few secrets there are

resource "azurerm_key_vault" "main" {
  name                          = substr("kv-${local.prefix}", 0, 24)
  location                      = azurerm_resource_group.main.location
  resource_group_name           = azurerm_resource_group.main.name
  tenant_id                     = data.azurerm_client_config.current.tenant_id
  sku_name                      = "standard"
  enable_rbac_authorization     = true
  purge_protection_enabled      = true
  soft_delete_retention_days    = 30
  public_network_access_enabled = var.public_network_access
  tags                          = local.tags
}

resource "azurerm_role_assignment" "deployer_secrets" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

resource "azurerm_role_assignment" "pipeline_secrets" {
  scope                = azurerm_key_vault.main.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_user_assigned_identity.pipeline.principal_id
}

resource "azurerm_key_vault_secret" "processor_kafka" {
  name         = "eventhubs-processor-connection"
  value        = azurerm_eventhub_namespace_authorization_rule.processor_listen.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.deployer_secrets]
}

resource "azurerm_key_vault_secret" "gateway_kafka" {
  name         = "eventhubs-gateway-connection"
  value        = azurerm_eventhub_namespace_authorization_rule.gateway_send.primary_connection_string
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.deployer_secrets]
}

resource "azurerm_key_vault_secret" "api_keys" {
  name         = "api-keys"
  value        = var.api_keys_yaml
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.deployer_secrets]
}

resource "azurerm_key_vault_secret" "pseudonym_key" {
  name         = "operator-pseudonym-key"
  value        = random_password.pseudonym.result
  key_vault_id = azurerm_key_vault.main.id
  depends_on   = [azurerm_role_assignment.deployer_secrets]
}

resource "random_password" "pseudonym" {
  length  = 48
  special = false
}

# ---------------------------------------------------------------- Container Apps: stream processor + API

resource "azurerm_container_app_environment" "main" {
  name                       = "cae-${local.prefix}"
  location                   = azurerm_resource_group.main.location
  resource_group_name        = azurerm_resource_group.main.name
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  tags                       = local.tags
}

locals {
  common_env = {
    TELEMETRY_ENV              = var.environment == "prod" ? "production" : var.environment
    LAKE_URI                   = "abfss://lake@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
    AZURE_STORAGE_ACCOUNT_NAME = azurerm_storage_account.lake.name
    AZURE_CLIENT_ID            = azurerm_user_assigned_identity.pipeline.client_id
    OPENLINEAGE_URL            = var.openlineage_url
    KAFKA_BOOTSTRAP            = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
    KAFKA_SECURITY_PROTOCOL    = "SASL_SSL"
    KAFKA_SASL_MECHANISM       = "PLAIN"
    KAFKA_SASL_USERNAME        = "$ConnectionString"
    GX_ANALYTICS_ENABLED       = "False"
  }
}

resource "azurerm_container_app" "processor" {
  name                         = "ca-${local.prefix}-processor"
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = azurerm_resource_group.main.name
  revision_mode                = "Single"
  tags                         = local.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.pipeline.id]
  }

  secret {
    name                = "kafka-password"
    key_vault_secret_id = azurerm_key_vault_secret.processor_kafka.versionless_id
    identity            = azurerm_user_assigned_identity.pipeline.id
  }

  template {
    # One replica per consumer group member; scale out up to the partition count.
    min_replicas = 1
    max_replicas = var.telemetry_partitions

    container {
      name    = "processor"
      image   = var.image
      cpu     = 1.0
      memory  = "2Gi"
      command = ["telemetry", "process", "--group", "stream-processor"]

      dynamic "env" {
        for_each = { for k, v in local.common_env : k => v if v != "" }
        content {
          name  = env.key
          value = env.value
        }
      }
      env {
        name        = "KAFKA_SASL_PASSWORD"
        secret_name = "kafka-password"
      }
    }
  }

  depends_on = [azurerm_role_assignment.pipeline_secrets, azurerm_role_assignment.pipeline_lake]
}

resource "azurerm_container_app" "api" {
  name                         = "ca-${local.prefix}-api"
  container_app_environment_id = azurerm_container_app_environment.main.id
  resource_group_name          = azurerm_resource_group.main.name
  revision_mode                = "Single"
  tags                         = local.tags

  identity {
    type         = "UserAssigned"
    identity_ids = [azurerm_user_assigned_identity.pipeline.id]
  }

  secret {
    name                = "api-keys"
    key_vault_secret_id = azurerm_key_vault_secret.api_keys.versionless_id
    identity            = azurerm_user_assigned_identity.pipeline.id
  }

  ingress {
    external_enabled = true
    target_port      = 8000
    transport        = "auto"
    traffic_weight {
      percentage      = 100
      latest_revision = true
    }
  }

  template {
    min_replicas = 1
    max_replicas = 3

    volume {
      name         = "secrets"
      storage_type = "Secret"
    }

    container {
      name    = "api"
      image   = var.image
      cpu     = 0.5
      memory  = "1Gi"
      command = ["telemetry", "serve", "--host", "0.0.0.0", "--port", "8000"]

      dynamic "env" {
        for_each = { for k, v in local.common_env : k => v if v != "" }
        content {
          name  = env.key
          value = env.value
        }
      }
      env {
        name  = "API_KEYS_FILE"
        value = "/mnt/secrets/api-keys"
      }

      volume_mounts {
        name = "secrets"
        path = "/mnt/secrets"
      }

      liveness_probe {
        transport = "HTTP"
        port      = 8000
        path      = "/healthz"
      }
    }
  }

  depends_on = [azurerm_role_assignment.pipeline_secrets, azurerm_role_assignment.pipeline_lake]
}

# ---------------------------------------------------------------- diagnostics to Log Analytics

resource "azurerm_monitor_diagnostic_setting" "eventhubs" {
  name                       = "to-log-analytics"
  target_resource_id         = azurerm_eventhub_namespace.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  enabled_log {
    category_group = "allLogs"
  }
}

resource "azurerm_monitor_diagnostic_setting" "keyvault" {
  name                       = "to-log-analytics"
  target_resource_id         = azurerm_key_vault.main.id
  log_analytics_workspace_id = azurerm_log_analytics_workspace.main.id
  enabled_log {
    category_group = "audit"
  }
}
