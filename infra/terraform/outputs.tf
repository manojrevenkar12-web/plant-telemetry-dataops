output "kafka_bootstrap" {
  description = "Kafka endpoint of the Event Hubs namespace (for plant gateways and the processor)."
  value       = "${azurerm_eventhub_namespace.main.name}.servicebus.windows.net:9093"
}

output "lake_uri" {
  value = "abfss://lake@${azurerm_storage_account.lake.name}.dfs.core.windows.net"
}

output "api_url" {
  value = "https://${azurerm_container_app.api.ingress[0].fqdn}"
}

output "gateway_secret_name" {
  description = "Key Vault secret the plant gateways read (send-only Event Hubs connection)."
  value       = azurerm_key_vault_secret.gateway_kafka.name
}

output "pipeline_identity_client_id" {
  value = azurerm_user_assigned_identity.pipeline.client_id
}
