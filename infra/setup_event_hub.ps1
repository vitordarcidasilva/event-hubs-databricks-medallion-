# =============================================================================
# Cria infraestrutura do Event Hub para o projeto Retail Media
# Uso: .\infra\setup_event_hub.ps1
# =============================================================================

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Variáveis
# ---------------------------------------------------------------------------
$RESOURCE_GROUP    = "rg-retail-media"
$LOCATION          = "brazilsouth"
$NAMESPACE         = "evhns-retail-media-dev"
$EVENTHUB_NAME     = "retail-media-events"
$PARTITION_COUNT   = 4
$MESSAGE_RETENTION = 1
$SKU               = "Standard"
$CAPACITY          = 1
$PRODUCER_POLICY   = "producer-policy"
$CONSUMER_POLICY   = "consumer-policy"

# ---------------------------------------------------------------------------
# 1. Resource Group
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Resource Group '$RESOURCE_GROUP'..." -ForegroundColor Cyan
az group create `
  --name $RESOURCE_GROUP `
  --location $LOCATION `
  --output table

# ---------------------------------------------------------------------------
# 2. Namespace
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Namespace '$NAMESPACE' (SKU: $SKU)..." -ForegroundColor Cyan
az eventhubs namespace create `
  --resource-group $RESOURCE_GROUP `
  --name $NAMESPACE `
  --location $LOCATION `
  --sku $SKU `
  --capacity $CAPACITY `
  --output table

# ---------------------------------------------------------------------------
# 3. Event Hub
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Event Hub '$EVENTHUB_NAME' (partitions: $PARTITION_COUNT)..." -ForegroundColor Cyan
az eventhubs eventhub create `
  --resource-group $RESOURCE_GROUP `
  --namespace-name $NAMESPACE `
  --name $EVENTHUB_NAME `
  --partition-count $PARTITION_COUNT `
  --output table

# ---------------------------------------------------------------------------
# 4. Políticas de acesso
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando política Send (producer)..." -ForegroundColor Cyan
az eventhubs eventhub authorization-rule create `
  --resource-group $RESOURCE_GROUP `
  --namespace-name $NAMESPACE `
  --eventhub-name $EVENTHUB_NAME `
  --name $PRODUCER_POLICY `
  --rights Send `
  --output table

Write-Host "`n==> Criando política Listen (consumer / Databricks)..." -ForegroundColor Cyan
az eventhubs eventhub authorization-rule create `
  --resource-group $RESOURCE_GROUP `
  --namespace-name $NAMESPACE `
  --eventhub-name $EVENTHUB_NAME `
  --name $CONSUMER_POLICY `
  --rights Listen `
  --output table

# ---------------------------------------------------------------------------
# 5. Consumer Group para o Databricks
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Consumer Group 'databricks-cg'..." -ForegroundColor Cyan
az eventhubs eventhub consumer-group create `
  --resource-group $RESOURCE_GROUP `
  --namespace-name $NAMESPACE `
  --eventhub-name $EVENTHUB_NAME `
  --name "databricks-cg" `
  --output table

# ---------------------------------------------------------------------------
# 6. Exibir connection strings
# ---------------------------------------------------------------------------
Write-Host "`n=======================================================================" -ForegroundColor Green
Write-Host "INFRAESTRUTURA CRIADA COM SUCESSO" -ForegroundColor Green
Write-Host "=======================================================================`n" -ForegroundColor Green

Write-Host "--- CONNECTION STRING (producer - apenas Send) ---" -ForegroundColor Yellow
az eventhubs eventhub authorization-rule keys list `
  --resource-group $RESOURCE_GROUP `
  --namespace-name $NAMESPACE `
  --eventhub-name $EVENTHUB_NAME `
  --name $PRODUCER_POLICY `
  --query "primaryConnectionString" `
  --output tsv

Write-Host "`n--- CONNECTION STRING (consumer / Databricks - apenas Listen) ---" -ForegroundColor Yellow
az eventhubs eventhub authorization-rule keys list `
  --resource-group $RESOURCE_GROUP `
  --namespace-name $NAMESPACE `
  --eventhub-name $EVENTHUB_NAME `
  --name $CONSUMER_POLICY `
  --query "primaryConnectionString" `
  --output tsv

Write-Host "`nCopie as strings acima para o .env (producer) e Secrets do Databricks (consumer)." -ForegroundColor Cyan
