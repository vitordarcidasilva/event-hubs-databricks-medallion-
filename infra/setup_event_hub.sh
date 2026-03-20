#!/bin/bash
# =============================================================================
# Cria infraestrutura do Event Hub para o projeto Retail Media
# Pré-requisito: az login já executado
# Uso: bash infra/setup_event_hub.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Variáveis — ajuste conforme necessário
# ---------------------------------------------------------------------------
RESOURCE_GROUP="rg-retail-media"
LOCATION="brazilsouth"
NAMESPACE="evhns-retail-media-dev"
EVENTHUB_NAME="retail-media-events"
PARTITION_COUNT=4
MESSAGE_RETENTION=1          # dias
SKU="Standard"               # Standard obrigatório para Consumer Groups
CAPACITY=1                   # Throughput Units

PRODUCER_POLICY="producer-policy"
CONSUMER_POLICY="consumer-policy"

# ---------------------------------------------------------------------------
# 1. Login (pula se já autenticado)
# ---------------------------------------------------------------------------
echo "==> Verificando autenticação..."
az account show > /dev/null 2>&1 || az login

echo "==> Subscription ativa:"
az account show --query "{name:name, id:id}" -o table

# ---------------------------------------------------------------------------
# 2. Resource Group
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando Resource Group '$RESOURCE_GROUP' em '$LOCATION'..."
az group create \
  --name "$RESOURCE_GROUP" \
  --location "$LOCATION" \
  --output table

# ---------------------------------------------------------------------------
# 3. Event Hub Namespace
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando Namespace '$NAMESPACE' (SKU: $SKU)..."
az eventhubs namespace create \
  --resource-group "$RESOURCE_GROUP" \
  --name "$NAMESPACE" \
  --location "$LOCATION" \
  --sku "$SKU" \
  --capacity "$CAPACITY" \
  --output table

# ---------------------------------------------------------------------------
# 4. Event Hub
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando Event Hub '$EVENTHUB_NAME' (partitions: $PARTITION_COUNT)..."
az eventhubs eventhub create \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --name "$EVENTHUB_NAME" \
  --partition-count "$PARTITION_COUNT" \
  --message-retention "$MESSAGE_RETENTION" \
  --output table

# ---------------------------------------------------------------------------
# 5. Políticas de acesso com escopo mínimo (princípio do menor privilégio)
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando política de Send (producer)..."
az eventhubs eventhub authorization-rule create \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --eventhub-name "$EVENTHUB_NAME" \
  --name "$PRODUCER_POLICY" \
  --rights Send \
  --output table

echo ""
echo "==> Criando política de Listen (consumer / Databricks)..."
az eventhubs eventhub authorization-rule create \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --eventhub-name "$EVENTHUB_NAME" \
  --name "$CONSUMER_POLICY" \
  --rights Listen \
  --output table

# ---------------------------------------------------------------------------
# 6. Consumer Group dedicado para o Databricks
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando Consumer Group 'databricks-cg'..."
az eventhubs eventhub consumer-group create \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --eventhub-name "$EVENTHUB_NAME" \
  --name "databricks-cg" \
  --output table

# ---------------------------------------------------------------------------
# 7. Exibir connection strings
# ---------------------------------------------------------------------------
echo ""
echo "======================================================================="
echo "INFRAESTRUTURA CRIADA COM SUCESSO"
echo "======================================================================="

echo ""
echo "--- CONNECTION STRING (producer — apenas Send) ---"
az eventhubs eventhub authorization-rule keys list \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --eventhub-name "$EVENTHUB_NAME" \
  --name "$PRODUCER_POLICY" \
  --query "primaryConnectionString" \
  -o tsv

echo ""
echo "--- CONNECTION STRING (consumer / Databricks — apenas Listen) ---"
az eventhubs eventhub authorization-rule keys list \
  --resource-group "$RESOURCE_GROUP" \
  --namespace-name "$NAMESPACE" \
  --eventhub-name "$EVENTHUB_NAME" \
  --name "$CONSUMER_POLICY" \
  --query "primaryConnectionString" \
  -o tsv

echo ""
echo "Copie as strings acima para o .env (producer) e para os Secrets do Databricks (consumer)."
