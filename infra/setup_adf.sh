#!/bin/bash
# =============================================================================
# Cria o Azure Data Factory e implanta pipeline, linked service e trigger
# para o projeto Retail Media Medallion
# Pré-requisito: az login executado
# Uso: bash infra/setup_adf.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Variáveis — ajuste conforme necessário
# ---------------------------------------------------------------------------
RESOURCE_GROUP="rg-retail-media"
LOCATION="brazilsouth"
ADF_NAME="adf-retail-media-dev"

DATABRICKS_WORKSPACE_URL="https://<SEU_WORKSPACE_ID>.azuredatabricks.net"
DATABRICKS_TOKEN="<DATABRICKS_ACCESS_TOKEN>"
DATABRICKS_CLUSTER_ID="<ID_DO_CLUSTER>"

# ---------------------------------------------------------------------------
# 1. Verificar autenticação
# ---------------------------------------------------------------------------
echo "==> Verificando autenticação..."
az account show > /dev/null 2>&1 || az login

echo "==> Subscription ativa:"
az account show --query "{name:name, id:id}" -o table

# ---------------------------------------------------------------------------
# 2. Criar Azure Data Factory
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando Azure Data Factory '$ADF_NAME'..."
az datafactory create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$ADF_NAME" \
  --location "$LOCATION" \
  --output table

# ---------------------------------------------------------------------------
# 3. Criar Linked Service — Databricks
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando Linked Service 'ls_databricks'..."

LS_BODY=$(cat <<EOF
{
  "properties": {
    "type": "AzureDatabricks",
    "typeProperties": {
      "domain": "${DATABRICKS_WORKSPACE_URL}",
      "accessToken": {
        "type": "SecureString",
        "value": "${DATABRICKS_TOKEN}"
      },
      "existingClusterId": "${DATABRICKS_CLUSTER_ID}"
    }
  }
}
EOF
)

az datafactory linked-service create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$ADF_NAME" \
  --linked-service-name "ls_databricks" \
  --properties "$LS_BODY" \
  --output table

# ---------------------------------------------------------------------------
# 4. Criar Pipeline
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando pipeline 'pipeline_retail_media'..."

az datafactory pipeline create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$ADF_NAME" \
  --name "pipeline_retail_media" \
  --pipeline "$(cat adf/pipeline/pipeline_retail_media.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(json.dumps(d["properties"]))')" \
  --output table

# ---------------------------------------------------------------------------
# 5. Criar Trigger (schedule 30 min)
# ---------------------------------------------------------------------------
echo ""
echo "==> Criando trigger 'tr_schedule_30min' (a cada 30 minutos)..."

az datafactory trigger create \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$ADF_NAME" \
  --trigger-name "tr_schedule_30min" \
  --properties "$(cat adf/trigger/tr_schedule_30min.json | python3 -c 'import sys,json; d=json.load(sys.stdin); print(json.dumps(d["properties"]))')" \
  --output table

# ---------------------------------------------------------------------------
# 6. Ativar trigger
# ---------------------------------------------------------------------------
echo ""
echo "==> Ativando trigger..."
az datafactory trigger start \
  --resource-group "$RESOURCE_GROUP" \
  --factory-name "$ADF_NAME" \
  --trigger-name "tr_schedule_30min"

# ---------------------------------------------------------------------------
# Resultado
# ---------------------------------------------------------------------------
echo ""
echo "======================================================================="
echo "AZURE DATA FACTORY CONFIGURADO COM SUCESSO"
echo "======================================================================="
echo ""
echo "ADF            : $ADF_NAME"
echo "Linked Service : ls_databricks"
echo "Pipeline       : pipeline_retail_media"
echo "Trigger        : tr_schedule_30min (a cada 30 min, fuso: America/Sao_Paulo)"
echo ""
echo "Acesse o ADF Studio para monitorar as execuções:"
echo "https://adf.azure.com"
