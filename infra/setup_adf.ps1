# =============================================================================
# Cria o Azure Data Factory e implanta pipeline, linked service e trigger
# para o projeto Retail Media Medallion
# Uso: .\infra\setup_adf.ps1
# =============================================================================

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Variáveis — ajuste conforme necessário
# ---------------------------------------------------------------------------
$RESOURCE_GROUP           = "rg-retail-media"
$LOCATION                 = "brazilsouth"
$ADF_NAME                 = "adf-retail-media-dev"

$DATABRICKS_WORKSPACE_URL = "https://<SEU_WORKSPACE_ID>.azuredatabricks.net"
$DATABRICKS_TOKEN         = "<DATABRICKS_ACCESS_TOKEN>"
$DATABRICKS_CLUSTER_ID    = "<ID_DO_CLUSTER>"

# ---------------------------------------------------------------------------
# 1. Criar Azure Data Factory
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Azure Data Factory '$ADF_NAME'..." -ForegroundColor Cyan
az datafactory create `
  --resource-group $RESOURCE_GROUP `
  --factory-name $ADF_NAME `
  --location $LOCATION `
  --output table

# ---------------------------------------------------------------------------
# 2. Criar Linked Service — Databricks
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Linked Service 'ls_databricks'..." -ForegroundColor Cyan

$LS_BODY = @"
{
  "type": "AzureDatabricks",
  "typeProperties": {
    "domain": "$DATABRICKS_WORKSPACE_URL",
    "accessToken": {
      "type": "SecureString",
      "value": "$DATABRICKS_TOKEN"
    },
    "existingClusterId": "$DATABRICKS_CLUSTER_ID"
  }
}
"@

az datafactory linked-service create `
  --resource-group $RESOURCE_GROUP `
  --factory-name $ADF_NAME `
  --linked-service-name "ls_databricks" `
  --properties $LS_BODY `
  --output table

# ---------------------------------------------------------------------------
# 3. Criar Pipeline
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando pipeline 'pipeline_retail_media'..." -ForegroundColor Cyan

$PIPELINE_PROPS = (Get-Content "adf\pipeline\pipeline_retail_media.json" -Raw | ConvertFrom-Json).properties | ConvertTo-Json -Depth 20

az datafactory pipeline create `
  --resource-group $RESOURCE_GROUP `
  --factory-name $ADF_NAME `
  --name "pipeline_retail_media" `
  --pipeline $PIPELINE_PROPS `
  --output table

# ---------------------------------------------------------------------------
# 4. Criar Trigger (schedule 30 min)
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando trigger 'tr_schedule_30min' (a cada 30 minutos)..." -ForegroundColor Cyan

$TRIGGER_PROPS = (Get-Content "adf\trigger\tr_schedule_30min.json" -Raw | ConvertFrom-Json).properties | ConvertTo-Json -Depth 20

az datafactory trigger create `
  --resource-group $RESOURCE_GROUP `
  --factory-name $ADF_NAME `
  --trigger-name "tr_schedule_30min" `
  --properties $TRIGGER_PROPS `
  --output table

# ---------------------------------------------------------------------------
# 5. Ativar trigger
# ---------------------------------------------------------------------------
Write-Host "`n==> Ativando trigger..." -ForegroundColor Cyan
az datafactory trigger start `
  --resource-group $RESOURCE_GROUP `
  --factory-name $ADF_NAME `
  --trigger-name "tr_schedule_30min"

# ---------------------------------------------------------------------------
# Resultado
# ---------------------------------------------------------------------------
Write-Host "`n=======================================================================" -ForegroundColor Green
Write-Host "AZURE DATA FACTORY CONFIGURADO COM SUCESSO" -ForegroundColor Green
Write-Host "=======================================================================`n" -ForegroundColor Green

Write-Host "ADF            : $ADF_NAME" -ForegroundColor Yellow
Write-Host "Linked Service : ls_databricks" -ForegroundColor Yellow
Write-Host "Pipeline       : pipeline_retail_media" -ForegroundColor Yellow
Write-Host "Trigger        : tr_schedule_30min (a cada 30 min, fuso: America/Sao_Paulo)" -ForegroundColor Yellow
Write-Host "`nAcesse o ADF Studio para monitorar as execucoes:" -ForegroundColor Cyan
Write-Host "https://adf.azure.com" -ForegroundColor Cyan
