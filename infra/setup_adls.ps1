# =============================================================================
# Cria ADLS Gen2 para o projeto Retail Media (Medallion Architecture)
# Uso: .\infra\setup_adls.ps1
# =============================================================================

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Variáveis
# ---------------------------------------------------------------------------
$RESOURCE_GROUP   = "rg-retail-media"
$LOCATION         = "brazilsouth"
$STORAGE_ACCOUNT  = "stretailmediadev"   # deve ser globalmente único, 3-24 chars, só minúsculas
$SKU              = "Standard_LRS"

# Containers da arquitetura Medallion
$CONTAINERS = @("bronze", "silver", "gold", "checkpoints")

# ---------------------------------------------------------------------------
# 1. Criar Storage Account com hierarchical namespace (ADLS Gen2)
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Storage Account '$STORAGE_ACCOUNT'..." -ForegroundColor Cyan
az storage account create `
  --resource-group $RESOURCE_GROUP `
  --name $STORAGE_ACCOUNT `
  --location $LOCATION `
  --sku $SKU `
  --kind StorageV2 `
  --enable-hierarchical-namespace true `
  --min-tls-version TLS1_2 `
  --output table

# ---------------------------------------------------------------------------
# 2. Criar containers (camadas Medallion + checkpoints do Spark)
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando containers..." -ForegroundColor Cyan

$STORAGE_KEY = $(az storage account keys list `
  --resource-group $RESOURCE_GROUP `
  --account-name $STORAGE_ACCOUNT `
  --query "[0].value" `
  --output tsv)

foreach ($container in $CONTAINERS) {
    Write-Host "    -> $container" -ForegroundColor Gray
    az storage container create `
      --name $container `
      --account-name $STORAGE_ACCOUNT `
      --account-key $STORAGE_KEY `
      --output none
}

# ---------------------------------------------------------------------------
# 3. Exibir informações para o Databricks
# ---------------------------------------------------------------------------
Write-Host "`n=======================================================================" -ForegroundColor Green
Write-Host "ADLS Gen2 CRIADO COM SUCESSO" -ForegroundColor Green
Write-Host "=======================================================================`n" -ForegroundColor Green

Write-Host "Storage Account : $STORAGE_ACCOUNT" -ForegroundColor Yellow
Write-Host "Containers      : $($CONTAINERS -join ', ')" -ForegroundColor Yellow

Write-Host "`n--- STORAGE ACCOUNT KEY (para Databricks Secret) ---" -ForegroundColor Yellow
Write-Host $STORAGE_KEY

Write-Host "`n--- ABFSS paths (usar nos notebooks Databricks) ---" -ForegroundColor Yellow
foreach ($container in $CONTAINERS) {
    Write-Host "abfss://${container}@${STORAGE_ACCOUNT}.dfs.core.windows.net/" -ForegroundColor Cyan
}

Write-Host "`nGuarde a Storage Key no Databricks Secret Scope." -ForegroundColor Gray
