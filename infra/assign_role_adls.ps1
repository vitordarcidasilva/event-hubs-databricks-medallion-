# =============================================================================
# Atribui papel "Storage Blob Data Contributor" ao Databricks no ADLS Gen2
# Pré-requisito: az login executado e com permissão de Owner/User Access Administrator
# Uso: .\infra\assign_role_adls.ps1
# =============================================================================

$ErrorActionPreference = "Stop"

# ---------------------------------------------------------------------------
# Variáveis — ajuste conforme seu ambiente
# ---------------------------------------------------------------------------
$SUBSCRIPTION_ID    = "<SUA_SUBSCRIPTION_ID>"         # az account show --query id -o tsv
$RESOURCE_GROUP     = "rg-retail-media"
$STORAGE_ACCOUNT    = "stretailmediadev"
$DATABRICKS_SP_OID  = "<OBJECT_ID_DO_SERVICE_PRINCIPAL_DATABRICKS>"
# Para obter o OID do managed identity do cluster Databricks:
#   az databricks workspace show --name <workspace> --resource-group <rg> --query identity.principalId -o tsv

# ---------------------------------------------------------------------------
# Atribuir papel
# ---------------------------------------------------------------------------
Write-Host "`n==> Atribuindo 'Storage Blob Data Contributor' ao Databricks SP..." -ForegroundColor Cyan

az role assignment create `
  --assignee $DATABRICKS_SP_OID `
  --role "Storage Blob Data Contributor" `
  --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT" `
  --output table

Write-Host "`nPapel atribuído com sucesso." -ForegroundColor Green
Write-Host "O Databricks agora pode ler e escrever no ADLS Gen2 '$STORAGE_ACCOUNT'." -ForegroundColor Yellow
