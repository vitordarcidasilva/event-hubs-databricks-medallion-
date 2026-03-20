# =============================================================================
# Cria Secret Scope e Secrets no Databricks para o projeto Retail Media
# Pré-requisito: databricks CLI instalado e configurado (databricks configure)
# Uso: .\infra\setup_databricks_secrets.ps1
# =============================================================================

$ErrorActionPreference = "Stop"

$SCOPE = "retail-media"

# ---------------------------------------------------------------------------
# Valores — preencha com os valores gerados pelos scripts de infra
# Execute setup_event_hub.ps1 e setup_adls.ps1 para obter essas strings
# ---------------------------------------------------------------------------
$EH_CONSUMER_CONN_STRING = "<CONNECTION_STRING_CONSUMER_DO_SETUP_EVENT_HUB>"
$ADLS_ACCOUNT_NAME       = "<NOME_DO_STORAGE_ACCOUNT>"       # ex: stretailmediadev
$ADLS_ACCOUNT_KEY        = "<CHAVE_DO_STORAGE_DO_SETUP_ADLS>"

# ---------------------------------------------------------------------------
# 1. Criar Secret Scope
# ---------------------------------------------------------------------------
Write-Host "`n==> Criando Secret Scope '$SCOPE'..." -ForegroundColor Cyan
databricks secrets create-scope --scope $SCOPE 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "    Scope ja existe, continuando..." -ForegroundColor Gray
}

# ---------------------------------------------------------------------------
# 2. Guardar secrets
# ---------------------------------------------------------------------------
Write-Host "`n==> Salvando secrets..." -ForegroundColor Cyan

Write-Host "    -> eventhub-consumer-connection-string" -ForegroundColor Gray
databricks secrets put --scope $SCOPE --key "eventhub-consumer-connection-string" --string-value $EH_CONSUMER_CONN_STRING

Write-Host "    -> eventhub-name" -ForegroundColor Gray
databricks secrets put --scope $SCOPE --key "eventhub-name" --string-value "retail-media-events"

Write-Host "    -> adls-account-name" -ForegroundColor Gray
databricks secrets put --scope $SCOPE --key "adls-account-name" --string-value $ADLS_ACCOUNT_NAME

Write-Host "    -> adls-account-key" -ForegroundColor Gray
databricks secrets put --scope $SCOPE --key "adls-account-key" --string-value $ADLS_ACCOUNT_KEY

# ---------------------------------------------------------------------------
# 3. Listar secrets criados
# ---------------------------------------------------------------------------
Write-Host "`n==> Secrets criados no scope '$SCOPE':" -ForegroundColor Green
databricks secrets list --scope $SCOPE

Write-Host "`nNo notebook Databricks use:" -ForegroundColor Yellow
Write-Host '  dbutils.secrets.get(scope="retail-media", key="<nome-do-secret>")' -ForegroundColor Cyan
