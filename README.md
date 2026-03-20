# Retail Media — Event Hub + Databricks Medallion

Pipeline de dados em tempo real para analytics de **Retail Media** (impressões, cliques e conversões de anúncios) usando Azure Event Hub, Databricks Structured Streaming e arquitetura Medallion com Delta Lake.

---

## Arquitetura

```
┌─────────────────────────┐
│  Event Generator (Python) │   gera eventos sintéticos (impressão → clique → conversão)
└────────────┬────────────┘
             │ azure-eventhub SDK
             ▼
┌─────────────────────────┐
│    Azure Event Hub       │   SKU Standard · 4 partições · porta Kafka 9093
└────────────┬────────────┘
             │ Kafka protocol (SASL_SSL)
             ▼
┌──────────────────────────────────────────────────────────────┐
│                   Azure Databricks (DBR 16.4)                 │
│                                                              │
│  01_bronze  →  02_silver  →  03_gold                         │
│  (raw JSON)    (typed/dedup)  (KPIs agregados)               │
└────────────────────────┬─────────────────────────────────────┘
                         │ Delta Lake (ABFSS)
                         ▼
┌─────────────────────────┐
│  Azure Data Lake Gen2    │   containers: bronze · silver · gold · checkpoints
└─────────────────────────┘
```

### Camadas Medallion

| Camada | Notebook | Modo | Descrição |
|--------|----------|------|-----------|
| **Bronze** | `01_bronze_event_hub_streaming.py` | Streaming append | JSON raw + metadados Kafka |
| **Silver** | `02_silver_campaign_events.py` | Streaming append | Typed, deduplicado por `event_id` |
| **Gold** | `03_gold_campaign_metrics.py` | Batch overwrite | KPIs por campanha e data |

### KPIs calculados (Gold)

- **CTR** — Click-Through Rate = cliques / impressões
- **CVR** — Conversion Rate = conversões / cliques
- **ROAS** — Return on Ad Spend = receita / gasto estimado
- Impressões, impressões viewable, conversões, receita, itens vendidos

---

## Pré-requisitos

| Ferramenta | Versão mínima | Instalação |
|------------|---------------|------------|
| Azure CLI | 2.x | [docs.microsoft.com/cli/azure/install-azure-cli](https://docs.microsoft.com/cli/azure/install-azure-cli) |
| Databricks CLI | 0.17+ | `pip install databricks-cli` |
| Python | 3.11+ | [python.org](https://python.org) |
| Databricks Workspace | DBR 16.4 | Azure Portal |

```bash
# Autenticar no Azure
az login
az account show   # confirmar subscription ativa

# Configurar Databricks CLI
databricks configure --token
# Host: https://<seu-workspace>.azuredatabricks.net
# Token: <personal access token gerado no Databricks UI>
```

---

## Setup da Infraestrutura

Execute os scripts na ordem abaixo. Todos os recursos ficam no resource group `rg-retail-media` na região `brazilsouth`.

### 1. Azure Event Hub

```bash
# Bash / Linux / macOS
bash infra/setup_event_hub.sh

# PowerShell / Windows
.\infra\setup_event_hub.ps1
```

O script cria:
- Resource Group `rg-retail-media`
- Namespace `evhns-retail-media-dev` (SKU Standard, 1 TU)
- Event Hub `retail-media-events` (4 partições, retenção 1 dia)
- Política `producer-policy` (Send)
- Política `consumer-policy` (Listen)
- Consumer Group `databricks-cg`

Ao final, imprime as **connection strings** de producer e consumer. Copie-as.

**Comandos Azure CLI equivalentes (referência):**

```bash
# Resource Group
az group create --name rg-retail-media --location brazilsouth

# Namespace
az eventhubs namespace create \
  --resource-group rg-retail-media \
  --name evhns-retail-media-dev \
  --location brazilsouth \
  --sku Standard \
  --capacity 1

# Event Hub
az eventhubs eventhub create \
  --resource-group rg-retail-media \
  --namespace-name evhns-retail-media-dev \
  --name retail-media-events \
  --partition-count 4 \
  --message-retention 1

# Política producer (Send)
az eventhubs eventhub authorization-rule create \
  --resource-group rg-retail-media \
  --namespace-name evhns-retail-media-dev \
  --eventhub-name retail-media-events \
  --name producer-policy \
  --rights Send

# Política consumer (Listen)
az eventhubs eventhub authorization-rule create \
  --resource-group rg-retail-media \
  --namespace-name evhns-retail-media-dev \
  --eventhub-name retail-media-events \
  --name consumer-policy \
  --rights Listen

# Consumer Group dedicado ao Databricks
az eventhubs eventhub consumer-group create \
  --resource-group rg-retail-media \
  --namespace-name evhns-retail-media-dev \
  --eventhub-name retail-media-events \
  --name databricks-cg

# Obter connection string producer
az eventhubs eventhub authorization-rule keys list \
  --resource-group rg-retail-media \
  --namespace-name evhns-retail-media-dev \
  --eventhub-name retail-media-events \
  --name producer-policy \
  --query "primaryConnectionString" -o tsv

# Obter connection string consumer (para o Databricks)
az eventhubs eventhub authorization-rule keys list \
  --resource-group rg-retail-media \
  --namespace-name evhns-retail-media-dev \
  --eventhub-name retail-media-events \
  --name consumer-policy \
  --query "primaryConnectionString" -o tsv
```

---

### 2. Azure Data Lake Storage Gen2

```powershell
.\infra\setup_adls.ps1
```

O script cria:
- Storage Account `stretailmediadev` (Standard LRS, hierarchical namespace habilitado)
- Containers: `bronze`, `silver`, `gold`, `checkpoints`

Imprime a **storage account key**. Copie-a para o próximo passo.

**Comandos Azure CLI equivalentes (referência):**

```bash
# Storage Account com ADLS Gen2 (hierarchical namespace)
az storage account create \
  --resource-group rg-retail-media \
  --name stretailmediadev \
  --location brazilsouth \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --min-tls-version TLS1_2

# Obter chave
STORAGE_KEY=$(az storage account keys list \
  --resource-group rg-retail-media \
  --account-name stretailmediadev \
  --query "[0].value" -o tsv)

# Criar containers
for container in bronze silver gold checkpoints; do
  az storage container create \
    --name $container \
    --account-name stretailmediadev \
    --account-key $STORAGE_KEY
done
```

---

### 3. Databricks — Secrets

Edite `infra/setup_databricks_secrets.ps1` preenchendo as variáveis com os valores obtidos nos passos 1 e 2. Depois execute:

```powershell
.\infra\setup_databricks_secrets.ps1
```

O script cria o Secret Scope `retail-media` com 4 secrets:

| Secret | Valor |
|--------|-------|
| `eventhub-consumer-connection-string` | Connection string com permissão Listen |
| `eventhub-name` | `retail-media-events` |
| `adls-account-name` | `stretailmediadev` |
| `adls-account-key` | Chave do storage account |

**Comandos Databricks CLI equivalentes (referência):**

```bash
# Criar scope
databricks secrets create-scope --scope retail-media

# Adicionar secrets
databricks secrets put --scope retail-media --key eventhub-consumer-connection-string
databricks secrets put --scope retail-media --key eventhub-name
databricks secrets put --scope retail-media --key adls-account-name
databricks secrets put --scope retail-media --key adls-account-key

# Listar secrets
databricks secrets list --scope retail-media
```

---

### 4. Permissão IAM — ADLS + Databricks

Edite `infra/assign_role_adls.ps1` com sua Subscription ID e o Object ID do Service Principal do Databricks. Depois execute:

```powershell
.\infra\assign_role_adls.ps1
```

**Comando Azure CLI equivalente:**

```bash
# Obter Object ID do managed identity do cluster Databricks
az databricks workspace show \
  --name <nome-workspace> \
  --resource-group rg-retail-media \
  --query identity.principalId -o tsv

# Atribuir papel
az role assignment create \
  --assignee <OBJECT_ID_DATABRICKS> \
  --role "Storage Blob Data Contributor" \
  --scope "/subscriptions/<SUBSCRIPTION_ID>/resourceGroups/rg-retail-media/providers/Microsoft.Storage/storageAccounts/stretailmediadev"
```

---

## Configurar o Gerador de Eventos

```bash
cd generate_data_event_hub

# Instalar dependências
pip install -r requirements.txt

# Configurar .env
cp .env.example .env
# Edite .env com a connection string do producer-policy
```

Conteúdo do `.env`:

```env
EVENT_HUB_CONNECTION_STRING=Endpoint=sb://evhns-retail-media-dev.servicebus.windows.net/;SharedAccessKeyName=producer-policy;SharedAccessKey=<KEY>;EntityPath=retail-media-events
EVENT_HUB_NAME=retail-media-events
EVENTS_PER_SECOND=10
TOTAL_EVENTS=1000
```

### Validar conexão

```bash
python validate_connection.py
```

### Enviar eventos

```bash
# Padrão: 1000 eventos a 10/s
python producer.py

# Personalizado
python producer.py --total 5000 --rate 20

# Dry-run (sem enviar)
python producer.py --dry-run
```

---

## Executar os Notebooks Databricks

Importe os notebooks da pasta `notebooks/` para seu workspace e execute na ordem:

```
1. 01_bronze_event_hub_streaming.py   ← consome Event Hub → Delta Bronze
2. 02_silver_campaign_events.py       ← Bronze → Delta Silver (typed/dedup)
3. 03_gold_campaign_metrics.py        ← Silver → Delta Gold (KPIs)
```

**Import via Databricks CLI:**

```bash
databricks workspace import_dir notebooks/ /Shared/retail-media --overwrite
```

**Cluster recomendado:** DBR 16.4 LTS · 1 driver + 2 workers · modo Standard

O trigger `availableNow=True` faz cada notebook processar os dados disponíveis e encerrar — ideal para orquestração via Azure Data Factory ou Databricks Workflows.

---

## Estrutura do Projeto

```
event_hubs_databricks_medalion/
├── .env.example                        # Template de configuração (sem secrets)
├── .gitignore
├── README.md
│
├── generate_data_event_hub/            # Gerador de eventos sintéticos
│   ├── config.py                       # Lê variáveis do .env
│   ├── schema.py                       # Modelos de dados (Impression/Click/Conversion)
│   ├── generators.py                   # Geração com funil realista (CTR 2%, CVR 5%)
│   ├── producer.py                     # Envia eventos ao Event Hub em batch
│   ├── validate_connection.py          # Testa conectividade com o Event Hub
│   └── requirements.txt
│
├── infra/                              # Infraestrutura como Código
│   ├── event_hub.bicep                 # Template Bicep (IaC declarativo)
│   ├── setup_event_hub.sh              # Cria Event Hub (Bash/Linux)
│   ├── setup_event_hub.ps1             # Cria Event Hub (PowerShell/Windows)
│   ├── setup_adls.ps1                  # Cria ADLS Gen2 + containers
│   ├── setup_databricks_secrets.ps1    # Cria Secret Scope e secrets
│   └── assign_role_adls.ps1            # Atribui IAM ao Databricks no ADLS
│
└── notebooks/                          # Notebooks Databricks
    ├── 01_bronze_event_hub_streaming.py  # Kafka → Delta Bronze (streaming)
    ├── 02_silver_campaign_events.py      # Bronze → Delta Silver (typed/dedup)
    └── 03_gold_campaign_metrics.py       # Silver → Delta Gold (KPIs de campanha)
```

---

## Eventos gerados

O gerador produz 3 tipos de eventos simulando um funil de Retail Media:

```
Impression → Click (CTR ~2%) → Conversion (CVR ~5% dos cliques)
```

| Tipo | Campos principais |
|------|-------------------|
| `impression` | campaign_id, ad_id, publisher_id, placement, device_type, viewable, viewable_seconds |
| `click` | campaign_id, ad_id, impression_id, device_type, channel |
| `conversion` | campaign_id, click_id, order_id, product_id, revenue, quantity, attribution_model |

Todos os eventos incluem: `event_id` (UUID), `event_type`, `event_timestamp`, `user_id_hashed` (SHA-256), `session_id`.

---

## Stack Tecnológica

| Camada | Tecnologia |
|--------|-----------|
| Ingestão | Azure Event Hub (protocolo Kafka) |
| Processamento | Azure Databricks · PySpark Structured Streaming |
| Armazenamento | Azure Data Lake Gen2 · Delta Lake |
| IaC | Azure Bicep · Azure CLI · PowerShell |
| Gerador | Python · azure-eventhub SDK |
| Segurança | Databricks Secret Scope · RBAC mínimo por policy |

---

## Decisões de design

- **Trigger `availableNow=True`** em Bronze e Silver: processa micro-batches disponíveis e encerra, permitindo orquestração externa (ADF, Workflows) sem streaming contínuo.
- **Gold em modo `overwrite`**: camada de negócio sempre recalculada, representa fonte única de verdade.
- **Protocolo Kafka no Event Hub**: elimina dependência de biblioteca Maven externa no DBR.
- **User IDs com SHA-256**: dados de usuário anonimizados desde a geração.
- **Políticas separadas (Send / Listen)**: princípio do menor privilégio.
