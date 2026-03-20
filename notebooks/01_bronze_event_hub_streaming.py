# Databricks notebook source
# Retail Media — Camada Bronze
# Consome eventos do Azure Event Hub via protocolo Kafka (built-in no DBR 16.4)
# Sem dependências externas / Maven.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze — Event Hub → Delta Lake
# MAGIC **Fluxo:** `Event Hub (Kafka protocol)` → `Structured Streaming` → `Delta Bronze`
# MAGIC
# MAGIC - Sem library externa: DBR 16.4 tem conector Kafka nativo
# MAGIC - Event Hub expõe endpoint Kafka na porta 9093
# MAGIC - Bronze = raw append-only, preserva JSON original + metadados

# COMMAND ----------

# DBTITLE 1,Secrets e configuração
EH_CONN_STRING = dbutils.secrets.get(scope="retail-media", key="eventhub-consumer-connection-string")
EH_NAME        = dbutils.secrets.get(scope="retail-media", key="eventhub-name")
ADLS_ACCOUNT   = dbutils.secrets.get(scope="retail-media", key="adls-account-name")
ADLS_KEY       = dbutils.secrets.get(scope="retail-media", key="adls-account-key")

EH_NAMESPACE   = "evhns-retail-media-dev"

BRONZE_PATH     = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/campaign_events"
CHECKPOINT_PATH = f"abfss://checkpoints@{ADLS_ACCOUNT}.dfs.core.windows.net/bronze_campaign_events"

print(f"Namespace  : {EH_NAMESPACE}")
print(f"Event Hub  : {EH_NAME}")
print(f"Bronze     : {BRONZE_PATH}")
print(f"Checkpoint : {CHECKPOINT_PATH}")

# COMMAND ----------

# DBTITLE 1,Configurar acesso ao ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net",
    ADLS_KEY,
)

# COMMAND ----------

# DBTITLE 1,Configuração Kafka (Event Hub Kafka endpoint)
# Event Hub expõe Kafka na porta 9093 com SASL/SSL
JAAS_CONFIG = (
    'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
    'username="$ConnectionString" '
    f'password="{EH_CONN_STRING}";'
)

kafka_options = {
    "kafka.bootstrap.servers"  : f"{EH_NAMESPACE}.servicebus.windows.net:9093",
    "subscribe"                : EH_NAME,
    "kafka.sasl.mechanism"     : "PLAIN",
    "kafka.security.protocol"  : "SASL_SSL",
    "kafka.sasl.jaas.config"   : JAAS_CONFIG,
    "kafka.request.timeout.ms" : "60000",
    "kafka.session.timeout.ms" : "60000",
    "startingOffsets"          : "earliest",   # lê tudo desde o início (dev)
    "failOnDataLoss"           : "false",
}

# COMMAND ----------

# DBTITLE 1,Ler stream do Event Hub via Kafka
raw_stream = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Schema do conector Kafka:
# key       binary
# value     binary  ← nosso JSON
# topic     string
# partition int
# offset    long
# timestamp timestamp
# timestampType int

raw_stream.printSchema()

# COMMAND ----------

# DBTITLE 1,Decodificar payload e extrair campos para particionamento
from pyspark.sql import functions as F

bronze_stream = (
    raw_stream
    .select(
        # payload original como string UTF-8
        F.col("value").cast("string").alias("raw_payload"),
        # metadados Kafka/Event Hub para rastreabilidade
        F.col("topic").alias("eh_topic"),
        F.col("partition").alias("eh_partition"),
        F.col("offset").alias("eh_offset"),
        F.col("timestamp").alias("eh_enqueued_time"),
        # extrai campos mínimos do JSON (Bronze = sem transformação pesada)
        F.get_json_object(F.col("value").cast("string"), "$.event_type").alias("event_type"),
        F.get_json_object(F.col("value").cast("string"), "$.event_timestamp").alias("event_timestamp"),
        F.get_json_object(F.col("value").cast("string"), "$.campaign_id").alias("campaign_id"),
        # particionamento por data no Delta
        F.to_date(
            F.get_json_object(F.col("value").cast("string"), "$.event_timestamp")
        ).alias("event_date"),
        # auditoria de ingestão
        F.current_timestamp().alias("ingested_at"),
    )
)

# COMMAND ----------

# DBTITLE 1,Gravar no Delta Lake Bronze (append-only)
query = (
    bronze_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT_PATH)
    .option("mergeSchema", "true")
    .partitionBy("event_date", "event_type")
    # availableNow: processa todos os offsets disponíveis e para
    # ideal para orquestração via ADF (a cada X minutos)
    .trigger(availableNow=True)
    .start(BRONZE_PATH)
)

query.awaitTermination()
print(f"Bronze finalizado | Batches processados: {query.lastProgress['batchId'] + 1}")

# COMMAND ----------

# DBTITLE 1,Monitorar progresso
import time

for _ in range(5):
    progress = query.lastProgress
    if progress:
        print(
            f"Batch {progress['batchId']} | "
            f"Rows: {progress['numInputRows']} | "
            f"Rows/s: {progress['processedRowsPerSecond']:.1f}"
        )
    else:
        print("Aguardando primeiro batch...")
    time.sleep(5)

# COMMAND ----------

# DBTITLE 1,Inspecionar Bronze (rodar após alguns batches)
bronze_df = spark.read.format("delta").load(BRONZE_PATH)

print(f"Total eventos na Bronze: {bronze_df.count()}")

display(
    bronze_df
    .groupBy("event_type", "event_date")
    .count()
    .orderBy("event_date", "event_type")
)

# COMMAND ----------

display(bronze_df.limit(20))
