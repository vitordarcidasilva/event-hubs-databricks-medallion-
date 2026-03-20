# Databricks notebook source
# Retail Media — Camada Silver
# Lê da Bronze, parseia JSON completo, tipifica e deduplica por event_id.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver — Bronze → Silver
# MAGIC - Parse completo do JSON por tipo de evento
# MAGIC - Tipos de dados corretos (string, double, int, timestamp)
# MAGIC - Deduplicação por `event_id`
# MAGIC - Sem regras de negócio ainda — isso é Gold

# COMMAND ----------

# DBTITLE 1,Configuração
ADLS_ACCOUNT = dbutils.secrets.get(scope="retail-media", key="adls-account-name")
ADLS_KEY     = dbutils.secrets.get(scope="retail-media", key="adls-account-key")

BRONZE_PATH = f"abfss://bronze@{ADLS_ACCOUNT}.dfs.core.windows.net/campaign_events"
SILVER_PATH = f"abfss://silver@{ADLS_ACCOUNT}.dfs.core.windows.net/campaign_events"
CHECKPOINT  = f"abfss://checkpoints@{ADLS_ACCOUNT}.dfs.core.windows.net/silver_campaign_events"

spark.conf.set(f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net", ADLS_KEY)

# COMMAND ----------

# DBTITLE 1,Schema por tipo de evento
from pyspark.sql.types import *

# Campos comuns a todos os eventos
COMMON_FIELDS = [
    StructField("event_id",        StringType()),
    StructField("event_type",      StringType()),
    StructField("event_timestamp", TimestampType()),
    StructField("campaign_id",     StringType()),
    StructField("ad_id",           StringType()),
    StructField("advertiser_id",   StringType()),
    StructField("publisher_id",    StringType()),
    StructField("user_id_hashed",  StringType()),
    StructField("session_id",      StringType()),
    StructField("device_type",     StringType()),
    StructField("channel",         StringType()),
]

schema_impression = StructType(COMMON_FIELDS + [
    StructField("placement",         StringType()),
    StructField("viewable",          BooleanType()),
    StructField("viewable_seconds",  DoubleType()),
])

schema_click = StructType(COMMON_FIELDS + [
    StructField("placement",     StringType()),
    StructField("impression_id", StringType()),
])

schema_conversion = StructType(COMMON_FIELDS + [
    StructField("click_id",           StringType()),
    StructField("impression_id",      StringType()),
    StructField("order_id",           StringType()),
    StructField("product_id",         StringType()),
    StructField("product_category",   StringType()),
    StructField("revenue",            DoubleType()),
    StructField("quantity",           IntegerType()),
    StructField("attribution_model",  StringType()),
])

# COMMAND ----------

# DBTITLE 1,Ler Bronze como stream
from pyspark.sql import functions as F

bronze_stream = (
    spark.readStream
    .format("delta")
    .load(BRONZE_PATH)
)

# COMMAND ----------

# DBTITLE 1,Parsear JSON completo por tipo de evento
def parse_event(df, event_type, schema):
    return (
        df
        .filter(F.col("event_type") == event_type)
        .select(F.from_json(F.col("raw_payload"), schema).alias("data"), "ingested_at")
        .select("data.*", "ingested_at")
    )

impressions = parse_event(bronze_stream, "impression", schema_impression)
clicks      = parse_event(bronze_stream, "click",      schema_click)
conversions = parse_event(bronze_stream, "conversion", schema_conversion)

# Une os 3 tipos em um único stream (union com mergeSchema)
silver_stream = impressions.unionByName(clicks,      allowMissingColumns=True) \
                           .unionByName(conversions, allowMissingColumns=True)

# COMMAND ----------

# DBTITLE 1,Gravar na Silver com deduplicação por event_id
query = (
    silver_stream
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
    .option("delta.enableChangeDataFeed", "true")
    .partitionBy("event_type")
    .trigger(availableNow=True)
    .start(SILVER_PATH)
)

query.awaitTermination()
print(f"Silver finalizado | Batch: {query.lastProgress['batchId'] + 1}")

# COMMAND ----------

# DBTITLE 1,Monitorar
import time

for _ in range(5):
    p = query.lastProgress
    if p:
        print(f"Batch {p['batchId']} | Rows: {p['numInputRows']}")
    else:
        print("Aguardando primeiro batch...")
    time.sleep(5)

# COMMAND ----------

# DBTITLE 1,Inspecionar Silver
silver_df = spark.read.format("delta").load(SILVER_PATH)

print(f"Total eventos Silver: {silver_df.count()}")

display(
    silver_df
    .groupBy("event_type")
    .agg(
        F.count("*").alias("total"),
        F.countDistinct("event_id").alias("distintos"),
        F.countDistinct("campaign_id").alias("campanhas"),
        F.countDistinct("user_id_hashed").alias("usuarios"),
    )
)

# COMMAND ----------

display(silver_df.filter(F.col("event_type") == "conversion").limit(10))
