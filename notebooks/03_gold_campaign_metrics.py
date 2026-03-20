# Databricks notebook source
# Retail Media — Camada Gold
# Calcula métricas de performance de campanha: CTR, CVR, ROAS, CPC.
# Roda em batch (não streaming) lendo da Silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gold — Métricas de Campanha
# MAGIC | Métrica | Fórmula |
# MAGIC |---|---|
# MAGIC | CTR | cliques / impressões |
# MAGIC | CVR | conversões / cliques |
# MAGIC | ROAS | receita / (cliques * CPC simulado) |
# MAGIC | Revenue | soma da receita das conversões |

# COMMAND ----------

# DBTITLE 1,Configuração
ADLS_ACCOUNT = dbutils.secrets.get(scope="retail-media", key="adls-account-name")
ADLS_KEY     = dbutils.secrets.get(scope="retail-media", key="adls-account-key")

SILVER_PATH = f"abfss://silver@{ADLS_ACCOUNT}.dfs.core.windows.net/campaign_events"
GOLD_PATH   = f"abfss://gold@{ADLS_ACCOUNT}.dfs.core.windows.net/campaign_metrics"

spark.conf.set(f"fs.azure.account.key.{ADLS_ACCOUNT}.dfs.core.windows.net", ADLS_KEY)

# COMMAND ----------

# DBTITLE 1,Ler Silver
from pyspark.sql import functions as F

silver = spark.read.format("delta").load(SILVER_PATH)

impressions = silver.filter(F.col("event_type") == "impression")
clicks      = silver.filter(F.col("event_type") == "click")
conversions = silver.filter(F.col("event_type") == "conversion")

print(f"Impressões : {impressions.count()}")
print(f"Cliques    : {clicks.count()}")
print(f"Conversões : {conversions.count()}")

# COMMAND ----------

# DBTITLE 1,Agregar por campanha e data
agg_impressions = (
    impressions
    .groupBy("campaign_id", "advertiser_id", F.to_date("event_timestamp").alias("event_date"))
    .agg(
        F.count("*").alias("impressions"),
        F.sum(F.when(F.col("viewable") == True, 1).otherwise(0)).alias("viewable_impressions"),
    )
)

agg_clicks = (
    clicks
    .groupBy("campaign_id", F.to_date("event_timestamp").alias("event_date"))
    .agg(F.count("*").alias("clicks"))
)

agg_conversions = (
    conversions
    .groupBy("campaign_id", F.to_date("event_timestamp").alias("event_date"))
    .agg(
        F.count("*").alias("conversions"),
        F.sum("revenue").alias("revenue"),
        F.sum("quantity").alias("items_sold"),
    )
)

# COMMAND ----------

# DBTITLE 1,Calcular métricas — CTR, CVR, ROAS
CPC_SIMULADO = 0.50  # R$ por clique (referência para ROAS)

metrics = (
    agg_impressions
    .join(agg_clicks,      ["campaign_id", "event_date"], "left")
    .join(agg_conversions, ["campaign_id", "event_date"], "left")
    .fillna(0, subset=["clicks", "conversions", "revenue", "items_sold"])
    .withColumn("ctr",     F.round(F.col("clicks")      / F.col("impressions"), 4))
    .withColumn("cvr",     F.round(F.col("conversions") / F.when(F.col("clicks") > 0, F.col("clicks")).otherwise(1), 4))
    .withColumn("spend",   F.round(F.col("clicks") * CPC_SIMULADO, 2))
    .withColumn("roas",    F.round(F.col("revenue") / F.when(F.col("spend") > 0, F.col("spend")).otherwise(1), 2))
    .withColumn("updated_at", F.current_timestamp())
    .select(
        "event_date",
        "campaign_id",
        "advertiser_id",
        "impressions",
        "viewable_impressions",
        "clicks",
        "conversions",
        "revenue",
        "items_sold",
        "ctr",
        "cvr",
        "spend",
        "roas",
        "updated_at",
    )
    .orderBy("event_date", "campaign_id")
)

display(metrics)

# COMMAND ----------

# DBTITLE 1,Gravar na Gold (overwrite — Gold é sempre recalculada)
(
    metrics
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .partitionBy("event_date")
    .save(GOLD_PATH)
)

print("Gold gravada com sucesso!")

# COMMAND ----------

# DBTITLE 1,Resultado final — ranking de campanhas por ROAS
gold = spark.read.format("delta").load(GOLD_PATH)

display(
    gold
    .orderBy(F.col("roas").desc())
)
