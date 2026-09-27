# Databricks notebook source
CATALOG    = "mvp_jogos"
SCHEMA_RAW = "bronze"
VOLUME     = "volumes_jogos"
ARQ_RAW    = "vgsales_1980_2024_raw.csv"
TABELA_RAW = "mvp_jogos.bronze.vgsales_raw"
URI_ARQ    = f"/Volumes/{CATALOG}/{SCHEMA_RAW}/{VOLUME}/{ARQ_RAW}"

LINHEAGEM = {
    "arquivo_origem": "Video Games Sales (1980-2024) - Raw.csv",
    "fonte_origem":   "Kaggle - 'Video Game Sales & Industry Data (1980 - 2024)' (Bhushan Divekar)",
    "url_fonte":      "https://www.kaggle.com/datasets/bhushandivekar/video-game-sales-and-industry-data-1980-2024",
    "licenca":        "CC0 - Public Domain (https://creativecommons.org/publicdomain/zero/1.0/)",
    "metodo_coleta":  "Download pontual de arquivo estático via portal Kaggle + upload manual ao Databricks Volume",
    "formato_origem": "CSV, 14 colunas, ~8,3 MB, 64.016 registros",
    "checksum_sha256": "33780cc77d2395d82628c6f9120cc0905355a4804d5616dee06bdae414aff0d1",
}

# COMMAND ----------

df_peek = spark.read.csv(URI_ARQ, header=True)  # sem schema: apenas inspeção
print("Colunas encontradas no arquivo:")
for i, c in enumerate(df_peek.columns):
    print(f"  {i+1:2d}. {c}")
df_peek.limit(3).display(truncate=60)

# COMMAND ----------

RAW_SCHEMA = """
img           STRING,
title         STRING,
console       STRING,
genre         STRING,
publisher     STRING,
developer     STRING,
critic_score  STRING,
total_sales   STRING,
na_sales      STRING,
jp_sales      STRING,
pal_sales     STRING,
other_sales   STRING,
release_date  STRING,
last_update   STRING
"""

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

df_raw = spark.read.csv(URI_ARQ, header=True, schema=RAW_SCHEMA)

df_bronze = df_raw
for nome_coluna, valor in LINHEAGEM.items():
    df_bronze = df_bronze.withColumn(nome_coluna, lit(valor))
df_bronze = df_bronze.withColumn("data_ingestao", current_timestamp())

df_bronze.write.format("delta").mode("overwrite").saveAsTable(TABELA_RAW)
print(f"\n✅ Persistida a tabela Delta: {TABELA_RAW}")

# COMMAND ----------

df_bronze.limit(5).display(truncate=50)

spark.sql(f"""
    SELECT count(*)                     AS total_linhas,
           count(DISTINCT title)        AS jogos_unicos,
           count(DISTINCT console)      AS consoles_distintos,
           count(DISTINCT genre)        AS generos_distintos,
           count(DISTINCT publisher)    AS publishers_distintos
    FROM {TABELA_RAW}
""").display()

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL mvp_jogos.bronze.vgsales_raw