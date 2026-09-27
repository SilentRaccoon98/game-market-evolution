# Databricks notebook source
# constantes e helper
from pyspark.sql import functions as F

SILVER = "mvp_jogos.silver.jogos_vendas"
G      = "mvp_jogos.gold."
MOD    = 2147483647  # 2^31 - 1

def sk(*cols):
    """Chave substituta determinística (reproduzível em re-execuções)."""
    return F.pmod(F.hash(F.concat_ws("\u0001", *cols)), MOD).cast("int")

df_s = spark.table(SILVER)

# COMMAND ----------

# dim_jogo (1 registro por título)
# developer: quando o mesmo título aparece com valores diferentes
# (inconsistência da fonte), usa-se o mínimo lexicográfico -> determinístico
dim_jogo = (df_s.groupBy("titulo")
            .agg(F.min("developer").alias("developer"))
            .withColumn("jogo_id", sk(F.col("titulo")))
            .select("jogo_id", "titulo", "developer")
            .orderBy("titulo"))
dim_jogo.write.format("delta").mode("overwrite").saveAsTable(G + "dim_jogo")
print(f"\n✅ {G}dim_jogo: {dim_jogo.count()} títulos únicos")
dim_jogo.limit(5).display(truncate=50)

# COMMAND ----------

# dim_console (1 registro por console original)
dim_console = (df_s.select("console_original", "console_canonico", "familia_plataforma")
               .distinct()
               .withColumn("console_id", sk(F.col("console_original")))
               .select("console_id", "console_original", "console_canonico", "familia_plataforma")
               .orderBy("console_original"))
dim_console.write.format("delta").mode("overwrite").saveAsTable(G + "dim_console")
print(f"\n✅ {G}dim_console: {dim_console.count()} consoles")
dim_console.limit(5).display()

# COMMAND ----------

# dim_genero (1 registro por gênero)
dim_genero = (df_s.select("genero")
              .distinct()
              .withColumn("genero_id", sk(F.col("genero")))
              .withColumn("genero_generico", F.col("genero") == "Misc")
              .select("genero_id", "genero", "genero_generico")
              .orderBy("genero"))
dim_genero.write.format("delta").mode("overwrite").saveAsTable(G + "dim_genero")
print(f"\n✅ {G}dim_genero: {dim_genero.count()} gêneros")
dim_genero.limit(5).display()

# COMMAND ----------

# dim_publisher (1 registro por publisher conhecido; sem NULL)
EA = ["Electronic Arts", "EA Sports", "EA Games", "EA"]
dim_publisher = (df_s.filter(F.col("publisher").isNotNull())
                 .select("publisher")
                 .distinct()
                 .withColumn("publisher_id", sk(F.col("publisher")))
                 .withColumn("publisher_grupo",
                     F.when(F.col("publisher").isin(EA), F.lit("Electronic Arts (grupo)"))
                      .otherwise(F.col("publisher")))
                 .select("publisher_id", "publisher", "publisher_grupo")
                 .orderBy("publisher"))
dim_publisher.write.format("delta").mode("overwrite").saveAsTable(G + "dim_publisher")
print(f"\n✅ {G}dim_publisher: {dim_publisher.count()} publishers")
dim_publisher.limit(5).display()

# COMMAND ----------

# dim_calendario (1 registro por ano presente nos dados)
dim_calendario = (df_s.filter(F.col("ano_lancamento").isNotNull())
                  .select(F.col("ano_lancamento").alias("ano_id"))
                  .distinct()
                  .withColumn("decada",
                     F.concat(F.substring(F.col("ano_id").cast("string"), 1, 3), F.lit("0")))
                  .withColumn("dentro_janela_vendas",
                     (F.col("ano_id") >= 1980) & (F.col("ano_id") <= 2024))
                  .select("ano_id", "decada", "dentro_janela_vendas")
                  .orderBy("ano_id"))
dim_calendario.write.format("delta").mode("overwrite").saveAsTable(G + "dim_calendario")
print(f"\n✅ {G}dim_calendario: {dim_calendario.count()} anos")
dim_calendario.limit(5).display()

# COMMAND ----------

# fato_vendas_jogo (1 registro por jogo × console, c/ chaves)
# Self-check: o COUNT deve bater com o Silver (63.791) — a dedup já
# garantiu unicidade de (titulo, console_original).
# ---------------------------------------------------------------------
dj = spark.table(G + "dim_jogo").alias("j")
dc = spark.table(G + "dim_console").alias("c")
dg = spark.table(G + "dim_genero").alias("g")
dp = spark.table(G + "dim_publisher").alias("p")
dk = spark.table(G + "dim_calendario").alias("k")

fato = (df_s
        .join(dj, on="titulo", how="left")
        .join(dc, on="console_original", how="left")
        .join(dg, on="genero", how="left")
        .join(dp, on="publisher", how="left")
        .join(dk, F.col("ano_lancamento") == F.col("k.ano_id"), how="left")
        .select(
            sk(F.col("titulo"), F.col("console_original")).alias("jogo_console_id"),
            F.col("j.jogo_id"), F.col("c.console_id"), F.col("g.genero_id"),
            F.col("p.publisher_id"), F.col("k.ano_id"),
            F.col("total_sales_mio").alias("vendas_total_mio"),
            F.col("na_sales_mio").alias("vendas_na_mio"),
            F.col("jp_sales_mio").alias("vendas_jp_mio"),
            F.col("pal_sales_mio").alias("vendas_pal_mio"),
            F.col("other_sales_mio").alias("vendas_other_mio"),
            "critic_score",
            "tem_dados_vendas", "tem_nota_critica",
            "data_aproximada", "img_placeholder",
            "data_ingestao", "checksum_sha256",
        ))
fato.write.format("delta").mode("overwrite").saveAsTable(G + "fato_vendas_jogo")
print(f"\n✅ {G}fato_vendas_jogo: {fato.count()} registros (self-check: 63.791 = Silver)")
fato.limit(5).display(truncate=40)

# COMMAND ----------

# view v_jogo_vendas (estrela achatada para SQL de negócio)
spark.sql(f"""
CREATE OR REPLACE VIEW {G}v_jogo_vendas AS
SELECT
    f.jogo_console_id,
    f.jogo_id,        d_j.titulo,              d_j.developer,
    f.console_id,     d_c.console_original,    d_c.console_canonico,
    d_c.familia_plataforma,
    f.genero_id,      d_g.genero,
    f.publisher_id,   d_p.publisher,           d_p.publisher_grupo,
    f.ano_id,         d_k.decada,              d_k.dentro_janela_vendas,
    f.vendas_total_mio, f.vendas_na_mio, f.vendas_jp_mio,
    f.vendas_pal_mio, f.vendas_other_mio,
    f.critic_score,
    f.tem_dados_vendas, f.tem_nota_critica, f.data_aproximada, f.img_placeholder,
    f.data_ingestao, f.checksum_sha256
FROM {G}fato_vendas_jogo f
LEFT JOIN {G}dim_jogo       d_j ON f.jogo_id      = d_j.jogo_id
LEFT JOIN {G}dim_console    d_c ON f.console_id   = d_c.console_id
LEFT JOIN {G}dim_genero     d_g ON f.genero_id    = d_g.genero_id
LEFT JOIN {G}dim_publisher  d_p ON f.publisher_id = d_p.publisher_id
LEFT JOIN {G}dim_calendario d_k ON f.ano_id       = d_k.ano_id
""")
print(f"\n✅ View criada: {G}v_jogo_vendas")
spark.sql(f"SELECT * FROM {G}v_jogo_vendas LIMIT 3").display(truncate=40)

# COMMAND ----------

# P1 (prints S12/S13)

# P1a — consoles dominantes por década (top 5, com fatia das vendas da década)
P1A = f"""
WITH por_decada_console AS (
    SELECT decada, console_canonico,
           ROUND(SUM(vendas_total_mio), 1) AS vendas_mio
    FROM {G}v_jogo_vendas
    WHERE vendas_total_mio IS NOT NULL
      AND decada IS NOT NULL
    GROUP BY decada, console_canonico
),
top AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY decada ORDER BY vendas_mio DESC) AS rn
    FROM por_decada_console
)
SELECT decada, console_canonico, vendas_mio,
       ROUND(100.0 * vendas_mio / SUM(vendas_mio) OVER (PARTITION BY decada), 1)
           AS fatia_da_decada_pct
FROM top
WHERE rn <= 5
ORDER BY decada, vendas_mio DESC
"""
print("— P1a — Consoles dominantes por década (top 5)")
spark.sql(P1A).display()

# P1b — vendas por região × década
P1B = f"""
SELECT decada,
       ROUND(SUM(vendas_na_mio), 1)    AS vendas_na_mio,
       ROUND(SUM(vendas_jp_mio), 1)    AS vendas_jp_mio,
       ROUND(SUM(vendas_pal_mio), 1)   AS vendas_pal_mio,
       ROUND(SUM(vendas_other_mio), 1) AS vendas_other_mio,
       ROUND(SUM(vendas_total_mio), 1) AS vendas_total_mio
FROM {G}v_jogo_vendas
WHERE vendas_total_mio IS NOT NULL
  AND decada IS NOT NULL
GROUP BY decada
ORDER BY decada
"""
print("— P1b — Vendas por região × década")
spark.sql(P1B).display()

# COMMAND ----------

# P2 (prints S14/S15)

# P2a — top 10 combos gênero × publisher, 2010–2019
P2A = f"""
SELECT genero, publisher,
       COUNT(*) AS jogos,
       ROUND(SUM(vendas_total_mio), 1) AS vendas_mio
FROM {G}v_jogo_vendas
WHERE ano_id BETWEEN 2010 AND 2019
  AND vendas_total_mio IS NOT NULL
  AND publisher IS NOT NULL
GROUP BY genero, publisher
ORDER BY vendas_mio DESC
LIMIT 10
"""
print("— P2a — Top 10 gênero × publisher (2010–2019)")
spark.sql(P2A).display()

# P2b — fatia dos top-5 publishers: 2000s vs 2010s
P2B = f"""
WITH pub_decada AS (
    SELECT CASE WHEN ano_id BETWEEN 2000 AND 2009 THEN '2000s'
                WHEN ano_id BETWEEN 2010 AND 2019 THEN '2010s' END AS decada,
           publisher,
           SUM(vendas_total_mio) AS vendas_mio
    FROM {G}v_jogo_vendas
    WHERE vendas_total_mio IS NOT NULL
      AND publisher IS NOT NULL
      AND ano_id BETWEEN 2000 AND 2019
    GROUP BY 1, 2
),
mercado AS (
    SELECT decada, SUM(vendas_mio) AS total_mercado
    FROM pub_decada
    GROUP BY decada
),
top5 AS (
    SELECT decada, publisher, vendas_mio,
           ROW_NUMBER() OVER (PARTITION BY decada ORDER BY vendas_mio DESC) AS rn
    FROM pub_decada
)
SELECT t.decada, t.publisher,
       ROUND(t.vendas_mio, 1) AS vendas_mio,
       ROUND(100.0 * t.vendas_mio / m.total_mercado, 1)
           AS fatia_do_mercado_da_decada_pct
FROM top5 t
JOIN mercado m ON t.decada = m.decada
WHERE t.rn <= 5
ORDER BY t.decada, t.vendas_mio DESC
"""
print("— P2b — Fatia de mercado real do top-5 publishers: 2000s vs 2010s")
spark.sql(P2B).display()

# P2b.2 — concentração: quanto do mercado total as 5 maiores controlam?

# COMMAND ----------

# P3 (prints S16/S17)

# P3a — venda média por faixa de nota crítica (0–10)
P3A = f"""
WITH base AS (
    SELECT
        CASE WHEN critic_score < 6 THEN '0-5'
             WHEN critic_score < 7 THEN '6-6,9'
             WHEN critic_score < 8 THEN '7-7,9'
             WHEN critic_score < 9 THEN '8-8,9'
             ELSE '9-10' END AS faixa_nota,
        critic_score,
        vendas_total_mio
    FROM {G}v_jogo_vendas
    WHERE critic_score IS NOT NULL
      AND vendas_total_mio IS NOT NULL
)
SELECT faixa_nota,
       COUNT(*) AS jogos,
       ROUND(AVG(vendas_total_mio), 3) AS venda_media_mio
FROM base
GROUP BY faixa_nota
ORDER BY MIN(critic_score)
"""
print("— P3a — Venda média por faixa de nota crítica")
spark.sql(P3A).display()

# P3b — top 100 jogos com nota crítica >= 9
P3B = f"""
SELECT titulo, console_canonico, ano_id AS ano, publisher,
       ROUND(critic_score, 1) AS critic_score,
       ROUND(vendas_total_mio, 2) AS vendas_mio
FROM {G}v_jogo_vendas
WHERE critic_score >= 9
  AND vendas_total_mio IS NOT NULL
ORDER BY vendas_mio DESC
LIMIT 100
"""
print("— P3b — Top 100 jogos com nota crítica >= 9")
spark.sql(P3B).display()

# COMMAND ----------

# evidências
spark.sql(f"""
    SELECT * FROM (
        SELECT 'dim_jogo' AS tabela, COUNT(*) AS registros FROM {G}dim_jogo
        UNION ALL SELECT 'dim_console', COUNT(*) FROM {G}dim_console
        UNION ALL SELECT 'dim_genero', COUNT(*) FROM {G}dim_genero
        UNION ALL SELECT 'dim_publisher', COUNT(*) FROM {G}dim_publisher
        UNION ALL SELECT 'dim_calendario', COUNT(*) FROM {G}dim_calendario
        UNION ALL SELECT 'fato_vendas_jogo', COUNT(*) FROM {G}fato_vendas_jogo
    ) t
    ORDER BY tabela
""").display()
spark.sql(f"DESCRIBE DETAIL {G}fato_vendas_jogo").display()

# COMMAND ----------

# gráficos: imports e estilo
import matplotlib.pyplot as plt

plt.rcParams.update({
    "figure.facecolor": "white",
    "axes.grid": True,
    "grid.alpha": 0.3,
    "axes.axisbelow": True,
})
print("📊 matplotlib pronto")

# COMMAND ----------

# top 5 consoles por década
df1a = spark.sql(P1A).toPandas()
df1a = df1a[df1a["decada"] != "2020"]
fig, axes = plt.subplots(2, 3, figsize=(16, 9), squeeze=False)
for ax, (dec, grp) in zip(axes.ravel(), df1a.groupby("decada", sort=True)):
    g = grp.sort_values("vendas_mio")
    ax.barh(g["console_canonico"], g["vendas_mio"])
    ax.set_title(f"Top 5 consoles — década de {dec}")
    ax.set_xlabel("vendas (mi)")
axes.ravel()[-1].axis("off")
plt.suptitle("P1a — Consoles dominantes por década", y=1.02)
plt.tight_layout()
plt.show()
plt.close(fig)

# COMMAND ----------

# vendas por região × década (empilhado)
df1b = spark.sql(P1B).toPandas()
regioes = [("vendas_na_mio", "NA"), ("vendas_jp_mio", "JP"),
           ("vendas_pal_mio", "PAL"), ("vendas_other_mio", "Outro")]
fig, ax = plt.subplots(figsize=(10, 6))
bottom = None
for col, label in regioes:
    vals = df1b[col].fillna(0)
    ax.bar(df1b["decada"], vals, bottom=bottom, label=label)
    bottom = vals.copy() if bottom is None else bottom + vals
ax.set_title("P1b — Vendas por região × década (mi)")
ax.set_ylabel("vendas (mi)")
ax.legend(title="região")
plt.tight_layout()
plt.show()
plt.close(fig)

# COMMAND ----------

# top 10 gênero × publisher (2010–2019)

df2a = spark.sql(P2A).toPandas()
df2a["combo"] = df2a["genero"] + " × " + df2a["publisher"]
g = df2a.sort_values("vendas_mio")
fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(g["combo"], g["vendas_mio"])
ax.set_title("P2a — Top 10 gênero × publisher (2010–2019)")
ax.set_xlabel("vendas (mi)")
plt.tight_layout()
plt.show()
plt.close(fig)

# COMMAND ----------

# concentração top-5 publishers (2000s vs 2010s)
df2b = spark.sql(P2B).toPandas()
fig, axes = plt.subplots(1, 2, figsize=(13, 5))
for ax, (dec, grp) in zip(axes, df2b.groupby("decada", sort=True)):
    g = grp.sort_values("fatia_do_mercado_da_decada_pct")
    ax.barh(g["publisher"], g["fatia_do_mercado_da_decada_pct"])
    for y, (f, v) in enumerate(zip(g["fatia_do_mercado_da_decada_pct"], g["vendas_mio"])):
        ax.text(f, y, f" {f:.1f}% ({v:.0f} mi)", va="center", fontsize=8)
    ax.set_title(f"Top 5 publishers — {dec}")
    ax.set_xlabel("% do mercado da década")
plt.suptitle("P2b — Concentração de publishers (top 5 por década)", y=1.02)
plt.tight_layout()
plt.show()
plt.close(fig)

# COMMAND ----------

# venda média por faixa de nota
df3a = spark.sql(P3A).toPandas()
fig, ax = plt.subplots(figsize=(9, 5))
ax.bar(df3a["faixa_nota"], df3a["venda_media_mio"], color="steelblue")
for x, (m, n) in enumerate(zip(df3a["venda_media_mio"], df3a["jogos"])):
    ax.text(x, m, f" n={n}", va="bottom", ha="center", fontsize=8)
ax.set_title("P3a — Venda média (mi) por faixa de nota crítica")
ax.set_xlabel("faixa de nota (0–10)")
ax.set_ylabel("venda média (mi)")
plt.tight_layout()
plt.show()
plt.close(fig)

# COMMAND ----------

# top 15 (dos 100) com nota >= 9
df3b = spark.sql(P3B).toPandas().head(15)
df3b["rotulo"] = (df3b["titulo"].str[:32]
                  + " (" + df3b["critic_score"].round(1).astype(str) + ")")
g = df3b.sort_values("vendas_mio")
fig, ax = plt.subplots(figsize=(11, 8))
ax.barh(g["rotulo"], g["vendas_mio"], color="darkorange")
ax.set_title("P3b — Top 15 (dos 100) jogos com nota crítica >= 9")
ax.set_xlabel("vendas (mi)")
plt.tight_layout()
plt.show()
plt.close(fig)