# game-market-evolution
MVP de Engenharia de Dados (PUC-Rio): pipeline de dados ponta a ponta (bronze/silver/gold) em Delta Lake no Databricks Free Edition sobre o mercado mundial de jogos eletrônicos 1980-2024.

Objetivo:
Ingerir um CSV único do Kaggle (licença CC0, 64.016 registros), aplicar regras nomeadas de padronização (R1-R8) e deduplicação, materializar um esquema estrela (fato + 5 dimensões) com view para consultas de negócio, e responder 6 perguntas de negócio: dominância de plataformas por década, distribuição regional das vendas, concentração de publicadoras nos anos 2000 e 2010 e a relação entre nota crítica e vendas.

Tecnologias:

    Databricks Free Edition (PySpark, Unity Catalog, Delta Lake).
    Notebooks Python para o pipeline e SQL para a análise de negócio.

Estrutura do repositório:

    notebooks/ - os 3 notebooks do pipeline (bronze, silver, gold), com as saídas das execuções
    sql/respostas_perguntas.sql - as 6 queries de negócio comentadas
    catalogo_dados.md - catálogo das tabelas, campo a campo
    evidencias/ - 29 figuras (prints da plataforma e saídas das queries)
    relatorio.pdf - relatório completo do projeto

Como executar:
O pipeline é reproduzível em qualquer workspace do Databricks Free Edition:

    Criar o catálogo mvp_jogos (schemas bronze, silver, gold) e o volume bronze/volumes_jogos.
    Enviar o CSV bruto para o volume.
    Executar os notebooks em ordem (01, 02, 03).

A reexecução é idempotente: o DESCRIBE DETAIL devolve o mesmo tableId e sizeInBytes, e o arquivo de origem é conferido por SHA-256 na própria carga.

Dados:
Video Game Sales & Industry Data (1980-2024), Kaggle (Bhushan Divekar). Licença CC0 1.0 (domínio público).
