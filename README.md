# game-market-evolution

MVP de Engenharia de Dados (PUC-Rio): pipeline de dados ponta a ponta (bronze/silver/gold) em Delta Lake no Databricks Free Edition sobre o mercado mundial de jogos eletrônicos, 1980-2024.

[Relatório completo do projeto (PDF)](./relatorio.pdf) — contém o catálogo de dados campo a campo, as seis queries de negócio comentadas e as 29 figuras de evidência.
Objetivo

Ingerir um CSV único do Kaggle (licença CC0, 64.016 registros), aplicar regras nomeadas de padronização (R1-R8) e deduplicação, materializar um esquema estrela (fato + 5 dimensões) com uma view para consultas de negócio, e responder a 6 perguntas de negócio:

- Dominância de plataformas por década — quais consoles lideraram cada década e com que fatia das vendas do período.
- Distribuição regional das vendas — como o mercado se dividiu entre América do Norte, Japão, PAL e demais regiões ao longo das décadas.
- Concentração de publicadoras — quais combinações de gênero × publicadora lideraram 2010-2019 e como a fatia do top 5 mudou dos anos 2000 para os anos 2010.
- Crítica × vendas — venda média por faixa de nota crítica e o top 100 de vendas entre os jogos com nota 9 ou superior.

Tecnologias

- Databricks Free Edition — PySpark, Unity Catalog, Delta Lake.
- Notebooks Python para o pipeline e SQL para a análise de negócio.
- Matplotlib para os gráficos das respostas.

Arquitetura

Arquitetura medallion sobre Delta Lake, com um notebook por camada:

    CSV (Kaggle)
      └─> Volume  mvp_jogos.bronze.volumes_jogos
        └─> [01] bronze.vgsales_raw        64.016 linhas, 22 colunas (linhagem + SHA-256)
              └─> [02] silver.jogos_vendas 63.791 linhas, 31 colunas (R1-R8 + dedup)
                    └─> [03] gold          5 dimensões + fato_vendas_jogo + view v_jogo_vendas
                          └─> 6 queries de negócio + 6 gráficos

Estrutura do repositório

- 01_bronze_ingestao.ipynb	Ingestão, carimbo de linhagem (incluindo SHA-256) e carga Delta. Exportado com as saídas da última execução.
- 02_silver_limpeza.ipynb	Regras R1-R8, deduplicação e perfil de qualidade (DMBOK). Exportado com as saídas da última execução.
- 03_gold_modelagem.ipynb	Esquema estrela, view, as 6 queries de negócio e os gráficos. Exportado com as saídas da última execução.
- relatorio.pdf	Relatório completo: catálogo de dados, queries comentadas e as 29 figuras de evidência.
- README.md	Este arquivo.

    O catálogo de dados campo a campo, as queries de negócio e todas as evidências de execução (prints da plataforma e saídas das consultas) estão consolidados no relatorio.pdf, conforme o item 5 da especificação da entrega.

Como executar

O pipeline é reproduzível em qualquer workspace do Databricks Free Edition:

    Criar o catálogo mvp_jogos com os schemas bronze, silver e gold, e o volume mvp_jogos.bronze.volumes_jogos.
    Baixar o CSV do Kaggle, renomeá-lo para vgsales_1980_2024_raw.csv e enviá-lo ao volume pelo Catalog Explorer.
    Executar os notebooks na ordem: 01 → 02 → 03.

Reprodutibilidade verificada: a reexecução completa é idempotente — o DESCRIBE DETAIL do fato devolve o mesmo tableId e o mesmo sizeInBytes (757.902 bytes), as chaves substitutas são determinísticas (pmod(hash(...)), e não monotonically_increasing_id) e as contagens da camada gold permanecem em 39.788 / 81 / 20 / 3.381 / 51 / 63.791.

O SHA-256 do arquivo de origem (33780cc7…f0d1) foi calculado no momento da coleta e é carimbado em todas as linhas da camada bronze, permitindo conferir a procedência do dado. A conferência automática desse hash durante a carga está registrada como ponto de melhoria no relatório.
Dados e licença

Video Game Sales & Industry Data (1980-2024) — Kaggle, por Bhushan Divekar.
Licença CC0 1.0 Universal (domínio público): uso irrestrito, inclusive comercial, sem atribuição obrigatória.

O conjunto descreve produtos comerciais (jogos, plataformas, publicadoras) e não contém dados pessoais, de modo que a LGPD é inaplicável e não houve necessidade de anonimização.
Observações sobre o ambiente

A Free Edition não oferece agendamento de pipelines (Databricks Workflows) nem integração nativa com o GitHub (Databricks Repos). Por isso, o pipeline roda sob demanda e os notebooks foram exportados manualmente para este repositório — mesmo caminho documentado no trabalho de referência do curso. Ambas as limitações, e suas alternativas, estão discutidas no relatório.
