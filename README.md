# NYC Taxi Data - Robust Spark Pipeline Project

## 📅 Contexto
O dataset de corridas de táxi de Nova York cobre dados de 2009 a 2025, com diferentes formatos, esquemas e colunas ao longo dos anos. Esse projeto demonstra como construir um pipeline **robusto e escalável com PySpark no Databricks**, tratando inconsistências de schema e gerando insights de valor.

---

## 🔍 Objetivos

- Ingerir e consolidar dados de múltiplos anos com schemas variados.
- Tratar anomalias e padronizar colunas e tipos.
- Criar camadas **bronze**, **silver** e **gold** com Delta Lake.
- Orquestrar com **Databricks Jobs** (ou DLT, opcional).
- Gerar KPIs e insights exploratórios.

---

## 📊 Estrutura de Pastas

```bash
nyc-taxi-pipeline/
├── notebooks/
│   ├── 1_bronze_ingestion.py         # Ingestão inicial e unificação dos schemas
│   ├── 2_silver_transformation.py    # Limpeza e enriquecimento
│   ├── 3_gold_aggregations.py        # Agregações e KPIs
├── configs/
│   └── schema_versions.json          # Mapeamento de colunas por ano (se precisar)
├── README.md
└── requirements.txt
```

---

## 📊 Notebooks e Funções-Chave

### `1_bronze_ingestion.py`
- Lê dados por ano/mês
- Padroniza nomes e tipos de colunas
- Adiciona colunas de particionamento (`year`, `month`)
- Escreve em Delta Lake (camada bronze)

### `2_silver_transformation.py`
- Remove registros inconsistentes (tarifa negativa, distância zero, etc.)
- Enriquece com colunas como: `duration_min`, `day_of_week`, `hour_of_day`
- Trata nulls e tipos definitivos

### `3_gold_aggregations.py`
- Calcula KPIs: média de tarifa, total de corridas, receita por bairro/horário
- Escreve tabelas Delta otimizadas para BI

---

## 🛠️ Tratamento de Schemas Variáveis

```python
# Exemplo: padronizando diferentes nomes de colunas por ano
schema_map = {
  "fare_amount": ["fare_amount", "FareAmt"],
  "pickup_datetime": ["tpep_pickup_datetime", "pickup_datetime", "pickup_time"],
  ...
}

def unify_schema(df, year):
    for std_col, variants in schema_map.items():
        for col_name in variants:
            if col_name in df.columns:
                df = df.withColumnRenamed(col_name, std_col)
                break
    return df
```

---

## 🌟 Features Técnicas Demonstradas

- Spark + Delta Lake com particionamento
- Leitura resiliente a schemas diferentes
- Pipelines modulares com Jobs
- Versionamento de dados com Delta
- Enriquecimento temporal (hora, dia da semana)
- Escrita eficiente com partição e modo `overwrite`

---

## 👁️ Possíveis KPIs no Gold Layer

| KPI | Descrição |
|-----|------------|
| `avg_fare_by_hour` | Tarifa média por hora |
| `trips_per_borough` | Total de corridas por região |
| `avg_duration_by_dow` | Tempo médio por dia da semana |
| `top_10_pickup_hours` | Horários com maior volume de corridas |

---

## 🚀 Orquestração
- Cada notebook pode ser agendado via **Databricks Jobs** com dependências.
- Para maior valor: use `dbutils.widgets` para parametrização de ano/mês e cluster.

---

## 🌐 Publicação no Medium e LinkedIn

**Sugestão de título para Medium:**  
"Como construí um pipeline robusto com Spark e Delta para analisar 15 anos de corridas de táxi em NY"

**Sugestão de post para LinkedIn:**  
> Usei Spark no Databricks para transformar um dataset desafiador e inconsistente em insights de valor sobre mobilidade urbana. Trabalhei com padronização de schemas, Delta Lake e agregados inteligentes. Em breve compartilho tudo no Medium! #engenhariadedados #spark #databricks

---

## 🔧 Requisitos

```txt
pyspark>=3.3
python>=3.8
databricks-connect (se rodar local)
```

---

## ✅ Resultados esperados
- Base limpa, confiável e historicamente consistente.
- KPIs prontos para BI e storytelling.
- Demonstração clara de conhecimento prático e escalabilidade.

---

## ✨ Autor
**Heitor Felix de Oliveira**  
[LinkedIn](https://www.linkedin.com/in/heitor-felix/) | [Portfólio](https://heitorfe.github.io/portfolio-projetos/)