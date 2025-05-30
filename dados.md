Claro! Abaixo está um relatório completo em formato Markdown sobre o **dataset NYC Taxi Trips**, incluindo a modelagem dos dados, descrição detalhada de cada campo e links úteis para consulta e uso em projetos de análise ou machine learning.

---

# 🗽 NYC Taxi Trips Dataset – Relatório Técnico

## 📦 Visão Geral

O **NYC Taxi Trips Dataset** é um conjunto de dados públicos disponibilizado pela **New York City Taxi and Limousine Commission (TLC)**. Ele contém registros detalhados de corridas de táxi em Nova York, abrangendo:

* **Yellow Taxis** (táxis tradicionais)
* **Green Taxis** (táxis de bairros periféricos)
* **For-Hire Vehicles (FHV)**: serviços como Uber, Lyft, Via e Juno

Cada linha representa uma corrida individual, com informações sobre horários, localizações, tarifas, formas de pagamento e outros detalhes relevantes.

---

## 📊 Esquema de Dados (Yellow Taxi)

Abaixo está a descrição dos campos presentes no dataset dos Yellow Taxis:

| Campo                   | Tipo de Dado | Descrição                                                                                                                                      |
| ----------------------- | ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| `VendorID`              | Integer      | Código do provedor de tecnologia: 1 = Creative Mobile Technologies, LLC; 2 = VeriFone Inc.                                                     |
| `tpep_pickup_datetime`  | Timestamp    | Data e hora de início da corrida.                                                                                                              |
| `tpep_dropoff_datetime` | Timestamp    | Data e hora de término da corrida.                                                                                                             |
| `passenger_count`       | Integer      | Número de passageiros. Valor inserido manualmente pelo motorista.                                                                              |
| `trip_distance`         | Float        | Distância percorrida em milhas, conforme registrado pelo taxímetro.                                                                            |
| `RatecodeID`            | Integer      | Código da tarifa aplicada: 1 = Tarifa padrão; 2 = JFK; 3 = Newark; 4 = Nassau ou Westchester; 5 = Tarifa negociada; 6 = Corrida compartilhada. |
| `store_and_fwd_flag`    | String       | Indica se os dados foram armazenados no veículo antes de serem enviados: 'Y' = sim; 'N' = não.                                                 |
| `PULocationID`          | Integer      | ID da zona de coleta (pickup), conforme definido pela TLC.                                                                                     |
| `DOLocationID`          | Integer      | ID da zona de desembarque (dropoff), conforme definido pela TLC.                                                                               |
| `payment_type`          | Integer      | Método de pagamento: 1 = Cartão de crédito; 2 = Dinheiro; 3 = Sem cobrança; 4 = Disputa; 5 = Desconhecido; 6 = Corrida anulada.                |
| `fare_amount`           | Float        | Valor da tarifa base calculada pelo taxímetro.                                                                                                 |
| `extra`                 | Float        | Encargos adicionais, como taxas de horário de pico ou noturnas.                                                                                |
| `mta_tax`               | Float        | Taxa de US\$0,50 destinada à Metropolitan Transportation Authority.                                                                            |
| `tip_amount`            | Float        | Valor da gorjeta. Preenchido automaticamente para pagamentos com cartão de crédito; gorjetas em dinheiro não são registradas.                  |
| `tolls_amount`          | Float        | Total de pedágios pagos durante a corrida.                                                                                                     |
| `improvement_surcharge` | Float        | Taxa de melhoria de US\$0,30 aplicada a todas as corridas desde 2015.                                                                          |
| `total_amount`          | Float        | Valor total cobrado ao passageiro, excluindo gorjetas em dinheiro.                                                                             |
| `congestion_surcharge`  | Float        | Taxa adicional aplicada em áreas de congestionamento, conforme regulamentação local.                                                           |
| `airport_fee`           | Float        | Taxa de US\$1,25 aplicada para corridas com origem nos aeroportos LaGuardia ou JFK.                                                            |

> 📚 Fonte: [GitHub - NYC Yellow Taxi Analysis](https://github.com/cvivieca/nyc-yellow-taxi-analysis/blob/master/README.md)

---

## 🗺️ Mapas e Localizações

* **Zonas de Táxi (Taxi Zones):** A TLC divide a cidade em zonas numeradas para identificar locais de coleta e desembarque. Os arquivos relacionados incluem:

  * [Tabela de Consulta de Zonas de Táxi (CSV)](https://www.nyc.gov/assets/tlc/downloads/pdf/taxi_zone_lookup.csv)
  * [Shapefile das Zonas de Táxi (ZIP)](https://www.nyc.gov/assets/tlc/downloads/pdf/taxi_zones.zip)
  * Mapas por bairro:

    * [Manhattan](https://www.nyc.gov/assets/tlc/images/content/pages/taxi_zone_map_manhattan.jpg)
    * [Brooklyn](https://www.nyc.gov/assets/tlc/images/content/pages/taxi_zone_map_brooklyn.jpg)
    * [Queens](https://www.nyc.gov/assets/tlc/images/content/pages/taxi_zone_map_queens.jpg)
    * [Bronx](https://www.nyc.gov/assets/tlc/images/content/pages/taxi_zone_map_bronx.jpg)
    * [Staten Island](https://www.nyc.gov/assets/tlc/images/content/pages/taxi_zone_map_staten_island.jpg)

> 🔗 Fonte: [FHV Trip Record Data - TLC](https://www.nyc.gov/site/tlc/about/fhv-trip-record-data.page)

---

## 🧱 Modelagem de Dados

Para projetos de análise ou machine learning, é comum utilizar ferramentas como Apache Spark e Hive para processar e armazenar os dados. Abaixo está um exemplo de esquema utilizado para criar tabelas Hive com os dados dos Yellow Taxis:

```python
from pyspark.sql.types import StructType, StructField, IntegerType, TimestampType, DoubleType, StringType

yellow_taxi_schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("airport_fee", DoubleType(), True)
])
```

> 📁 Exemplo de uso: [GitHub - alaminxtration/NYCTaxi](https://github.com/alaminxtration/NYCTaxi)

---

## 📂 Fontes Oficiais e Documentação

* **Portal de Dados da TLC:** [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
* **Dicionários de Dados:**

  * [Yellow Taxi Data Dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)
  * [Green Taxi Data Dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)
  * [FHV Data Dictionary (PDF)](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf)
* **Relatórios Agregados Mensais:** [Aggregated Reports - TLC](https://www.nyc.gov/site/tlc/about/aggregated-reports.page)

---

## 🧠 Aplicações e Casos de Uso

* **Análise de Mobilidade Urbana:** Estudo de padrões de deslocamento e identificação de áreas com alta demanda por transporte.
* **Modelagem Preditiva:** Previsão de demanda por táxis, estimativa de tempo de viagem e análise de tarifas.
* **Planejamento Urbano:** Apoio na tomada de decisões para melhorias na infraestrutura de transporte.
* **Benchmarking de Tecnologias:** Avaliação de desempenho de sistemas de processamento de dados em larga escala.

---

Se desejar, posso auxiliar na criação de pipelines de ingestão de dados no Databricks, elaboração de dashboards interativos ou desenvolvimento de modelos de machine learning utilizando esse dataset. Basta informar o escopo do seu projeto!
