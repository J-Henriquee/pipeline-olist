
---

# 🛒 E-commerce Olist — AWS Data Pipeline

### Overview

An end-to-end data pipeline built on real Brazilian e-commerce data (the Olist public dataset, ~100k orders across 9 relational tables). The goal was to design and build a production-style architecture — from raw ingestion to a dimensional data warehouse — using AWS as the cloud infrastructure.

### Architecture

The pipeline follows a Medallion architecture (Bronze → Silver → Gold):

* **Bronze** — raw CSV files, uploaded untouched to Amazon S3. No transformation happens here; this is the permanent, replayable source of truth.
* **Silver** — cleaned, validated data, written as partitioned Parquet files back to S3. This is where PySpark applies data quality rules (see below).
* **Gold** — a dimensional model (star schema) inside Amazon Redshift Serverless, built for analytical queries.

Extract/Transform is handled by PySpark (Bronze → Silver). Transform/Load into the warehouse is handled by SQL inside Redshift (Silver → Gold) — this project deliberately follows an ELT approach for the Gold layer: raw tables are loaded into staging (`stg_`) tables via the `COPY` command, and the dimensional modeling (joins, surrogate keys, the fact table) is built with SQL directly in the warehouse. This was a conscious choice to get hands-on practice with SQL modeling, rather than doing all transformation in Spark.

### Tech Stack

* **Languages:** Python, SQL
* **Extraction:** Python + boto3
* **Transformation (Bronze → Silver):** PySpark
* **Storage:** Amazon S3 (Data Lake)
* **Data Warehouse:** Amazon Redshift Serverless
* **Credentials:** environment variables via `.env` (never committed)

### Data Quality (Silver layer)

Each of the 9 raw tables was profiled individually before writing any cleaning logic — nulls, duplicates, and value ranges were checked first, and cleaning was only applied where the data actually needed it.

Highlights:

* `orders`: rows marked as "delivered" but missing a required timestamp (approval, carrier handoff, or delivery date) were removed — these represent genuine data integrity issues, not valid business states.
* `customers`, `order_items`: profiled and found already clean (zero nulls, zero duplicates) — passed through Bronze → Silver without modification, documented as a deliberate decision rather than an oversight.
* `products`: joined with the category translation table (PT → EN); non-positive physical measurements (weight, length, height, width) are nulled out rather than dropping the whole product row.
* `sellers`, `geolocation`: city/state strings standardized (trim + case); duplicate `seller_id` / zip-code-prefix rows removed.
* `order_payments`: non-positive payment values and undefined payment types filtered out.
* `order_reviews`: kept only valid review scores (1–5).

> **Note:** No imputation was performed anywhere in the pipeline. Nulls that represent a real business state (e.g. an order that hasn't been delivered yet) are preserved as NULL, not replaced with an inferred value — that kind of decision belongs to data science / analytics, not to the engineering layer.

### Data Modeling (Gold layer)

A star schema was designed around a central fact table:

* `fato_vendas` (fact) — grain: one row per order item. Holds price, freight_value, and foreign keys to every dimension.
* `dim_pedidos`, `dim_produtos`, `dim_vendedores`, `dim_clientes` — standard dimensions, loaded directly from their Silver counterparts.
* `dim_tempo` — a date dimension generated entirely in SQL (there is no source file for it), used twice in the fact table as a role-playing dimension: once for the purchase date, once for the delivery date.
* `dim_avaliacoes`, `dim_pagamentos` — dimensions linked to an order (reviews and payments), rather than to the fact directly.

*Note: Redshift does not enforce PRIMARY KEY / FOREIGN KEY constraints at write time — they're informational only, used by the query planner. Referential integrity in this project is guaranteed by the load process itself (staging tables + filtered inserts), not by the database engine.*

### Business Questions Answered

Three analytical SQL queries validate the model and answer real business questions (full queries in `sql/analytics/`):

1. **General Overview & KPIs (`01_kpi_gerais.sql`)** — What is the total volume, revenue, and average ticket of the e-commerce?
2. **Worst-Rated Product Categories (`02_piores_categorias.sql`)** — Which categories generate the most customer dissatisfaction?
3. **Month-over-Month Revenue Growth (`03_crescimento_mensal.sql`)** — How is the revenue trending, using the `LAG()` window function to calculate MoM % growth?

#### Query Results (Sample Data)

**1. General KPIs**

| Total Orders | Total Revenue | Average Ticket |
| --- | --- | --- |
| 98,643 | R$ 13,588,545.09 | R$ 137.75 |

**2. Worst-Rated Categories (Bottom 4)**

| Category Name | Avg Review Score |
| --- | --- |
| security_and_services | 2.0 |
| diapers_and_hygiene | 3.0 |
| home_confort | 3.0 |
| fashion_male_clothing | 3.0 |

**3. Month-over-Month Growth (Sample 2016-2017)**

| Year-Month | Current Revenue | Previous Revenue | Growth (%) |
| --- | --- | --- | --- |
| 2016-09 | R$ 267.36 | NULL | NULL |
| 2016-10 | R$ 49,507.66 | R$ 267.36 | 18417.22% |
| 2016-12 | R$ 10.90 | R$ 49,507.66 | -99.97% |
| 2017-01 | R$ 118,966.34 | R$ 10.90 | 1091334.31% |
| 2017-02 | R$ 243,484.25 | R$ 118,966.34 | 104.66% |

### Project Structure

```text
03_pipeline_olist/
├── data/
│   └── raw/                          # local copy of the Kaggle CSVs
├── docs/
│   └── architecture.svg              # pipeline architecture diagram
├── sql/
│   ├── create_gold_tables.sql        # DDL for the star schema
│   └── analytics/
│       ├── 01_kpi_gerais.sql
│       ├── 02_piores_categorias.sql
│       └── 03_crescimento_mensal.sql
├── src/
│   ├── upload_to_s3.py               # Bronze layer ingestion
│   ├── schemas.py                    # explicit PySpark schemas (9 tables)
│   └── process_spark.py              # Bronze -> Silver transformation
├── .env                              # local secrets (not committed)
├── .gitignore
├── requirements.txt
└── README.md

```

### How to Run Locally

1. Set up the environment:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

```

2. Create a `.env` file with your AWS credentials:

```env
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret

```

3. Upload the raw Kaggle CSVs to S3 (Bronze layer):

```bash
python src/upload_to_s3.py

```

4. Run the Bronze → Silver transformation:

```bash
python src/process_spark.py

```

5. In the Redshift Query Editor, run `sql/create_gold_tables.sql`, then load the Silver Parquet files via `COPY` into staging tables, and populate the Gold star schema (see the ELT approach above).
6. Run the analytical queries in `sql/analytics/` to reproduce the business insights.

### Known Limitations & Next Steps

* No orchestration tool (Airflow) yet — the pipeline is run manually, step by step. This was a deliberate scope decision to avoid adding Docker + Airflow on top of a first AWS project; it's a natural next step once the current pipeline is stable.
* No automated tests yet on the Spark transformations.
* Could be extended with dbt for the Gold-layer SQL transformations, replacing the current hand-written `INSERT INTO ... SELECT` scripts.