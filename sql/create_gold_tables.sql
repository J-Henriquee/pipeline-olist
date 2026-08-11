-- ============================================================
-- Gold layer DDL — Star Schema for the Olist e-commerce pipeline
-- ============================================================
-- Note: Redshift does not enforce PRIMARY KEY / FOREIGN KEY
-- constraints at write time — they are informational only, used
-- by the query planner. Referential integrity is guaranteed by
-- the ETL load process instead (see analytics/ queries for
-- orphan-record checks).

CREATE SCHEMA IF NOT EXISTS camada_gold;

-- ------------------------------------------------------------
-- Independent dimensions
-- ------------------------------------------------------------

CREATE TABLE camada_gold.dim_pedidos (
    order_id VARCHAR PRIMARY KEY,
    customer_id VARCHAR,
    order_status VARCHAR,
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP
);

CREATE TABLE camada_gold.dim_produtos (
    product_id VARCHAR PRIMARY KEY,
    product_category_name VARCHAR,
    product_category_name_english VARCHAR,
    product_name_lenght INTEGER,
    product_description_lenght INTEGER,
    product_photos_qty INTEGER,
    product_weight_g DOUBLE PRECISION,
    product_length_cm DOUBLE PRECISION,
    product_height_cm DOUBLE PRECISION,
    product_width_cm DOUBLE PRECISION
);

CREATE TABLE camada_gold.dim_vendedores (
    seller_id VARCHAR PRIMARY KEY,
    seller_zip_code_prefix VARCHAR,
    seller_city VARCHAR,
    seller_state VARCHAR
);

CREATE TABLE camada_gold.dim_clientes (
    customer_id VARCHAR PRIMARY KEY,
    customer_unique_id VARCHAR,
    customer_zip_code_prefix VARCHAR,
    customer_city VARCHAR,
    customer_state VARCHAR
);

-- Date dimension, generated from scratch via generate_series
-- (there is no source file for this — it's built directly in SQL).
CREATE TABLE camada_gold.dim_tempo (
    data_id INTEGER PRIMARY KEY,     -- format: YYYYMMDD
    data_completa DATE,
    ano INTEGER,
    mes INTEGER,
    dia INTEGER,
    dia_da_semana VARCHAR
);

-- ------------------------------------------------------------
-- Dependent dimensions (linked to an order)
-- ------------------------------------------------------------

CREATE TABLE camada_gold.dim_avaliacoes (
    review_id VARCHAR PRIMARY KEY,
    order_id VARCHAR REFERENCES camada_gold.dim_pedidos(order_id),
    review_score INTEGER,
    review_comment_title VARCHAR(2000),
    review_comment_message VARCHAR(2000),
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP
);

CREATE TABLE camada_gold.dim_pagamentos (
    order_id VARCHAR REFERENCES camada_gold.dim_pedidos(order_id),
    payment_sequential INTEGER,
    payment_type VARCHAR,
    payment_installments INTEGER,
    payment_value DOUBLE PRECISION,
    PRIMARY KEY (order_id, payment_sequential)
);

-- ------------------------------------------------------------
-- Fact table
-- ------------------------------------------------------------
-- Grain: one row per order item (not per order). Each item keeps
-- its own product, seller, price and freight value.

CREATE TABLE camada_gold.fato_vendas (
    order_id VARCHAR REFERENCES camada_gold.dim_pedidos(order_id),
    order_item_id INTEGER,
    product_id VARCHAR REFERENCES camada_gold.dim_produtos(product_id),
    seller_id VARCHAR REFERENCES camada_gold.dim_vendedores(seller_id),
    customer_id VARCHAR REFERENCES camada_gold.dim_clientes(customer_id),
    price DOUBLE PRECISION,
    freight_value DOUBLE PRECISION,
    -- dim_tempo used twice as a role-playing dimension:
    -- once for the purchase date, once for the delivery date.
    order_purchase_date_id INTEGER REFERENCES camada_gold.dim_tempo(data_id),
    order_delivery_date_id INTEGER REFERENCES camada_gold.dim_tempo(data_id),
    PRIMARY KEY (order_id, order_item_id)
);