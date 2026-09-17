"""
process_spark.py

Raw -> Silver transformation layer (Medallion architecture).

Reads raw CSV files from the Bronze layer in S3, applies table-specific
data quality rules (null handling, deduplication, type casting, category
translation), and writes the cleaned result as Parquet to the Silver
layer in S3.

Each table has its own `clean_*` function. Tables that were profiled and
found to already be clean pass straight through, with a comment
documenting what was checked.
"""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from schemas import (
    schema_customers, schema_orders, schema_category_translation,
    schema_geolocation, schema_order_items, schema_order_payments,
    schema_order_reviews, schema_products, schema_sellers
)
import pyspark.sql.functions as F


def get_spark_session():
    """
    Builds and returns a SparkSession configured with S3A credentials
    loaded from the local .env file.
    """
    load_dotenv()

    spark = SparkSession.builder \
        .appName("Olist-Raw-To-Silver") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("Spark session started successfully.")
    print(f"Spark version: {spark.version}")
    return spark


def process_raw_to_silver(spark, file_name, schema, clean_function):
    """
    Generic Raw -> Silver runner for a single table.

    Reads the raw CSV from the Bronze layer using the given schema,
    applies the table-specific `clean_function`, and writes the result
    as Parquet to the Silver layer (overwrite mode, so re-running the
    pipeline is idempotent and never duplicates data).
    """
    path_raw = f"s3a://olist-datalake-nean/raw/olist/{file_name}"
    silver_path = f"s3a://olist-datalake-nean/silver/{file_name.replace('.csv', '')}"

    print(f"Reading: {file_name}...")
    df_raw = spark.read.csv(path_raw, schema=schema, header=True)

    print("Applying cleaning rules...")
    df_clean = clean_function(df_raw)

    print(f"Writing Parquet to: {silver_path}...")
    df_clean.write.parquet(silver_path, mode="overwrite")
    print("Done!\n")


def clean_customers(df):
    """
    customers table.
    Profiling confirmed no nulls or duplicates in this table.
    Passed straight through from Raw to Silver.
    """
    return df


def clean_orders(df):
    """
    orders table.

    Removes inconsistent rows where order_status is "delivered" but a
    required timestamp is missing (data integrity issue, not a valid
    business case):
      - "delivered" with no delivery date
      - "delivered" with no approval date
      - "delivered" with no carrier handoff date

    Also casts all date columns to a proper timestamp type.
    """
    is_missing_delivery_date = (
        (F.col("order_status") == "delivered")
        & (F.col("order_delivered_customer_date").isNull())
    )

    is_missing_approval_date = (
        (F.col("order_status") == "delivered")
        & (F.col("order_approved_at").isNull())
    )

    is_missing_carrier_date = (
        (F.col("order_status") == "delivered")
        & (F.col("order_delivered_carrier_date").isNull())
    )

    df = df.filter(~is_missing_delivery_date) \
           .filter(~is_missing_approval_date) \
           .filter(~is_missing_carrier_date)

    date_columns = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in date_columns:
        df = df.withColumn(col, F.to_timestamp(F.col(col)))

    return df


def clean_order_items(df):
    """
    order_items table.

    Profiling confirmed:
      1. Zero nulls found.
      2. Zero duplicates on the composite key (order_id, order_item_id).
      3. Financial sanity validated (no price <= 0 or freight_value < 0).

    Table is already clean. Passed straight through to Silver.
    """
    return df


def clean_products(df, df_translation):
    """
    products table.

    - Joins with the category translation table to convert
      product_category_name from Portuguese to English.
    - Explicitly selects and casts every column to enforce a fixed
      schema and column order (protects downstream loads from silent
      type mismatches).
    - Treats non-positive values in the physical measurement columns
      (weight/length/height/width) as invalid and nulls them out,
      instead of dropping the whole row.
    """
    # 1. Resolve the category translation join first
    df_clean = df.join(df_translation, on="product_category_name", how="left")
    df_clean = df_clean.drop("product_category_name") \
                        .withColumnRenamed("product_category_name_english", "product_category_name")

    # 2. Explicit selection: enforce casting and exact column order.
    # Non-positive measurements are nulled out rather than dropped.
    df_final = df_clean.select(
        F.col("product_id").cast("string"),
        F.col("product_category_name").cast("string"),
        F.col("product_name_lenght").cast("integer"),
        F.col("product_description_lenght").cast("integer"),
        F.col("product_photos_qty").cast("integer"),
        F.when(F.col("product_weight_g").cast("integer") <= 0, F.lit(None).cast("integer"))
         .otherwise(F.col("product_weight_g").cast("integer")).alias("product_weight_g"),
        F.when(F.col("product_length_cm").cast("integer") <= 0, F.lit(None).cast("integer"))
         .otherwise(F.col("product_length_cm").cast("integer")).alias("product_length_cm"),
        F.when(F.col("product_height_cm").cast("integer") <= 0, F.lit(None).cast("integer"))
         .otherwise(F.col("product_height_cm").cast("integer")).alias("product_height_cm"),
        F.when(F.col("product_width_cm").cast("integer") <= 0, F.lit(None).cast("integer"))
         .otherwise(F.col("product_width_cm").cast("integer")).alias("product_width_cm"),
    )

    return df_final


def clean_sellers(df):
    """
    sellers table.
    Standardizes city (lowercase, trimmed) and state (uppercase, trimmed)
    formatting, and removes duplicate seller_id rows.
    """
    df = df.withColumn("seller_city", F.lower(F.trim(F.col("seller_city"))))
    df_clean = df.withColumn("seller_state", F.upper(F.trim(F.col("seller_state"))))
    df_clean = df_clean.dropDuplicates(["seller_id"])
    return df_clean


def clean_order_payments(df):
    """
    order_payments table.
    Filters out invalid payment records: non-positive payment values
    and payments with an undefined payment type.
    """
    df_clean = df.filter(
        (F.col("payment_value") > 0) & (F.col("payment_type") != "not_defined")
    )
    return df_clean


def clean_geolocation(df):
    """
    geolocation table.
    Deduplicates by zip code prefix (the raw file has multiple
    lat/lng pairs per prefix) and standardizes city/state formatting.
    """
    df = df.dropDuplicates(["geolocation_zip_code_prefix"])
    df = df.withColumn("geolocation_city", F.lower(F.trim(F.col("geolocation_city"))))
    df_clean = df.withColumn("geolocation_state", F.upper(F.trim(F.col("geolocation_state"))))
    return df_clean


def clean_order_reviews(df):
    """
    order_reviews table.
    Keeps only rows with a valid review_score (1 to 5), discarding
    corrupted or out-of-range values.
    """
    df_clean = df.filter(F.col("review_score").between(1, 5))
    return df_clean


if __name__ == "__main__":
    spark = get_spark_session()

    # Category translation table is read once and reused across the
    # products cleaning step (passed in via the lambda below).
    path_translation = "s3a://olist-datalake-nean/raw/olist/product_category_name_translation.csv"
    df_translation = spark.read.csv(path_translation, schema=schema_category_translation, header=True)

    # Table registry: maps each raw file to its schema and cleaning function.
    # The loop below drives the whole Raw -> Silver process from this list.
    raw_values = [
        {"file_name": "olist_customers_dataset.csv", "schema": schema_customers, "clean_function": clean_customers},
        {"file_name": "olist_orders_dataset.csv", "schema": schema_orders, "clean_function": clean_orders},
        {"file_name": "olist_order_items_dataset.csv", "schema": schema_order_items, "clean_function": clean_order_items},
        {"file_name": "olist_products_dataset.csv", "schema": schema_products, "clean_function": lambda df: clean_products(df, df_translation)},
        {"file_name": "olist_sellers_dataset.csv", "schema": schema_sellers, "clean_function": clean_sellers},
        {"file_name": "olist_order_payments_dataset.csv", "schema": schema_order_payments, "clean_function": clean_order_payments},
        {"file_name": "olist_geolocation_dataset.csv", "schema": schema_geolocation, "clean_function": clean_geolocation},
        {"file_name": "olist_order_reviews_dataset.csv", "schema": schema_order_reviews, "clean_function": clean_order_reviews},
    ]

    for raw_dict in raw_values:
        process_raw_to_silver(
            spark=spark,
            file_name=raw_dict["file_name"],
            schema=raw_dict["schema"],
            clean_function=raw_dict["clean_function"]
        )
