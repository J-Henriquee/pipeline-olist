import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from schemas import schema_customers, schema_orders, schema_category_translation, schema_geolocation, schema_order_items, schema_order_payments, schema_order_reviews, schema_products, schema_sellers
import pyspark.sql.functions as F


def get_spark_session():
    # Carrega suas chaves da AWS do .env
    load_dotenv()

    # Inicia a Sessão do Spark baixando o conector do S3
    spark = SparkSession.builder \
        .appName("Olist-Raw-To-Silver") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    print("🔥 Sessão PySpark iniciada com sucesso!")
    print(f"Versão do Spark: {spark.version}")
    return spark
def process_raw_to_silver(spark, file_name, schema, clean_function):
    path_raw = f"s3a://olist-datalake-nean/raw/olist/{file_name}"
    silver_path = f"s3a://olist-datalake-nean/silver/{file_name.replace('.csv', '')}"
    
    print(f"Lendo: {file_name}...")
    df_raw = spark.read.csv(path_raw, schema=schema, header=True)
    
    print("Aplicando limpeza...")
    df_clean = clean_function(df_raw)
    
    print(f"Salvando Parquet em: {silver_path}...")
    df_clean.write.parquet(silver_path, mode="overwrite")
    print("Sucesso!\n")

def clean_customers(df):
# O profiling confirmou que a tabela de cadastro não possui nulos ou duplicatas.
    # Passagem direta de Raw para Silver.
    return df 

def clean_orders(df):
    regra_fantasmas = (((F.col("order_status") == "delivered") 
                          & (F.col("order_delivered_customer_date").isNull())))
    
    regra_sem_pagamento = ((F.col("order_status") == "delivered") &
                              (F.col("order_approved_at").isNull()))
    
    regra_teletransporte =  ((F.col("order_status") == "delivered") & 
                               (F.col("order_delivered_carrier_date").isNull()))
    
    df = df.filter(~regra_fantasmas) \
           .filter(~regra_sem_pagamento) \
           .filter(~regra_teletransporte) 
           
    colunas_data = [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date"
    ]
    for col in colunas_data:
        df = df.withColumn(col, F.to_timestamp(F.col(col)))
        
    return df

def clean_order_items(df):
    # PROFILING REALIZADO:
    # 1. Zero nulos encontrados.
    # 2. Zero duplicatas na chave composta (order_id, order_item_id).
    # 3. Sanidade financeira validada (Nenhum price <= 0 ou freight_value < 0).
    # Tabela íntegra. Passagem direta para a Silver.

    return df

def clean_products(df, df_translation):
    # 1. Resolve o join e o nome da categoria primeiro
    df_limpo = df.join(df_translation, on="product_category_name", how="left")
    df_limpo = df_limpo.drop("product_category_name") \
                       .withColumnRenamed("product_category_name_english", "product_category_name")

    # 2. SELEÇÃO EXPLÍCITA: Crava a tipagem (cast) e a ORDEM EXATA das colunas
    # Tratando os zeros e garantindo que até o nulo seja lido como Inteiro (INT32)
    df_final = df_limpo.select(
        F.col("product_id").cast("string"),
        F.col("product_category_name").cast("string"),
        F.col("product_name_lenght").cast("integer"),
        F.col("product_description_lenght").cast("integer"),
        F.col("product_photos_qty").cast("integer"),
        F.when(F.col("product_weight_g").cast("integer") <= 0, F.lit(None).cast("integer")).otherwise(F.col("product_weight_g").cast("integer")).alias("product_weight_g"),
        F.when(F.col("product_length_cm").cast("integer") <= 0, F.lit(None).cast("integer")).otherwise(F.col("product_length_cm").cast("integer")).alias("product_length_cm"),
        F.when(F.col("product_height_cm").cast("integer") <= 0, F.lit(None).cast("integer")).otherwise(F.col("product_height_cm").cast("integer")).alias("product_height_cm"),
        F.when(F.col("product_width_cm").cast("integer") <= 0, F.lit(None).cast("integer")).otherwise(F.col("product_width_cm").cast("integer")).alias("product_width_cm")
    )

    return df_final

def clean_sellers(df):
    df = df.withColumn('seller_city', F.lower(F.trim(F.col("seller_city"))))
    df_limpo = df.withColumn('seller_state', F.upper(F.trim(F.col("seller_state"))))
    df_limpo = df_limpo.dropDuplicates(["seller_id"])
    df_limpo.show(5)
    return df_limpo

def clean_order_payments(df):
    df_limpo =  df.filter((F.col("payment_value") > 0) & (F.col("payment_type") != "not_defined"))
    return df_limpo

def clean_geolocation(df):
    df = df.dropDuplicates(["geolocation_zip_code_prefix"])   
    df = df.withColumn('geolocation_city', F.lower(F.trim(F.col("geolocation_city"))))
    df_limpo = df.withColumn('geolocation_state', F.upper(F.trim(F.col("geolocation_state"))))

    return df_limpo
    

def clean_order_reviews(df):
    df_limpo = df.filter(F.col("review_score").between(1, 5))
    return df_limpo
    




if __name__ == "__main__":
    spark = get_spark_session()
    path_translation = "s3a://olist-datalake-nean/raw/olist/product_category_name_translation.csv" 
    df_translation = spark.read.csv(path_translation, schema=schema_category_translation, header=True)
    # Lendo a tabela de tradução lá do seu bucket Raw
    raw_values = [{"file_name": "olist_customers_dataset.csv", "schema": schema_customers, "clean_function": clean_customers}, 
                  {"file_name": "olist_orders_dataset.csv", "schema": schema_orders, "clean_function": clean_orders},
                  {"file_name": "olist_order_items_dataset.csv", "schema": schema_order_items, "clean_function": clean_order_items},
                 {"file_name": "olist_products_dataset.csv", "schema": schema_products, "clean_function": lambda df: clean_products(df, df_translation)},
                  {"file_name": "olist_sellers_dataset.csv", "schema": schema_sellers, "clean_function": clean_sellers},
                                {"file_name": "olist_order_payments_dataset.csv", "schema": schema_order_payments, "clean_function": clean_order_payments},
                {"file_name": "olist_geolocation_dataset.csv", "schema": schema_geolocation, "clean_function": clean_geolocation},
                 {"file_name": "olist_order_reviews_dataset.csv", "schema": schema_order_reviews, "clean_function": clean_order_reviews}]

    for raw_dict in raw_values:
        process_raw_to_silver(
            spark=spark,
            file_name=raw_dict["file_name"],
            schema=raw_dict["schema"],                 
            clean_function=raw_dict["clean_function"]     
            )







 