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
    regra_tempo_irreal = (F.col("order_delivered_customer_date") < F.col("order_purchase_timestamp"))
    print(df.filter(regra_tempo_irreal).count())

    return df


if __name__ == "__main__":
    spark = get_spark_session()

    raw_values = [{"file_name": "olist_customers_dataset.csv", "schema": schema_customers, "clean_function": clean_customers}, 
                  {"file_name": "olist_orders_dataset.csv", "schema": schema_orders, "clean_function": clean_orders}]

    for raw_dict in raw_values:
        process_raw_to_silver(
            spark=spark,
            file_name=raw_dict["file_name"],
            schema=raw_dict["schema"],                 
            clean_function=raw_dict["clean_function"]     
            )
 