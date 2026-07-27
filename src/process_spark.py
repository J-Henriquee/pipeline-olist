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
    df.show(5)
    df.printSchema(1)
    print(df.count() - df.dropDuplicates().count())
    df.select([F.sum(F.when(F.col(c).isNull() | F.isnan(c), 1).otherwise(0)).alias(c) for c in df.columns]).show()
    print(df.select('customer_state').distinct().count())

    df_teste = df.withColumn("cidade_trim", F.trim(F.col("customer_city"))) \
                .withColumn("cidade_lower", F.lower(F.col("customer_city"))) \
                .withColumn("cidade_perfeita", F.lower(F.trim(F.col("customer_city"))))

    print("Distintos Originais:", df_teste.select("customer_city").distinct().count())
    print("Distintos só com Trim:", df_teste.select("cidade_trim").distinct().count())
    print("Distintos só com Lower:", df_teste.select("cidade_lower").distinct().count())
    print("Distintos Combinados (Lower + Trim):", df_teste.select("cidade_perfeita").distinct().count())

    return df 



if __name__ == "__main__":
    spark = get_spark_session()
    
    process_raw_to_silver(
        spark=spark,
        file_name="olist_customers_dataset.csv", 
        schema=schema_customers,                 
        clean_function=clean_customers           
    )