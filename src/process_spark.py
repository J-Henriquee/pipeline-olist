import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession

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