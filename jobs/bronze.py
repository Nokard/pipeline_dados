#!/usr/bin/env python3
from pyspark.sql import SparkSession
from config import S3_BRONZE, SPARK_MASTER, SPARK_APP_NAME
import sys

spark = SparkSession.builder \
    .appName(f"{SPARK_APP_NAME}-bronze") \
    .master(SPARK_MASTER) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.261") \
    .getOrCreate()

try:
    print("=" * 60)
    print("📦 BRONZE LAYER: Extração de dados brutos")
    print("=" * 60)

    # Lê dados do CSV local
    df_bronze = spark.read.option("header", "true").csv("data/dados.csv")

    print(f"\n✅ Dados carregados: {df_bronze.count()} registros")
    df_bronze.show(5)

    # Salva em Bronze
    df_bronze.write.mode("overwrite").option("header", "true").csv(f"{S3_BRONZE}/dados")
    print(f"\n💾 Salvo em: {S3_BRONZE}/dados")

    print("\n" + "=" * 60)
    print("✅ BRONZE CONCLUÍDO COM SUCESSO!")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
