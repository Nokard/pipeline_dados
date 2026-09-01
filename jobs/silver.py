#!/usr/bin/env python3
from pyspark.sql import SparkSession
from config import S3_BRONZE, S3_SILVER, SPARK_MASTER, SPARK_APP_NAME
import sys

spark = SparkSession.builder \
    .appName(f"{SPARK_APP_NAME}-silver") \
    .master(SPARK_MASTER) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localstack:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

try:
    print("=" * 60)
    print("🔧 SILVER LAYER: Limpeza e validação de dados")
    print("=" * 60)

    # Lê dados do Bronze
    print(f"\n📖 Lendo dados de: {S3_BRONZE}/dados")
    df_bronze = spark.read.option("header", "true").csv(f"{S3_BRONZE}/dados")

    print(f"✅ Bronze: {df_bronze.count()} registros")

    # Remove duplicatas
    df_silver = df_bronze.dropDuplicates()

    # Remove nulos
    df_silver = df_silver.dropna()

    # Filtra dados válidos (valor > 0)
    df_silver = df_silver.filter(df_silver["valor"] > 0)

    print(f"✅ Silver: {df_silver.count()} registros (após limpeza)")
    df_silver.show(5)

    # Salva em Silver
    df_silver.write.mode("overwrite").option("header", "true").csv(f"{S3_SILVER}/dados")
    print(f"\n💾 Salvo em: {S3_SILVER}/dados")

    print("\n" + "=" * 60)
    print("✅ SILVER CONCLUÍDO COM SUCESSO!")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
