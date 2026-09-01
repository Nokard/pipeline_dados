#!/usr/bin/env python3
from pyspark.sql import SparkSession
from config import S3_SILVER, S3_GOLD, SPARK_MASTER, SPARK_APP_NAME
import sys

spark = SparkSession.builder \
    .appName(f"{SPARK_APP_NAME}-gold") \
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
    print("✨ GOLD LAYER: Transformações para análise")
    print("=" * 60)

    # Lê dados do Silver
    print(f"\n📖 Lendo dados de: {S3_SILVER}/dados")
    df_silver = spark.read.option("header", "true").csv(f"{S3_SILVER}/dados")

    print(f"✅ Silver: {df_silver.count()} registros")

    # ===== Tabela 1: Produtos Premium (valor > 100) =====
    print("\n📊 Criando tabela: Produtos Premium...")
    df_gold_premium = df_silver.filter(df_silver["valor"] > 100) \
        .select("id", "nome", "valor", "categoria") \
        .orderBy("valor", ascending=False)

    print(f"✅ Gold (Premium): {df_gold_premium.count()} produtos")
    df_gold_premium.show()

    df_gold_premium.write.mode("overwrite").option("header", "true").csv(f"{S3_GOLD}/produtos_premium")
    print(f"💾 Salvo em: {S3_GOLD}/produtos_premium")

    # ===== Tabela 2: Agregação por Categoria =====
    print("\n📊 Criando tabela: Agregação por categoria...")
    df_gold_categoria = df_silver.groupBy("categoria") \
        .agg({"valor": "sum", "id": "count"}) \
        .withColumnRenamed("sum(valor)", "valor_total") \
        .withColumnRenamed("count(id)", "qtd_produtos") \
        .orderBy("valor_total", ascending=False)

    print("✅ Gold (Categoria):")
    df_gold_categoria.show()

    df_gold_categoria.write.mode("overwrite").option("header", "true").csv(f"{S3_GOLD}/categoria_summary")
    print(f"💾 Salvo em: {S3_GOLD}/categoria_summary")

    print("\n" + "=" * 60)
    print("✅ GOLD CONCLUÍDO COM SUCESSO!")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
