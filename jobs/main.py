from pyspark.sql import SparkSession
from config import S3_BRONZE, S3_SILVER, S3_GOLD, SPARK_MASTER, SPARK_APP_NAME
import sys

# Inicializa Spark
spark = SparkSession.builder \
    .appName(SPARK_APP_NAME) \
    .master(SPARK_MASTER) \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.261") \
    .getOrCreate()

print("=" * 60)
print("🏗️  MEDALLION ARCHITECTURE PIPELINE")
print("=" * 60)

try:
    # ===== BRONZE LAYER (Dados Brutos) =====
    print("\n📦 BRONZE LAYER: Lendo dados brutos...")
    df_bronze = spark.read.option("header", "true").csv(f"{S3_BRONZE}/dados.csv")
    print(f"✅ Bronze: {df_bronze.count()} registros")
    df_bronze.show(5)

    # ===== SILVER LAYER (Dados Limpos) =====
    print("\n🔧 SILVER LAYER: Limpeza e validação...")

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
    print(f"💾 Salvo em: {S3_SILVER}/dados")

    # ===== GOLD LAYER (Dados Prontos para Análise) =====
    print("\n✨ GOLD LAYER: Transformações para análise...")

    # Produtos premium (valor > 100)
    df_gold_premium = df_silver.filter(df_silver["valor"] > 100) \
        .select("id", "nome", "valor", "categoria") \
        .orderBy("valor", ascending=False)

    print(f"✅ Gold (Premium): {df_gold_premium.count()} produtos")
    df_gold_premium.show()

    # Salva em Gold
    df_gold_premium.write.mode("overwrite").option("header", "true").csv(f"{S3_GOLD}/produtos_premium")
    print(f"💾 Salvo em: {S3_GOLD}/produtos_premium")

    # Agregação por categoria
    print("\n📊 Agregação por categoria...")
    df_gold_categoria = df_silver.groupBy("categoria") \
        .agg({"valor": "sum", "id": "count"}) \
        .withColumnRenamed("sum(valor)", "valor_total") \
        .withColumnRenamed("count(id)", "qtd_produtos") \
        .orderBy("valor_total", ascending=False)

    print("✅ Gold (Categoria):")
    df_gold_categoria.show()

    # Salva em Gold
    df_gold_categoria.write.mode("overwrite").option("header", "true").csv(f"{S3_GOLD}/categoria_summary")
    print(f"💾 Salvo em: {S3_GOLD}/categoria_summary")

    print("\n" + "=" * 60)
    print("✅ PIPELINE CONCLUÍDO COM SUCESSO!")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
