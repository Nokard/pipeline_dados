#!/usr/bin/env python3
"""BRONZE: tipa os eventos de CDC e particiona por dia.

Lê os CSVs crus de raw/ e grava um Parquet por fonte em bronze/, particionado
por transaction_date. Nenhuma fonte é unida aqui — as 3 seguem separadas; a
junção é trabalho da silver.
"""
import sys

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType, DecimalType, IntegerType, LongType,
    StringType, StructField, StructType, TimestampNTZType,
)

sys.path.insert(0, '/opt/spark-jobs/config')
from config import S3_RAW, S3_BRONZE, get_spark

# Schema explícito por fonte. Nunca inferSchema: a inferência olha uma amostra
# dos dados, então o mesmo arquivo pode virar tipos diferentes entre execuções
# (release_date vazio do purchase_id=56 é o caso clássico).
#
# transaction_datetime é TimestampNTZType (sem fuso), não TimestampType.
# TimestampType guarda um INSTANTE: o Spark converte na escrita e reconverte na
# leitura usando o fuso da sessão, então o mesmo dado renderiza diferente em
# máquinas diferentes — e um evento às 00:05 chega a mudar de dia, logo de
# partição. NTZ guarda o relógio de parede literal, igual DateType: entra
# "2023-01-23 00:05:00", sai "2023-01-23 00:05:00", em qualquer fuso, sem
# depender de ninguém lembrar de configurar sessão.
#
# Semanticamente também é o tipo certo: transaction_datetime vem do log de CDC
# da origem, é hora local daquele sistema — não um instante global.
SCHEMAS = {
    "purchase": StructType([
        StructField("transaction_datetime", TimestampNTZType()),
        StructField("transaction_date", StringType()),
        StructField("purchase_id", LongType()),
        StructField("buyer_id", LongType()),
        StructField("prod_item_id", LongType()),
        StructField("order_date", DateType()),
        StructField("release_date", DateType()),
        StructField("producer_id", LongType()),
    ]),
    "product_item": StructType([
        StructField("transaction_datetime", TimestampNTZType()),
        StructField("transaction_date", StringType()),
        StructField("purchase_id", LongType()),
        StructField("product_id", LongType()),
        StructField("item_quantity", IntegerType()),
        # Dinheiro entra em SUM(): DECIMAL, nunca float, senão o GMV não fecha.
        StructField("purchase_value", DecimalType(18, 2)),
    ]),
    "purchase_extra_info": StructType([
        StructField("transaction_datetime", TimestampNTZType()),
        StructField("transaction_date", StringType()),
        StructField("purchase_id", LongType()),
        StructField("subsidiary", StringType()),
    ]),
}

# Fuso fixo e partitionOverwriteMode=dynamic vêm de get_spark(), para não
# divergirem entre os jobs.
spark = get_spark("bronze")

try:
    print("=" * 60)
    print("📦 BRONZE: tipagem e particionamento dos eventos de CDC")
    print("=" * 60)

    # Lendo minhas base da
    for fonte, schema in SCHEMAS.items():
        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(f"{S3_RAW}/{fonte}")

        # transaction_date vem pronta no CSV, mas é redundante com o datetime.
        # Se a origem mandar as duas divergentes, a partição vai pro dia errado
        # e o passado deixa de ser imutável. Deriva de novo e ignora a original.
        df = df.withColumn("transaction_date", F.to_date("transaction_datetime"))

        total = df.count()
        dias = df.select("transaction_date").distinct().count()

        df.write \
            .mode("overwrite") \
            .partitionBy("transaction_date") \
            .parquet(f"{S3_BRONZE}/{fonte}")

        print(f"\n✅ {fonte}: {total} eventos em {dias} partições")
        df.orderBy("transaction_datetime").show(truncate=False)

    print("=" * 60)
    print("✅ BRONZE CONCLUÍDO")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
