#!/usr/bin/env python3
"""SILVER 2/2: consolida o estado da compra em cada dia.

Lê silver/eventos_unificados (1 linha por compra/fonte/dia, com NULLs) e grava
silver/purchase_diario (1 linha por compra/dia, com todos os campos ativos).

Atende ao requisito: "Se uma tabela sofreu atualização e as demais não, os dados
ativos das demais deverão ser repetidos."
"""
import sys

from pyspark.sql import Window, functions as F

sys.path.insert(0, '/opt/spark-jobs/config')
from config import S3_SILVER, COLUNAS_POR_FONTE, get_spark

spark = get_spark("silver-diario")

try:
    print("=" * 60)
    print("📆 SILVER 2/2: estado consolidado da compra por dia")
    print("=" * 60)

    df = spark.read.parquet(f"{S3_SILVER}/eventos_unificados")
    print(f"\n📥 {df.count()} eventos lidos de eventos_unificados")

    # --- 4.1 Forward fill por BLOCO DE FONTE --------------------------------
    # O jeito ingênuo — last(coluna, ignorenulls=True) — está ERRADO aqui,
    # porque NULL carrega dois significados incompatíveis na mesma coluna:
    #   a) "essa fonte não fala sobre esse campo"  -> deve herdar
    #   b) "essa fonte falou, e o valor é vazio"   -> NÃO deve herdar
    # Coluna a coluna os dois são indistinguíveis, e o ignorenulls pula os dois.
    # Resultado: o cancelamento do purchase_id=72 (release_date volta a NULL em
    # 10/05) seria ignorado e a compra contaria GMV para sempre.
    #
    # A correção é agrupar as colunas de cada fonte num struct que só existe
    # quando o evento veio daquela fonte. O struct inteiro é não-nulo sempre que
    # a fonte falou — mesmo que todos os campos dentro dele sejam NULL. Assim o
    # ignorenulls pula apenas eventos de OUTRAS fontes, nunca um evento real.
    
    linha_do_tempo = Window.partitionBy("purchase_id").orderBy(
        F.col("transaction_datetime").asc(),
        F.col("hash_evento").asc(),
    ).rowsBetween(Window.unboundedPreceding, Window.currentRow)

    for fonte, colunas in COLUNAS_POR_FONTE.items():
        bloco = F.when(F.col("origem_evento") == fonte, F.struct(*colunas))
        df = df.withColumn(
            f"_ultimo_{fonte}", F.last(bloco, ignorenulls=True).over(linha_do_tempo)
        )

    # Expande os structs de volta em colunas planas. Só depois de TODOS estarem
    # calculados, senão sobrescrever uma coluna afetaria o struct seguinte.
    for fonte, colunas in COLUNAS_POR_FONTE.items():
        for coluna in colunas:
            df = df.withColumn(coluna, F.col(f"_ultimo_{fonte}.{coluna}"))
        df = df.drop(f"_ultimo_{fonte}")

    # --- 4.2 Colapsa para o grão diário -------------------------------------
    # Uma linha por (purchase_id, transaction_date): a última do dia, que já
    # carrega o estado consolidado das 3 fontes até aquele momento.
    dia = Window.partitionBy("purchase_id", "transaction_date")

    # Quais fontes dispararam evento nesse dia — rastreabilidade diária.
    # Substitui origem_evento, que após o colapso só diria a última.
    df = df.withColumn(
        "fontes_no_dia", F.sort_array(F.collect_set("origem_evento").over(dia))
    )

    ultimo_do_dia = dia.orderBy(
        F.col("transaction_datetime").desc(),
        F.col("hash_evento").desc(),
    )

    df = df \
        .withColumn("_rn", F.row_number().over(ultimo_do_dia)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn", "origem_evento")

    print(f"4️⃣  {df.count()} linhas após forward fill e colapso diário\n")

    df.drop("hash_evento") \
        .orderBy("purchase_id", "transaction_date") \
        .show(50, truncate=False)

    df.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(f"{S3_SILVER}/purchase_diario")

    print(f"💾 Salvo em {S3_SILVER}/purchase_diario")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
