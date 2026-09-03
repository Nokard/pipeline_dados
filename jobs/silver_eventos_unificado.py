#!/usr/bin/env python3
"""SILVER 1/2: empilha os eventos das 3 fontes numa linha do tempo única.

Grão de saída: 1 linha por (purchase_id, origem_evento, transaction_date).
Ainda com NULLs — responde "qual fonte disse o quê, e quando?".

UNION, não JOIN. O JOIN assumiria que as 3 fontes existem ao mesmo tempo — e
não existem: o purchase_id=71 recebe extra_info em 03-10, product_item em
03-15 e purchase só em 03-20. Um INNER JOIN devolveria zero linhas até o dia
20, e um LEFT JOIN ancorado em purchase não teria nem a linha-âncora.

O union empilha os eventos e deixa cada fonte preencher apenas as colunas de
que ela é dona; o resto vem NULL.
"""
import sys

from pyspark.sql import Window, functions as F

sys.path.insert(0, '/opt/spark-jobs/config')
from config import S3_BRONZE, S3_SILVER, FONTES, COLUNAS, get_spark

spark = get_spark("silver-eventos")

try:
    print("=" * 60)
    print("🔗 SILVER 1/2: união dos eventos das 3 fontes")
    print("=" * 60)

    df_eventos = None

    for fonte in FONTES:
        df = spark.read.parquet(f"{S3_BRONZE}/{fonte}")

        # origem_evento não é só rastreabilidade. É ela que vai distinguir
        # "essa fonte não fala sobre a coluna" (herda o valor anterior) de
        # "essa fonte falou e o valor é vazio" (não herda) — a diferença que
        # o forward fill precisa respeitar para não ignorar cancelamentos.
        df = df.withColumn("origem_evento", F.lit(fonte))

        # unionByName casa por nome e preenche com NULL as colunas ausentes.
        # union() casaria por posição e erraria em silêncio se a ordem das
        # colunas divergisse entre as fontes.
        df_eventos = df if df_eventos is None else df_eventos.unionByName(
            df, allowMissingColumns=True
        )

    df_eventos = df_eventos.select(*COLUNAS)
    print(f"\n1️⃣  {df_eventos.count()} eventos empilhados de {len(FONTES)} fontes")

    # --- Fingerprint determinístico do evento -------------------------------
    # Precisa ser estável entre execuções: o mesmo conteúdo tem que gerar o
    # mesmo hash em qualquer máquina, em qualquer ordem de leitura. Por isso
    # sha2 sobre o conteúdo, e não monotonically_increasing_id() ou afins, que
    # dependem do particionamento e mudam a cada run.
    conteudo = [
        F.coalesce(F.col(c).cast("string"), F.lit("<null>"))
        for c in sorted(COLUNAS)
    ]
    df_eventos = df_eventos.withColumn(
        "hash_evento", F.sha2(F.concat_ws("||", *conteudo), 256)
    )

    # --- 3.1 Deduplicação exata (reenvio) -----------------------------------
    # O enunciado avisa que "o reenvio de eventos pode acontecer". Dois eventos
    # byte-idênticos são o mesmo evento: não há informação em "chegou 2x", e
    # manter os dois dobraria o GMV.
    df_eventos = df_eventos.dropDuplicates(["hash_evento"])
    print(f"2️⃣  {df_eventos.count()} após remover reenvios idênticos")

    # --- 3.2 Último estado de cada fonte, em cada dia -----------------------
    # Se a mesma fonte falou duas vezes no mesmo dia, só o último estado vale.
    #
    # O desempate por hash_evento não é decoração: transaction_datetime NÃO é
    # único (purchase_id=76 tem dois eventos de purchase às 12:00:00 em 25/06).
    # Sem um critério total, o Spark escolheria arbitrariamente e a escolha
    # mudaria entre execuções — quebrando "o passado não pode ser alterado
    # mesmo com reprocessamento full". Qual linha vence é arbitrário; o que
    # importa é vencer SEMPRE A MESMA. Num CDC real o desempate correto seria
    # o offset do log (LSN no Postgres, binlog no MySQL), que aqui não temos.
    ultimo_do_dia = Window.partitionBy(
        "purchase_id", "origem_evento", "transaction_date"
    ).orderBy(
        F.col("transaction_datetime").desc(),
        F.col("hash_evento").desc(),
    )

    df_eventos = df_eventos \
        .withColumn("_rn", F.row_number().over(ultimo_do_dia)) \
        .filter(F.col("_rn") == 1) \
        .drop("_rn")

    print(f"3️⃣  {df_eventos.count()} após manter só o último estado por fonte/dia\n")

    df_eventos.drop("hash_evento") \
        .orderBy("purchase_id", "transaction_datetime", "origem_evento") \
        .show(50, truncate=False)

    df_eventos.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(f"{S3_SILVER}/eventos_unificados")

    print(f"💾 Salvo em {S3_SILVER}/eventos_unificados")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
