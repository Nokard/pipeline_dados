#!/usr/bin/env python3
"""GOLD: a tabela final — histórico imutável da compra, com validade SCD2.

Lê silver/purchase_diario (1 linha por compra/dia) e grava
gold/purchase_historico com as mesmas linhas mais três colunas de controle:

    dt_inicio_validade  a partir de quando essa versão vale
    dt_fim_validade     até quando valeu (NULL = ainda vale)
    is_current          atalho para "essa é a versão vigente"

É o dataset final do exercício. Em cima dele, o GMV sai de um SELECT simples:

    -- hoje
    SELECT release_date, subsidiary, SUM(purchase_value)
    FROM gold.purchase_historico
    WHERE is_current AND release_date IS NOT NULL
    GROUP BY release_date, subsidiary;

    -- como se via em qualquer data passada: troca só o WHERE
    WHERE date'2023-03-31' BETWEEN dt_inicio_validade
                               AND COALESCE(dt_fim_validade, date'9999-12-31')

Atende aos requisitos de "recuperar facilmente quais são os registros correntes
da base histórica" e de navegar entre o valor de um mês visto em datas
diferentes — sem window function na consulta.

NOTA SOBRE IMUTABILIDADE
As colunas de negócio (buyer_id, purchase_value, subsidiary, ...) foram
calculadas no purchase_diario com uma janela que olha só para trás — o estado
de 20/01 não sabe que existirá um evento em julho, e por isso nunca muda.

As colunas deste job são o oposto: para saber quando uma versão deixou de
valer é preciso olhar o evento SEGUINTE. "Ser o registro corrente" não é uma
propriedade da linha, e sim dela em relação às que vieram depois — logo muda
quando chega dado novo, por definição.

Isso não quebra o requisito de passado imutável: os valores de negócio de cada
versão permanecem intactos. O que se atualiza é metadado de vigência.
"""
import sys

from pyspark.sql import Window, functions as F

sys.path.insert(0, '/opt/spark-jobs/config')
from config import S3_SILVER, S3_GOLD, get_spark

spark = get_spark("gold-historico")

try:
    print("=" * 60)
    print("🏆 GOLD: histórico da compra com validade SCD2")
    print("=" * 60)

    df = spark.read.parquet(f"{S3_SILVER}/purchase_diario")
    print(f"\n📥 {df.count()} linhas lidas de purchase_diario")

    # --- 5. Intervalo de validade ------------------------------------------
    # Uma versão vale do dia do seu evento até a véspera do próximo evento
    # daquela compra. A última fica aberta (dt_fim NULL).
    #
    # Os intervalos cobrem todos os dias sem buraco nem sobreposição: entre
    # 24/01 e 04/02 o purchase_id=55 não teve evento algum, mas a versão de
    # 23/01 continua vigente nesse período. É isso que faz o BETWEEN responder
    # "como estava em qualquer data" sem precisar de window function.
    versoes = Window.partitionBy("purchase_id").orderBy("transaction_date")

    df = df \
        .withColumn("dt_inicio_validade", F.col("transaction_date")) \
        .withColumn(
            "dt_fim_validade",
            F.date_sub(F.lead("transaction_date").over(versoes), 1),
        ) \
        .withColumn("is_current", F.col("dt_fim_validade").isNull())

    correntes = df.filter("is_current").count()
    print(f"5️⃣  {df.count()} versões, {correntes} correntes "
          f"(1 por purchase_id)\n")

    df = (df.drop("hash_evento").orderBy("purchase_id", "dt_inicio_validade"))

    df.write \
        .mode("overwrite") \
        .partitionBy("transaction_date") \
        .parquet(f"{S3_GOLD}/purchase_historico")

    print(f"💾 Salvo em {S3_GOLD}/purchase_historico")
    print("=" * 60)

except Exception as e:
    print(f"❌ Erro: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
finally:
    spark.stop()
