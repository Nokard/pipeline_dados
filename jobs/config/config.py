import os

from pyspark.sql import SparkSession

# Configuração do projeto - Medallion Architecture

S3_BUCKET = "datalake-teste2"

# Raw: CSV cru dos eventos de CDC, exatamente como a origem entregou
S3_RAW = f"s3a://{S3_BUCKET}/raw"

# Bronze: eventos tipados e particionados por transaction_date
S3_BRONZE = f"s3a://{S3_BUCKET}/bronze"

# Silver: dados limpos, validados, sem duplicatas
S3_SILVER = f"s3a://{S3_BUCKET}/silver"

# Gold: dados transformados, prontos para análise
S3_GOLD = f"s3a://{S3_BUCKET}/gold"

# Formato dos arquivos
FORMATO = "parquet"

# Fontes de eventos (CDC) que alimentam a tabela final
FONTES = ["purchase", "product_item", "purchase_extra_info"]

# Colunas que identificam o evento, presentes em todas as fontes.
CHAVES = ["transaction_datetime", "transaction_date", "purchase_id", "origem_evento"]

# De quais colunas cada fonte é dona. Nenhuma fonte escreve em coluna de outra —
# é isso que permite que qualquer uma chegue primeiro sem travar as demais, e é
# a unidade em que o forward fill opera (bloco de fonte, não coluna isolada).
COLUNAS_POR_FONTE = {
    "purchase": ["buyer_id", "prod_item_id", "order_date", "release_date", "producer_id"],
    "product_item": ["product_id", "item_quantity", "purchase_value"],
    "purchase_extra_info": ["subsidiary"],
}

# Ordem estável das colunas: chaves primeiro, depois um bloco por fonte.
COLUNAS = CHAVES + [c for cols in COLUNAS_POR_FONTE.values() for c in cols]

# Spark config
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
SPARK_APP_NAME = "medallion-pipeline"

# Dentro do container o LocalStack atende em localstack:4566; de fora (notebook,
# scripts na máquina) o mesmo serviço é localhost:4566.
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://localstack:4566")

# TimestampType no Spark é um INSTANTE, não texto de relógio. A string do CSV é
# interpretada no fuso da sessão na escrita e re-renderizada no fuso da sessão
# na leitura — então, sem fuso fixo, o container (Etc/UTC) e a máquina local
# (America/Sao_Paulo) produzem horários diferentes para o mesmo dado.
#
# Isso não é cosmético aqui: transaction_date é a partição que congela o
# passado. Um evento às 00:05 deslocado em 3h muda de dia, muda de partição e
# muda o GMV diário. Fixando o fuso igual em toda sessão, a string do CSV entra
# e sai idêntica — nenhuma conversão acontece no caminho.
SPARK_TIMEZONE = "UTC"

# O conector S3A não vem no pyspark instalado via pip, mas já está na imagem do
# container. Fora do container (notebook, scripts locais) ele precisa ser
# baixado; dentro, pedir os jars de novo só atrasaria o job. O endpoint denuncia
# onde estamos: localhost = fora, localstack = dentro.
S3A_PACKAGES = os.getenv(
    "SPARK_JARS_PACKAGES",
    "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.261"
    if "localhost" in S3_ENDPOINT else "",
)


def get_spark(sufixo_app=None):
    """Sessão Spark do projeto.

    Ponto único de configuração: se cada job montar a sua, mais cedo ou mais
    tarde uma vai divergir da outra — foi assim que o fuso passou despercebido.
    Vale para os jobs do pipeline e para os notebooks de validação.
    """
    nome = f"{SPARK_APP_NAME}-{sufixo_app}" if sufixo_app else SPARK_APP_NAME

    builder = SparkSession.builder \
        .appName(nome) \
        .master(SPARK_MASTER) \
        .config("spark.sql.session.timeZone", SPARK_TIMEZONE) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
        .config("spark.hadoop.fs.s3a.access.key", "test") \
        .config("spark.hadoop.fs.s3a.secret.key", "test") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    if S3A_PACKAGES:
        builder = builder.config("spark.jars.packages", S3A_PACKAGES)

    return builder.getOrCreate()
