# Configuração do projeto - Medallion Architecture

S3_BUCKET = "datalake-teste"

# Bronze: dados brutos, como chegam do source
S3_BRONZE = f"s3a://{S3_BUCKET}/bronze"

# Silver: dados limpos, validados, sem duplicatas
S3_SILVER = f"s3a://{S3_BUCKET}/silver"

# Gold: dados transformados, prontos para análise
S3_GOLD = f"s3a://{S3_BUCKET}/gold"

# Formato dos arquivos
FORMATO = "parquet"  # ou "parquet" se preferir

# Spark config
SPARK_MASTER = "local[*]"
SPARK_APP_NAME = "medallion-pipeline"
