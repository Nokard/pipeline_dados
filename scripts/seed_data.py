#!/usr/bin/env python3
import boto3
from pyspark.sql import SparkSession
from pathlib import Path

# Configuração do S3 local (LocalStack)
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)

BUCKET_NAME = 'datalake-teste'
DATA_DIR = Path(__file__).parent.parent / 'data'

# Inicializa Spark para converter para Parquet
spark = SparkSession.builder \
    .appName("seed-data") \
    .master("local[*]") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:4566") \
    .config("spark.hadoop.fs.s3a.access.key", "test") \
    .config("spark.hadoop.fs.s3a.secret.key", "test") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.261") \
    .getOrCreate()

def criar_bucket():
    """Cria o bucket se não existir"""
    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket '{BUCKET_NAME}' já existe")
    except:
        print(f"📦 Criando bucket '{BUCKET_NAME}'...")
        s3_client.create_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Bucket criado com sucesso")

def upload_data():
    """Carrega CSV para S3 Bronze (sem Spark, direto com boto3)"""
    csv_file = DATA_DIR / 'dados.csv'

    if not csv_file.exists():
        print(f"❌ Arquivo não encontrado: {csv_file}")
        return False

    print(f"📖 Lendo {csv_file.name}...")
    with open(csv_file, 'rb') as f:
        content = f.read()
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key='bronze/dados.csv',
            Body=content
        )

    print(f"✅ Dados salvos em: s3a://{BUCKET_NAME}/bronze/dados.csv")

    # Conta linhas
    lines = content.decode().split('\n')
    print(f"   Registros: {len([l for l in lines if l.strip()]) - 1}")
    return True

def listar_bucket():
    """Lista o conteúdo do bucket"""
    print(f"\n📂 Conteúdo do bucket '{BUCKET_NAME}':")
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)

    if 'Contents' not in response:
        print("  (vazio)")
        return

    for obj in response['Contents']:
        print(f"  - {obj['Key']}")

if __name__ == '__main__':
    try:
        print("🚀 Preparando dados para teste (Medallion Architecture)...\n")
        criar_bucket()
        if upload_data():
            listar_bucket()
            print("\n✅ Pronto! Dados em Bronze. Você pode rodar o pipeline agora.")
            print(f"   make run-job")
        else:
            print("\n❌ Falha ao preparar dados.")
    finally:
        spark.stop()
