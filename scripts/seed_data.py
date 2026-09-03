#!/usr/bin/env python3
"""Deposita os CSVs de eventos (CDC) na camada raw do LocalStack.

O seed simula a origem entregando os arquivos no lake: nenhuma transformação
acontece aqui. Tipagem, qualidade e particionamento são trabalho do bronze.
"""
import sys
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).parent.parent / 'jobs' / 'config'))
from config import S3_BUCKET, FONTES

DATA_DIR = Path(__file__).parent.parent / 'data' / 'events'

s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:4566',
    aws_access_key_id='test',
    aws_secret_access_key='test',
    region_name='us-east-1'
)


def criar_bucket():
    """Cria o bucket se ainda não existir."""
    try:
        s3_client.head_bucket(Bucket=S3_BUCKET)
        print(f"✅ Bucket '{S3_BUCKET}' já existe")
    except s3_client.exceptions.ClientError:
        print(f"📦 Criando bucket '{S3_BUCKET}'...")
        s3_client.create_bucket(Bucket=S3_BUCKET)
        print("✅ Bucket criado")


def upload_eventos():
    """Sobe um CSV por fonte para raw/<fonte>/."""
    faltando = []

    for fonte in FONTES:
        csv_file = DATA_DIR / f"{fonte}.csv"

        if not csv_file.exists():
            print(f"⚠️  Arquivo não encontrado: {csv_file}")
            faltando.append(fonte)
            continue

        key = f"raw/{fonte}/{fonte}.csv"
        s3_client.upload_file(str(csv_file), S3_BUCKET, key)

        linhas = sum(1 for _ in csv_file.open()) - 1  # desconta o header
        print(f"✅ {fonte}.csv ({linhas} eventos) → s3://{S3_BUCKET}/{key}")

    return not faltando


def listar_bucket():
    """Lista o conteúdo do bucket."""
    print(f"\n📂 Conteúdo de '{S3_BUCKET}':")
    conteudo = s3_client.list_objects_v2(Bucket=S3_BUCKET).get('Contents', [])

    if not conteudo:
        print("  (vazio)")
        return

    for obj in conteudo:
        print(f"  - {obj['Key']}")


if __name__ == '__main__':
    print("🚀 Depositando eventos de CDC na camada raw...\n")
    criar_bucket()

    if upload_eventos():
        listar_bucket()
        print("\n✅ Raw populado. Próximo passo: make run-bronze")
    else:
        print("\n❌ Faltaram arquivos de origem.")
        sys.exit(1)
