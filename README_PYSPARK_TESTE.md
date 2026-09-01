# PySpark + LocalStack - Guia Rápido

Teste básico para rodar PySpark lendo/escrevendo dados no S3 emulado do LocalStack.

## 📋 Pré-requisitos

- Docker e Docker Compose instalados
- Token do LocalStack no `.env` (ou rode com versão old: `localstack/localstack:3.0`)

## 🚀 Passo a passo

### 1. Suba os containers

```bash
make up
# ou
docker compose --profile full -f docker/docker-compose.yml up -d
```

Isso inicia:
- **LocalStack** (S3 + DynamoDB emulados na porta 4566)
- **Spark Master** (UI na porta 8080)

### 2. Prepare os dados de teste

```bash
make seed
# ou
python3 scripts/seed_data.py
```

Isso:
- Cria o bucket `dados-teste` no S3 local
- Faz upload do CSV de teste (`data/dados.csv`)

Você deve ver:
```
✅ Bucket 'dados-teste' já existe
📤 Uploading dados.csv...
✅ Arquivo enviado para: s3://dados-teste/entrada/dados.csv
📂 Conteúdo do bucket 'dados-teste':
  - entrada/dados.csv
```

### 3. Rode o job Spark

```bash
make run-job
# ou
docker exec -it spark spark-submit --master spark://spark:7077 /opt/spark-jobs/main.py
```

Você deve ver:
```
✅ Sessão Spark criada com sucesso!
📖 Lendo dados do S3...
✅ Dataset carregado: 10 linhas
[mostra os dados]

🔍 Aplicando filtro (valor > 100)...
✅ Após filtro: 5 linhas
[mostra resultado]

💾 Escrevendo resultado em S3...
✅ Processamento concluído! Resultado em: s3://dados-teste/saida/resultado
```

### 4. Verifique o resultado

```bash
aws --endpoint-url=http://localhost:4566 s3 ls s3://dados-teste/saida/resultado/
```

Deve aparecer o arquivo CSV com o resultado do filtro.

## 📁 Estrutura

```
projeto/
├── docker/
│   └── docker-compose.yml    # LocalStack + Spark
├── jobs/
│   └── main.py               # Script PySpark de exemplo
├── data/
│   └── dados.csv             # CSV de entrada
├── scripts/
│   └── seed_data.py          # Popula S3 com dados de teste
├── .env                       # Token LocalStack
└── Makefile                  # Comandos úteis
```

## 🛠️ Comandos úteis

| Comando | O que faz |
|---------|-----------|
| `make up` | Sobe LocalStack + Spark |
| `make down` | Para os containers |
| `make seed` | Faz upload dos dados de teste |
| `make run-job` | Executa o job PySpark |
| `make logs` | Mostra logs do Spark |
| `make logs-localstack` | Mostra logs do LocalStack |
| `make clean` | Remove containers e dados |

## 🌐 UIs disponíveis

- **Spark Master**: http://localhost:8080
- **Spark Application**: http://localhost:4040 (durante execução)
- **LocalStack Dashboard**: http://localhost:4566 (não tem UI, mas API responde)

## 🐛 Troubleshooting

### "Connection refused" ao rodar o job
- Confirme que os containers estão rodando: `docker ps`
- Aguarde um pouco para o Spark inicializar

### Erro de credenciais
- Verifique que está usando as credenciais fake: `test` / `test`
- Confirme que o LocalStack tem o auth token válido

### Bucket não encontrado
- Rode `make seed` antes do job
- Confirme o bucket existe: `aws --endpoint-url=http://localhost:4566 s3 ls`

## ✏️ Customize

Edite `jobs/main.py` para:
- Mudar qual CSV é lido
- Aplicar diferentes transformações
- Escrever em outros formatos (Parquet, etc)

Edite `data/dados.csv` para testar com seus próprios dados.

---

**Próximos passos:** Terraform pra provisionar S3/DynamoDB, ou adicionar mais jobs Spark.
