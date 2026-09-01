# 📊 Medallion Architecture Pipeline

Arquitetura completa de dados com **PySpark**, **LocalStack** (AWS emulado), **Terraform** e **Jupyter** pra exploração interativa.

## 🏗️ Arquitetura

```
Bronze (Raw) → Silver (Cleaned) → Gold (Analysis-Ready)
```

- **Bronze**: Dados brutos do source
- **Silver**: Dados limpos, validados, sem duplicatas
- **Gold**: Dados transformados, prontos para análise

## 📋 Pré-requisitos

- Docker e Docker Compose
- Python 3.8+ com venv
- Git configurado

## 🚀 Quick Start

### 1. Suba infraestrutura + Terraform

```bash
make up-infra
```

Isso automaticamente:
- ✅ Sobe LocalStack (S3 + DynamoDB na porta 4566)
- ✅ Sobe Spark Master (UI na porta 8080)
- ✅ Roda Terraform (cria bucket `datalake-teste` + bronze/silver/gold)

### 2. Popula dados de teste

```bash
make seed
```

Carrega `data/dados.csv` para S3 Bronze layer.

### 3. Executa pipeline

```bash
make run-job
```

Processa dados através das 3 camadas (Bronze → Silver → Gold).

### 4. Explore dados interativamente

```bash
jupyter notebook jobs/jupyter/analise.ipynb
```

Notebook já está configurado pra conectar ao LocalStack + S3a.

## 📁 Estrutura

```
projeto/
├── docker/
│   └── docker-compose.yml        # LocalStack, Spark, Terraform
├── terraform/
│   ├── main.tf                   # Bucket S3 + camadas (bronze/silver/gold)
│   ├── variables.tf              # Configurações
│   └── terraform.tfvars          # Valores (localstack endpoint, etc)
├── jobs/
│   ├── main.py                   # Pipeline Medallion (PySpark)
│   └── jupyter/analise.ipynb     # Exploração interativa
├── scripts/
│   └── seed_data.py              # Popula S3 com dados de teste
├── config.py                      # Configurações centralizadas
├── data/
│   └── dados.csv                 # Dados de teste
├── .env                          # Token LocalStack
└── Makefile                      # Comandos úteis
```

## 🛠️ Comandos

| Comando | O que faz |
|---------|-----------|
| `make up-infra` | **NOVO**: Sobe Docker + cria infra com Terraform |
| `make up` | Sobe só containers (LocalStack + Spark) |
| `make down` | Para containers |
| `make seed` | Popula dados no S3 |
| `make run-job` | Executa pipeline PySpark |
| `make logs` | Logs do Spark |
| `make logs-localstack` | Logs do LocalStack |
| `make clean` | Remove tudo (containers + volumes + dados) |
| `make tf-plan` | Mostra plano Terraform |
| `make tf-apply` | Aplica Terraform manualmente |
| `make tf-destroy` | Remove infraestrutura |

## 🌐 Acesso

- **Spark Master UI**: http://localhost:8080
- **Spark Application UI**: http://localhost:4040 (durante execução)
- **LocalStack**: http://localhost:4566 (API)
- **Jupyter**: http://localhost:8888 (após rodar `jupyter notebook`)

## 🧪 Exemplo de uso

```bash
# Setup completo
make up-infra       # Sobe infra
make seed           # Carrega dados
make run-job        # Executa pipeline

# Explorar dados
jupyter notebook jobs/jupyter/analise.ipynb
```

## 🔧 Configuração

Edite `config.py` para mudar:
- `S3_BUCKET`: Nome do bucket
- `SPARK_MASTER`: URL do Spark Master
- Paths das camadas (Bronze, Silver, Gold)

Edite `terraform/terraform.tfvars` para mudar:
- `bucket_name`: Nome do bucket S3
- `localstack_endpoint`: URL do LocalStack
- `environment`: Nome do ambiente

## 📝 Desenvolvimento

### Adicionar novo job Spark

1. Crie arquivo em `jobs/seu_job.py`
2. Use as constantes de `config.py`
3. Execute via Docker:

```bash
docker exec -it spark spark-submit /opt/spark-jobs/seu_job.py
```

### Testar localmente com Jupyter

Use `jobs/jupyter/analise.ipynb` como template. Já tem:
- Configuração S3a + LocalStack
- Leitura de dados Bronze
- Transformações Silver/Gold

## 🐛 Troubleshooting

### Erro "terraform: not found"
Não precisa instalar! Terraform roda dentro do Docker agora.

### Bucket já existe
Rode `make clean` pra limpar tudo, depois `make up-infra` de novo.

### PySpark não consegue ler S3
- Verifique que LocalStack tá rodando: `docker ps`
- Rode `make seed` pra criar bucket
- Check logs: `make logs`

### Jupyter não conecta
- Verifique que LocalStack tá saudável: `docker exec localstack aws s3 ls --endpoint-url=http://localhost:4566`
- Reinicie kernel do Jupyter

## 📚 Stack

- **PySpark 3.5.1** — processamento distribuído
- **LocalStack 3.x** — emulação AWS local
- **Terraform 1.7+** — IaC (em Docker)
- **Docker Compose** — orquestração
- **Jupyter** — exploração interativa
- **boto3** — client AWS/S3

## 🚀 Próximos passos

- [ ] Adicionar mais jobs Spark
- [ ] Criar jobs agendados (Airflow, etc)
- [ ] Adicionar testes automatizados
- [ ] Deploy em ambiente real (AWS)
