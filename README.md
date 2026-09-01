# 📊 Medallion Architecture Pipeline com PySpark

Pipeline de dados completo com arquitetura **Medallion** (Bronze → Silver → Gold), usando **PySpark**, **LocalStack** (AWS emulado) e **Terraform** para provisionamento de infraestrutura.

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────┐
│  CAMADAS DO MEDALLION ARCHITECTURE              │
├─────────────────────────────────────────────────┤
│  🟫 BRONZE  → Dados brutos (raw)                │
│  🟪 SILVER  → Dados limpos e validados          │
│  🟨 GOLD    → Dados prontos para análise        │
└─────────────────────────────────────────────────┘
```

## 📋 Pré-requisitos

✅ **Obrigatório:**
- Docker + Docker Compose (instalado)
- Git (para clonar o repo)

✅ **Opcional:**
- Python 3.8+ (para desenvolvimento local)
- Jupyter (para exploração interativa)

## 🚀 Como Executar (Passo a Passo)

### PASSO 1️⃣ - Clonar o projeto

```bash
git clone git@github.com:Nokard/pipeline_dados.git
cd pipeline_dados
```

### PASSO 2️⃣ - Subir infraestrutura + criar recursos AWS

```bash
make up-infra
```

**O que acontece:**
- ✅ Sobe **LocalStack** (S3 + DynamoDB emulados) na porta `4566`
- ✅ Sobe **Spark Master** (Spark UI) na porta `8080`
- ✅ Executa **Terraform** (cria bucket `datalake-teste` + pastas bronze/silver/gold)
- ✅ Aguarda ~15 segundos enquanto Terraform cria infraestrutura

**Output esperado:**
```
🚀 Iniciando infraestrutura completa (Docker + Terraform)...
⏳ Aguardando LocalStack ficar pronto...
📦 Inicializando Terraform...
🏗️ Criando infraestrutura S3...
✅ Infraestrutura completa criada!
```

**Verificar se funcionou:**
```bash
docker ps
# Deve listar: localstack, spark, terraform
```

### PASSO 3️⃣ - Carregar dados de teste

```bash
make seed
```

**O que acontece:**
- Cria bucket S3 em LocalStack
- Faz upload do arquivo `data/dados.csv` para `s3://datalake-teste/bronze/dados.csv`

**Output esperado:**
```
🚀 Preparando dados para teste...
✅ Bucket 'datalake-teste' já existe
📖 Lendo dados.csv...
✅ Dados salvos em: s3a://datalake-teste/bronze/dados.csv
   Registros: 10

📂 Conteúdo do bucket 'datalake-teste':
  - bronze/dados.csv
✅ Pronto! Você pode rodar o pipeline agora.
```

### PASSO 4️⃣ - Executar pipeline PySpark

```bash
make run-job
```

**O que acontece:**
- Lê dados brutos do **Bronze** (`dados.csv`)
- Limpa, valida e deduplica no **Silver** (remove nulos, filtra valores > 0)
- Transforma e agregaa no **Gold** (produtos premium + resumo por categoria)
- Salva resultados em S3 em cada camada

**Output esperado:**
```
============================================================
🏗️  MEDALLION ARCHITECTURE PIPELINE
============================================================

📦 BRONZE LAYER: Lendo dados brutos...
✅ Bronze: 10 registros
+---+---------+-----+-----------+
| id|     nome|valor|  categoria|
+---+---------+-----+-----------+
|  1|Produto A|   50|Eletrônicos|
...

🔧 SILVER LAYER: Limpeza e validação...
✅ Silver: 10 registros (após limpeza)
💾 Salvo em: s3a://datalake-teste/silver/dados

✨ GOLD LAYER: Transformações para análise...
✅ Gold (Premium): 5 produtos
+---+---------+-----+-----------+
| id|     nome|valor|  categoria|
+---+---------+-----+-----------+
|  7|Produto G|  250|     Livros|
|  4|Produto D|  200|     Livros|
...

📊 Agregação por categoria...
✅ Gold (Categoria):
+-----------+-----------+------------+
|  categoria|valor_total|qtd_produtos|
+-----------+-----------+------------+
|     Livros|        450|           3|
|Eletrônicos|        395|           4|
...

============================================================
✅ PIPELINE CONCLUÍDO COM SUCESSO!
============================================================
```

### PASSO 5️⃣ (Opcional) - Explorar dados com Jupyter

```bash
jupyter notebook jobs/jupyter/analise.ipynb
```

**O que faz:**
- Abre notebook interativo em `http://localhost:8888`
- Já está configurado para conectar ao LocalStack
- Permite ler/escrever dados nas camadas Bronze/Silver/Gold

## 📊 Verificar dados em S3

Se quiser listar os arquivos salvos:

```bash
# Listar conteúdo do bucket
docker exec localstack aws s3 ls s3://datalake-teste --recursive \
  --endpoint-url=http://localhost:4566

# Ver arquivos específicos
docker exec localstack aws s3 ls s3://datalake-teste/gold/ --recursive \
  --endpoint-url=http://localhost:4566
```

## 🛠️ Outros Comandos Úteis

| Comando | O que faz |
|---------|-----------|
| `make up` | Sobe só containers (sem Terraform) |
| `make down` | Para os containers |
| `make logs` | Mostra logs do Spark |
| `make logs-localstack` | Mostra logs do LocalStack |
| `make clean` | Remove TUDO (containers + volumes + dados) |
| `make clean-volumes` | Remove só volumes (deixa containers) |
| `make stop-all` | Para todos os containers |
| `make tf-plan` | Mostra plano Terraform |
| `make tf-apply` | Aplica Terraform manualmente |
| `make tf-destroy` | Remove infraestrutura S3 |

## 🌐 Acesso às UIs

Durante a execução, acesse:

- **Spark Master Dashboard**: http://localhost:8080
  - Mostra status do Spark Master
  - Lista workers e jobs executados

- **Spark Application UI**: http://localhost:4040
  - Aparece durante execução de `make run-job`
  - Mostra stages, tasks, métricas

- **Jupyter**: http://localhost:8888
  - Acesso ao notebook de exploração
  - Token vem no terminal quando faz `jupyter notebook`

## 🔧 Configuração (se precisar customizar)

### Mudar nome do bucket

Edite em 3 lugares:

1. **`terraform/terraform.tfvars`:**
```hcl
bucket_name = "seu-novo-bucket"
```

2. **`config.py`:**
```python
S3_BUCKET = "seu-novo-bucket"
```

3. **`scripts/seed_data.py`:**
```python
BUCKET_NAME = 'seu-novo-bucket'
```

### Mudar dados de teste

Edite `data/dados.csv` com seus próprios dados.

Depois rode:
```bash
make clean
make up-infra
make seed
make run-job
```

## 📁 Estrutura do Projeto

```
pipeline_dados/
├── README.md                           # Este arquivo
├── Makefile                            # Comandos (make up-infra, make seed, etc)
├── config.py                           # Configurações centralizadas
│
├── docker/
│   └── docker-compose.yml              # Define: LocalStack, Spark, Terraform
│
├── terraform/
│   ├── main.tf                         # Cria bucket S3 + camadas
│   ├── variables.tf                    # Variáveis Terraform
│   ├── terraform.tfvars                # Valores (endpoint, bucket name)
│   └── outputs.tf                      # Saídas (paths do S3)
│
├── jobs/
│   ├── main.py                         # Pipeline Medallion (Bronze→Silver→Gold)
│   └── jupyter/
│       └── analise.ipynb               # Notebook para exploração
│
├── scripts/
│   └── seed_data.py                    # Popula bucket com dados.csv
│
├── data/
│   └── dados.csv                       # Dados de teste
│
├── .env                                # Token LocalStack
├── .gitignore                          # Arquivos ignorados no Git
└── LICENSE                             # Licença do projeto
```

## 🐛 Troubleshooting

### ❌ "Erro: Connection refused ao rodar make seed"

**Causa:** LocalStack não está pronto ainda

**Solução:**
```bash
# Aguarde mais um pouco
sleep 10
make seed

# Ou verifique se containers estão rodando
docker ps
```

### ❌ "Erro: Bucket 'datalake-teste' já existe"

**Causa:** Estava executado anteriormente

**Solução:**
```bash
# Limpa tudo
make clean

# Recria do zero
make up-infra
make seed
make run-job
```

### ❌ "Erro: PySpark não consegue ler S3"

**Causa:** Problema de conexão LocalStack ↔ Spark

**Solução:**
```bash
# 1. Verifique se LocalStack tá saudável
docker exec localstack aws s3 ls --endpoint-url=http://localhost:4566

# 2. Verifique logs
make logs

# 3. Reinicie tudo
make clean
make up-infra
make seed
```

### ❌ "Erro: Jupyter não conecta em LocalStack"

**Causa:** Kernel do notebook precisa ser reiniciado

**Solução:**
- No Jupyter, vá em `Kernel` → `Restart`
- Ou feche e reabra a aba do notebook

## 📚 Stack Tecnológico

| Tecnologia | Versão | Função |
|-----------|--------|--------|
| **PySpark** | 3.5.1 | Processamento distribuído de dados |
| **LocalStack** | 3.x | Emulação local de AWS (S3, DynamoDB) |
| **Terraform** | 1.7+ | Infraestrutura como Código (IaC) |
| **Docker** | Latest | Containerização |
| **Python** | 3.13 | Scripting |
| **Jupyter** | Latest | Exploração interativa |

## 🚀 Próximos Passos

Depois de entender o pipeline:

- [ ] Adicionar mais transformações em `jobs/main.py`
- [ ] Criar novos jobs Spark (ex: análise de tendências)
- [ ] Integrar com Airflow para orquestração
- [ ] Deploy em AWS real (trocar LocalStack)
- [ ] Adicionar testes automatizados
- [ ] Criar alertas/monitoramento

## 📞 Suporte

Dúvidas? Verifique:
1. Logs: `make logs` ou `make logs-localstack`
2. Docker: `docker ps` (containers rodando?)
3. S3: `docker exec localstack aws s3 ls --endpoint-url=http://localhost:4566`
4. Terraform: `docker logs terraform` (sucesso?)

## 🔄 Usando Airflow (Orquestração)

### O que mudou com Airflow

Ao invés de rodar tudo manualmente, Airflow **orquestra e agenda** o pipeline:

- **Antes**: `make run-job` (manual, tudo junto)
- **Agora**: DAG Airflow (automático, diário, tolerância a falhas)

### Estrutura dos Jobs

Os 3 jobs foram separados:

```
jobs/
├── bronze.py  → Extrai dados brutos
├── silver.py  → Limpa e valida
└── gold.py    → Transforma para análise
```

Cada um pode rodar **independente ou orquestrado**.

### PASSO 1️⃣ - Subir Airflow

```bash
make up-airflow
```

Acessa UI: **http://localhost:8081**

### PASSO 2️⃣ - Triggar a DAG

1. Vá em: http://localhost:8081/home
2. Procure a DAG `medallion_pipeline`
3. Clique no ▶️ (play) pra rodar

**Ou via CLI:**
```bash
docker exec airflow airflow dags trigger medallion_pipeline
```

### PASSO 3️⃣ - Ver execução em tempo real

Na UI Airflow, você vê:
- ✅ Cada task (Bronze → Silver → Gold)
- ⏱️ Tempo de execução
- ✋ Se falhou, qual task
- 📊 Logs detalhados

### Rodando jobs individualmente

Mesmo com Airflow, pode rodar jobs isolados:

```bash
make run-bronze   # Só Bronze
make run-silver   # Só Silver
make run-gold     # Só Gold
```

### Agendamento automático

A DAG está configurada para rodar **todo dia às 2:00 AM**:

```python
schedule_interval='0 2 * * *'  # Cron: 2am todo dia
```

**Para mudar**, edite `airflow/dags/pipeline_medallion.py`:

```python
schedule_interval='0 */6 * * *'  # A cada 6 horas
schedule_interval='@hourly'       # A cada hora
schedule_interval=None            # Manual (não agenda)
```

### DAG Airflow - Fluxo Visual

```
Bronze (extrai)
    ↓
Silver (limpa)
    ↓
Gold (transforma)
```

**Tolerância a falhas:**
- Se Bronze falhar → Silver/Gold não rodam
- Se Silver falha 2x → para e avisa

### Logs Airflow

```bash
make logs-airflow
```

Ou via UI: clique na task → "Log"

---

**Resumo:**
- ✅ Jobs separados e independentes
- ✅ Orquestração automática com Airflow
- ✅ Agendamento automático (diário às 2am)
- ✅ Retry e tratamento de erros
- ✅ UI pra monitorar

---

**Criado com ❤️ para aprender PySpark + Arquitetura de Dados + Orquestração**
