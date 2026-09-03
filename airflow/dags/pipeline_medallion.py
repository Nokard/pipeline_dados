"""Pipeline do GMV diário por subsidiária — arquitetura medallion.

    raw/                        CSVs de CDC entregues pela origem
      └─ bronze                 tipagem + partição por dia, uma tabela por fonte
           └─ silver_eventos    união das 3 fontes numa linha do tempo, sem reenvios
                └─ silver_diario   estado consolidado da compra em cada dia
                     └─ gold        histórico com validade SCD2 (dataset final)

O `raw/` NÃO é produzido aqui: ele é a entrega da origem (no projeto, simulada
por `make seed`). O pipeline começa lendo o que já chegou — por isso a primeira
task é o bronze.

D-1 E REPROCESSAMENTO
O DAG roda diariamente processando o que chegou até o dia anterior. Hoje cada
job reprocessa a tabela inteira, não só a partição do dia: é mais caro, mas o
resultado é idêntico ao incremental porque o pipeline é determinístico
(ordenação total por hash_evento, janelas que não olham para o futuro,
partitionOverwriteMode=dynamic). Verificado: duas execuções completas, e uma
partida a frio com o S3 vazio, produzem os mesmos dados byte a byte.

A otimização incremental seria passar `{{ ds }}` aos jobs e reprocessar apenas
a partição do dia — vale quando o volume crescer, não antes.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}


def spark_submit(job):
    """Roda um job no container do Spark.

    O Airflow tem o socket do Docker montado, então dispara o job no container
    vizinho em vez de manter um Spark próprio — o mesmo caminho que o
    `make run-*` usa, o que evita o pipeline se comportar diferente quando
    rodado à mão e pelo scheduler.

    O caminho completo do spark-submit é obrigatório: ele não está no PATH da
    imagem do Spark.
    """
    return (
        "docker exec spark /opt/spark/bin/spark-submit "
        "--master spark://spark:7077 "
        "--deploy-mode client "
        f"/opt/spark-jobs/{job}"
    )


with DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='GMV diário por subsidiária: bronze → silver → gold (D-1)',
    schedule='0 3 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    doc_md=__doc__,
    tags=['medallion', 'gmv', 'cdc'],
) as dag:

    bronze = BashOperator(
        task_id='bronze',
        bash_command=spark_submit('bronze.py'),
        doc='Lê os CSVs de raw/, aplica schema explícito e particiona por '
            'transaction_date. Uma tabela por fonte — nada é unido aqui.',
    )

    silver_eventos = BashOperator(
        task_id='silver_eventos_unificados',
        bash_command=spark_submit('silver_eventos_unificado.py'),
        doc='Empilha as 3 fontes numa linha do tempo única (UNION, não JOIN), '
            'remove reenvios e mantém o último estado de cada fonte por dia.',
    )

    silver_diario = BashOperator(
        task_id='silver_purchase_diario',
        bash_command=spark_submit('silver_purchase_diario.py'),
        doc='Forward fill por bloco de fonte: repete os dados ativos das fontes '
            'que não enviaram evento no dia, sem apagar os campos que a fonte '
            'enviou vazios (cancelamentos). Colapsa para uma linha por dia.',
    )

    gold = BashOperator(
        task_id='gold_purchase_historico',
        bash_command=spark_submit('gold_purchase_historico.py'),
        doc='Fecha a validade SCD2 de cada versão (dt_inicio/dt_fim/is_current). '
            'Dataset final: o GMV sai de um SELECT em cima desta tabela.',
    )

    bronze >> silver_eventos >> silver_diario >> gold
