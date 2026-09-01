from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineer',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    'medallion_pipeline',
    default_args=default_args,
    description='Medallion Architecture Pipeline: Bronze → Silver → Gold',
    schedule='0 2 * * *',  # Roda todo dia às 2am
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['medallion', 'data-pipeline']
) as dag:

    # Task 1: Bronze - Extração de dados brutos
    task_bronze = BashOperator(
        task_id='extract_bronze',
        bash_command='''
            docker exec spark /opt/spark/bin/spark-submit \
                --master spark://spark:7077 \
                --deploy-mode client \
                /opt/spark-jobs/bronze.py
        ''',
        doc='Extrai dados brutos do CSV para S3 Bronze',
    )

    # Task 2: Silver - Limpeza e validação (depende de Bronze)
    task_silver = BashOperator(
        task_id='transform_silver',
        bash_command='''
            docker exec spark /opt/spark/bin/spark-submit \
                --master spark://spark:7077 \
                --deploy-mode client \
                /opt/spark-jobs/silver.py
        ''',
        doc='Limpa e valida dados Bronze em Silver',
    )

    # Task 3: Gold - Transformações para análise (depende de Silver)
    task_gold = BashOperator(
        task_id='transform_gold',
        bash_command='''
            docker exec spark /opt/spark/bin/spark-submit \
                --master spark://spark:7077 \
                --deploy-mode client \
                /opt/spark-jobs/gold.py
        ''',
        doc='Transforma dados Silver em tabelas analíticas Gold',
    )

    # Define ordem de execução
    task_bronze >> task_silver >> task_gold
