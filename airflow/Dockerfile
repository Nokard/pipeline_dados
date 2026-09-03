FROM apache/airflow:2.9.3

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Script para criar usuário se não existir
COPY entrypoint.sh /entrypoint.sh

ENTRYPOINT ["bash", "/entrypoint.sh"]
