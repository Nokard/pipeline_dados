.PHONY: up down up-infra up-airflow seed run-bronze run-silver run-silver-eventos run-silver-diario run-gold logs logs-airflow logs-localstack clean clean-volumes stop-all tf-plan tf-apply tf-destroy

up:
	docker compose --env-file .env --profile full -f docker/docker-compose.yml up -d
	@echo "✅ Containers iniciados (LocalStack + Spark)"

up-infra:
	@echo "🚀 Iniciando infraestrutura completa (Docker + Terraform)..."
	docker compose --env-file .env --profile full -f docker/docker-compose.yml up -d
	@echo "⏳ Aguardando Terraform criar infraestrutura S3..."
	docker compose --env-file .env -f docker/docker-compose.yml logs -f terraform 2>/dev/null | grep -q "Apply complete" && echo "✅ Infraestrutura criada!" || sleep 15
	@echo ""
	@echo "Próximos passos:"
	@echo "  make seed    # popula dados"
	@echo "  make run-bronze / run-silver / run-gold  # executa o pipeline"

# Os serviços têm profiles ("full", "orchestration"). Sem repetir os profiles
# aqui, o compose ignora todos eles e o down não derruba nada.
down:
	docker compose --env-file .env --profile full --profile orchestration -f docker/docker-compose.yml down
	@echo "✅ Containers parados"

seed:
	./venv/bin/python scripts/seed_data.py

run-bronze:
	docker exec spark /opt/spark/bin/spark-submit \
		--master spark://spark:7077 \
		--deploy-mode client \
		/opt/spark-jobs/bronze.py

run-silver: run-silver-eventos run-silver-diario

run-silver-eventos:
	docker exec spark /opt/spark/bin/spark-submit \
		--master spark://spark:7077 \
		--deploy-mode client \
		/opt/spark-jobs/silver_eventos_unificado.py

run-silver-diario:
	docker exec spark /opt/spark/bin/spark-submit \
		--master spark://spark:7077 \
		--deploy-mode client \
		/opt/spark-jobs/silver_purchase_diario.py


run-gold:
	docker exec spark /opt/spark/bin/spark-submit \
		--master spark://spark:7077 \
		--deploy-mode client \
		/opt/spark-jobs/gold_purchase_historico.py


up-airflow:
	@echo "🚀 Construindo imagem customizada do Airflow..."
	docker compose --env-file .env --profile orchestration -f docker/docker-compose.yml build airflow
	@echo "🚀 Iniciando Airflow..."
	docker compose --env-file .env --profile orchestration -f docker/docker-compose.yml up -d airflow
	@echo "✅ Airflow iniciado (UI: http://localhost:8081)"
	@echo "📝 Login: admin / admin"

logs:
	docker compose --env-file .env -f docker/docker-compose.yml logs -f spark

logs-airflow:
	docker compose --env-file .env -f docker/docker-compose.yml logs -f airflow

logs-localstack:
	docker compose --env-file .env -f docker/docker-compose.yml logs -f localstack

clean-volumes:
	docker compose --env-file .env -f docker/docker-compose.yml down -v
	@echo "✅ Volumes deletados"

stop-all:
	docker stop spark localstack airflow 2>/dev/null || true
	docker rm spark localstack airflow 2>/dev/null || true
	@echo "✅ Containers parados e removidos"

clean:
	docker compose --env-file .env -f docker/docker-compose.yml down -v
	rm -rf localstack-data/
	@echo "✅ Tudo limpo (volumes + dados)"

# O serviço terraform tem entrypoint /bin/sh, então o comando precisa vir
# como -c "..." — passar "apply" direto faz o sh procurar um arquivo com esse nome.
tf-plan:
	docker compose --env-file .env -f docker/docker-compose.yml run --rm terraform -c "terraform init -upgrade && terraform plan"
	@echo "✅ Plano gerado"

tf-apply:
	docker compose --env-file .env -f docker/docker-compose.yml run --rm terraform -c "terraform init -upgrade && terraform apply -auto-approve"
	@echo "✅ Infraestrutura criada"

tf-destroy:
	docker compose --env-file .env -f docker/docker-compose.yml run --rm terraform -c "terraform init -upgrade && terraform destroy -auto-approve"
	@echo "✅ Infraestrutura destruída"
