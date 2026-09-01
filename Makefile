.PHONY: up down up-infra seed run-job logs clean clean-volumes stop-all tf-init tf-plan tf-apply tf-destroy

up:
	docker compose --env-file .env --profile full -f docker/docker-compose.yml up -d
	@echo "✅ Containers iniciados (LocalStack + Spark)"

up-infra:
	@echo "🚀 Iniciando infraestrutura completa (Docker + Terraform)..."
	docker compose --env-file .env --profile full -f docker/docker-compose.yml up -d
	@echo "⏳ Aguardando Terraform criar infraestrutura S3..."
	docker compose -f docker/docker-compose.yml logs -f terraform 2>/dev/null | grep -q "Apply complete" && echo "✅ Infraestrutura criada!" || sleep 15
	@echo ""
	@echo "Próximos passos:"
	@echo "  make seed    # popula dados"
	@echo "  make run-job # executa pipeline"

down:
	docker compose -f docker/docker-compose.yml down
	@echo "✅ Containers parados"

seed:
	python3 scripts/seed_data.py

run-job:
	docker exec -it spark spark-submit \
		--master spark://spark:7077 \
		--deploy-mode client \
		/opt/spark-jobs/main.py
logs:
	docker compose -f docker/docker-compose.yml logs -f spark

logs-localstack:
	docker compose -f docker/docker-compose.yml logs -f localstack

clean-volumes:
	docker compose -f docker/docker-compose.yml down -v
	@echo "✅ Volumes deletados"

stop-all:
	docker stop spark localstack 2>/dev/null || true
	docker rm spark localstack 2>/dev/null || true
	@echo "✅ Containers parados e removidos"

clean:
	docker compose -f docker/docker-compose.yml down -v
	rm -rf localstack-data/
	@echo "✅ Tudo limpo (volumes + dados)"

tf-plan:
	docker compose -f docker/docker-compose.yml run --rm terraform plan
	@echo "✅ Plano gerado"

tf-apply:
	docker compose -f docker/docker-compose.yml run --rm terraform apply -auto-approve
	@echo "✅ Infraestrutura criada"

tf-destroy:
	docker compose -f docker/docker-compose.yml run --rm terraform destroy -auto-approve
	@echo "✅ Infraestrutura destruída"
