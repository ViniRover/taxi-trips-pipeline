.PHONY: up down logs validate

up:
	docker compose up --build -d

down:
	docker compose down

logs:
	docker compose logs -f airflow-scheduler

validate:
	docker compose config --quiet

