# Raccourcis. Le pipeline tourne sans eux : `docker compose up -d` suffit.
.PHONY: up down ps logs dbt test lint results figures archive

up:            ## démarre tout le pipeline
	docker compose up -d

down:          ## arrête les conteneurs sans supprimer les volumes
	docker compose down

ps:
	docker compose ps

logs:
	docker compose logs -f consumer producer

dbt:           ## exécute modèles et tests de qualité immédiatement
	docker compose exec dbt dbt build

test:
	uv run pytest -q

lint:
	uv run ruff check src tests scripts

results:       ## exporte les chiffres publiés depuis PostgreSQL
	uv run python scripts/export_results.py

figures:
	uv run --with matplotlib python scripts/figures.py

archive:       ## repli sans Docker : archive brute du flux
	python scripts/archive_gbfs.py data/archive
