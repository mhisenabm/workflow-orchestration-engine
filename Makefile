.PHONY: install lint typecheck test integration migrate-api migrate-engine migrate-worker up down

install:
	python -m pip install -e '.[dev]'

lint:
	ruff check .

format:
	ruff format .

format-check:
	ruff format --check .

typecheck:
	mypy apps packages

test:
	pytest -m 'not integration'

integration:
	pytest -m integration

migrate-api:
	alembic -c infrastructure/postgres/api/alembic.ini upgrade head

migrate-engine:
	alembic -c infrastructure/postgres/engine/alembic.ini upgrade head

migrate-worker:
	alembic -c infrastructure/postgres/worker/alembic.ini upgrade head

up:
	docker compose up --build

down:
	docker compose down -v
