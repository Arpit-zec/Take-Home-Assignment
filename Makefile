.PHONY: install format check test up down
install:
	python -m pip install -e '.[dev]'
format:
	python -m black app tests
	python -m ruff check --fix app tests
check:
	python -m ruff check app tests
	python -m black --check app tests
	python -m mypy app
test:
	python -m pytest -q
up:
	docker compose up --build -d
down:
	docker compose down
