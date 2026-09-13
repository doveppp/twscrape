.PHONY: prepare install update lint check test test-py test-matrix-py

prepare: lint check

install:
	uv sync

update:
	uv run scripts/update-gql-ops.py
	uv run scripts/update-mocked-data.py refresh

update-deps:
	uv sync --upgrade --all-groups
	uv --preview-features audit audit

lint:
	uv run ruff check --select I --fix .
	uv run ruff format .

check:
	uv run ruff format --check .
	uv run ruff check .
	uv run ty check

# tests run against a real MySQL server; pass its url via DOVEXSCRAPE_DB
test:
	@test -n "$$DOVEXSCRAPE_DB" || (echo "error: DOVEXSCRAPE_DB is not set"; exit 1)
	@uv run pytest -s --cov=dovexscrape tests/

test-py:
	$(eval name=dovexscrape_py$(v))
	@docker -l warning build -f Dockerfile.py-matrix --build-arg VER=$(v) -t $(name) .
	@docker run --network host -e DOVEXSCRAPE_DB $(name)

test-matrix-py:
	@make test-py v=3.10
	@make test-py v=3.11
	@make test-py v=3.12
	@make test-py v=3.13
	@make test-py v=3.14
