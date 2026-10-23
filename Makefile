.PHONY: prepare install update lint check test test-py test-matrix-py

prepare: lint check

install:
	uv sync

update:
	uv run --frozen scripts/update-gql-ops.py
	uv run --frozen scripts/update-mocked-data.py refresh

update-deps:
	uv sync --upgrade --all-groups
	uv --preview-features audit audit

lint:
	uv run --frozen ruff check --select I --fix twscrape tests
	uv run --frozen ruff format twscrape tests

check:
	uv run --frozen ruff format --check twscrape tests
	uv run --frozen ruff check twscrape tests
	uv run --frozen ty check twscrape tests

test:
	@uv run --frozen pytest -s --cov=twscrape tests/

test-py:
	$(eval name=twscrape_py$(v))
	@docker -l warning build -f Dockerfile.py-matrix --build-arg VER=$(v) -t $(name) .
	@docker run $(name)

test-matrix-py:
	@make test-py v=3.10
	@make test-py v=3.11
	@make test-py v=3.12
	@make test-py v=3.13
	@make test-py v=3.14
