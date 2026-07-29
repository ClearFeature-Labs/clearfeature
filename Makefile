SHELL := /bin/bash

.PHONY: verify test lint format

verify:
	bash scripts/verify.sh

test:
	PYTHONPATH=src python -m pytest -q

lint:
	python -m ruff check .

format:
	python -m ruff format .
