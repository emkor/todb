all: test build

PY3 = python3
VENV_PY3 = ~/.venv/todb/bin/python3

config:
	@echo "---- Doing cleanup ----"
	@mkdir -p ~/.venv
	@rm -rf ~/.venv/todb
	@echo "---- Setting virtualenv ----"
	@$(PY3) -m venv ~/.venv/todb
	@echo "---- Installing dependencies and app itself in editable mode ----"
	@$(VENV_PY3) -m pip install --upgrade pip
	@$(VENV_PY3) -m pip install wheel
	@$(VENV_PY3) -m pip install -r requirements-dev.txt
	@$(VENV_PY3) -m pip install -e .

test:
	@echo "---- Testing ---- "
	@$(VENV_PY3) -m mypy --ignore-missing-imports ./todb
	@$(VENV_PY3) -m pytest -v --cov=./todb ./test

build:
	@echo "---- Building package ---- "
	@$(VENV_PY3) setup.py bdist_wheel --python-tag py3 --dist-dir ./

.PHONY: all config test build
