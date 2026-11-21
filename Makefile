.PHONY: help install install-dev test test-unit test-integration lint format format-check type-check quality clean setup venv fix

# Python interpreter and virtual environment
PYTHON := python3
VENV_DIR := .venv
VENV_ACTIVATE := . $(VENV_DIR)/bin/activate

# Default target
help:
	@echo "Available commands:"
	@echo "  setup           - Initial environment setup (create venv and install dependencies)"
	@echo "  venv            - Create virtual environment"
	@echo "  install         - Install package in development mode"
	@echo "  install-dev     - Install development dependencies"
	@echo "  test            - Run all tests with coverage"
	@echo "  test-unit       - Run unit tests only"
	@echo "  test-integration - Run integration tests only"
	@echo "  quality         - Run all checks without modifying files (lint, format-check, type-check)"
	@echo "  fix             - Auto-fix formatting and lint issues with ruff"
	@echo "  lint            - Run ruff linter (pycodestyle, pyflakes, isort rules)"
	@echo "  format          - Format code with ruff"
	@echo "  format-check    - Check formatting with ruff without modifying files"
	@echo "  type-check      - Run mypy type checker"
	@echo "  clean           - Clean up build artifacts and cache files"

# Initial environment setup
setup: venv install-dev install
	@echo "Environment setup complete!"
	@echo "To activate virtual environment, run: source $(VENV_DIR)/bin/activate"

# Create virtual environment
venv:
	$(PYTHON) -m venv $(VENV_DIR)
	$(VENV_ACTIVATE) && pip install --upgrade pip
	@echo "Virtual environment created at $(VENV_DIR)"
	@echo "Activate with: source $(VENV_DIR)/bin/activate"

# Install package in development mode
install:
	$(VENV_ACTIVATE) && pip install -e .

# Install development dependencies
install-dev:
	$(VENV_ACTIVATE) && pip install -r requirements-dev.txt

# Run all tests with coverage
test:
	$(VENV_ACTIVATE) && pytest

# Run unit tests only
test-unit:
	$(VENV_ACTIVATE) && pytest -m unit

# Run integration tests only
test-integration:
	$(VENV_ACTIVATE) && pytest -m integration

# Run all code quality checks (read-only; same as CI)
quality: lint format-check type-check
	@echo "All quality checks passed!"

# Run linter (settings in pyproject.toml)
lint:
	$(VENV_ACTIVATE) && ruff check .

# Format code
format:
	$(VENV_ACTIVATE) && ruff format .

# Check formatting without modifying files
format-check:
	$(VENV_ACTIVATE) && ruff format --check .

# Run type checker (settings in pyproject.toml)
type-check:
	$(VENV_ACTIVATE) && mypy

# Auto-fix formatting and lint issues
fix:
	$(VENV_ACTIVATE) && ruff check --fix .
	$(VENV_ACTIVATE) && ruff format .
	@echo "Auto-fixes applied. Run 'make quality' to check for remaining issues."

# Clean up build artifacts and cache files
clean:
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf $(VENV_DIR)/