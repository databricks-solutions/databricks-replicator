.PHONY: help install install-dev test test-unit test-integration lint format type-check quality clean setup venv fix

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
	@echo "  quality         - Run all code quality checks (lint, format, type-check)"
	@echo "  fix             - Auto-fix formatting, linting, and type issues"
	@echo "  lint            - Run flake8 linter"
	@echo "  format          - Format code with black and isort"
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

# Run all code quality checks
quality: format lint type-check
	@echo "All quality checks passed!"

# Run linter
lint:
	$(VENV_ACTIVATE) && flake8 src/

# Format code
format:
	$(VENV_ACTIVATE) && black src/ tests/
	$(VENV_ACTIVATE) && isort src/ tests/

# Run type checker
type-check:
	$(VENV_ACTIVATE) && mypy src/

# Auto-fix formatting, linting, and type issues
fix:
	$(VENV_ACTIVATE) && black --line-length 78 src/ tests/
	$(VENV_ACTIVATE) && isort src/ tests/
	$(VENV_ACTIVATE) && autoflake --remove-all-unused-imports --remove-unused-variables --remove-duplicate-keys --in-place --recursive src/ tests/
	find src/ tests/ -name "*.py" -exec sed -i '' 's/[[:space:]]*$$//' {} \;
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