# Data Replication System for Databricks

A comprehensive data replication system for Databricks with support for backup, replication, and reconciliation of data across different cloud and metastores.

## Overview

This system provides incremental data replication capabilities between Databricks metastores, with specialized handling for Streaming Tables. It supports multiple operation types that can be run independently or together:

- **Backup**: Deep clone operations from DLT internal tables to backup catalogs
- **Replication**: Cross-metastore incremental table replication with schema enforcement
- **Reconciliation**: Data validation with row counts, schema checks, and missing data detection

## Supported Object Types
- Streaming Tables (data only, no checkpoints)
- Managed Table
- External Table

## Unsupported Object Types
- Materialized Views
- SQL Views
- Volume Files

## Key Features

### Incremental Data Refresh
The system leverages Deep Clone for incrementality

### Streaming Table Handling
The system automatically handles Streaming Tables complexities:
- Extracts pipeline IDs using `DESCRIBE DETAIL`
- Constructs internal table path using pipeline ID
- Performs operations on internal tables rather than DLT tables directly

### Robust Error Handling
- Configurable retry logic with exponential backoff using tenacity
- Graceful degradation where operations continue if individual tables fail
- Comprehensive error logging with correlation IDs and full stack traces
- All operations tracked in audit tables for troubleshooting

### Flexible Configuration
- YAML-based configuration with Pydantic validation
- Support for multiple catalogs with different operation configurations
- Schema and table filtering capabilities
- Configurable concurrency and timeout settings

## Installation

### Prerequisites
- Cloud Token based D2D Delta Sharing (DS) Enabled, i.e. Source as DS Provider, Target as DS Recipient
- Source and target Service Principal with metastore admin and workspace admin access
- SP OAuth Token stored in Databricks secrets created if executed outside Databricks
- For Streaming Table replication, target tables need to already exist


### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd data_replication
```

2. Create and activate virtual environment:
```bash
python3 -m venv .venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements-dev.txt
```

4. Install the package in development mode:
```bash
pip install -e .
```

## Configuration

Create a YAML configuration file (see `configs/config.yaml` for example):

```yaml
version: "1.0"
replication_group: "my_test_group"

# Source Databricks Connect configuration
source_databricks_connect_config:
  name: "source_workspace"
  host: "https://source-databricks-instance"
  token:
    secret_scope: "my_secret_scope"
    secret_key: "source_token_key"

# Target Databricks Connect configuration  
target_databricks_connect_config:
  name: "target_workspace"
  host: "https://target-databricks-instance"
  token:
    secret_scope: "my_secret_scope"
    secret_key: "target_token_key"

# Audit configuration
audit_config:
  audit_table: "my_catalog.audit.replication_logs"

# Target catalogs configuration
target_catalogs:
  - catalog_name: "my_catalog"
    schema_filter_expression: "databaseName like 'prod_%'"
    
    backup_config:
      enabled: true
      source_catalog: "source_catalog"
      backup_catalog: "backup_catalog"
    
    replication_config:
      enabled: true
      source_catalog: "shared_catalog"
      intermediate_catalog: "staging_catalog"
      enforce_schema: true
      
    reconciliation_config:
      enabled: true
      source_catalog: "source_catalog"
      recon_outputs_catalog: "recon_results"
      schema_check: true
      row_count_check: true
      missing_data_check: true
    
    target_schemas:
      - schema_name: "prod_schema1"
      - schema_name: "prod_schema2"
        tables:
          - table_name: "important_table"
        exclude_tables:
          - table_name: "temp_table"

# Performance settings
concurrency:
  max_workers: 4
  timeout_seconds: 3600

retry:
  max_attempts: 3
  retry_delay_seconds: 5
```

## Usage

### Command Line Interface

The system provides a CLI tool `data-replicator` with the following commands:

```bash
# Run all enabled operations
data-replicator config.yaml

# Run specific operation only
data-replicator config.yaml --operation backup
data-replicator config.yaml --operation replication
data-replicator config.yaml --operation reconciliation

# Validate configuration without running
data-replicator config.yaml --validate-only

# Dry run to preview operations
data-replicator config.yaml --dry-run

# Combine dry run with specific operation
data-replicator config.yaml --operation backup --dry-run

# Enable verbose logging
data-replicator config.yaml --verbose
```

### Operation Types

#### Backup Operations
Deep clones tables from source to backup catalogs, handling DLT internal tables automatically.

#### Replication Operations  
Replicates tables across workspaces with schema enforcement and Delta Sharing integration.

#### Reconciliation Operations
Validates data consistency between source and target with configurable checks:
- Row count validation
- Schema structure comparison  
- Missing data detection

## Architecture

### Core Components
- **main.py**: Primary CLI interface and orchestration logic
- **config/**: Configuration models with Pydantic validation and YAML loading
- **providers/**: Operation-specific providers (backup, replication, reconciliation)
- **audit/**: Structured logging and audit trail system
- **databricks_operations.py**: Core Databricks utilities and table operations
- **utils.py**: Spark session management and retry utilities

### Provider Architecture
Each operation type is implemented as a provider:
- **BackupProvider**: Handles deep clone operations
- **ReplicationProvider**: Manages cross-workspace replication
- **ReconciliationProvider**: Performs data validation checks
- **ProviderFactory**: Creates and manages provider instances

## Development

### Code Quality Tools
```bash
# Format code
black src/ tests/

# Sort imports  
isort src/ tests/

# Type checking
mypy src/

# Lint code
flake8 src/ tests/

# Run all quality checks
black src/ tests/ && isort src/ tests/ && flake8 src/ tests/ && mypy src/
```

### Testing
```bash
# Run all tests with coverage
pytest

# Run specific test types
pytest -m unit      # Unit tests only
pytest -m integration  # Integration tests only
pytest -m slow      # Slow running tests

# Run tests with detailed coverage report
pytest --cov=data_replication --cov-report=html --cov-report=term-missing
```

## Security Considerations

- All sensitive credentials are managed through Databricks secret scopes
- No hardcoded secrets or tokens in configuration files
- Audit logging tracks all operations with unique run IDs
- Configurable timeout and retry settings prevent runaway operations

## Troubleshooting

### Common Issues

1. **DLT Table Access**: Ensure the system has access to `__databricks_internal` catalog for DLT operations
2. **Token Permissions**: Verify secret scope access and token permissions for cross-workspace operations
3. **Catalog Creation**: Some environments may not support automatic catalog creation
4. **Schema Filtering**: Check filter expressions if schemas are not being processed as expected

### Audit Logging
All operations are logged to the configured audit table with:
- Unique run IDs for correlation
- Operation types and statuses
- Error details and stack traces
- Timing and performance metrics

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation for significant changes
4. Run the full test suite before submitting changes
5. Use the provided development tools for code quality

## License

This project is proprietary to Databricks.