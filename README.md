# Data Replication System for Databricks

A Python plug-in solution to replicate data between Databricks envs. Support and accelerate workloads in multi-cloud migration, single-cloud migration, workspace migration, DR, backup and recovery, multi-cloud data mesh.

Cloud agnostic - cross cloud, cross region

## Overview

This system provides incremental data replication capabilities between Databricks metastores with D2D Delta Share, with specialized handling for Streaming Tables. It supports multiple operation types that can be run independently or together:

- **Backup**: Export Streaming Table backing tables and add tables to Share
- **Replication**: Cross-metastore/same metastore incremental table replication with schema enforcement
- **Reconciliation**: Data validation with row counts, schema checks, and missing data detection

## Supported Object Types
- Streaming Tables (data only, no checkpoints)
- Managed Table
- External Table

## Unsupported Object Types
- Volume Files - WIP
- SQL Views - WIP
- Materialized Views

## Key Features

### Delta Sharing
Option to let the tool setup Delta share automatically for you, i.e. Recipient, Shares and Shared Catalogs. Or BYO Delta share infra

### Incremental Data Replication
The system leverages Deep Clone for incrementality

### Streaming Table Handling
The system automatically handles Streaming Tables complexities:
- Export ST backing tables
- Constructs internal table path using pipeline ID
- Deep clone ST backing tables rather than ST tables directly

### Robust Error Handling
- Configurable retry logic with exponential backoff using tenacity
- Graceful degradation where operations continue if individual tables fail
- Comprehensive error logging with correlation IDs and full stack traces
- All operations tracked in audit tables for troubleshooting

### Flexible Configuration
- YAML-based configuration with Pydantic validation
- CLI args to override YAML configuration
- Schema and table filtering capabilities
- Configurable concurrency and timeout settings

## Installation

### Prerequisites
- Enable Delta Sharing (DS) across clouds. https://docs.databricks.com/aws/en/delta-sharing/set-up#gsc.tab=0
- Source and target Service Principal with metastore admin and workspace admin access
- SP OAuth Token stored in Databricks secrets created if executed outside Databricks
- For Streaming Table replication, tables need to already exist in target DBX


### Getting Started

1. Setup dev env:
```bash
git clone <repository-url>
cd <repository folder>
make setup
```

2. Create first configuration - Follow README.yaml and sample configs in configs folder

3. Run - the system provides a CLI tool `data-replicator` with the following commands:

```bash
# Check all available args
data-replicator --help

# Run all enabled operations
data-replicator <config.yaml>

# Run specific operation only
data-replicator <config.yaml> --operation backup
data-replicator <config.yaml> --operation replication
data-replicator <config.yaml> --operation reconciliation

# Validate configuration without running
data-replicator <config.yaml> --validate-only

# Combine dry run with specific operation
data-replicator <config.yaml> --operation backup --dry-run
```

### Operation Types

#### Backup Operations
- For ST, deep clones ST backing tables from source to backup catalogs.
- Add schemas to share.

#### Replication Operations  
- Deep clone tables across workspaces from share with schema enforcement

#### Reconciliation Operations
- Row count validation
- Schema structure comparison  
- Missing data detection
## Development

### Code Quality Tools
```bash
make quality
```

### Testing
```bash
make test
```

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

### Audit Logging
All operations are logged to the configured audit table with:
- Unique run IDs for correlation
- Operation types and statuses
- Error details and stack traces
- Timing and performance metrics
- 
## Security Considerations

- All sensitive credentials are managed through Databricks secret scopes
- No hardcoded secrets or tokens in configuration files
- Audit logging tracks all operations with unique run IDs
- Configurable timeout and retry settings prevent runaway operations

## Troubleshooting

### Common Issues

1. **DLT Table Access**: Ensure the system has access to `__databricks_internal` catalog for ST backing table operations
2. **Token Permissions**: Verify secret scope access and token permissions for cross-workspace operations

## Contributing

1. Follow the existing code style and patterns
2. Add tests for new functionality
3. Update documentation for significant changes
4. Run the full test suite before submitting changes
5. Use the provided development tools for code quality

## License

This project is proprietary to Databricks.