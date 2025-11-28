# Data Replication System for Databricks

A Python plug-in solution to replicate data & uc metadata between Databricks envs. Support and accelerate workloads in multi-cloud migration, single-cloud migration, workspace migration, DR, backup and recovery, multi-cloud data mesh.

Cloud agnostic - cross metastore or same metastore replication

## Overview

This system provides incremental data and UC metadata replication capabilities between Databricks env or within same env with D2D Delta Share and deep clone, with specialized handling for Streaming Tables. It supports multiple operation types that can be run independently or together.

## Supported Object Types
- Data Replication
  - Streaming Tables (data only, no checkpoints)
  - Managed Table
  - External Table
- UC metadata
  - Tags (catalog, schema, table, columns, views, volume)
  - Column Comments
  
## WIP
- Data Replication
  - Volume Files
- UC metadata
  - Storage Credentials
  - External Location
  - Catalog
  - Schema
  - Views
  - Volume
  - Permissions

## Unsupported Object Types
- Materialized Views
- Streaming checkpoints

## Supported Operation Types
### Backup Operations - Export Streaming Table backing tables and add schema to Share
- For ST, deep clones ST backing tables from source to backup catalogs.
- For all table and volume types, add containing schemas to share.
- Not required for uc metadata replication

### Replication Operations - Cross-metastore/same metastore incremental data and uc replication
- Deep clone tables across workspaces from shared catalog with schema enforcement
- Incremental copy volume files across workspaces from shared catalog using autoloader
- Replicate UC metadata from source uc to target uc (not through delta share)

### Reconciliation Operations (Table only)
- Row count validation
- Schema structure comparison  
- Missing data detection

## Key Features

### Delta Sharing
Flexibility to let the tool setup Delta share infra automatically for you, i.e. Recipient, Shares and Shared Catalogs. Alternatively, use existing Delta share infra

### Incremental Data Replication
The system leverages Deep Clone and Autoloader for incrementality and replication performance

### Streaming Table Handling
The system automatically handles Streaming Tables complexities:
- Export ST backing tables
- Constructs backing table path using pipeline ID
- Deep clone ST backing tables rather than ST tables directly

### UC Metadata Replication
Export and import UC metadata including support for tags

### Run Anywhere
- The tool can be executed in source, target workspace, or via external compute
- The tool can be executed in cli, or deployed via DAB as workflow job

### Flexible Configuration
- YAML-based configuration with Pydantic validation
- Hierarchical configuration with overrides, i.e. cli args -> yaml config file level -> yaml config catalog level
- Catalog, schema and table flexible selective replication
- Replicate from/into catalog of same or different name
- Configurable concurrency and timeout settings

### Robust Logging & Error Handling
- Configurable retry logic with exponential backoff
- Graceful degradation where operations continue if individual tables fail
- Comprehensive error logging with run id and full stack traces
- All operations tracked in audit tables for monitoring and alerting
- Print out all executed SQL in DEBUG mode for easy troubleshooting

### Logging Details
Object level result details are recorded in configurable audit table location in either source or target workspace (default to target). Detailed execution log can be configured to store in json/txt in files
#### Audit Log Table Schema
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| run_id | STRING | Unique identifier for each execution run |
| logging_time | TIMESTAMP | When the log entry was created |
| operation_type | STRING | Type of operation (backup,replication,uc_replication,reconciliation) |
| catalog_name | STRING | Target catalog name |
| schema_name | STRING | Target schema name |
| object_name | STRING | Target Object name |
| object_type | STRING | Type of object (table, view, etc.) |
| status | STRING | Operation status (success or failed) |
| start_time | TIMESTAMP | Operation start time |
| end_time | TIMESTAMP | Operation end time |
| duration_seconds | DOUBLE | Operation duration in seconds |
| error_message | STRING | Error message if operation failed |
| details | STRING | Additional operation details in json string|
| attempt_number | INT | Current retry attempt number |
| max_attempts | INT | Maximum allowed retry attempts configured |
| config_details | STRING | JSON serialized configuration details |
| execution_user | STRING | User executing the operation |

## Prerequisites
- User or Service Principal in source and target workspace created with metastore admin right. If metastore admin permission is not available, check <a href=./permissions.md>here</a> to apply more granular UC access control
- For cross-metastore replication, enable Delta Sharing (DS) including network connectivity https://docs.databricks.com/aws/en/delta-sharing/set-up#gsc.tab=0

- PAT or OAuth Token for user or sp created and stored in Databricks Key Vault.
**Note**: if this tool is run in source workspace, only target workspace token secrets need to be created in source. Conversely, if run in target workspace, source token needs to be created in target.
- Network connectivity to source or target workspace. e.g. if tool runs in source workspace, source data plane (outbound) should be able to establish connect to target workspace control plane (inbound). And vica versa.
**Note**: UC replication requires connect to both source and target workspace using Databricks Connect.
- If tool is running outside of Databricks Workspace and Serverless is unavailable in source and/or target workspace, cluster id for all-purpose cluster in source or/and target workspace


## Getting Started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Setup dev env:
```bash
git clone <repository-url>
cd <repository folder>
make setup
source .venv/bin/activate
```
3. Create your first configuration 
- Clone and modify sample configs in configs folder. Configs with _default suffix allows you to set up replication with minimum configuration.
- Detailed instruction are also provided in the sample config
- For more comprehensive understanding of available configs, check <a href=./configs/README.yaml>README.yaml</a>

4. Run - the system provides a CLI tool `data-replicator` with the following commands:
```bash
# Check all available args
data-replicator --help

# Validate configuration without running
data-replicator <config.yaml> --validate-only

# Replicate delta tables for specific schemas
data-replicator configs/cross_metastore/delta_tables_defaults.yaml --target-catalog aaron_replication --target-schemas bronze_1,silver_1

# Replicate streaming tables for specific catalog
data-replicator configs/cross_metastore/streaming_tables_defaults.yaml --target-catalog aaron_replication

# Replicate uc metadata - tags
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --target-catalog aaron_replication --uc-object-types table_tag,column_tag,catalog_tag,schema_tag,volume_tag

# Replicate uc metadata - column comment
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --target-catalog aaron_replication --uc-object-types column_comment
```

5. Deploy - the tool can be deployed as Workflow Job using DAB. Check resources folder
```bash
databricks bundle validate
databricks bundle deploy
```

## Video Overview

Include a GIF overview of what your project does. Use a service like Quicktime, Zoom or Loom to create the video, then convert to a GIF.



## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.


## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

