# UC Replication System for Databricks

A Python plug-in solution to replicate UC data & metadata between Databricks envs. Support and accelerate workloads in multi-cloud migration, single-cloud migration, workspace migration, DR, backup and recovery, multi-cloud data mesh.

Cloud agnostic - cross metastore or same metastore replication

## Overview

This system provides incremental data and UC metadata replication capabilities between Databricks env or within same env with D2D Delta Share, Deep Clone and Autoloader, with specialized handling for Streaming Tables. It supports multiple operation types that can be run independently or together.

## Object Types
### Supported Object Types
- Data Replication
  - Managed Tables
  - External Tables
  - Volume Files
  - Streaming Tables (data only, no checkpoints)
- UC metadata
  - Storage Credentials
  - External Locations
  - Catalogs
  - Schemas
  - Tags (catalog, schema, table, columns, views, volume)
  - Column Comments 
### In Development
- UC metadata
  - Tables
  - Views
  - Volumes
  - Permissions
  - Materialized Views

### Unsupported Object Types
- Databricks Workspace Assets is not yet supported, but maybe considered in future roadmap
- Streaming checkpoints are not replicated. Streaming state handling should be managed outside this solution.

## Supported Operation Types
### Backup Operations
- For ST, deep clones ST backing tables from source to backup catalogs.
- For all table and volume types, add containing schemas to share.
- Not required for UC metadata replication

### Data Replication Operations
- Deep clone tables across workspaces from shared catalog with schema enforcement
- Incremental copy volume files across workspaces from shared catalog using autoloader

### Metadata Replication Operations
- Replicate UC metadata from source uc to target uc using Databricks Connect and Databricks SDK

### Reconciliation Operations (Table only)
- Row count validation
- Schema structure comparison  
- Missing data detection

## Key Features

### Delta Sharing
Flexibility to let the tool setup Delta share infra automatically for you with default names, i.e. Recipient, Shares and Shared Catalogs. Alternatively, use existing Delta share infra

### Run Anywhere
- Run on both Serverless (recommended) and non-Serverless (DBR 16.4+).\
- Run in source, target Databricks workspace, or outside Databricks
- Run in cli, or deployed via DAB as workflow job

### Flexible Configuration
- YAML-based configuration with Pydantic validation
- Hierarchical configuration with inheritance, i.e. table level -> schema level -> catalog level -> replication group
- Environments to manage env specific connection and configurations
- Oject types flexible selective replication
- Catalog, schema and table flexible selective replication
- Replicate from/into catalog of same or different name
  
### Incremental Data Replication
The system leverages native Deep Clone and Autoloader for incrementality and replication performance
- Deep clone for delta table
- Autoloader for volume files with option to specify starting timestamp
- Option to replicate external table as managed table to support managed table migration

### Streaming Table Handling
The system automatically handles Streaming Tables complexities:
- Export ST backing tables to share
- Constructs backing table path using pipeline ID
- Deep clone ST backing tables rather than ST tables directly

### UC Metadata Replication
Replicate UC metadata
- Create or update target UC objects with Databricks SDK
- Incremental tag replication

### Parrallel Replication
- Configurable concurrency with multithreading
- Parrallel tables/volumes replication
- Concurrent volume file copy

### Robust Logging & Error Handling
- Configurable retry logic with exponential backoff
- Graceful degradation where operations continue if individual object fail
- Comprehensive error logging with run id and full stack traces
- All operations tracked in audit tables for monitoring and alerting
- Print out all executed SQL in DEBUG mode for easy troubleshooting
- Check <a href=./docs/loggings.md>here</a> to learn more about loggings

## Prerequisites
- User or Service Principal in source and target workspace created with metastore admin right. If metastore admin permission is not available, check <a href=./docs/permissions.md>here</a> to apply more granular UC access control
- For cross-metastore replication, enable Delta Sharing (DS) including network connectivity https://docs.databricks.com/aws/en/delta-sharing/set-up#gsc.tab=0

- PAT or OAuth Token for user or sp created and stored in Databricks Key Vault.
**Note**: if this tool is run in source workspace, only target workspace token secrets need to be created in source. Conversely, if run in target workspace, source token needs to be created in target.
- Network connectivity to source or target workspace. e.g. if tool runs in source workspace, source data plane (outbound) should be able to establish connect to target workspace control plane (inbound). And vica versa.
**Note**: UC replication requires connect to both source and target workspace using Databricks Connect.
- If tool is running outside of Databricks Workspace and Serverless is unavailable in source and/or target workspace, cluster id for all-purpose cluster in source or/and target workspace needs to be provided


## Getting Started

1. Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html

2. Setup dev env:
```bash
git clone <repository-url>
cd <repository folder>
make setup
source .venv/bin/activate
```

3. Configure <a href=./configs/environments.yaml>environments.yaml</a> file with connection details
   
4. **START REPLICATION**
- Clone and modify sample configs in <a href=./configs>configs</a> folder. Configs with _default suffix allows you to set up replication using system generated default names and settings with minimum configuration.
- High level replication steps are also descrbied in the sample config
- For more comprehensive understanding of available configs, check <a href=./configs/README.yaml>README.yaml</a>


```bash
# Replicate data in the following logical order - The solution can be flexibly configured to replicate all or selected objects and data. Some objects such as storage credentials might be created centrally with Terraform instead
# Storage credentials -> External locations -> Catalogs -> Schemas -> Tables -> Volumes -> Materialized Views -> Views -> Tags -> Column Comments -> Permissions -> Volume Files

# Check all available args
data-replicator --help

# Validate configuration without running
data-replicator <config.yaml> --validate-only

# Replicate Storage credentials and External locations
# Cloud identities setup (AWS role or Azure Managed Identity) with required access to cloud storage
# Configure uc_metadata_defaults.yaml
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --uc-object-types storage_credential,external_location

# Replicate catalogs and schemas
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --uc-object-types catalog,schema --target-catalogs catalog1,catalog2,catalog3 

# Replicate tables for specific catalogs
# If streaming tables are in the catalog, they must firstly be created in the target env using DLT
data-replicator configs/cross_metastore/all_tables_defaults.yaml --target-catalogs catalog1,catalog2,catalog3

# Alternatively replicate tables for specific schemas under a catalog
data-replicator configs/cross_metastore/all_tables_defaults.yaml --target-catalogs catalog1 --target-schemas bronze_1,silver_1

# Alternatively replicate streaming tables only - streaming tables must already exist in target
data-replicator configs/cross_metastore/streaming_tables_defaults.yaml --target-catalogs catalog1 --target-schemas bronze_1,silver_1

# Alternatively replicate delta tables only - streaming tables must already exist in target
data-replicator configs/cross_metastore/delta_tables_defaults.yaml --target-catalogs catalog1 --target-schemas bronze_1,silver_1

# Replicate volume for specific catalogs (Not yet supported - WIP)
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --uc-object-types volume --target-catalogs catalog1,catalog2,catalog3 

# Replicate materialized views for specific catalogs (Not yet supported - WIP)
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --uc-object-types materialized_view --target-catalogs catalog1,catalog2,catalog3 

# Replicate views for specific catalogs (Not yet supported - WIP)
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --uc-object-types view --target-catalogs catalog1,catalog2,catalog3

# Replicate tags & comments
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --uc-object-types table_tag,column_tag,catalog_tag,schema_tag,volume_tag,column_comment --target-catalogs catalog1,catalog2,catalog3 

# Replicate permissions for specific catalogs (Not yet supported - WIP)
# Prerequisite: All user principals should be provisioned in the migrated workspace
data-replicator configs/cross_metastore/uc_metadata_defaults.yaml --uc-object-types permission --target-catalogs catalog1,catalog2,catalog3

# Replicate volume files for specific schemas
data-replicator configs/cross_metastore/volume_defaults.yaml --target-catalogs aaron_replication --target-schemas bronze_1,silver_1

# Alternatively replicate volume files for specific volume
data-replicator configs/cross_metastore/volume_defaults.yaml --target-catalogs aaron_replication --target-schemas bronze_1,silver_1 --target-volumes raw
```

5. Deploy - the tool can be deployed as Workflow Job using DAB. Check example job in <a href=./resources>resources</a> folder
```bash
databricks bundle validate
databricks bundle deploy
```

## How to get help

Databricks support doesn't cover this content. For questions or bugs, please open a GitHub issue and the team will help on a best effort basis.


## License

&copy; 2025 Databricks, Inc. All rights reserved. The source in this notebook is provided subject to the Databricks License [https://databricks.com/db-license-source].  All included or referenced third party libraries are subject to the licenses set forth below.

