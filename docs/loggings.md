## Logging Details

### Audit Log
Object level detailed logs are recorded in configurable audit table using ***audit_config*** in yaml config. 
```yaml
# Mandatory: Audit logging configuration
audit_config:
  # Mandatory: audit table full name
  audit_table: "data_replication.audit.logging"
  # Mandatory: if logged at source or target. default to target
  logging_workspace: "target"
  # Optional: whether to create audit catalog if it does not exist. default is false.
  create_audit_catalog: false
  # Optional: audit catalog location. default is None.
  audit_catalog_location: null
```

#### Audit Log Table Schema
| Column Name | Data Type | Description |
|-------------|-----------|-------------|
| run_id | STRING | Unique identifier for each execution run |
| logging_time | TIMESTAMP | When the log entry was created |
| operation_type | STRING | Type of operation (backup,replication,uc_replication,reconciliation) |
| catalog_name | STRING | Target catalog name |
| schema_name | STRING | Target schema name |
| object_name | STRING | Target object name |
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

### Execution Log
Detailed execution log can also be configured to store in json/txt in files
```yaml
# Optional: Logging settings
logging:
  # Optional: logging level. default is "INFO".
  # Supported levels: DEBUG, INFO, WARNING, ERROR, CRITICAL
  level: "INFO"
  # Optional: log format. default is "json".
  # Supported formats: "text" or "json"
  format: "json"
  # Optional: whether to log to file. default is false.
  log_to_file: false
  # Optional: log file path. default is None.
  # Required if log_to_file is true
  log_file_path: null
```

### Volume File Ingestion Log
File level ingestion logs are recorded in table configured in ***volume_config***
```yaml
volume_config:
# Optional: whether to create detailed file ingestion logging catalog if it does not exist. default is false.
create_file_ingestion_logging_catalog: false
# Optional: detailed file ingestion logging catalog name. default to same as audit catalog.
file_ingestion_logging_catalog: "replication"
# Optional: detailed file ingestion logging catalog location. default is None.
file_ingestion_logging_catalog_location: null
# Optional: detailed file ingestion logging outputs schema name. default to same as audit schema.
file_ingestion_logging_schema: "audit"
# Optional: detailed file ingestion logging outputs schema name. default to detail_file_ingestion_logging.
file_ingestion_logging_table: "detail_file_ingestion_logging"
```

### Detailed Reconciliation Log
Detailed recon mismatched results are recorded in table configured in ***reconciliation_config***
```yaml
reconciliation_config:
  # Mandatory at replication group level or catalog level: whether to perform reconciliation.
  enabled: true
  # Optional: whether to create reconciliation catalog if it does not exist. default is false.
  create_recon_catalog: false
  # Optional: reconciliation outputs catalog name. default to same as audit catalog.
  recon_outputs_catalog: "replication"
  # Optional: reconciliation catalog location. default is None.
  recon_catalog_location: null
  # Optional: reconciliation outputs schema name. default to same as audit schema.
  recon_outputs_schema: "audit"
  # Optional: reconciliation outputs table name for schema check mismatches. default is recon_schema_comparison.
  recon_schema_check_table: "recon_schema_comparison"
  # Optional: reconciliation outputs table name for missing data mismatches. default is recon_missing_data_comparison.
  recon_missing_data_table: "recon_missing_data_comparison"  
```