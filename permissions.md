## Minimum Permission Required for User/Service Principal at Source Workspace

### Delta Sharing Infra Setup (Source):
```
- CREATE_SHARE - for creating share. controlled by backup_config.create_share
- CREATE_RECIPIENT - for creating recipient. controlled by backup_config.create_recipient
- USE_SHARE, SET SHARE PERMISSION - if share already exists, for granting share to recipient
- USE_RECIPIENT - if recipient already exists, for granting share to recipient
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```

### Data Replication (Source):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on source catalogs
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
- SHARE OWNER - ONLY share owner can add schema to share. controlled by backup_config.add_to_share.
- CREATE_CATALOG - backup_config.create_backup_catalog - required for streaming table only 
- USE_CATALOG, CREATE_SCHEMA on backup catalog if not owner - required for streaming table only 
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on backup catalog if not owner - required for streaming table only 
- USE_CATALOG, USE_SCHEMA, SELECT on __databricks__internal catalog - required for streaming table only 
```

### UC Replication (Source):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on source catalogs
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
- USE_SHARE, MODIFY_SHARE - backup_config.add_to_share
```

### Logging (Source): if audit_config.logging_workspace = 'source'
```
- CREATE_CATALOG - audit_config.create_audit_catalog - required only if audit catalog needs creation
- USE_CATALOG, CREATE_SCHEMA on audit catalog if not owner - required only if audit catalog needs creation
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on audit catalog if not owner
```

## Minimum Permission Required for User/Service Principal at Target Workspace

### Delta Sharing Infra Setup (Target):
```
- CREATE_CATALOG, USE_PROVIDER - replication_config.create_shared_catalog (for replication)
- CREATE_CATALOG, USE_PROVIDER - reconciliation_config.create_shared_catalog (for reconciliation)
```

### Data Replication (Target):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on shared catalogs
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE, WRITE_VOLUME on target catalog
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY on __databricks__internal catalog - required for streaming table only
- CREATE_CATALOG - replication_config.create_intermediate_catalog - required only if intermediate catalog needs creation
- USE_CATALOG, CREATE_SCHEMA on intermediate catalog if not owner - required only if intermediate schema needs creation
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on intermediate catalog if not owner - required only if intermediate table is used
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```

### UC Replication (Target):
```
- CREATE_CATALOG
- USE_CATALOG, CREATE_SCHEMA on target catalog
- USE_CATALOG, USE_SCHEMA, CREATE_VOLUME, MODIFY, CREATE_TABLE, APPLY_TAG on target schema
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```

### Reconciliation (Target):
```
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on shared catalogs
- USE_CATALOG, USE_SCHEMA, SELECT, READ_VOLUME on target catalog
- CREATE_CATALOG - reconciliation_config.create_recon_catalog - required only if recon result catalog needs creation
- USE_CATALOG, CREATE_SCHEMA on recon result catalog if not owner - required only if recon result schema needs creation
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on recon result catalog if not owner
- USE_CATALOG, USE_SCHEMA, SELECT ON system.information_schema schema
```

### Logging (Target): if audit_config.logging_workspace = 'target'
```
- CREATE_CATALOG - audit_config.create_audit_catalog - required only if audit catalog needs creation
- USE_CATALOG, CREATE_SCHEMA on audit catalog if not owner - required only if audit catalog needs creation
- USE_CATALOG, USE_SCHEMA, SELECT, MODIFY, CREATE_TABLE on audit catalog if not owner
```

This granular breakdown provides the specific permissions required for each Delta Sharing operation, avoiding the need for broad administrative roles.