## Configuration Inheritance

Configurations can be defined at different level within the yaml file, child object will inherit all configs from its parent but can override by defining different config value at child level. Dictionary type config will be merged from child to parent recursively, but List type config will be completed replaced.

The hierararchical inheritance follows the following sequence: Replication Group -> Catalog -> Schema -> Table


```yaml
# replication level 
table_types: ["managed","external"]
reconciliation_config:
  enabled: true
  missing_data_check: false.  # by default, apply to all catalogs

target_catalogs:
  # catalog level  
  - catalog_name: "catalog_1"
    reconciliation_config:
      missing_data_check: true # override at catalog level, true for to all schemas
    target_schemas:
      # schema_level  
      - schema_name: "bonze_1"    
        reconciliation_config:
          missing_data_check: false   # override at schema level, false for to all tables
      # schema_level  
      - schema_name: "silver_1"   # no override at schema level, true for to all tables
        tables:
          # table level
          - table_name: "table_1"
            reconciliation_config:
              missing_data_check: false     # override at table level, false for table_1
          # table level
          - table_name: "table_2"         # no override at schema level, true for table_2   
  - catalog_name: "catalog_2"        # missing_data_check no override at catalog level, false for all tables 
    table_types: ["streaming_table"]   # table_types override at catalog level, streaming_table for all tables (replace table_type list at replication group level)
```

#### Config Availability Levels
Table below shows which configs are available to be defined at each hierarchy level.
| Config Group Name | Replication Group Level | Catalog Level | Schema Level | Table Level |
|-------------|-----------|-------------|-------------|-------------|
| audit_config| YES |  |
| logging | YES
| storage_credential_config | YES |  |
| cloud_url_mapping | YES |  
| external_location_config | YES | 
| target_catalogs | YES  
| uc_object_types | YES | YES |YES |
| table_types | YES | YES |YES |
| volume_types | YES | YES |YES |
| backup_config | YES | YES |YES |
| replication_config| YES | YES |YES |YES |
| reconciliation_config| YES | YES |YES |YES |
| concurrency | YES | YES |YES |YES |
| retry | YES | YES |YES |YES |


## Predictive Optimization Check

Predictive optimization (PO) runs background `OPTIMIZE` / `VACUUM` on tables. If PO is enabled on an existing target table that the replicator deep clones into, those rewrites change the target's files and Delta versions underneath the clone, which can cause concurrent-write conflicts and version drift on repeat/incremental clones.

Set `replication_config.predictive_optimization_check` (default `warn`) at any config level:

| Value | Behaviour |
|-------|-----------|
| `off` | No check. |
| `warn` | Logs a warning and records the check in the table's RunResult `details.predictive_optimization`. The table is still cloned. |
| `fail` | Skips the table. Its RunResult is `failed` with `error_message` starting `PO_ENABLED_ON_TARGET`. |

How the check works:
- Runs only when the target table already exists. Before the clone, the replicator calls `tables.get` on the target workspace and reads `effective_predictive_optimization_flag`. The flag is resolved server-side through catalog -> schema -> table inheritance, so `inherited_from_name` / `inherited_from_type` show where ENABLE came from.
- Only a confirmed `ENABLE` on a `MANAGED` target warns or blocks. `DISABLE`, `INHERIT`, external / streaming / view targets, and missing targets are not acted on.
- Fail-open: an API error or an unreadable flag continues the replication and logs at debug level.
- Not in scope: the replicator does not change PO settings on existing target catalogs / schemas / tables. PO is only disabled on catalogs and schemas the replicator creates (`replicate_enable_predictive_optimization`).

```yaml
replication_config:
  predictive_optimization_check: fail   # skip tables whose existing managed target has PO enabled
```


## Replication Group Configuration Override
Replication group level configs can be overriden by <a href=../configs/environments.yaml>environments.yaml</a> and CLI arguments (see blow). CLI takes precedence over environments.yaml, which takes precedence over replication group.


### Environments.yaml Override
All replication group level configs can be defined in <a href=../configs/environments.yaml>environments.yaml</a>, which is used defined connections and env specific configs. environments.yaml takes precedence over replication group level. Similar to config inheritance, dictionary type config will be merged from environments.yaml to replication group recursively, but List type config will be completed replaced.

### CLI Override
Some replication group level configs can be overriden/provided by CLI args.
| Config Group Name | CLI argument Name 
|-------------|-----------|
| logging.level | --logging-level 
| target_catalogs | --target-catalogs, --target-schemas,--target-tables,--target-volumes,--schema-table-filter-expression 
| uc_object_types |  --uc-object-types
| table_types | --table-types
| volume_types | --volume-types
| replication_config.max_concurrent_copies | --volume-max-concurrent-copies
| replication_config.delete_and_reload | --volume-delete-and-reload
| replication_config.delete_checkpoint |  --volume-delete-and-reload
| replication_config.folder_path |  --volume-folder-path
| replication_config.autoloader_options | --volume-autoloader-options
| replication_config.streaming_timeout_seconds | --volume-streaming-timeout-secs

**Note:** CLI arg only overrides at replication group level, catalog/schema/table level config still take precedence

## Substitutes
The following substitutes are available in yaml config. Scope defines where the substitution will take place. e.g. {{schema_name}} will only be substitute within the schema being processed, i.e. scope=Schema, so any schema level or table level config can use it
| Substitute Name | Substituted By | Scope
|-------------|-----------|-----------|
| {{source_name}} | source_databricks_connect_config.name | Replication Group
| {{target_name}} | target_databricks_connect_config.name | Replication Group
| {{catalog_name}}|  target catalog name being processed | Catalog
| {{schema_name}} |  target schema name being processed | Schema
| {{table_name}} | target table name being processed | Table

Below is an example:
```yaml
table_types: ["external", "managed"]
backup_config:
  enabled: true
  share_name: "{{catalog_name}}_share"
replication_config:
  enabled: true
  source_catalog: "{{catalog_name}}_share_{{source_name}}"
reconciliation_config:
  enabled: true
  source_catalog: "{{catalog_name}}_share_{{source_name}}"

target_catalogs:
  - catalog_name: "aaron_replication"
    target_schemas:
      - schema_name: "bronze_1"
        tables:
          - table_name: "{{schema_name}}_managed_table_1"
          - table_name: "{{schema_name}}_managed_table_2"
          - table_name: "external_table_1"
            replication_config:
          - table_name: "file_6"
      - schema_name: "{{catalog_name}}_silver_1"
```