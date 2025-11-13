"""
Configuration models for the data replication system using Pydantic.

This module defines all the configuration models that validate and parse
the YAML configuration file for the data replication system.
"""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

class ExecuteAt(str, Enum):
    """Enumeration for where to execute certain operations."""

    SOURCE = "source"
    TARGET = "target"

class TableType(str, Enum):
    """Enumeration of supported table types."""

    MANAGED = "managed"
    STREAMING_TABLE = "streaming_table"
    EXTERNAL = "external"


class VolumeType(str, Enum):
    """Enumeration of supported volume types."""

    MANAGED = "managed"
    EXTERNAL = "external"
    ALL = "all"

class AuthType(str, Enum):
    """Enumeration of authentication types."""

    PAT = "pat"
    OAUTH = "oauth"

class UCObjectType(str, Enum):
    """Enumeration of Unity Catalog object types for replication."""

    CATALOG = "catalog"
    CATALOG_TAG = "catalog_tag"
    SCHEMA = "schema"
    SCHEMA_TAG = "schema_tag"
    VIEW = "view"
    VIEW_TAG = "view_tag"
    VOLUME = "volume"
    VOLUME_TAG = "volume_tag"
    TABLE_TAG = "table_tag"
    COLUMN_TAG = "column_tag"
    COLUMN_COMMENT = "column_comment"
    ALL = "all"

class SecretConfig(BaseModel):
    """Configuration for Databricks secrets."""

    secret_scope: str
    secret_pat: Optional[str] = None
    secret_client_id: Optional[str] = None
    secret_client_secret: Optional[str] = None

    @model_validator(mode="after")
    def validate_secret_fields(self):
        """Ensure at least one secret field is provided."""
        if not any([self.secret_pat, self.secret_client_id, self.secret_client_secret]):
            raise ValueError("At least one secret field must be provided")
        if self.secret_client_id and not self.secret_client_secret:
            raise ValueError("secret_client_secret is required when secret_client_id is provided")
        if self.secret_client_secret and not self.secret_client_id:
            raise ValueError("secret_client_id is required when secret_client_secret is provided")
        return self
class AuditConfig(BaseModel):
    """Configuration for audit tables"""

    audit_table: str
    logging_workspace: ExecuteAt = ExecuteAt.TARGET
    create_audit_catalog: Optional[bool] = False
    audit_catalog_location: Optional[str] = None

    @field_validator("audit_table")
    @classmethod
    def validate_audit_table(cls, v):
        """Validate and normalize audit table name."""
        if not v or not v.strip():
            raise ValueError("audit_table cannot be empty")
        # Convert to lowercase
        table_name = v.lower().strip()
        # Ensure it's in the format catalog.schema.table
        parts = table_name.split(".")
        if len(parts) != 3:
            raise ValueError(
                f"audit_table must be in format 'catalog.schema.table', got: {v}"
            )
        return table_name


class DatabricksConnectConfig(BaseModel):
    """Configuration for Databricks Connect."""

    name: str
    sharing_identifier: Optional[str] = None
    host: str
    auth_type: Optional[AuthType] = AuthType.PAT
    token: Optional[SecretConfig] = None
    cluster_id: Optional[str] = None


class TableConfig(BaseModel):
    """Configuration for individual tables."""

    table_name: str

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v):
        """Convert table name to lowercase."""
        return v.lower() if v else v


class VolumeConfig(BaseModel):
    """Configuration for individual volumes."""

    volume_name: str

    @field_validator("volume_name")
    @classmethod
    def validate_volume_name(cls, v):
        """Convert volume name to lowercase."""
        return v.lower() if v else v


class SchemaConfig(BaseModel):
    """Configuration for individual schemas."""

    schema_name: str
    tables: Optional[List[TableConfig]] = None
    exclude_tables: Optional[List[TableConfig]] = None
    volumes: Optional[List[VolumeConfig]] = None
    exclude_volumes: Optional[List[VolumeConfig]] = None

    @field_validator("schema_name")
    @classmethod
    def validate_schema_name(cls, v):
        """Convert schema name to lowercase."""
        return v.lower() if v else v


class BackupConfig(BaseModel):
    """Configuration for backup operations."""

    enabled: Optional[bool] = None
    source_catalog: Optional[str] = None
    create_recipient: Optional[bool] = False
    recipient_name: Optional[str] = None
    create_share: Optional[bool] = False
    add_to_share: Optional[bool] = False
    share_name: Optional[str] = None
    create_backup_catalog: Optional[bool] = False
    backup_catalog: Optional[str] = None
    backup_catalog_location: Optional[str] = None
    backup_share_name: Optional[str] = None
    backup_schema_prefix: Optional[str] = None

    @field_validator("source_catalog", "backup_catalog")
    @classmethod
    def validate_catalog_names(cls, v):
        """Convert catalog names to lowercase."""
        return v.lower() if v else v


class ReplicationConfig(BaseModel):
    """Configuration for replication operations."""

    enabled: Optional[bool] = None
    create_target_catalog: Optional[bool] = False
    target_catalog_location: Optional[str] = None
    create_shared_catalog: Optional[bool] = False
    share_name: Optional[str] = None
    source_catalog: Optional[str] = None
    create_intermediate_catalog: Optional[bool] = False
    intermediate_catalog: Optional[str] = None
    intermediate_catalog_location: Optional[str] = None
    enforce_schema: Optional[bool] = True
    overwrite_tags: Optional[bool] = True
    overwrite_comments: Optional[bool] = True
    copy_files: Optional[bool] = True

    @field_validator("source_catalog", "intermediate_catalog")
    @classmethod
    def validate_catalog_names(cls, v):
        """Convert catalog names to lowercase."""
        return v.lower() if v else v


class ReconciliationConfig(BaseModel):
    """Configuration for reconciliation operations."""

    enabled: Optional[bool] = None
    create_recon_catalog: Optional[bool] = False
    recon_outputs_catalog: Optional[str] = None
    recon_outputs_schema: Optional[str] = None
    recon_catalog_location: Optional[str] = None
    create_shared_catalog: Optional[bool] = False
    share_name: Optional[str] = None
    source_catalog: Optional[str] = None
    schema_check: Optional[bool] = True
    row_count_check: Optional[bool] = True
    missing_data_check: Optional[bool] = True
    exclude_columns: Optional[List[str]] = None
    source_filter_expression: Optional[str] = None
    target_filter_expression: Optional[str] = None

    @field_validator("source_catalog", "recon_outputs_catalog")
    @classmethod
    def validate_catalog_names(cls, v):
        """Convert catalog names to lowercase."""
        return v.lower() if v else v

    @field_validator("recon_outputs_schema")
    @classmethod
    def validate_schema_name(cls, v):
        """Convert schema name to lowercase."""
        return v.lower() if v else v


class TargetCatalogConfig(BaseModel):
    """Configuration for target catalogs."""

    catalog_name: str
    table_types: Optional[List[TableType]] = None
    volume_types: Optional[List[VolumeType]] = None
    uc_object_types: Optional[List[UCObjectType]] = None
    target_schemas: Optional[List[SchemaConfig]] = None
    exclude_schemas: Optional[List[SchemaConfig]] = None
    schema_filter_expression: Optional[str] = None
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None

    @field_validator("catalog_name")
    @classmethod
    def validate_catalog_name(cls, v):
        """Convert catalog name to lowercase."""
        return v.lower() if v else v


class ConcurrencyConfig(BaseModel):
    """Configuration for concurrency settings."""

    max_workers: int = Field(default=4, ge=1, le=32)
    timeout_seconds: int = Field(default=3600, ge=60)


class LoggingConfig(BaseModel):
    """Configuration for logging settings."""

    level: str = Field(default="INFO")
    format: str = Field(default="json")  # "text" or "json"
    log_to_file: bool = Field(default=False)
    log_file_path: Optional[str] = None

    @field_validator("level")
    @classmethod
    def validate_level(cls, v):
        """Validate logging level."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if v.upper() not in valid_levels:
            raise ValueError(
                f"Invalid logging level: {v}. Must be one of {valid_levels}"
            )
        return v.lower()

    @field_validator("format")
    @classmethod
    def validate_format(cls, v):
        """Validate logging format."""
        valid_formats = ["text", "json"]
        if v.lower() not in valid_formats:
            raise ValueError(
                f"Invalid logging format: {v}. Must be one of {valid_formats}"
            )
        return v.lower()


class RetryConfig(BaseModel):
    """Configuration for retry settings."""

    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_delay_seconds: int = Field(default=5, ge=1)


class ReplicationSystemConfig(BaseModel):
    """Root configuration model for the replication system."""

    version: str
    replication_group: str
    source_databricks_connect_config: DatabricksConnectConfig
    target_databricks_connect_config: DatabricksConnectConfig
    audit_config: AuditConfig
    table_types: Optional[List[TableType]] = None
    volume_types: Optional[List[VolumeType]] = None
    uc_object_types: Optional[List[UCObjectType]] = None
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    target_catalogs: List[TargetCatalogConfig]
    external_location_mapping: Optional[dict] = None
    concurrency: Optional[ConcurrencyConfig] = Field(default_factory=ConcurrencyConfig)
    retry: Optional[RetryConfig] = Field(default_factory=RetryConfig)
    logging: Optional[LoggingConfig] = Field(default_factory=LoggingConfig)

    @field_validator("target_catalogs")
    @classmethod
    def validate_target_catalogs(cls, v):
        """Ensure at least one target catalog is configured."""
        if not v:
            raise ValueError("At least one target catalog must be configured")
        return v

    @model_validator(mode="after")
    def set_table_types(self):
        """Merge catalog-level table types into system-level config"""
        for catalog in self.target_catalogs:
            if self.table_types is not None and catalog.table_types is None:
                catalog.table_types = self.table_types
        return self

    @model_validator(mode="after")
    def set_volume_types(self):
        """Merge catalog-level volume types into system-level config"""
        for catalog in self.target_catalogs:
            if self.volume_types is not None and catalog.volume_types is None:
                catalog.volume_types = self.volume_types
        return self

    @model_validator(mode="after")
    def set_uc_object_types(self):
        """Merge catalog-level UC object types into system-level config"""
        for catalog in self.target_catalogs:
            if self.uc_object_types is not None and catalog.uc_object_types is None:
                catalog.uc_object_types = self.uc_object_types
        return self

    @model_validator(mode="after")
    def set_backup_defaults(self):
        """Merge catalog-level backup configs into system-level config"""
        for catalog in self.target_catalogs:
            if catalog.backup_config:
                # Merge system-level config into catalog config (catalog takes precedence)
                if self.backup_config:
                    system_dict = self.backup_config.model_dump()
                    catalog_dict = catalog.backup_config.model_dump()

                    # Merge: system values as base, catalog values override
                    merged_dict = {
                        **system_dict,
                        **{k: v for k, v in catalog_dict.items() if v is not None},
                    }
                    catalog.backup_config = BackupConfig(**merged_dict)
                # If no system config, catalog config stays as is
            else:
                # If catalog has no backup_config, use system-level config
                if self.backup_config:
                    catalog.backup_config = BackupConfig(
                        **self.backup_config.model_dump()
                    )

        return self

    @model_validator(mode="after")
    def set_replication_defaults(self):
        """Merge catalog-level replication configs into system-level config"""
        for catalog in self.target_catalogs:
            if catalog.replication_config:
                # Merge system-level config into catalog config (catalog takes precedence)
                if self.replication_config:
                    system_dict = self.replication_config.model_dump()
                    catalog_dict = catalog.replication_config.model_dump()

                    # Merge: system values as base, catalog values override
                    merged_dict = {
                        **system_dict,
                        **{k: v for k, v in catalog_dict.items() if v is not None},
                    }
                    catalog.replication_config = ReplicationConfig(**merged_dict)
                # If no system config, catalog config stays as is
            else:
                if self.replication_config:
                    catalog.replication_config = ReplicationConfig(
                        **self.replication_config.model_dump()
                    )

        return self

    @model_validator(mode="after")
    def set_reconciliation_defaults(self):
        """Set default recon_outputs_catalog and recon_outputs_schema from audit_config.
        Merge catalog-level reconciliation configs into system-level config"""
        # Extract catalog and schema from audit_table
        audit_parts = self.audit_config.audit_table.split(".")
        audit_catalog = audit_parts[0]
        audit_schema = audit_parts[1]

        # Set default recon_outputs_catalog to audit catalog if not provided
        if (
            self.reconciliation_config
            and self.reconciliation_config.recon_outputs_catalog is None
        ):
            self.reconciliation_config.recon_outputs_catalog = audit_catalog

        # Set default recon_outputs_schema to audit schema if not provided
        if (
            self.reconciliation_config
            and self.reconciliation_config.recon_outputs_schema is None
        ):
            self.reconciliation_config.recon_outputs_schema = audit_schema

        # Merge catalog-level reconciliation configs into system-level config
        for catalog in self.target_catalogs:
            if catalog.reconciliation_config:
                # Merge system-level config into catalog config (catalog takes precedence)
                if self.reconciliation_config:
                    system_dict = self.reconciliation_config.model_dump()
                    catalog_dict = catalog.reconciliation_config.model_dump()

                    # Merge: system values as base, catalog values override
                    merged_dict = {
                        **system_dict,
                        **{k: v for k, v in catalog_dict.items() if v is not None},
                    }
                    catalog.reconciliation_config = ReconciliationConfig(**merged_dict)
                # If no system config, catalog config stays as is
            else:
                if self.reconciliation_config:
                    catalog.reconciliation_config = ReconciliationConfig(
                        **self.reconciliation_config.model_dump()
                    )
            if catalog.reconciliation_config:
                if catalog.reconciliation_config.recon_outputs_catalog is None:
                    catalog.reconciliation_config.recon_outputs_catalog = audit_catalog
                if catalog.reconciliation_config.recon_outputs_schema is None:
                    catalog.reconciliation_config.recon_outputs_schema = audit_schema

        return self

    @model_validator(mode="after")
    def substitute_variables(self):
        """
        Substitute template variables in configuration values.
        Dynamically processes all string fields in config objects.
        """

        def replace_in_object(obj, catalog_name: str):
            """Recursively replace placeholders in all string fields of an object."""
            if obj is None:
                return

            # Get all fields from the model
            for field_name, field_value in obj.__dict__.items():
                if isinstance(field_value, str):
                    field_value_tmp = field_value.strip()
                    # Replace placeholders
                    if "{target_catalogs.catalog_name}" in field_value_tmp:
                        field_value_tmp = field_value_tmp.replace(
                            "{target_catalogs.catalog_name}", catalog_name
                        )
                    if "{source_databricks_connect_config.name}" in field_value_tmp:
                        field_value_tmp = field_value_tmp.replace(
                            "{source_databricks_connect_config.name}",
                            self.source_databricks_connect_config.name,
                        )
                    if "{target_databricks_connect_config.name}" in field_value_tmp:
                        field_value_tmp = field_value_tmp.replace(
                            "{target_databricks_connect_config.name}",
                            self.target_databricks_connect_config.name,
                        )
                    setattr(obj, field_name, field_value_tmp)
                elif isinstance(field_value, BaseModel):
                    # Recursively process nested BaseModel objects
                    replace_in_object(field_value, catalog_name)

        for catalog in self.target_catalogs:
            # Replace in all config objects
            replace_in_object(catalog.backup_config, catalog.catalog_name)
            replace_in_object(catalog.replication_config, catalog.catalog_name)
            replace_in_object(catalog.reconciliation_config, catalog.catalog_name)
            # print(catalog)

        return self

    @model_validator(mode="after")
    def derive_default_catalogs(self):
        """
        Derive default catalogs when not provided in the config.
        - Backup catalogs: __replication_internal_{catalog_name}_to_{target_name}
        - Share names: {source_catalog}_to_{target_name}_share
        - Backup share names: {backup_catalog}_share
        - Replication source catalogs: __replication_internal_{catalog_name}_from_{source_name}
        """
        target_name = self.target_databricks_connect_config.name
        source_name = self.source_databricks_connect_config.name

        for catalog in self.target_catalogs:
            # Derive default backup catalogs
            if catalog.backup_config and catalog.backup_config.enabled:
                # Derive default source catalogs
                if catalog.backup_config.source_catalog is None:
                    catalog.backup_config.source_catalog = catalog.catalog_name

                # Default share name
                default_share_name = (
                    f"{catalog.backup_config.source_catalog}_to_{target_name}_share"
                )

                # Assign default share names
                if (
                    catalog.backup_config.create_share
                    or catalog.backup_config.add_to_share
                ) and catalog.backup_config.share_name is None:
                    catalog.backup_config.share_name = default_share_name

                # Default backup catalog name for streaming tables
                default_backup_catalog = (
                    f"__replication_internal_{catalog.catalog_name}_to_{target_name}"
                )

                # Assign default backup catalog
                if (
                    catalog.backup_config.backup_catalog is None
                    and catalog.table_types
                    and catalog.table_types == [TableType.STREAMING_TABLE]
                ):
                    catalog.backup_config.backup_catalog = default_backup_catalog

                # Default share name for streaming backup tables
                default_backup_share_name = (
                    f"{catalog.backup_config.backup_catalog}_share"
                )

                if (
                    (
                        catalog.backup_config.create_share
                        or catalog.backup_config.add_to_share
                    )
                    and catalog.backup_config.backup_share_name is None
                    and catalog.backup_config.backup_catalog is not None
                ):
                    catalog.backup_config.backup_share_name = default_backup_share_name

                # Derive default recipient names
                if catalog.backup_config.recipient_name is None:
                    catalog.backup_config.recipient_name = f"{target_name}_recipient"

            # Derive default replication catalogs
            if catalog.replication_config and catalog.replication_config.enabled:

                # for uc replication, default source_catalog is the same as target catalog_name
                if catalog.uc_object_types and len(catalog.uc_object_types) > 0:
                    if catalog.replication_config.source_catalog is None:
                        catalog.replication_config.source_catalog = catalog.catalog_name
                else:
                # for table/volume replication, derive defaults shared catalogs
                    if (
                        catalog.replication_config.create_shared_catalog
                        and catalog.replication_config.share_name is None
                    ):
                        catalog.replication_config.share_name = (
                            (
                                f"__replication_internal_{catalog.catalog_name}_to_{target_name}_share"
                            )
                            if catalog.table_types and catalog.table_types == [TableType.STREAMING_TABLE]
                            else (f"{catalog.catalog_name}_to_{target_name}_share")
                        )
                    if catalog.replication_config.source_catalog is None:
                        catalog.replication_config.source_catalog = (
                            f"__replication_internal_{catalog.catalog_name}_from_{source_name}"
                            if catalog.table_types and catalog.table_types == [TableType.STREAMING_TABLE]
                            else f"{catalog.catalog_name}_from_{source_name}"
                        )

            # Derive default reconciliation catalogs
            if catalog.reconciliation_config and catalog.reconciliation_config.enabled:
                if (
                    catalog.reconciliation_config.create_shared_catalog
                    and catalog.reconciliation_config.share_name is None
                ):
                    catalog.reconciliation_config.share_name = (
                        f"{catalog.catalog_name}_to_{target_name}_share"
                    )
                if catalog.reconciliation_config.source_catalog is None:
                    catalog.reconciliation_config.source_catalog = (
                        f"{catalog.catalog_name}_from_{source_name}"
                    )

        return self

    @model_validator(mode="after")
    def validate_catalog_config(self):
        """
        Validate catalog configuration including streaming table constraints.
        """
        for catalog in self.target_catalogs:
            # Ensure at least one of backup, replication, or reconciliation is configured
            if (
                catalog.backup_config is None
                and catalog.replication_config is None
                and catalog.reconciliation_config is None
            ):
                raise ValueError(
                    f"At least one of backup_config, replication_config, or reconciliation_config must be provided in catalog: {catalog.catalog_name}"
                )

            # Ensure 'enabled' is set for each configured operation
            if catalog.backup_config:
                if catalog.backup_config.enabled is None:
                    raise ValueError(
                        f"Backup 'enabled' must be set in catalog: {catalog.catalog_name}"
                    )
            if catalog.replication_config:
                if catalog.replication_config.enabled is None:
                    raise ValueError(
                        f"Replication 'enabled' must be set in catalog: {catalog.catalog_name}"
                    )
            if catalog.reconciliation_config:
                if catalog.reconciliation_config.enabled is None:
                    raise ValueError(
                        f"Reconciliation 'enabled' must be set in catalog: {catalog.catalog_name}"
                    )

            # Ensure only one object type is specified
            object_types_provided = []
            if catalog.table_types is not None and len(catalog.table_types) > 0:
                object_types_provided.append("table_types")
            if catalog.volume_types is not None and len(catalog.volume_types) > 0:
                object_types_provided.append("volume_types")
            if catalog.uc_object_types is not None and len(catalog.uc_object_types) > 0:
                object_types_provided.append("uc_object_types")

            if len(object_types_provided) != 1:
                raise ValueError(
                    f"exactly one of table_types, uc_object_types and volume_types must be provided in catalog: {catalog.catalog_name}"
                )

            # Ensure only one of schema_filter_expression or target_schemas is provided
            if catalog.schema_filter_expression and catalog.target_schemas:
                raise ValueError(
                    f"""
                    'schema_filter_expression' and 'target_schemas' must not be provided at the same time in catalog: {catalog.catalog_name}
            """
                )

            # Validate streaming table constraints
            has_streaming_table = (
                catalog.table_types
                and TableType.STREAMING_TABLE in catalog.table_types
            )
            has_other_table_types = (
                catalog.table_types
                and len([t for t in catalog.table_types if t != TableType.STREAMING_TABLE]) > 0
            )

            # Streaming tables can't be backed up and replicated with other object types
            if has_streaming_table and has_other_table_types:
                if (catalog.backup_config and catalog.backup_config.enabled) or (
                    catalog.replication_config and catalog.replication_config.enabled
                ):
                    raise ValueError(
                        f"""
                        Streaming tables cannot be backed up and replicated with other object types in catalog: {catalog.catalog_name}
                        """
                    )

            # backup_catalog should only be set for streaming tables
            if (
                not has_streaming_table
                and catalog.backup_config
                and catalog.backup_config.enabled
                and catalog.backup_config.backup_catalog
            ):
                raise ValueError(
                    f"""
                    backup_catalog should only be set for streaming table configurations in catalog: {catalog.catalog_name}
                    """
                )

        return self


class AuditLogEntry(BaseModel):
    """Model for audit log entries."""

    run_id: str
    timestamp: str
    operation_type: str  # backup, delta_share, replication, reconciliation
    catalog_name: str
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    status: str  # started, completed, failed
    details: Optional[str] = None
    error_message: Optional[str] = None
    duration_seconds: Optional[float] = None
    attempt_number: Optional[int] = None
    max_attempts: Optional[int] = None
    config_details: Optional[str] = None  # JSON string of the full configuration
    execution_user: Optional[str] = None  # User who executed the operation

    @field_validator("catalog_name")
    @classmethod
    def validate_catalog_name(cls, v):
        """Convert catalog name to lowercase."""
        return v.lower() if v else v

    @field_validator("schema_name", "table_name")
    @classmethod
    def validate_names(cls, v):
        """Convert schema and table names to lowercase."""
        return v.lower() if v else v


class RunSummary(BaseModel):
    """Model for run summary logging."""

    run_id: str
    start_time: str
    end_time: Optional[str] = None
    duration: Optional[float] = None
    status: str  # started, completed, failed
    total_catalogs: int
    total_schemas: int
    total_tables: int
    successful_operations: int = 0
    failed_operations: int = 0
    summary: Optional[str] = None


class RunResult(BaseModel):
    """Model for operation run results."""

    operation_type: str
    catalog_name: str
    schema_name: Optional[str] = None
    object_name: Optional[str] = None
    object_type: Optional[str] = None
    status: str  # success, failed
    start_time: str
    end_time: str
    error_message: Optional[str] = None
    details: Optional[dict] = None
    attempt_number: Optional[int] = None
    max_attempts: Optional[int] = None

    @field_validator("catalog_name")
    @classmethod
    def validate_catalog_name(cls, v):
        """Convert catalog name to lowercase."""
        return v.lower() if v else v

    @field_validator("schema_name", "object_name")
    @classmethod
    def validate_names(cls, v):
        """Convert schema and object names to lowercase."""
        return v.lower() if v else v
