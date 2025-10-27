"""
Configuration models for the data replication system using Pydantic.

This module defines all the configuration models that validate and parse
the YAML configuration file for the data replication system.
"""

from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class TableType(str, Enum):
    """Enumeration of supported table types."""

    MANAGED = "managed"
    STREAMING_TABLE = "streaming_table"
    EXTERNAL = "external"


class ExecuteAt(str, Enum):
    """Enumeration of execution locations for operations."""

    SOURCE = "source"
    TARGET = "target"
    EXTERNAL = "external"


class SecretConfig(BaseModel):
    """Configuration for Databricks secrets."""

    secret_scope: str
    secret_key: str


class AuditConfig(BaseModel):
    """Configuration for audit tables"""

    audit_table: str
    audit_catalog_location: Optional[str] = None


class DatabricksConnectConfig(BaseModel):
    """Configuration for Databricks Connect."""

    name: str
    sharing_identifier: str
    host: Optional[str] = None
    token: Optional[SecretConfig] = None


class TableConfig(BaseModel):
    """Configuration for individual tables."""

    table_name: str

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v):
        """Convert table name to lowercase."""
        return v.lower() if v else v


class SchemaConfig(BaseModel):
    """Configuration for individual schemas."""

    schema_name: str
    tables: Optional[List[TableConfig]] = None
    exclude_tables: Optional[List[TableConfig]] = None

    @field_validator("schema_name")
    @classmethod
    def validate_schema_name(cls, v):
        """Convert schema name to lowercase."""
        return v.lower() if v else v


class BackupConfig(BaseModel):
    """Configuration for backup operations."""

    enabled: bool = True
    source_catalog: Optional[str] = None
    create_recipient: Optional[bool] = False
    recipient_name: Optional[str] = None
    create_share: Optional[bool] = False
    add_to_share: Optional[bool] = True
    share_name: Optional[str] = None
    backup_catalog: Optional[str] = None
    backup_catalog_location: Optional[str] = None
    backup_share_name: Optional[str] = None

    @field_validator("source_catalog", "backup_catalog")
    @classmethod
    def validate_catalog_names(cls, v):
        """Convert catalog names to lowercase."""
        return v.lower() if v else v


class ReplicationConfig(BaseModel):
    """Configuration for replication operations."""

    enabled: bool = True
    create_target_catalog: Optional[bool] = False
    target_catalog_location: Optional[str] = None
    create_shared_catalog: Optional[bool] = False
    share_name: Optional[str] = None
    source_catalog: Optional[str] = None
    intermediate_catalog: Optional[str] = None
    intermediate_catalog_location: Optional[str] = None
    enforce_schema: Optional[bool] = True
    copy_files: Optional[bool] = True

    @field_validator("source_catalog", "intermediate_catalog")
    @classmethod
    def validate_catalog_names(cls, v):
        """Convert catalog names to lowercase."""
        return v.lower() if v else v


class ReconciliationConfig(BaseModel):
    """Configuration for reconciliation operations."""

    enabled: bool = True
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

    @model_validator(mode="after")
    def validate_reconciliation_config(self):
        """Validate required fields when reconciliation is enabled."""
        if self.enabled:
            required_fields = [
                "recon_outputs_catalog",
                "recon_outputs_schema",
            ]
            missing_fields = [
                field for field in required_fields if getattr(self, field) is None
            ]

            if missing_fields:
                raise ValueError(
                    f"When reconciliation is enabled, the following fields are "
                    f"required: {missing_fields}"
                )
        return self


class TargetCatalogConfig(BaseModel):
    """Configuration for target catalogs."""

    catalog_name: str
    table_types: List[TableType]
    target_schemas: Optional[List[SchemaConfig]] = None
    schema_filter_expression: Optional[str] = None
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None

    @field_validator("catalog_name")
    @classmethod
    def validate_catalog_name(cls, v):
        """Convert catalog name to lowercase."""
        return v.lower() if v else v

    @model_validator(mode="after")
    def validate_catalog_config(self):
        """
        Validate catalog configuration including streaming table constraints.
        """
        if not self.schema_filter_expression and not self.target_schemas:
            raise ValueError(
                "At least one of 'schema_filter_expression' or 'target_schemas' "
                "must be provided"
            )

        # Validate streaming table constraints
        has_streaming_table = TableType.STREAMING_TABLE in self.table_types
        has_other_table_types = (
            len([t for t in self.table_types if t != TableType.STREAMING_TABLE]) > 0
        )

        # Streaming tables can't be backed up and replicated with other object types
        if has_streaming_table and has_other_table_types:
            if (self.backup_config and self.backup_config.enabled) or (
                self.replication_config and self.replication_config.enabled
            ):
                raise ValueError(
                    "Streaming tables cannot be backed up and replicated with other object types. "
                    "Use separate catalog configurations for streaming tables."
                )

        # backup_catalog should only be set for streaming tables
        if (
            not has_streaming_table
            and self.backup_config
            and self.backup_config.backup_catalog
        ):
            raise ValueError(
                "backup_catalog should only be set for streaming table configurations"
            )

        return self


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
        if v.lower() not in valid_levels:
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
    execute_at: ExecuteAt = Field(default=ExecuteAt.TARGET)
    source_databricks_connect_config: DatabricksConnectConfig
    target_databricks_connect_config: DatabricksConnectConfig
    audit_config: AuditConfig
    target_catalogs: List[TargetCatalogConfig]
    external_location_mapping: Optional[dict] = None
    concurrency: Optional[ConcurrencyConfig] = Field(default_factory=ConcurrencyConfig)
    retry: Optional[RetryConfig] = Field(default_factory=RetryConfig)
    logging: Optional[LoggingConfig] = Field(default_factory=LoggingConfig)

    @field_validator("version")
    @classmethod
    def validate_version(cls, v):
        """Validate configuration version."""
        if v != "1.0":
            raise ValueError(f"Unsupported configuration version: {v}")
        return v

    @field_validator("target_catalogs")
    @classmethod
    def validate_target_catalogs(cls, v):
        """Ensure at least one target catalog is configured."""
        if not v:
            raise ValueError("At least one target catalog must be configured")
        return v

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
                if (
                    catalog.replication_config.create_shared_catalog
                    and catalog.replication_config.share_name is None
                ):
                    catalog.replication_config.share_name = (
                        (
                            f"__replication_internal_{catalog.catalog_name}_to_{target_name}_share"
                        )
                        if catalog.table_types == [TableType.STREAMING_TABLE]
                        else (f"{catalog.catalog_name}_to_{target_name}_share")
                    )
                if catalog.replication_config.source_catalog is None:
                    catalog.replication_config.source_catalog = (
                        f"__replication_internal_{catalog.catalog_name}_from_{source_name}"
                        if catalog.table_types == [TableType.STREAMING_TABLE]
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
