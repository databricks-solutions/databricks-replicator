"""
Configuration models for the data replication system using Pydantic.

This module defines all the configuration models that validate and parse
the YAML configuration file for the data replication system.
"""

from copy import deepcopy
from enum import Enum
from typing import List, Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from data_replication.utils import merge_models_recursive, recursive_substitute


class ExecuteAt(str, Enum):
    """Enumeration for where to execute certain operations."""

    model_config = ConfigDict(extra="forbid")

    SOURCE = "source"
    TARGET = "target"


class TableType(str, Enum):
    """Enumeration of supported table types."""

    model_config = ConfigDict(extra="forbid")

    MANAGED = "managed"
    STREAMING_TABLE = "streaming_table"
    EXTERNAL = "external"
    MATERIALIZED_VIEW = "materialized_view"
    VIEW = "view"
    ALL = "all"


class VolumeType(str, Enum):
    """Enumeration of supported volume types."""

    model_config = ConfigDict(extra="forbid")
    MANAGED = "managed"
    EXTERNAL = "external"
    ALL = "all"


class AuthType(str, Enum):
    """Enumeration of authentication types."""

    model_config = ConfigDict(extra="forbid")
    PAT = "pat"
    OAUTH = "oauth"


class ConcurrencyConfig(BaseModel):
    """Configuration for concurrency settings."""

    model_config = ConfigDict(extra="forbid")

    process_schemas_in_serial: bool = Field(default=False)
    max_workers: int = Field(default=8, ge=1, le=64)
    parallel_table_filter: int = Field(default=8, ge=1, le=64)
    timeout_seconds: int = Field(default=1800, ge=60)


class RetryConfig(BaseModel):
    """Configuration for retry settings."""

    model_config = ConfigDict(extra="forbid")
    max_attempts: int = Field(default=3, ge=1, le=10)
    retry_delay_seconds: int = Field(default=5, ge=1)


class UCObjectType(str, Enum):
    """Enumeration of Unity Catalog object types for replication."""

    model_config = ConfigDict(extra="forbid")
    CATALOG = "catalog"
    CATALOG_TAG = "catalog_tag"
    SCHEMA = "schema"
    SCHEMA_TAG = "schema_tag"
    VIEW = "view"
    TABLE = "table"
    VOLUME = "volume"
    VOLUME_TAG = "volume_tag"
    TABLE_TAG = "table_tag"
    COLUMN_TAG = "column_tag"
    TABLE_COMMENT = "table_comment"
    COLUMN_COMMENT = "column_comment"
    STORAGE_CREDENTIAL = "storage_credential"
    EXTERNAL_LOCATION = "external_location"
    MATERIALIZED_VIEW = "materialized_view"
    STREAMING_TABLE = "streaming_table"
    ALL = "all"


class CredentialType(str, Enum):
    """Enumeration of supported credential types for storage credentials."""

    model_config = ConfigDict(extra="forbid")
    AWS = "aws"
    AZURE_ACCESS_CONNECTOR = "azure_access_connector"
    AZURE_MANAGED_IDENTITY = "azure_managed_identity"


class SecretConfig(BaseModel):
    """Configuration for Databricks secrets."""

    model_config = ConfigDict(extra="forbid")

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
            raise ValueError(
                "secret_client_secret is required when secret_client_id is provided"
            )
        if self.secret_client_secret and not self.secret_client_id:
            raise ValueError(
                "secret_client_id is required when secret_client_secret is provided"
            )
        return self


class AuditConfig(BaseModel):
    """Configuration for audit tables"""

    model_config = ConfigDict(extra="forbid")

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


class WarehouseType(str, Enum):
    """Enumeration of supported warehouse types."""

    PRO = "PRO"
    CLASSIC = "CLASSIC"


class ClusterSize(str, Enum):
    """Enumeration of supported cluster sizes."""

    TWO_X_SMALL = "2X-Small"
    X_SMALL = "X-Small"
    SMALL = "Small"
    MEDIUM = "Medium"
    LARGE = "Large"
    X_LARGE = "X-Large"
    TWO_X_LARGE = "2X-Large"
    THREE_X_LARGE = "3X-Large"
    FOUR_X_LARGE = "4X-Large"


class WarehouseConfig(BaseModel):
    """Configuration for Databricks SQL Warehouse."""

    model_config = ConfigDict(extra="forbid")
    create_if_not_exists: Optional[bool] = True
    name: Optional[str] = None
    enable_serverless_compute: Optional[bool] = True
    warehouse_type: Optional[WarehouseType] = WarehouseType.PRO
    cluster_size: Optional[ClusterSize] = ClusterSize.X_SMALL
    auto_stop_mins: Optional[int] = 10
    min_num_clusters: Optional[int] = 1
    max_num_clusters: Optional[int] = 1


class DatabricksConnectConfig(BaseModel):
    """Configuration for Databricks Connect."""

    model_config = ConfigDict(extra="forbid")
    name: str
    sharing_identifier: Optional[str] = None
    host: str
    auth_type: Optional[AuthType] = AuthType.PAT
    token: Optional[SecretConfig] = None
    cluster_id: Optional[str] = None
    warehouse_id: Optional[str] = None
    warehouse_config: Optional[WarehouseConfig] = None

class BackupConfig(BaseModel):
    """Configuration for backup operations."""

    model_config = ConfigDict(extra="forbid")
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
    create_backup_share: Optional[bool] = False
    backup_share_name: Optional[str] = None
    backup_schema_prefix: Optional[str] = None
    backup_legacy_backing_tables: Optional[bool] = False
    create_dpm_backing_table_share: Optional[bool] = False
    dpm_backing_table_share_name: Optional[str] = None

    @field_validator("source_catalog", "backup_catalog")
    @classmethod
    def validate_catalog_names(cls, v):
        """Convert catalog names to lowercase."""
        return v.lower() if v else v


class ReplicationConfig(BaseModel):
    """Configuration for replication operations."""

    model_config = ConfigDict(extra="forbid")
    enabled: Optional[bool] = None
    create_target_catalog: Optional[bool] = False
    target_catalog_location: Optional[str] = None
    create_shared_catalog: Optional[bool] = False
    provider_name: Optional[str] = None
    share_name: Optional[str] = None
    source_catalog: Optional[str] = None
    replicate_enable_predictive_optimization: Optional[bool] = False
    create_backup_shared_catalog: Optional[bool] = False
    backup_share_name: Optional[str] = None
    backup_catalog: Optional[str] = None
    create_dpm_backing_table_shared_catalog: Optional[bool] = False
    dpm_backing_table_share_name: Optional[str] = None
    dpm_backing_table_catalog: Optional[str] = None
    create_intermediate_catalog: Optional[bool] = False
    intermediate_catalog: Optional[str] = None
    intermediate_catalog_location: Optional[str] = None
    enforce_schema: Optional[bool] = True
    create_or_replace_table: Optional[bool] = False
    create_or_replace_view: Optional[bool] = True
    create_or_replace_materialized_view: Optional[bool] = True
    create_or_replace_streaming_table: Optional[bool] = False
    overwrite_tags: Optional[bool] = True
    overwrite_comments: Optional[bool] = True
    replicate_as_managed: Optional[bool] = False
    copy_files: Optional[bool] = True
    volume_config: Optional["VolumeFilesReplicationConfig"] = None

    @field_validator("source_catalog", "intermediate_catalog")
    @classmethod
    def validate_catalog_names(cls, v):
        """Convert catalog names to lowercase."""
        return v.lower() if v else v


class ReconciliationConfig(BaseModel):
    """Configuration for reconciliation operations."""

    model_config = ConfigDict(extra="forbid")
    enabled: Optional[bool] = None
    create_recon_catalog: Optional[bool] = False
    recon_outputs_catalog: Optional[str] = None
    recon_outputs_schema: Optional[str] = None
    recon_catalog_location: Optional[str] = None
    recon_schema_check_table: Optional[str] = "recon_schema_check"
    recon_missing_data_table: Optional[str] = "recon_missing_data"
    create_shared_catalog: Optional[bool] = False
    share_name: Optional[str] = None
    source_catalog: Optional[str] = None
    schema_check: Optional[bool] = True
    row_count_check: Optional[bool] = True
    missing_data_check: Optional[bool] = True
    exclude_columns: Optional[List[str]] = None
    source_filter_expression: Optional[str] = None
    target_filter_expression: Optional[str] = None
    threshold: Optional[float] = 100.0
    enable_sampling: Optional[bool] = False
    no_of_sampling_tables: Optional[int] = 10

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


class TableConfig(BaseModel):
    """Configuration for individual tables."""

    model_config = ConfigDict(extra="forbid")

    table_name: str
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    retry: Optional[RetryConfig] = None

    @field_validator("table_name")
    @classmethod
    def validate_table_name(cls, v):
        """Convert table name to lowercase."""
        return v.lower() if v else v


class VolumeConfig(BaseModel):
    """Configuration for individual volumes."""

    model_config = ConfigDict(extra="forbid")

    volume_name: str
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    retry: Optional[RetryConfig] = None

    @field_validator("volume_name")
    @classmethod
    def validate_volume_name(cls, v):
        """Convert volume name to lowercase."""
        return v.lower() if v else v


class SchemaConfig(BaseModel):
    """Configuration for individual schemas."""

    model_config = ConfigDict(extra="forbid")

    schema_name: str
    tables: Optional[List[TableConfig]] = None
    table_types: Optional[List[TableType]] = None
    volume_types: Optional[List[VolumeType]] = None
    uc_object_types: Optional[List[UCObjectType]] = None
    exclude_tables: Optional[List[TableConfig]] = None
    table_filter_expression: Optional[str] = None
    volumes: Optional[List[VolumeConfig]] = None
    exclude_volumes: Optional[List[VolumeConfig]] = None
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    concurrency: Optional[ConcurrencyConfig] = None
    retry: Optional[RetryConfig] = None

    @field_validator("schema_name")
    @classmethod
    def validate_schema_name(cls, v):
        """Convert schema name to lowercase."""
        return v.lower() if v else v


class TargetCatalogConfig(BaseModel):
    """Configuration for target catalogs."""

    model_config = ConfigDict(extra="forbid")

    catalog_name: str
    table_types: Optional[List[TableType]] = None
    volume_types: Optional[List[VolumeType]] = None
    uc_object_types: Optional[List[UCObjectType]] = None
    target_schemas: Optional[List[SchemaConfig]] = None
    exclude_schemas: Optional[List[SchemaConfig]] = None
    schema_table_filter_expression: Optional[str] = None
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    concurrency: Optional[ConcurrencyConfig] = None
    retry: Optional[RetryConfig] = None

    @field_validator("catalog_name")
    @classmethod
    def validate_catalog_name(cls, v):
        """Convert catalog name to lowercase."""
        return v.lower() if v else v


class LoggingConfig(BaseModel):
    """Configuration for logging settings."""

    model_config = ConfigDict(extra="forbid")
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


class VolumeFilesReplicationConfig(BaseModel):
    """Configuration for volume files replication."""

    model_config = ConfigDict(extra="forbid")
    max_concurrent_copies: int = Field(default=10, ge=1, le=100)
    delete_and_reload: bool = False
    delete_checkpoint: bool = False
    folder_path: Optional[str] = None
    autoloader_options: Optional[dict] = None
    streaming_timeout_seconds: int = Field(default=43200, ge=60)
    create_file_ingestion_logging_catalog: bool = False
    file_ingestion_logging_catalog: Optional[str] = None
    file_ingestion_logging_catalog_location: Optional[str] = None
    file_ingestion_logging_schema: Optional[str] = None
    file_ingestion_logging_table: Optional[str] = "detail_file_ingestion_logging"


class StorageCredentialConfig(BaseModel):
    """Configuration for storage credential replication."""

    model_config = ConfigDict(extra="forbid")
    target_credential_type: CredentialType = Field(
        ..., description="Target credential type: aws, azure, gcp, or cloudflare"
    )
    mapping: dict[str, str] = Field(
        ...,
        description="Mapping of source storage credential names to target cloud principal IDs",
    )


class ExternalLocationConfig(BaseModel):
    """Configuration for external location replication."""

    model_config = ConfigDict(extra="forbid")
    external_locations: Optional[List[str]] = Field(
        default=None,
        description="List of external location names to replicate. If not provided, all external locations matched in cloud_url_mapping will be replicated",
    )


class EnvironmentConfig(BaseModel):
    """Configuration for a single environment."""

    model_config = ConfigDict(extra="forbid")
    description: Optional[str] = None
    is_default: Optional[bool] = False
    source_databricks_connect_config: DatabricksConnectConfig
    target_databricks_connect_config: DatabricksConnectConfig
    audit_config: AuditConfig = None
    cloud_url_mapping: Optional[dict] = None
    storage_credential_config: Optional[StorageCredentialConfig] = None
    external_location_config: Optional[ExternalLocationConfig] = None
    table_types: Optional[List[TableType]] = None
    volume_types: Optional[List[VolumeType]] = None
    uc_object_types: Optional[List[UCObjectType]] = None
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    target_catalogs: Optional[List[TargetCatalogConfig]] = None
    concurrency: Optional[ConcurrencyConfig] = None
    retry: Optional[RetryConfig] = None
    logging: Optional[LoggingConfig] = None

    @field_validator("cloud_url_mapping")
    @classmethod
    def validate_cloud_url_mappings(cls, v):
        """Validate that all keys and values in cloud_url_mapping end with '/'."""
        if v is None:
            return v

        for key, value in v.items():
            if not key.endswith("/"):
                raise ValueError(f"Cloud URL mapping key must end with '/': {key}")
            if not value.endswith("/"):
                raise ValueError(f"Cloud URL mapping value must end with '/': {value}")

        return v


class EnvironmentsConfig(BaseModel):
    """Configuration model for environments.yaml file."""

    model_config = ConfigDict(extra="forbid")
    version: str
    environments: dict[str, EnvironmentConfig]

    @model_validator(mode="after")
    def validate_single_default(self):
        """Ensure only one environment is marked as default."""
        default_envs = [
            env_name
            for env_name, env_config in self.environments.items()
            if env_config.is_default
        ]

        if len(default_envs) > 1:
            raise ValueError(
                f"Only one environment can be marked as default. "
                f"Found multiple default environments: {default_envs}"
            )

        if len(default_envs) == 0:
            raise ValueError(
                "At least one environment must be marked as default (is_default: true)"
            )

        return self

    def get_default_environment(self) -> tuple[str, EnvironmentConfig]:
        """Get the default environment name and config."""
        for env_name, env_config in self.environments.items():
            if env_config.is_default:
                return env_name, env_config
        raise ValueError("No default environment found")

    def get_environment(self, env_name: str) -> EnvironmentConfig:
        """Get a specific environment by name."""
        if env_name not in self.environments:
            available_envs = list(self.environments.keys())
            raise ValueError(
                f"Environment '{env_name}' not found. "
                f"Available environments: {available_envs}"
            )
        return self.environments[env_name]


class ReplicationSystemConfig(BaseModel):
    """Root configuration model for the replication system."""

    model_config = ConfigDict(extra="forbid")

    version: str
    env_name: Optional[str] = None
    replication_group: str
    source_databricks_connect_config: DatabricksConnectConfig
    target_databricks_connect_config: DatabricksConnectConfig
    audit_config: AuditConfig
    cloud_url_mapping: Optional[dict] = None
    storage_credential_config: Optional[StorageCredentialConfig] = None
    external_location_config: Optional[ExternalLocationConfig] = None
    table_types: Optional[List[TableType]] = None
    volume_types: Optional[List[VolumeType]] = None
    uc_object_types: Optional[List[UCObjectType]] = None
    backup_config: Optional[BackupConfig] = None
    replication_config: Optional[ReplicationConfig] = None
    reconciliation_config: Optional[ReconciliationConfig] = None
    target_catalogs: List[TargetCatalogConfig] = []
    concurrency: Optional[ConcurrencyConfig] = Field(default_factory=ConcurrencyConfig)
    retry: Optional[RetryConfig] = Field(default_factory=RetryConfig)
    logging: Optional[LoggingConfig] = Field(default_factory=LoggingConfig)

    @model_validator(mode="after")
    def validate_target_catalogs(self):
        """Ensure at least one target catalog is configured unless only
        metastore-level objects are being replicated."""
        if not self.target_catalogs:
            # Allow empty target_catalogs if only metastore-level objects are configured
            if self.uc_object_types:
                # Normalize to a list if a single enum value was provided
                uc_types = (
                    self.uc_object_types
                    if isinstance(self.uc_object_types, list)
                    else [self.uc_object_types]
                )

                metastore_only_types = {
                    UCObjectType.STORAGE_CREDENTIAL,
                    UCObjectType.EXTERNAL_LOCATION,
                }
                # Check if all configured types are metastore-level
                if all(obj_type in metastore_only_types for obj_type in uc_types):
                    return self

            raise ValueError(
                "At least one target catalog must be configured unless only "
                "metastore-level objects (storage_credential, external_location) "
                "are being replicated"
            )
        return self

    @field_validator("cloud_url_mapping")
    @classmethod
    def validate_cloud_url_mappings(cls, v):
        """Validate that all keys and values in cloud_url_mapping end with '/'."""
        if v is None:
            return v

        for key, value in v.items():
            if not key.endswith("/"):
                raise ValueError(f"Cloud URL mapping key must end with '/': {key}")
            if not value.endswith("/"):
                raise ValueError(f"Cloud URL mapping value must end with '/': {value}")

        return v

    @model_validator(mode="after")
    def merge_configs(self):
        """Merge catalog-level configs into system-level config."""
        if self.target_catalogs:
            for i, catalog in enumerate(self.target_catalogs):
                self.target_catalogs[i] = merge_models_recursive(
                    deepcopy(self), catalog
                )
        return self

    @model_validator(mode="after")
    def set_table_types(self):
        """Set default table types and expand 'all' type"""
        # Expand system-level table_types if it contains "all"
        if self.table_types and TableType.ALL in self.table_types:
            self.table_types = [
                TableType.MANAGED,
                TableType.EXTERNAL,
                TableType.STREAMING_TABLE,
                TableType.MATERIALIZED_VIEW,
                TableType.VIEW,
            ]

        for catalog in self.target_catalogs:
            # Expand catalog-level table_types if it contains "all"
            if catalog.table_types and TableType.ALL in catalog.table_types:
                catalog.table_types = [
                    TableType.MANAGED,
                    TableType.EXTERNAL,
                    TableType.STREAMING_TABLE,
                    TableType.MATERIALIZED_VIEW,
                    TableType.VIEW,
                ]
            for schema in catalog.target_schemas or []:
                # Expand schema-level table_types if it contains "all"
                if schema.table_types and TableType.ALL in schema.table_types:
                    schema.table_types = [
                        TableType.MANAGED,
                        TableType.EXTERNAL,
                        TableType.STREAMING_TABLE,
                        TableType.MATERIALIZED_VIEW,
                        TableType.VIEW,
                    ]
        return self

    @model_validator(mode="after")
    def set_volume_types(self):
        """Set default volume types and expand 'all' type"""
        # Expand system-level volume_types if it contains "all"
        if self.volume_types and VolumeType.ALL in self.volume_types:
            self.volume_types = [
                VolumeType.MANAGED,
                VolumeType.EXTERNAL,
            ]

        for catalog in self.target_catalogs:
            # Expand catalog-level volume_types if it contains "all"
            if catalog.volume_types and VolumeType.ALL in catalog.volume_types:
                catalog.volume_types = [
                    VolumeType.MANAGED,
                    VolumeType.EXTERNAL,
                ]
            for schema in catalog.target_schemas or []:
                # Expand schema-level volume_types if it contains "all"
                if schema.volume_types and VolumeType.ALL in schema.volume_types:
                    schema.volume_types = [
                        VolumeType.MANAGED,
                        VolumeType.EXTERNAL,
                    ]
        return self

    @model_validator(mode="after")
    def set_file_ingestion_logging_defaults(self):
        """Set default file ingestion logging catalog and schema from audit_config."""
        # Extract catalog and schema from audit_table
        audit_parts = self.audit_config.audit_table.split(".")
        audit_catalog = audit_parts[0]
        audit_schema = audit_parts[1]

        for catalog in self.target_catalogs:
            if catalog.replication_config and catalog.replication_config.volume_config:
                if (
                    catalog.replication_config.volume_config.file_ingestion_logging_catalog
                    is None
                ):
                    catalog.replication_config.volume_config.file_ingestion_logging_catalog = audit_catalog
                if (
                    catalog.replication_config.volume_config.file_ingestion_logging_schema
                    is None
                ):
                    catalog.replication_config.volume_config.file_ingestion_logging_schema = audit_schema

        return self

    @model_validator(mode="after")
    def set_reconciliation_defaults(self):
        """
        Set default recon_outputs_catalog and recon_outputs_schema from audit_config.
        """
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

        for i, catalog in enumerate(self.target_catalogs):
            # Replace {{catalog_name}} in all catalog configs
            catalog = recursive_substitute(
                catalog, catalog.catalog_name, "{{catalog_name}}"
            )
            # Replace {{source_name}} in all catalog configs
            catalog = recursive_substitute(
                catalog, self.source_databricks_connect_config.name, "{{source_name}}"
            )
            # Replace {{target_name}} in all catalog configs
            catalog = recursive_substitute(
                catalog, self.target_databricks_connect_config.name, "{{target_name}}"
            )

            # Update the catalog in the list
            self.target_catalogs[i] = catalog

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
                    catalog.table_types
                    and TableType.STREAMING_TABLE in catalog.table_types
                ):
                    if catalog.backup_config.backup_catalog is None:
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
                        catalog.backup_config.backup_share_name = (
                            default_backup_share_name
                        )

                # Default share name for dpm streaming backup tables
                default_dpm_backing_table_share_name = f"__replication_internal_dpm_{catalog.catalog_name}_to_{target_name}_share"
                if not catalog.backup_config.dpm_backing_table_share_name:
                    catalog.backup_config.dpm_backing_table_share_name = (
                        default_dpm_backing_table_share_name
                    )

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
                    if catalog.replication_config.create_shared_catalog:
                        if catalog.replication_config.share_name is None:
                            catalog.replication_config.share_name = (
                                f"{catalog.catalog_name}_to_{target_name}_share"
                            )
                        if (
                            catalog.table_types
                            and TableType.STREAMING_TABLE in catalog.table_types
                        ):
                            if not catalog.replication_config.backup_share_name:
                                catalog.replication_config.backup_share_name = f"__replication_internal_{catalog.catalog_name}_to_{target_name}_share"

                            if not catalog.replication_config.backup_catalog:
                                catalog.replication_config.backup_catalog = f"__replication_internal_{catalog.catalog_name}_from_{source_name}_shared"
                    if (
                        catalog.table_types
                        and TableType.STREAMING_TABLE in catalog.table_types
                    ):
                        if not catalog.replication_config.dpm_backing_table_share_name:
                            catalog.replication_config.dpm_backing_table_share_name = f"__replication_internal_dpm_{catalog.catalog_name}_to_{target_name}_share"
                        if not catalog.replication_config.dpm_backing_table_catalog:
                            catalog.replication_config.dpm_backing_table_catalog = f"__replication_internal_dpm_{catalog.catalog_name}_from_{source_name}_shared"

                    if catalog.replication_config.source_catalog is None:
                        catalog.replication_config.source_catalog = (
                            f"{catalog.catalog_name}_from_{source_name}_shared"
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
                        f"{catalog.catalog_name}_from_{source_name}_shared"
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

            # Ensure only one of schema_table_filter_expression, or target_schemas or exclude_schemas is provided
            schema_selection_methods = []
            if catalog.schema_table_filter_expression:
                schema_selection_methods.append("schema_table_filter_expression")
            if catalog.target_schemas:
                schema_selection_methods.append("target_schemas")
            if catalog.exclude_schemas:
                schema_selection_methods.append("exclude_schemas")

            if len(schema_selection_methods) > 1:
                raise ValueError(
                    f"Only one of 'schema_table_filter_expression', 'target_schemas', or 'exclude_schemas' "
                    f"can be provided at the same time in catalog: {catalog.catalog_name}. "
                    f"Found: {', '.join(schema_selection_methods)}"
                )

            # Validate streaming table constraints
            has_streaming_table = (
                catalog.table_types and TableType.STREAMING_TABLE in catalog.table_types
            )

            # backup_catalog should only be set for streaming tables
            if not has_streaming_table and (
                (
                    catalog.backup_config
                    and catalog.backup_config.enabled
                    and catalog.backup_config.backup_catalog
                )
                or (
                    catalog.replication_config
                    and catalog.replication_config.enabled
                    and catalog.replication_config.backup_catalog
                )
            ):
                raise ValueError(
                    f"""
                    backup_catalog should only be set for streaming table configurations in catalog: {catalog.catalog_name}
                    """
                )

            # Validate table_filter_expression usage within target_schemas
            if catalog.target_schemas:
                for schema in catalog.target_schemas:
                    if schema.table_filter_expression and schema.tables:
                        raise ValueError(
                            f"""
                            'table_filter_expression' and 'tables' must not be provided at the same time in schema: {schema.schema_name} of catalog: {catalog.catalog_name}
                    """
                        )
                    # Ensure only one object type is specified
                    object_types_provided = []
                    if schema.table_types and len(schema.table_types) > 0:
                        object_types_provided.append("table_types")
                    if schema.volume_types and len(schema.volume_types) > 0:
                        object_types_provided.append("volume_types")
                    if schema.uc_object_types and len(schema.uc_object_types) > 0:
                        object_types_provided.append("uc_object_types")

                    if len(object_types_provided) > 1:
                        raise ValueError(
                            f"exactly one of table_types, uc_object_types and volume_types must be provided in schema: {schema.schema_name}"
                        )

        return self


class AuditLogEntry(BaseModel):
    """Model for audit log entries."""

    model_config = ConfigDict(extra="forbid")

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

    model_config = ConfigDict(extra="forbid")

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

    model_config = ConfigDict(extra="forbid")
    operation_type: str
    catalog_name: Optional[str] = None
    schema_name: Optional[str] = None
    object_name: Optional[str] = None
    object_type: Optional[str] = None
    status: str  # success, failed
    start_time: str
    end_time: str
    duration_seconds: Optional[float] = None
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
