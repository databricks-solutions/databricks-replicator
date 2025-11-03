"""
Configuration loader for the data replication system.

This module handles loading and validating YAML configuration files
using Pydantic models.
"""

from pathlib import Path
from typing import Union

import yaml
from pydantic import ValidationError

from data_replication.config.models import (
    ReplicationSystemConfig,
    SchemaConfig,
    ConcurrencyConfig,
    TableConfig,
)


class ConfigurationError(Exception):
    """Raised when configuration loading or validation fails."""


class ConfigLoader:
    """Configuration loader for the data replication system."""

    @staticmethod
    def load_from_file(
        config_path: Union[str, Path],
        target_catalog_override: str = None,
        target_schemas_override: str = None,
        target_tables_override: str = None,
        concurrency_override: int = None,
    ) -> ReplicationSystemConfig:
        """
        Load and validate configuration from a YAML file.

        Args:
            config_path: Path to the YAML configuration file
            target_schemas_override: Comma-separated string of schema names
                (e.g., "schema1,schema2")
            target_catalog_override: Target catalog name to override in config
            target_tables_override: Comma-separated string of table names
                (e.g., "table1,table2")
            concurrency_override: integer containing concurrency configuration override

        Returns:
            Validated ReplicationSystemConfig instance

        Raises:
            ConfigurationError: If the configuration is invalid
        """
        config_path = Path(config_path)

        if not config_path.exists():
            raise ConfigurationError(f"Configuration file not found: {config_path}")

        try:
            with open(config_path, "r", encoding="utf-8") as f:
                config_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing YAML configuration: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Error reading configuration file: {e}") from e

        if config_data is None:
            raise ConfigurationError(f"Configuration file is empty: {config_path}")

        # Handle target_catalog override
        if target_catalog_override:
            try:
                # Validate catalog name format (alphanumeric, dashes, and underscores)
                if (
                    not target_catalog_override.replace("_", "")
                    .replace("-", "")
                    .isalnum()
                ):
                    raise ValidationError(
                        f"Invalid catalog name '{target_catalog_override}': "
                        "catalog names can only contain alphanumeric characters, "
                        "dashes, and underscores"
                    )

                # Generate TargetCatalogConfig with target_catalog_override and replication group level configs
                filtered_catalogs = []
                if "target_catalogs" in config_data:
                    filtered_catalogs = [
                        catalog
                        for catalog in config_data["target_catalogs"]
                        if catalog.get("catalog_name") == target_catalog_override
                    ]

                # If no matching catalog found, create one with replication group level configs
                if not filtered_catalogs:
                    new_catalog = {"catalog_name": target_catalog_override}

                    # Inherit table_types from replication group level
                    if "table_types" in config_data:
                        new_catalog["table_types"] = config_data["table_types"]

                    # Inherit backup_config from replication group level
                    if "backup_config" in config_data:
                        new_catalog["backup_config"] = config_data["backup_config"]

                    # Inherit replication_config from replication group level
                    if "replication_config" in config_data:
                        new_catalog["replication_config"] = config_data[
                            "replication_config"
                        ]

                    # Inherit reconciliation_config from replication group level
                    if "reconciliation_config" in config_data:
                        new_catalog["reconciliation_config"] = config_data[
                            "reconciliation_config"
                        ]

                    filtered_catalogs = [new_catalog]

                config_data["target_catalogs"] = filtered_catalogs

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid target_catalog configuration: {e}"
                ) from e

        # Handle target_schemas & target_tables override
        if target_schemas_override:
            try:
                # Parse comma-separated schema names
                schema_names = [
                    name.strip().lower() for name in target_schemas_override.split(",")
                ]
                # Validate schema names format (alphanumeric and underscores only)
                for schema_name in schema_names:
                    if not schema_name.replace("_", "").replace("-", "").isalnum():
                        raise ValidationError(
                            f"Invalid schema name '{schema_name}': "
                            "schema names can only contain alphanumeric characters, dashes, and underscores"
                        )

                # Create schema configs
                validated_schemas = []
                for schema_name in schema_names:
                    # Handle target_tables override
                    validated_tables = []
                    if target_tables_override:
                        # Parse comma-separated table names
                        table_names = [
                            name.strip().lower()
                            for name in target_tables_override.split(",")
                        ]

                        for table_name in table_names:
                            if table_name:
                                validated_tables.append(
                                    TableConfig(table_name=table_name)
                                )

                    validated_schemas.append(
                        SchemaConfig(schema_name=schema_name, tables=validated_tables)
                    )

                # Apply override to all target catalogs
                if "target_catalogs" in config_data:
                    for catalog in config_data["target_catalogs"]:
                        catalog["target_schemas"] = [
                            schema.model_dump() for schema in validated_schemas
                        ]

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid target_schemas configuration: {e}"
                ) from e

        # Handle concurrency override
        if concurrency_override:
            try:
                # Validate concurrency value
                if (
                    not isinstance(concurrency_override, int)
                    or concurrency_override < 1
                ):
                    raise ValidationError(
                        "concurrency_override must be a positive integer"
                    )

                # Create concurrency config with max_workers override
                validated_concurrency = ConcurrencyConfig(
                    max_workers=concurrency_override
                )

                # Apply override to config data
                config_data["concurrency"] = validated_concurrency.model_dump()

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid concurrency configuration: {e}"
                ) from e

        try:
            config = ReplicationSystemConfig(**config_data)
            return config
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

    @staticmethod
    def load_from_dict(config_data: dict) -> ReplicationSystemConfig:
        """
        Validate a configuration dictionary.

        Args:
            config_data: Dictionary containing configuration data

        Returns:
            Validated ReplicationSystemConfig instance

        Raises:
            ConfigurationError: If the configuration is invalid
        """
        try:
            config = ReplicationSystemConfig(**config_data)
            return config
        except ValidationError as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e
        except Exception as e:
            raise ConfigurationError(f"Configuration validation failed: {e}") from e

    @staticmethod
    def save_to_file(
        config: ReplicationSystemConfig, config_path: Union[str, Path]
    ) -> None:
        """
        Save a configuration to a YAML file.

        Args:
            config: ReplicationSystemConfig instance to save
            config_path: Path where to save the configuration

        Raises:
            ConfigurationError: If saving fails
        """
        config_path = Path(config_path)

        try:
            config_dict = config.model_dump(exclude_none=True)

            with open(config_path, "w", encoding="utf-8") as f:
                yaml.dump(config_dict, f, default_flow_style=False, indent=2)
        except Exception as e:
            raise ConfigurationError(f"Error saving configuration: {e}") from e


# Convenience functions
def load_config(config_path: Union[str, Path]) -> ReplicationSystemConfig:
    """Load configuration from file."""
    return ConfigLoader.load_from_file(config_path)


def validate_config_dict(config_dict: dict) -> ReplicationSystemConfig:
    """Validate configuration dictionary."""
    return ConfigLoader.load_from_dict(config_dict)


def save_config(config: ReplicationSystemConfig, config_path: Union[str, Path]) -> None:
    """Save configuration to file."""
    return ConfigLoader.save_to_file(config, config_path)
