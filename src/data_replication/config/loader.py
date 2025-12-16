"""
Configuration loader for the data replication system.

This module handles loading and validating YAML configuration files
using Pydantic models.
"""

from pathlib import Path
from typing import Union

import json
import yaml
from pydantic import ValidationError

from data_replication.config.models import (
    ReplicationSystemConfig,
    ConcurrencyConfig,
    LoggingConfig,
    TargetCatalogConfig,
    EnvironmentsConfig,
)
from data_replication.utils import merge_dicts_recursive


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
        target_volumes_override: str = None,
        schema_table_filter_expression_override: str = None,
        concurrency_override: int = None,
        uc_object_types_override: list = None,
        table_types_override: list = None,
        volume_types_override: list = None,
        logging_level_override: str = None,
        source_host_override: str = None,
        target_host_override: str = None,
        volume_max_concurrent_copies_override: int = None,
        volume_delete_and_reload_override: str = None,
        volume_folder_path_override: str = None,
        volume_delete_checkpoint_override: str = None,
        volume_autoloader_options_override: str = None,
        volume_streaming_timeout_secs_override: int = None,
        environment_name: str = None,
        env_path: str = None,
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
            target_volumes_override: Comma-separated string of volume names
                (e.g., "volume1,volume2")
            schema_table_filter_expression_override: SQL filter expression to select tables
                (e.g., "tableName like 'fact_%'")
            concurrency_override: integer containing concurrency configuration override
            uc_object_types_override: List of UCObjectType enums to override in config
            table_types_override: List of TableType enums to override in config
            volume_types_override: List of VolumeType enums to override in config
            logging_level_override: Logging level to override in config
                (e.g., "DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
            source_host_override: Source workspace URL to override in config
                (e.g., "https://adb-123.11.azuredatabricks.net/")
            target_host_override: Target workspace URL to override in config
                (e.g., "https://e2-demo-field-eng.cloud.databricks.com/")

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

        # Load environment configuration and merge with config data
        config_data = ConfigLoader._load_and_merge_environment(
            config_data, config_path, environment_name, env_path
        )

        # Handle uc_object_types override
        if uc_object_types_override is not None:
            try:
                # Convert enum values to string values for serialization
                uc_object_types_values = [
                    obj_type.value for obj_type in uc_object_types_override
                ]

                # Apply override to system level
                config_data["uc_object_types"] = uc_object_types_values

            except Exception as e:
                raise ConfigurationError(
                    f"Invalid uc_object_types configuration: {e}"
                ) from e

        # Handle table_types override
        if table_types_override is not None:
            try:
                # Convert enum values to string values for serialization
                table_types_values = [
                    table_type.value for table_type in table_types_override
                ]

                # Apply override to system level
                config_data["table_types"] = table_types_values

            except Exception as e:
                raise ConfigurationError(
                    f"Invalid table_types configuration: {e}"
                ) from e

        # Handle volume_types override
        if volume_types_override is not None:
            try:
                # Convert enum values to string values for serialization
                volume_types_values = [
                    volume_type.value for volume_type in volume_types_override
                ]

                # Apply override to system level
                config_data["volume_types"] = volume_types_values

            except Exception as e:
                raise ConfigurationError(
                    f"Invalid volume_types configuration: {e}"
                ) from e

        # Handle target_catalog override
        if target_catalog_override:
            try:
                # Parse comma-separated catalog names
                catalog_names = [
                    name.strip() for name in target_catalog_override.split(",")
                ]
                catalog_names = [
                    name for name in catalog_names if name
                ]  # Filter empty strings

                # Validate catalog name format (alphanumeric, dashes, and underscores)
                for catalog_name in catalog_names:
                    if not catalog_name.replace("_", "").replace("-", "").isalnum():
                        raise ValidationError(
                            f"Invalid catalog name '{catalog_name}': "
                            "catalog names can only contain alphanumeric characters, "
                            "dashes, and underscores"
                        )

                # Generate TargetCatalogConfig with target_catalog_override and replication group level configs
                filtered_catalogs = []
                for catalog_name in catalog_names:
                    existing_catalog = None
                    if "target_catalogs" in config_data:
                        existing_catalog = next(
                            (
                                catalog
                                for catalog in config_data["target_catalogs"]
                                if catalog.get("catalog_name") == catalog_name
                            ),
                            None,
                        )

                    if existing_catalog:
                        filtered_catalogs.append(existing_catalog)
                    else:
                        # Filter config_data to only include TargetCatalogConfig fields
                        valid_fields = set(TargetCatalogConfig.model_fields.keys())
                        filtered_config_data = {
                            k: v for k, v in config_data.items() if k in valid_fields
                        }

                        new_catalog = merge_dicts_recursive(
                            filtered_config_data,
                            {"catalog_name": catalog_name},
                        )
                        filtered_catalogs.append(new_catalog)

                config_data["target_catalogs"] = filtered_catalogs

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid target_catalog configuration: {e}"
                ) from e

        #  Handle schema_table_filter_expression override
        if schema_table_filter_expression_override:
            try:
                # Apply override to all target catalogs
                if "target_catalogs" in config_data:
                    for catalog in config_data["target_catalogs"]:
                        catalog["schema_table_filter_expression"] = (
                            schema_table_filter_expression_override
                        )

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid schema_table_filter_expression configuration: {e}"
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
                schemas = []
                for schema_name in schema_names:
                    # Handle target_tables override
                    schema = {}
                    schema["schema_name"] = schema_name
                    if target_tables_override:
                        # Parse comma-separated table names
                        table_names = [
                            {"table_name": name.strip().lower()}
                            for name in target_tables_override.split(",")
                        ]
                        schema["tables"] = table_names

                    # Handle target_volumes override
                    if target_volumes_override:
                        # Parse comma-separated volume names
                        volume_names = [
                            {"volume_name": name.strip().lower()}
                            for name in target_volumes_override.split(",")
                        ]
                        schema["volumes"] = volume_names

                    schemas.append(schema)

                # Apply override to all target catalogs
                if "target_catalogs" in config_data:
                    for catalog in config_data["target_catalogs"]:
                        catalog["target_schemas"] = schemas

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

        # Handle logging_level override
        if logging_level_override:
            try:
                # Validate logging level value
                valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
                if logging_level_override.upper() not in valid_levels:
                    raise ValidationError(
                        f"Invalid logging level '{logging_level_override}'. "
                        f"Must be one of {valid_levels}"
                    )

                # Get existing logging config or create default
                existing_logging = config_data.get("logging", {})

                # Create logging config with level override
                validated_logging = LoggingConfig(
                    level=logging_level_override.upper(),
                    format=existing_logging.get("format", "json"),
                    log_to_file=existing_logging.get("log_to_file", False),
                    log_file_path=existing_logging.get("log_file_path"),
                )

                # Apply override to config data
                config_data["logging"] = validated_logging.model_dump()

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid logging_level configuration: {e}"
                ) from e

        # Handle source_host override
        if source_host_override:
            try:
                # Validate URL format
                if not source_host_override.startswith(("https://", "http://")):
                    raise ValidationError(
                        f"Invalid source host URL '{source_host_override}': "
                        "must start with https:// or http://"
                    )

                # Override source host in config data
                if "source_databricks_connect_config" in config_data:
                    config_data["source_databricks_connect_config"]["host"] = (
                        source_host_override
                    )
                else:
                    raise ValidationError(
                        "Cannot override source host: source_databricks_connect_config not found in config"
                    )

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid source_host configuration: {e}"
                ) from e

        # Handle target_host override
        if target_host_override:
            try:
                # Validate URL format
                if not target_host_override.startswith(("https://", "http://")):
                    raise ValidationError(
                        f"Invalid target host URL '{target_host_override}': "
                        "must start with https:// or http://"
                    )

                # Override target host in config data
                if "target_databricks_connect_config" in config_data:
                    config_data["target_databricks_connect_config"]["host"] = (
                        target_host_override
                    )
                else:
                    raise ValidationError(
                        "Cannot override target host: target_databricks_connect_config not found in config"
                    )

            except ValidationError as e:
                raise ConfigurationError(
                    f"Invalid target_host configuration: {e}"
                ) from e

        # Handle volume configuration overrides
        volume_overrides = {}

        if volume_max_concurrent_copies_override is not None:
            volume_overrides["max_concurrent_copies"] = (
                volume_max_concurrent_copies_override
            )

        if volume_delete_and_reload_override is not None:
            volume_overrides["delete_and_reload"] = (
                volume_delete_and_reload_override.lower() == "true"
            )

        if volume_folder_path_override is not None:
            volume_overrides["folder_path"] = volume_folder_path_override

        if volume_delete_checkpoint_override is not None:
            volume_overrides["delete_checkpoint"] = (
                volume_delete_checkpoint_override.lower() == "true"
            )

        if volume_streaming_timeout_secs_override is not None:
            volume_overrides["streaming_timeout_seconds"] = (
                volume_streaming_timeout_secs_override
            )

        if volume_autoloader_options_override is not None:
            try:
                volume_overrides["autoloader_options"] = json.loads(
                    volume_autoloader_options_override
                )
            except json.JSONDecodeError as e:
                raise ConfigurationError(
                    f"Invalid volume_autoloader_options_override JSON: {e}"
                ) from e

        # Apply volume overrides to all catalogs with replication config
        if volume_overrides and "target_catalogs" in config_data:
            for catalog in config_data["target_catalogs"]:
                # Create replication_config if it doesn't exist
                if "replication_config" not in catalog:
                    catalog["replication_config"] = {}

                # Get existing volume_config or create empty dict
                existing_volume_config = catalog["replication_config"].get(
                    "volume_config", {}
                )

                # Merge overrides with existing config (overrides take precedence)
                merged_volume_config = merge_dicts_recursive(
                    existing_volume_config, volume_overrides
                )
                catalog["replication_config"]["volume_config"] = merged_volume_config

        # Apply volume overrides to system level replication config if it exists
        if volume_overrides and "replication_config" in config_data:
            # Get existing volume_config or create empty dict
            existing_volume_config = config_data["replication_config"].get(
                "volume_config", {}
            )

            # Merge overrides with existing config (overrides take precedence)
            merged_volume_config = merge_dicts_recursive(
                existing_volume_config, volume_overrides
            )
            config_data["replication_config"]["volume_config"] = merged_volume_config

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

    @staticmethod
    def _load_and_merge_environment(
        config_data: dict,
        config_path: Path,
        environment_name: str = None,
        env_path: str = None,
    ) -> dict:
        """
        Load environment configuration and merge with main config. configs in environments.yaml
        take precedence over those in the main config.

        Args:
            config_data: Main configuration data
            config_path: Path to main config file
            environment_name: Environment name to use, or None for default
            env_path: Custom path to environments.yaml file, or None for auto-search

        Returns:
            Updated config data with environment settings merged

        Raises:
            ConfigurationError: If environment loading fails
        """

        if env_path:
            # Use custom env_path if provided
            environments_path = Path(env_path)
            if not environments_path.exists():
                raise ConfigurationError(
                    f"Custom environments file not found: {environments_path}"
                )
        else:
            # Look for environments.yaml in the same directory as config file
            environments_path = config_path.parent / "environments.yaml"

            # If not found in config directory, look in repo root
            if not environments_path.exists():
                current = config_path.parent
                while current != current.parent:  # not root
                    env_candidate = current / "environments.yaml"
                    if env_candidate.exists():
                        environments_path = env_candidate
                        break
                    current = current.parent

            if not environments_path.exists():
                raise ConfigurationError(
                    f"environments.yaml not found. Looked in {config_path.parent} "
                    f"and parent directories. Connection configurations are required but missing from config."
                )

        try:
            # Load environments.yaml
            with open(environments_path, "r", encoding="utf-8") as f:
                env_data = yaml.safe_load(f)

            # Validate environment configuration
            environments_config = EnvironmentsConfig(**env_data)

            # Determine which environment to use
            if environment_name:
                env_config = environments_config.get_environment(environment_name)
            else:
                environment_name, env_config = (
                    environments_config.get_default_environment()
                )

            env_config_dict = env_config.model_dump()
            env_config_dict.pop("description", None)
            env_config_dict.pop("is_default", None)
            env_config_dict["env_name"] = environment_name

            config_data = merge_dicts_recursive(config_data, env_config_dict)

            return config_data

        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing environments.yaml: {e}") from e
        except ValidationError as e:
            raise ConfigurationError(
                f"Invalid environments.yaml configuration: {e}"
            ) from e
        except Exception as e:
            raise ConfigurationError(
                f"Error loading environment configuration: {e}"
            ) from e

    @staticmethod
    def load_environments_config(
        environments_path: Union[str, Path],
    ) -> EnvironmentsConfig:
        """
        Load and validate environments configuration from file.

        Args:
            environments_path: Path to environments.yaml file

        Returns:
            Validated EnvironmentsConfig instance

        Raises:
            ConfigurationError: If loading or validation fails
        """
        environments_path = Path(environments_path)

        if not environments_path.exists():
            raise ConfigurationError(
                f"Environments file not found: {environments_path}"
            )

        try:
            with open(environments_path, "r", encoding="utf-8") as f:
                env_data = yaml.safe_load(f)

            if env_data is None:
                raise ConfigurationError(
                    f"Environments file is empty: {environments_path}"
                )

            environments_config = EnvironmentsConfig(**env_data)
            return environments_config

        except yaml.YAMLError as e:
            raise ConfigurationError(f"Error parsing environments.yaml: {e}") from e
        except ValidationError as e:
            raise ConfigurationError(
                f"Invalid environments.yaml configuration: {e}"
            ) from e
        except Exception as e:
            raise ConfigurationError(
                f"Error loading environments configuration: {e}"
            ) from e


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
