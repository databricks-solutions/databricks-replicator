"""
Argument parsing and validation for the data replication CLI.

This module handles command line argument parsing, validation, and processing
for the data replication system.
"""

import argparse
import json
from pathlib import Path
from typing import List

from data_replication.config.models import TableType, VolumeType, UCObjectType


def parse_comma_delimited_operations(value: str) -> List[str]:
    """
    Parse comma-delimited string into list of operation values.

    Args:
        value: Comma-delimited string of operations

    Returns:
        List of operation strings

    Raises:
        ValueError: If any value is not a valid operation
    """
    if not value:
        return ["all"]

    valid_operations = ["all", "backup", "replication", "reconciliation"]
    items = [item.strip().lower() for item in value.split(",")]
    items = [item for item in items if item]  # Filter empty strings

    # Check for "all" - if present, it must be the only operation
    if "all" in items:
        if len(items) > 1:
            raise ValueError(
                "'all' operation cannot be combined with other operations"
            )
        return ["all"]

    # Validate each operation
    for item in items:
        if item not in valid_operations:
            raise ValueError(
                f"Invalid operation '{item}'. "
                f"Valid operations are: {', '.join(valid_operations)}"
            )

    # Remove duplicates while preserving order
    result = []
    for item in items:
        if item not in result:
            result.append(item)

    return result


def parse_comma_delimited_enums(value: str, enum_class, arg_name: str):
    """
    Parse comma-delimited string into list of enum values.

    Args:
        value: Comma-delimited string
        enum_class: Enum class to validate against
        arg_name: Argument name for error messages

    Returns:
        List of enum values

    Raises:
        ValueError: If any value is not valid for the enum
    """
    if not value:
        return None

    items = [item.strip() for item in value.split(",")]
    items = [item for item in items if item]  # Filter empty strings

    result = []
    for item in items:
        try:
            # Try to create enum value
            result.append(enum_class(item))
        except ValueError as exc:
            valid_values = [e.value for e in enum_class]
            raise ValueError(
                f"Invalid value '{item}' for {arg_name}. "
                f"Valid values are: {', '.join(valid_values)}"
            ) from exc

    return result


def validate_args(args) -> None:
    """
    Validate command line arguments according to business rules.

    Args:
        args: Parsed command line arguments

    Raises:
        ValueError: If validation rules are violated
    """
    # Rule: target-catalogs should only contain 1 catalog when target-schemas is provided
    if args.target_catalogs and args.target_schemas:
        catalog_names = [name.strip() for name in args.target_catalogs.split(",")]
        catalog_names = [name for name in catalog_names if name]  # Filter empty strings
        if len(catalog_names) > 1:
            raise ValueError(
                "target-catalogs should only contain 1 catalog when target-schemas is provided, "
                f"but got {len(catalog_names)}: {', '.join(catalog_names)}"
            )

    # Rule: when target-schemas is provided, target-catalogs must be provided
    if args.target_schemas and not args.target_catalogs:
        raise ValueError(
            "When target-schemas is provided, target-catalogs must also be provided"
        )

    # Rule: when target-tables is provided, target-schemas and target-catalogs must be provided
    if args.target_tables:
        if not args.target_schemas:
            raise ValueError(
                "When target-tables is provided, target-schemas must also be provided"
            )
        if not args.target_catalogs:
            raise ValueError(
                "When target-tables is provided, target-catalogs must also be provided"
            )

    # Rule: if target-tables is provided, target-schemas should only contain 1 schema
    if args.target_tables and args.target_schemas:
        schema_names = [name.strip() for name in args.target_schemas.split(",")]
        schema_names = [name for name in schema_names if name]  # Filter empty strings
        if len(schema_names) > 1:
            raise ValueError(
                "When target-tables is provided, target-schemas should only contain 1 schema, "
                f"but got {len(schema_names)}: {', '.join(schema_names)}"
            )

    # Rule: when target-volumes is provided, target-schemas and target-catalogs must be provided
    if args.target_volumes:
        if not args.target_schemas:
            raise ValueError(
                "When target-volumes is provided, target-schemas must also be provided"
            )
        if not args.target_catalogs:
            raise ValueError(
                "When target-volumes is provided, target-catalogs must also be provided"
            )

    # Rule: table-filter-expression and target-tables cannot be used together
    if args.table_filter_expression and args.target_tables:
        raise ValueError(
            "table-filter-expression and target-tables cannot be used together. "
            "Use either --table-filter-expression to filter tables by SQL expression "
            "or --target-tables to specify exact table names, but not both."
        )

    # Rule: when table-filter-expression is provided, target-schemas and target-catalogs must be provided
    if args.table_filter_expression:
        if not args.target_schemas:
            raise ValueError(
                "When table-filter-expression is provided, target-schemas must also be provided"
            )
        if not args.target_catalogs:
            raise ValueError(
                "When table-filter-expression is provided, target-catalogs must also be provided"
            )

    # Volume argument validation
    if hasattr(args, 'volume_max_concurrent_copies') and args.volume_max_concurrent_copies is not None:
        if not (1 <= args.volume_max_concurrent_copies <= 100):
            raise ValueError(
                f"volume-max-concurrent-copies must be between 1 and 100, got {args.volume_max_concurrent_copies}"
            )

    if hasattr(args, 'volume_streaming_timeout_secs') and args.volume_streaming_timeout_secs is not None:
        if args.volume_streaming_timeout_secs < 60:
            raise ValueError(
                f"volume-streaming-timeout-secs must be at least 60 seconds, got {args.volume_streaming_timeout_secs}"
            )

    if hasattr(args, 'volume_autoloader_options') and args.volume_autoloader_options is not None:
        try:
            json.loads(args.volume_autoloader_options)
        except json.JSONDecodeError as e:
            raise ValueError(
                f"volume-autoloader-options must be a valid JSON string: {e}"
            ) from e


def setup_argument_parser():
    """Setup and configure the command line argument parser."""
    parser = argparse.ArgumentParser(
        description="Data Replication Tool for Databricks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("config_file", help="Path to the YAML configuration file")

    parser.add_argument(
        "--operation",
        "-o",
        type=str,
        default="all",
        help="Comma-separated list of operations to run. "
             "Valid operations: all, backup, replication, reconciliation. "
             "Use 'all' to run all enabled operations (default: all)",
    )

    parser.add_argument(
        "--validate-only",
        "-v",
        action="store_true",
        help="Only validate configuration without running operations",
    )

    parser.add_argument("--run-id", type=str, help="Unique Run ID")

    parser.add_argument(
        "--target-catalogs",
        "-tc",
        type=str,
        help="comma-separated list of target catalog names, e.g. catalog1,catalog2",
    )

    parser.add_argument(
        "--target-schemas",
        "-ts",
        type=str,
        help="list of schemas separated by comma, e.g. schema1,schema2",
    )

    parser.add_argument(
        "--target-tables",
        "-tb",
        type=str,
        help="list of tables separated by comma, e.g. table1,table2",
    )

    parser.add_argument(
        "--target-volumes",
        "-tv",
        type=str,
        help="list of volumes separated by comma, e.g. volume1,volume2",
    )

    parser.add_argument(
        "--table-filter-expression",
        "-tfe",
        type=str,
        help="SQL filter expression to select tables within schemas, e.g. \"tableName like 'fact_%%'\" (use quotes in shell)",
    )

    parser.add_argument(
        "--concurrency",
        "-con",
        type=int,
        help="maximum number of concurrent tasks",
    )

    parser.add_argument(
        "--uc-object-types",
        "-uc",
        type=str,
        help="comma-separated list of UC metadata types to replicate. "
             "Acceptable values: all,catalog,catalog_tag,schema,schema_tag,"
             "view,view_tag,table_tag,column_tag,volume,volume_tag,column_comment",
    )

    parser.add_argument(
        "--table-types",
        "-tt",
        type=str,
        help="comma-separated list of table types to process. "
             "Acceptable values: managed,external,streaming_table",
    )

    parser.add_argument(
        "--volume-types",
        "-vt",
        type=str,
        help="comma-separated list of volume types to process. "
             "Acceptable values: all,managed,external",
    )

    parser.add_argument(
        "--logging-level",
        "-ll",
        type=str,
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Logging level. Acceptable values: DEBUG,INFO,WARNING,ERROR,CRITICAL",
    )

    parser.add_argument(
        "--replication-wait-secs",
        "-ws",
        type=int,
        default=60,
        help="Wait time for replication operations in seconds. Default is 60 seconds.",
    )

    parser.add_argument(
        "--source-host",
        "-sh",
        type=str,
        help="Source Databricks workspace URL to override config file setting. "
             "e.g. https://adb-123456789.11.azuredatabricks.net/",
    )

    parser.add_argument(
        "--target-host",
        "-th",
        type=str,
        help="Target Databricks workspace URL to override config file setting. "
             "e.g. https://e2-demo-field-eng.cloud.databricks.com/",
    )

    parser.add_argument(
        "--volume-max-concurrent-copies",
        type=int,
        help="Maximum concurrent file copies per volume (default: 10, range: 1-100)",
    )

    parser.add_argument(
        "--volume-delete-and-reload",
        type=str,
        choices=["true", "false"],
        help="Delete existing target data and reload from source (default: false). Accepts: true, false",
    )

    parser.add_argument(
        "--volume-folder-path",
        type=str,
        help="Target folder path under the target volume to copy files to (default: root of target volume)",
    )

    parser.add_argument(
        "--volume-delete-checkpoint",
        type=str,
        choices=["true", "false"],
        help="Delete checkpoint data before starting volume replication (default: false). Accepts: true, false",
    )

    parser.add_argument(
        "--volume-autoloader-options",
        type=str,
        help="JSON string of autoloader options for volume replication, e.g. '{\"cloudFiles.maxFilesPerTrigger\": \"1000\"}'",
    )

    parser.add_argument(
        "--volume-streaming-timeout-secs",
        type=int,
        help="Streaming timeout in seconds for volume replication (default: 43200, minimum: 60)",
    )

    parser.add_argument(
        "--environment",
        "-env",
        type=str,
        help="Environment name to use from environments.yaml. If not specified, uses the default environment.",
    )

    parser.add_argument(
        "--env-path",
        type=str,
        help="Path to environments.yaml file. If not specified, searches in config directory and parent directories.",
    )

    return parser


def parse_and_validate_args():
    """Parse and validate command line arguments."""
    parser = setup_argument_parser()
    args = parser.parse_args()
    
    # Validate arguments
    validate_args(args)
    
    # Parse operations
    operations = parse_comma_delimited_operations(args.operation)
    
    # Parse enum arguments
    uc_object_types_override = parse_comma_delimited_enums(
        args.uc_object_types, UCObjectType, "--uc-object-types"
    )
    table_types_override = parse_comma_delimited_enums(
        args.table_types, TableType, "--table-types"
    )
    volume_types_override = parse_comma_delimited_enums(
        args.volume_types, VolumeType, "--volume-types"
    )
    
    # Validate config file exists
    config_path = Path(args.config_file)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    return (
        args,
        operations,
        uc_object_types_override,
        table_types_override,
        volume_types_override,
        config_path,
    )