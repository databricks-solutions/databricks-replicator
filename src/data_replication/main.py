#!/usr/bin/env python3
"""
Main entry point for the data replication system.

This module provides the primary CLI interface for all replication operations
including backup, delta share, replication, and reconciliation.
"""

import os
import sys

# Determine the parent directory of the current script for module imports
PWD = ""
try:
    PWD = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore  # noqa: E501
    parent_folder = os.path.dirname(os.path.dirname(PWD))
except NameError:
    # Fallback when running outside Databricks (e.g. local development or tests)
    parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if not parent_folder.startswith("/Workspace"):
    parent_folder = "/Workspace" + parent_folder
# Append the framework path to the system path for module resolution
if parent_folder not in sys.path:
    sys.path.append(parent_folder)

import argparse
import uuid
from pathlib import Path

from databricks.sdk import WorkspaceClient

from data_replication.audit.logger import DataReplicationLogger
from data_replication.config.loader import ConfigLoader
from data_replication.exceptions import ConfigurationError
from data_replication.providers.provider_factory import ProviderFactory
from data_replication.utils import (
    create_spark_session,
    validate_spark_session,
    get_workspace_url_from_host,
)


def create_logger(config) -> DataReplicationLogger:
    """Create logger instance from configuration."""
    logger = DataReplicationLogger("data_replication")
    if hasattr(config, "logging") and config.logging:
        logger.setup_logging(config.logging)
    return logger


def validate_args(args) -> None:
    """
    Validate command line arguments according to business rules.

    Args:
        args: Parsed command line arguments

    Raises:
        ValueError: If validation rules are violated
    """
    # Rule: target-catalog should only contain 1 catalog
    if args.target_catalog:
        catalog_names = [name.strip() for name in args.target_catalog.split(",")]
        catalog_names = [name for name in catalog_names if name]  # Filter empty strings
        if len(catalog_names) > 1:
            raise ValueError(
                f"target-catalog should only contain 1 catalog, but got {len(catalog_names)}: "
                f"{', '.join(catalog_names)}"
            )

    # Rule: when target-schema is provided, target-catalog must be provided
    if args.target_schemas and not args.target_catalog:
        raise ValueError(
            "When target-schemas is provided, target-catalog must also be provided"
        )

    # Rule: when target-tables is provided, target-schemas and target-catalog must be provided
    if args.target_tables:
        if not args.target_schemas:
            raise ValueError(
                "When target-tables is provided, target-schemas must also be provided"
            )
        if not args.target_catalog:
            raise ValueError(
                "When target-tables is provided, target-catalog must also be provided"
            )

    # Rule: if target-tables is provided, target-schemas should only contain 1 schema
    if args.target_tables and args.target_schemas:
        schema_names = [name.strip() for name in args.target_schemas.split(",")]
        schema_names = [name for name in schema_names if name]  # Filter empty strings
        if len(schema_names) > 1:
            raise ValueError(
                f"When target-tables is provided, target-schemas should only contain 1 schema, "
                f"but got {len(schema_names)}: {', '.join(schema_names)}"
            )


def run_backup_only(
    config, logger, logging_spark, run_id: str, w: WorkspaceClient
) -> int:
    """Run only backup operations."""
    source_host = config.source_databricks_connect_config.host
    source_token = None
    source_cluster_id = config.source_databricks_connect_config.cluster_id
    if config.source_databricks_connect_config.token:
        source_token = w.dbutils.secrets.get(
            config.source_databricks_connect_config.token.secret_scope,
            config.source_databricks_connect_config.token.secret_key,
        )
    # create and validate Spark sessions
    spark = create_spark_session(source_host, source_token, source_cluster_id)
    if not validate_spark_session(spark, get_workspace_url_from_host(source_host)):
        logger.error(
            "Spark session is not connected to the configured source workspace"
        )
        raise ConfigurationError(
            "Spark session is not connected to the configured source workspace"
        )

    backup_factory = ProviderFactory(
        "backup", config, spark, logging_spark, logger, run_id
    )
    summary = backup_factory.run_backup_operations()

    if summary.failed_operations > 0:
        logger.error(f"Backup completed with {summary.failed_operations} failures")
        return 1

    logger.info("All backup operations completed successfully")
    return 0


def run_replication_only(
    config, logger, logging_spark, run_id: str, w: WorkspaceClient
) -> int:
    """Run only replication operations."""
    target_host = config.target_databricks_connect_config.host
    target_token = None
    target_cluster_id = config.target_databricks_connect_config.cluster_id
    if config.target_databricks_connect_config.token:
        target_token = w.dbutils.secrets.get(
            config.target_databricks_connect_config.token.secret_scope,
            config.target_databricks_connect_config.token.secret_key,
        )
    spark = create_spark_session(target_host, target_token, target_cluster_id)
    if not validate_spark_session(spark, get_workspace_url_from_host(target_host)):
        logger.error(
            "Spark session is not connected to the configured target workspace"
        )
        raise ConfigurationError(
            "Spark session is not connected to the configured target workspace"
        )

    replication_factory = ProviderFactory(
        "replication", config, spark, logging_spark, logger, run_id
    )
    summary = replication_factory.run_replication_operations()

    if summary.failed_operations > 0:
        logger.error(f"Replication completed with {summary.failed_operations} failures")
        return 1

    logger.info("All replication operations completed successfully")
    return 0


def run_reconciliation_only(
    config, logger, logging_spark, run_id: str, w: WorkspaceClient
) -> int:
    """Run only reconciliation operations."""
    target_host = config.target_databricks_connect_config.host
    target_token = None
    target_cluster_id = config.target_databricks_connect_config.cluster_id
    if config.target_databricks_connect_config.token:
        target_token = w.dbutils.secrets.get(
            config.target_databricks_connect_config.token.secret_scope,
            config.target_databricks_connect_config.token.secret_key,
        )
    spark = create_spark_session(target_host, target_token, target_cluster_id)
    if not validate_spark_session(spark, get_workspace_url_from_host(target_host)):
        logger.error(
            "Spark session is not connected to the configured target workspace"
        )
        raise ConfigurationError(
            "Spark session is not connected to the configured target workspace"
        )

    reconciliation_factory = ProviderFactory(
        "reconciliation", config, spark, logging_spark, logger, run_id
    )
    summary = reconciliation_factory.run_reconciliation_operations()

    if summary.failed_operations > 0:
        logger.error(
            f"Reconciliation completed with {summary.failed_operations} failures"
        )
        return 1

    logger.info("All reconciliation operations completed successfully")
    return 0


def main():
    """Main entry point for data replication system."""
    parser = argparse.ArgumentParser(
        description="Data Replication Tool for Databricks",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument("config_file", help="Path to the YAML configuration file")

    parser.add_argument(
        "--operation",
        "-o",
        choices=["all", "backup", "replication", "reconciliation"],
        default="all",
        help="Specific operation to run (default: all enabled operations)",
    )

    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration without running operations",
    )

    parser.add_argument("--run-id", type=str, help="Unique Run ID")

    parser.add_argument(
        "--target-catalog",
        type=str,
        help="target catalog name, e.g. catalog1",
    )

    parser.add_argument(
        "--target-schemas",
        type=str,
        help="list of schemas separated by comma, e.g. schema1,schema2",
    )

    parser.add_argument(
        "--target-tables",
        type=str,
        help="list of tables separated by comma, e.g. table1,table2",
    )

    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="maximum number of concurrent tasks, default is 4",
    )

    args = parser.parse_args()

    # Validate command line arguments
    try:
        validate_args(args)
    except ValueError as e:
        print(f"Argument validation error: {e}")
        return 1

    # Validate config file exists
    config_path = Path(args.config_file)
    if not config_path.exists():
        print(
            f"Error: Configuration file not found: {config_path}",
            file=sys.stderr,
        )
        return 1

    try:
        # Load and validate configuration
        config = ConfigLoader.load_from_file(
            config_path=config_path,
            target_catalog_override=args.target_catalog,
            target_schemas_override=args.target_schemas,
            target_tables_override=args.target_tables,
            concurrency_override=args.concurrency,
        )
        logger = create_logger(config)

        logger.info(f"Loaded configuration from {config_path}")

        if args.validate_only:
            logger.info("Configuration validation completed successfully")
            return 0

        run_id = str(uuid.uuid4())
        if args.run_id:
            run_id = args.run_id

        w = WorkspaceClient()

        if config.audit_config.logging_workspace == "source":
            logging_host = config.source_databricks_connect_config.host
            logging_token = (
                w.dbutils.secrets.get(
                    config.source_databricks_connect_config.token.secret_scope,
                    config.source_databricks_connect_config.token.secret_key,
                )
                if config.source_databricks_connect_config.token
                else None
            )
            logging_cluster_id = config.source_databricks_connect_config.cluster_id
        else:
            logging_host = config.target_databricks_connect_config.host
            logging_token = (
                w.dbutils.secrets.get(
                    config.target_databricks_connect_config.token.secret_scope,
                    config.target_databricks_connect_config.token.secret_key,
                )
                if config.target_databricks_connect_config.token
                else None
            )
            logging_cluster_id = config.target_databricks_connect_config.cluster_id

        logging_spark = create_spark_session(
            logging_host, logging_token, logging_cluster_id
        )
        logging_workspace_url = get_workspace_url_from_host(logging_host)
        if not validate_spark_session(logging_spark, logging_workspace_url):
            logger.error(
                "Logging Spark session is not connected to the configured logging workspace"
            )
            raise ConfigurationError(
                "Logging Spark session is not connected to the configured logging workspace"
            )

        logger.debug(f"Config: {config}")
        logger.info(
            f"Source Metastore: {config.source_databricks_connect_config.sharing_identifier}"
        )
        logger.info(
            f"Target Metastore: {config.target_databricks_connect_config.sharing_identifier}"
        )
        logger.info(
            f"Log run_id {run_id} in {config.audit_config.audit_table} at {logging_workspace_url}"
        )
        logger.info(f"All Operations Begins {'-' * 60}")

        if args.operation in ["all", "backup"]:
            # Check if backup is configured
            backup_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.backup_config and cat.backup_config.enabled
            ]

            if backup_catalogs:
                logger.info(f"Backup Begins {'-' * 60}")
                logger.info(
                    f"Running backup operations for {len(backup_catalogs)} catalogs"
                )

                run_backup_only(config, logger, logging_spark, run_id, w)
                logger.info(f"Backup Ends {'-' * 60}")
            elif args.operation == "backup":
                logger.info("Backup disabled or No catalogs configured for backup")

        if args.operation in ["all", "replication"]:
            # Check if replication is configured
            replication_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.replication_config and cat.replication_config.enabled
            ]

            if replication_catalogs:
                logger.info(f"Replication Begins {'-' * 60}")
                logger.info(
                    f"Running replication operations for {len(replication_catalogs)} catalogs"
                )

                run_replication_only(config, logger, logging_spark, run_id, w)
                logger.info(f"Replication Ends {'-' * 60}")
            elif args.operation == "replication":
                logger.info(
                    "Replication disabled or No catalogs configured for replication"
                )

        if args.operation in ["all", "reconciliation"]:
            # Check if reconciliation is configured
            reconciliation_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.reconciliation_config and cat.reconciliation_config.enabled
            ]

            if reconciliation_catalogs:
                logger.info(f"Reconciliation Begins {'-' * 60}")
                logger.info(
                    f"Running reconciliation operations for {len(reconciliation_catalogs)} catalogs"
                )

                run_reconciliation_only(config, logger, logging_spark, run_id, w)
                logger.info(f"Reconciliation Ends {'-' * 60}")
            elif args.operation == "reconciliation":
                logger.info(
                    "Reconciliation disabled or No catalogs configured for reconciliation"
                )

        logger.info(f"All Operations Ends {'-' * 60}")

    except ConfigurationError as e:
        print(f"Configuration error: {e}", file=sys.stderr)
    except Exception as e:
        print(f"Operation failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    main()
