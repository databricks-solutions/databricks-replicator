#!/usr/bin/env python3
"""
Main entry point for the data replication system.

This module provides the primary CLI interface for all replication operations
including backup, delta share, replication, and reconciliation.
"""

import os
import sys

# Determine the parent directory of the current script for module imports
pwd = ""
try:
    pwd = (
        dbutils.notebook.entry_point.getDbutils()
        .notebook()
        .getContext()
        .notebookPath()
        .get()
    )  # type: ignore  # noqa: E501
    parent_folder = os.path.dirname(os.path.dirname(pwd))
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
from data_replication.utils import create_spark_session


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


def validate_execution_environment(config, operation, logger) -> int:
    """Validate configuration requirements based on execution environment and operation."""
    if config.execute_at == "source" and operation not in ["backup"]:
        logger.error("When execute_at is 'source', only 'backup' operation is allowed")
        return 1

    if config.execute_at == "target" and operation in [
        "backup",
        "all",
    ]:
        if (
            not config.source_databricks_connect_config.host
            or not config.source_databricks_connect_config.token
        ):
            logger.error(
                "Source Databricks Connect configuration must be provided for backup operations when execute_at is 'target'"
            )
            return 1

    if config.execute_at == "external":
        if operation in ["backup", "all"]:
            if (
                not config.source_databricks_connect_config.host
                or not config.source_databricks_connect_config.token
            ):
                logger.error(
                    "Source Databricks Connect configuration must be provided for backup operations when execute_at is 'external'"
                )
                return 1
        if operation in ["replication", "reconciliation", "all"]:
            if (
                not config.target_databricks_connect_config.host
                or not config.target_databricks_connect_config.token
            ):
                logger.error(
                    "Target Databricks Connect configuration must be provided for replication and reconciliation operations when execute_at is 'external'"
                )
                return 1

    return 0


def run_backup_only(
    config,
    logger,
    run_id: str,
    source_host: str,
    source_token: str,
    source_cluster_id: str,
    logging_host: str,
    logging_token: str,
    logging_cluster_id: str,
) -> int:
    """Run only backup operations."""
    spark = create_spark_session(source_host, source_token, source_cluster_id)
    logging_spark = create_spark_session(logging_host, logging_token, logging_cluster_id)
    backup_factory = ProviderFactory(
        "backup", config, spark, logging_spark, logger, run_id
    )
    # logger.info(f"Backup operations in {config.execute_at.value} environment")
    summary = backup_factory.run_backup_operations()

    if summary.failed_operations > 0:
        logger.error(f"Backup completed with {summary.failed_operations} failures")
        return 1

    logger.info("All backup operations completed successfully")
    return 0


def run_replication_only(
    config, logger, run_id: str, target_host: str, target_token: str, target_cluster_id: str
) -> int:
    """Run only replication operations."""
    spark = create_spark_session(target_host, target_token, target_cluster_id)

    replication_factory = ProviderFactory(
        "replication", config, spark, spark, logger, run_id
    )
    logger.info(f"Replication operations in {config.execute_at.value} environment")
    summary = replication_factory.run_replication_operations()

    if summary.failed_operations > 0:
        logger.error(f"Replication completed with {summary.failed_operations} failures")
        return 1

    logger.info("All replication operations completed successfully")
    return 0


def run_reconciliation_only(
    config, logger, run_id: str, target_host: str, target_token: str, target_cluster_id: str
) -> int:
    """Run only reconciliation operations."""
    spark = create_spark_session(target_host, target_token, target_cluster_id)

    reconciliation_factory = ProviderFactory(
        "reconciliation", config, spark, spark, logger, run_id
    )
    logger.info(f"Reconciliation operations in {config.execute_at.value} environment")
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

        validation_result = validate_execution_environment(
            config, args.operation, logger
        )
        if validation_result != 0:
            return validation_result

        run_id = str(uuid.uuid4())
        if args.run_id:
            run_id = args.run_id

        w = WorkspaceClient()
        # Note: In production, tokens should be retrieved from Databricks secrets
        source_host = None
        source_token = None
        source_cluster_id = config.source_databricks_connect_config.cluster_id
        target_host = None
        target_token = None
        target_cluster_id = config.target_databricks_connect_config.cluster_id

        if (
            config.source_databricks_connect_config.host
            and config.source_databricks_connect_config.token
        ):
            source_host = config.source_databricks_connect_config.host
            source_token = w.dbutils.secrets.get(
                config.source_databricks_connect_config.token.secret_scope,
                config.source_databricks_connect_config.token.secret_key,
            )
        if (
            config.target_databricks_connect_config.host
            and config.target_databricks_connect_config.token
        ):
            target_host = config.target_databricks_connect_config.host
            target_token = w.dbutils.secrets.get(
                config.target_databricks_connect_config.token.secret_scope,
                config.target_databricks_connect_config.token.secret_key,
            )
        logger.debug(f"Config: {config}")
        logger.info(
            f"Source Host: {source_host if source_host else 'Not Configured'}"
        )
        logger.info(
            f"Target Host: {target_host if target_host else 'Not Configured'}"
        )
        logger.info(f"Source Metastore: {config.source_databricks_connect_config.sharing_identifier}")
        logger.info(f"Target Metastore: {config.target_databricks_connect_config.sharing_identifier}")
        logger.info(
            f"Log run_id {run_id} in {config.audit_config.audit_table if config.audit_config and config.audit_config.audit_table else 'No audit table configured'}"
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

                logging_host = target_host
                logging_token = target_token
                logging_cluster_id = target_cluster_id
                if config.execute_at == "source":
                    logging_host = source_host
                    logging_token = source_token
                    logging_cluster_id = source_cluster_id

                logger.info(f"Logging backup operations to {logging_host}")

                run_backup_only(
                    config,
                    logger,
                    run_id,
                    source_host,
                    source_token,
                    source_cluster_id,
                    logging_host,
                    logging_token,
                    logging_cluster_id,
                )
                logger.info(f"Backup Ends {'-' * 60}")
            elif args.operation == "backup":
                logger.info("Backup disabled or No catalogs configured for backup")

        # if args.operation in ["all", "delta_share"]:
        #     logger.info("Delta share operations not yet implemented")

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

                run_replication_only(config, logger, run_id, target_host, target_token, target_cluster_id)
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

                run_reconciliation_only(
                    config, logger, run_id, target_host, target_token, target_cluster_id
                )
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
