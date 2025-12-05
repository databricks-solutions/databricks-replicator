#!/usr/bin/env python3
"""
Main entry point for the data replication system.

This module provides the primary CLI interface for all replication operations
including backup, delta share, replication, reconciliation, and UC replication.
"""

import os
import sys
import time

# Determine the parent directory of the current script for module imports
EXECUTED_IN_WORKSPACE = False
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
    EXECUTED_IN_WORKSPACE = True
except NameError:
    # Fallback when running outside Databricks (e.g. local development or tests)
    parent_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

if not parent_folder.startswith("/Workspace"):
    parent_folder = "/Workspace" + parent_folder
# Append the framework path to the system path for module resolution
if parent_folder not in sys.path:
    sys.path.append(parent_folder)

import uuid

from databricks.sdk import WorkspaceClient

from data_replication.audit.logger import DataReplicationLogger
from data_replication.cli.args import parse_and_validate_args
from data_replication.config.loader import ConfigLoader
from data_replication.exceptions import ConfigurationError
from data_replication.providers.provider_factory import ProviderFactory
from data_replication.utils import (
    create_spark_session,
    get_spark_workspace_url,
    validate_spark_session,
    get_workspace_url_from_host,
    unwrap_retry_error,
)


def create_logger(config) -> DataReplicationLogger:
    """Create logger instance from configuration."""
    logger = DataReplicationLogger("data_replication")
    if hasattr(config, "logging") and config.logging:
        logger.setup_logging(config.logging)
    return logger


def run_backup(
    config, logger, logging_spark, run_id: str, workspace_client: WorkspaceClient
) -> int:
    """Run only backup operations."""
    if config.audit_config.logging_workspace == "source":
        spark = logging_spark
    else:
        # Create source Spark session
        source_host = config.source_databricks_connect_config.host
        source_auth_type = config.source_databricks_connect_config.auth_type
        source_secret_config = config.source_databricks_connect_config.token
        source_cluster_id = config.source_databricks_connect_config.cluster_id
        logger.info(f"Creating source Spark session for workspace at {source_host}")
        # create and validate Spark sessions
        spark = create_spark_session(
            source_host,
            source_secret_config,
            source_cluster_id,
            workspace_client,
            source_auth_type,
        )
        if not validate_spark_session(spark, get_workspace_url_from_host(source_host)):
            logger.error(
                "Spark session is not connected to the configured source workspace"
            )
            raise ConfigurationError(
                f"Spark session is not connected to the configured source workspace."
                f"Expected: {get_workspace_url_from_host(source_host)}, "
                f"Actual: {get_spark_workspace_url(spark)}"
            )

    backup_factory = ProviderFactory(
        "backup", config, spark, logging_spark, workspace_client, logger, run_id
    )
    summary = backup_factory.run_backup_operations()

    if summary.failed_operations > 0:
        logger.error(f"Backup completed with {summary.failed_operations} failures")
        return 1

    logger.info("All backup operations completed successfully")
    return 0


def run_replication(
    config, logger, logging_spark, run_id: str, workspace_client: WorkspaceClient
) -> int:
    """Run only replication operations."""

    if config.audit_config.logging_workspace == "target":
        spark = logging_spark
    else:
        # Create target Spark session
        target_host = config.target_databricks_connect_config.host
        target_auth_type = config.target_databricks_connect_config.auth_type
        target_secret_config = config.target_databricks_connect_config.token
        target_cluster_id = config.target_databricks_connect_config.cluster_id
        logger.info(f"Creating target Spark session for workspace at {target_host}")
        spark = create_spark_session(
            target_host,
            target_secret_config,
            target_cluster_id,
            workspace_client,
            target_auth_type,
        )
        if not validate_spark_session(spark, get_workspace_url_from_host(target_host)):
            logger.error(
                "Spark session is not connected to the configured target workspace"
            )
            raise ConfigurationError(
                f"Spark session is not connected to the configured target workspace. "
                f"Expected: {get_workspace_url_from_host(target_host)}, "
                f"Actual: {get_spark_workspace_url(spark)}"
            )

    replication_factory = ProviderFactory(
        "replication", config, spark, logging_spark, workspace_client, logger, run_id
    )
    summary = replication_factory.run_replication_operations()

    if summary.failed_operations > 0:
        logger.error(f"Replication completed with {summary.failed_operations} failures")
        return 1

    logger.info("All replication operations completed successfully")
    return 0


def run_reconciliation(
    config, logger, logging_spark, run_id: str, workspace_client: WorkspaceClient
) -> int:
    """Run only reconciliation operations."""
    if config.audit_config.logging_workspace == "target":
        spark = logging_spark
    else:
        # Create target Spark session
        target_host = config.target_databricks_connect_config.host
        target_auth_type = config.target_databricks_connect_config.auth_type
        target_secret_config = config.target_databricks_connect_config.token
        target_cluster_id = config.target_databricks_connect_config.cluster_id
        logger.info(f"Creating target Spark session for workspace at {target_host}")
        spark = create_spark_session(
            target_host,
            target_secret_config,
            target_cluster_id,
            workspace_client,
            target_auth_type,
        )
        if not validate_spark_session(spark, get_workspace_url_from_host(target_host)):
            logger.error(
                "Spark session is not connected to the configured target workspace"
            )
            raise ConfigurationError(
                f"Spark session is not connected to the configured target workspace. "
                f"Expected: {get_workspace_url_from_host(target_host)}, "
                f"Actual: {get_spark_workspace_url(spark)}"
            )

    reconciliation_factory = ProviderFactory(
        "reconciliation", config, spark, logging_spark, workspace_client, logger, run_id
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
    try:
        # Parse and validate arguments
        (
            args,
            operations,
            uc_object_types_override,
            table_types_override,
            volume_types_override,
            config_path,
        ) = parse_and_validate_args()
    except (ValueError, FileNotFoundError) as e:
        print(f"Argument validation error: {e}", file=sys.stderr)
        return 1

    try:
        # Load and validate configuration
        config = ConfigLoader.load_from_file(
            config_path=config_path,
            target_catalog_override=args.target_catalogs,
            target_schemas_override=args.target_schemas,
            target_tables_override=args.target_tables,
            target_volumes_override=args.target_volumes,
            table_filter_expression_override=args.table_filter_expression,
            concurrency_override=args.concurrency,
            uc_object_types_override=uc_object_types_override,
            table_types_override=table_types_override,
            volume_types_override=volume_types_override,
            logging_level_override=args.logging_level,
            source_host_override=args.source_host,
            target_host_override=args.target_host,
            volume_max_concurrent_copies_override=getattr(args, 'volume_max_concurrent_copies', None),
            volume_delete_and_reload_override=getattr(args, 'volume_delete_and_reload', None),
            volume_folder_path_override=getattr(args, 'volume_folder_path', None),
            volume_delete_checkpoint_override=getattr(args, 'volume_delete_checkpoint', None),
            volume_autoloader_options_override=getattr(args, 'volume_autoloader_options', None),
            volume_streaming_timeout_secs_override=getattr(args, 'volume_streaming_timeout_secs', None),
            environment_name=getattr(args, 'environment', None),
            env_path=getattr(args, 'env_path', None),
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
        default_user = w.current_user.me().user_name
        if not EXECUTED_IN_WORKSPACE:
            logger.info("Running from external non-Databricks environment")
        else:
            logger.info("Running from Databricks environment")

        logger.info(f"Connecting to default workspace as user {default_user}")

        if config.audit_config.logging_workspace == "source":
            logging_host = config.source_databricks_connect_config.host
            logging_auth_type = config.source_databricks_connect_config.auth_type
            logging_secret_config = config.source_databricks_connect_config.token
            logging_cluster_id = config.source_databricks_connect_config.cluster_id
        else:
            logging_host = config.target_databricks_connect_config.host
            logging_auth_type = config.target_databricks_connect_config.auth_type
            logging_secret_config = config.target_databricks_connect_config.token
            logging_cluster_id = config.target_databricks_connect_config.cluster_id

        logger.info(f"Creating logging Spark session for workspace at {logging_host}")
        # create and validate logging Spark session
        logging_spark = create_spark_session(
            logging_host,
            logging_secret_config,
            logging_cluster_id,
            w,
            logging_auth_type,
        )
        logging_workspace_url = get_workspace_url_from_host(logging_host)

        if not validate_spark_session(logging_spark, logging_workspace_url):
            logger.error(
                "Logging Spark session is not connected to the configured logging workspace"
            )
            raise ConfigurationError(
                f"""Logging Spark session is not connected to the configured logging workspace. "
                Expected: {logging_workspace_url}, Actual: {get_spark_workspace_url(logging_spark)}"""
            )
        logger.debug(f"Config: {config}")

        logger.info(
            f"Log run_id {run_id} in {config.audit_config.audit_table} at {logging_workspace_url}"
        )
        logger.info(f"All Operations Begins {'-' * 60}")

        if "all" in operations or "backup" in operations:
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

                run_backup(config, logger, logging_spark, run_id, w)
                logger.info(f"Backup Ends {'-' * 60}")
            elif "backup" in operations:
                logger.info("Backup disabled or No catalogs configured for backup")

        if "all" in operations or "replication" in operations:
            # wait for shared schemas to be available in target workspace for non-UC metedata replication
            replication_wait_secs = (
                args.replication_wait_secs if args.replication_wait_secs else 60
            )
            if (
                "all" in operations
                and not args.uc_object_types
                and not config.uc_object_types
            ):
                logger.info(
                    f"Waiting {replication_wait_secs} seconds for Delta Share "
                    f"schemas to be available in target workspace"
                )
                time.sleep(replication_wait_secs)
            # Check if replication is configured
            replication_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.replication_config and cat.replication_config.enabled
            ]

            if replication_catalogs or config.uc_object_types:
                logger.info(f"Replication Begins {'-' * 60}")
                logger.info(
                    f"Running replication operations for "
                    f"{len(replication_catalogs)} catalogs"
                )

                run_replication(config, logger, logging_spark, run_id, w)
                logger.info(f"Replication Ends {'-' * 60}")
            elif "replication" in operations:
                logger.info(
                    "Replication disabled or No catalogs configured for replication"
                )

        if "all" in operations or "reconciliation" in operations:
            # Check if reconciliation is configured
            reconciliation_catalogs = [
                cat
                for cat in config.target_catalogs
                if cat.reconciliation_config and cat.reconciliation_config.enabled
            ]

            if reconciliation_catalogs:
                logger.info(f"Reconciliation Begins {'-' * 60}")
                logger.info(
                    f"Running reconciliation operations for "
                    f"{len(reconciliation_catalogs)} catalogs"
                )

                run_reconciliation(config, logger, logging_spark, run_id, w)
                logger.info(f"Reconciliation Ends {'-' * 60}")
            elif "reconciliation" in operations:
                logger.info(
                    "Reconciliation disabled or No catalogs configured for reconciliation"
                )

        logger.info(f"All Operations Ends {'-' * 60}")

    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        return 1
    except Exception as e:
        logger.error(unwrap_retry_error(e))
        return 1


if __name__ == "__main__":
    main()
