"""
Backup provider implementation for data replication system.

This module handles backup operations using deep clone functionality
for both delta tables and streaming tables/materialized views.
"""

from datetime import datetime, timezone
from typing import List

from data_replication.databricks_operations import DatabricksOperations

from ..config.models import RunResult, SchemaConfig, TableConfig, TableType
from ..exceptions import BackupError
from ..utils import (
    create_spark_session,
    get_workspace_url_from_host,
    retry_with_logging,
    validate_spark_session,
    recursive_substitute,
)
from .base_provider import BaseProvider


class BackupProvider(BaseProvider):
    """Provider for backup operations using deep clone."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Setup target Spark session if create_recipient or create_share is True but target_databricks_connect_config.sharing_identifier is not provided
        if (
            self.catalog_config.backup_config
            and (
                self.catalog_config.backup_config.create_recipient
                or self.catalog_config.backup_config.create_share
                or self.catalog_config.backup_config.create_backup_share
                or self.catalog_config.backup_config.create_dpm_backing_table_share
            )
            and not self.target_databricks_config.sharing_identifier
        ):
            target_host = self.target_databricks_config.host
            target_auth_type = self.target_databricks_config.auth_type
            target_secret_config = self.target_databricks_config.token
            target_cluster_id = self.target_databricks_config.cluster_id
            self.logger.info(
                f"Setting up target Spark session for backup operations on host: {target_host}"
            )
            self.target_spark = create_spark_session(
                host=target_host,
                secret_config=target_secret_config,
                cluster_id=target_cluster_id,
                workspace_client=self.workspace_client,
                auth_type=target_auth_type,
            )
            validate_spark_session(
                self.target_spark, get_workspace_url_from_host(target_host)
            )
            self.target_dbops = DatabricksOperations(self.target_spark, self.logger)

        if (
            self.catalog_config.backup_config
            and self.catalog_config.backup_config.dpm_backing_table_share_name
        ):
            self.shared_tables = self.db_ops.get_shared_tables(
                self.catalog_config.backup_config.dpm_backing_table_share_name
            )

    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return "backup"

    def get_backup_schema_name(self, schema_name: str, backup_config) -> str:
        """Get the backup schema name with prefix applied."""
        if backup_config and backup_config.backup_catalog:
            prefix = backup_config.backup_schema_prefix or ""
            return f"{prefix}{schema_name}"
        return None

    def is_operation_enabled(self) -> bool:
        """Check if the backup operation is enabled in the configuration."""
        return (
            self.catalog_config.backup_config
            and self.catalog_config.backup_config.enabled
        )

    def setup_operation_catalogs(self) -> str:
        """Setup backup-specific catalogs."""
        backup_config = self.catalog_config.backup_config

        if backup_config.create_backup_catalog and backup_config.backup_catalog:
            self.logger.info(
                f"""Creating backup catalog: {backup_config.backup_catalog} at location: {backup_config.backup_catalog_location}"""
            )
            try:
                self.db_ops.create_catalog_if_not_exists(
                    backup_config.backup_catalog,
                    backup_config.backup_catalog_location,
                )
            except Exception as e:
                self.logger.error(
                    f"""Failed to create backup catalog: {backup_config.backup_catalog}. Error: {e}"""
                )
                raise e

        # Create delta share recipient if configured
        if backup_config.create_recipient:
            sharing_identifier = self.target_databricks_config.sharing_identifier
            if not sharing_identifier:
                sharing_identifier = self.target_dbops.get_metastore_id()
            self.logger.info(
                f"""Creating delta share recipient: {backup_config.recipient_name} for id: {sharing_identifier}"""
            )
            recipient_name = self.db_ops.create_recipient(
                sharing_identifier,
                backup_config.recipient_name,
            )
            if recipient_name != backup_config.recipient_name:
                self.logger.warning(
                    f"""Recipient name mismatch: expected {backup_config.recipient_name}, got {recipient_name}"""
                )
                backup_config.recipient_name = recipient_name

        # Create delta shares at catalog level if configured
        if (
            backup_config.create_share
            or backup_config.create_backup_share
            or backup_config.create_dpm_backing_table_share
        ):
            sharing_identifier = self.target_databricks_config.sharing_identifier
            if not sharing_identifier:
                sharing_identifier = self.target_dbops.get_metastore_id()
            backup_config.recipient_name = self.db_ops.get_recipient_name(
                sharing_identifier
            )
            if backup_config.create_share:
                self.logger.info(
                    f"""Creating delta share: {backup_config.share_name} and granting access to recipient: {backup_config.recipient_name}"""
                )
                self.db_ops.create_delta_share(
                    backup_config.share_name,
                    backup_config.recipient_name,
                )
            if backup_config.create_backup_share and backup_config.backup_share_name:
                # Create backup delta shares at catalog level if configured
                self.logger.info(
                    f"""Creating backup delta share: {backup_config.backup_share_name} and granting access to recipient: {backup_config.recipient_name}"""
                )
                self.db_ops.create_delta_share(
                    backup_config.backup_share_name,
                    backup_config.recipient_name,
                )
            if (
                backup_config.create_dpm_backing_table_share
                and backup_config.dpm_backing_table_share_name
            ):
                self.logger.info(
                    f"""Creating DPM backing tables delta share: {backup_config.dpm_backing_table_share_name} and granting access to recipient: {backup_config.recipient_name}"""
                )
                self.db_ops.create_delta_share(
                    backup_config.dpm_backing_table_share_name,
                    backup_config.recipient_name,
                )

        return backup_config.source_catalog

    def process_schema(
        self,
        schema_config: SchemaConfig,
    ):
        """Override to add backup-specific schema setup."""
        backup_config = schema_config.backup_config

        # Ensure schema exists in backup catalog before processing
        if backup_config.backup_catalog:
            self.db_ops.create_schema_if_not_exists(
                backup_config.backup_catalog,
                self.get_backup_schema_name(schema_config.schema_name, backup_config),
            )

        results = []
        if backup_config.add_to_share:
            schema_share_result = self._add_schema_to_shares(
                schema_config.schema_name, backup_config
            )
            results.append(schema_share_result)

        if (
            schema_config.table_types
            and TableType.STREAMING_TABLE not in schema_config.table_types
        ) or not schema_config.table_types:
            self.logger.info(
                f"""Skipping backup for schema {schema_config.schema_name} as only streaming tables requires backup."""
            )
            return results, [], []

        # Set table types to only streaming tables for backup
        schema_config.table_types = [TableType.STREAMING_TABLE]
        self.logger.info(
            f"""Processing streaming tables only for schema {schema_config.schema_name}."""
        )
        schema_tables = []
        table_results, schema_tables, _ = super().process_schema(schema_config)
        results.extend(table_results)
        return results, schema_tables, []

    def _add_schema_to_shares(self, schema_name: str, backup_config) -> RunResult:
        """Add schema to delta shares and return a RunResult for the operation."""
        start_time = datetime.now(timezone.utc)
        error_msg = None
        attempt = 1
        max_attempts = 1
        status = "success"
        backup_schema_name = self.get_backup_schema_name(schema_name, backup_config)

        try:
            if backup_config.add_to_share:
                # Ensure schema exists in source catalog before processing
                if backup_config.source_catalog and backup_config.share_name:
                    self.logger.info(
                        f"""Adding schema {backup_config.source_catalog}.{schema_name} to delta share: {backup_config.share_name}"""
                    )
                    self.db_ops.add_schema_to_share(
                        backup_config.share_name,
                        backup_config.source_catalog,
                        schema_name,
                    )

                # Ensure schema exists in backup catalog before processing
                if backup_config.backup_catalog and backup_config.backup_share_name:
                    self.logger.info(
                        f"""Adding schema {backup_config.backup_catalog}.{backup_schema_name} to backup delta share: {backup_config.backup_share_name}"""
                    )
                    self.db_ops.add_schema_to_share(
                        backup_config.backup_share_name,
                        backup_config.backup_catalog,
                        backup_schema_name,
                    )
        except Exception as e:
            self.logger.error(
                f"Failed to add schema {schema_name} to shares: {str(e)}",
                extra={"run_id": self.run_id, "operation": "backup"},
            )
            error_msg = f"Failed to add schema {schema_name} to shares: {str(e)}"
            status = "failed"
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        return RunResult(
            operation_type="backup",
            catalog_name=self.catalog_config.catalog_name,
            schema_name=schema_name,
            object_name="",
            object_type="schema",
            status=status,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            error_message=error_msg,
            details={
                "share_name": backup_config.share_name,
                "backup_catalog": backup_config.backup_catalog,
                "backup_schema_name": backup_schema_name,
                "backup_share_name": backup_config.backup_share_name,
                "backup_schema_prefix": backup_config.backup_schema_prefix,
            },
            attempt_number=attempt,
            max_attempts=max_attempts,
        )

    def process_table(
        self, schema_config: SchemaConfig, table_config: TableConfig
    ) -> List[RunResult]:
        """Process a single table for backup."""
        # Substitute table name in table config
        table_config = recursive_substitute(
            table_config, table_config.table_name, "{{table_name}}"
        )
        results = []
        result = self._backup_table(schema_config, table_config)
        if result:
            results.extend(result)
            self.audit_logger.log_results(results)
        return results

    def _backup_table(
        self,
        schema_config: SchemaConfig,
        table_config: TableConfig,
    ) -> List[RunResult]:
        """
        Backup a single table using deep clone.

        Args:
            schema_name: Schema name
            table_config: Table configuration

        Returns:
            RunResult object for the backup operation
        """
        start_time = datetime.now(timezone.utc)
        schema_name = schema_config.schema_name
        table_name = table_config.table_name
        backup_config = table_config.backup_config
        source_catalog = backup_config.source_catalog
        dpm_backing_table_share_name = backup_config.dpm_backing_table_share_name
        source_table = f"{source_catalog}.{schema_name}.{table_name}"
        backup_schema_name = self.get_backup_schema_name(schema_name, backup_config)
        backup_table = (
            f"{backup_config.backup_catalog}.{backup_schema_name}.{table_name}"
        )
        actual_source_table = None
        dlt_flag = None
        dlt_type = None
        step1_query = None
        step2_query = None
        attempt = 1
        max_attempts = table_config.retry.max_attempts

        try:
            table_details = self.db_ops.get_table_details(source_table)
            table_type = self.db_ops.get_table_type(source_table)
            actual_source_table = table_details.get("table_name", None)
            dlt_flag = table_details.get("is_dlt", None)
            dlt_type = table_details.get("dlt_type", None)

            if dlt_flag:
                if dlt_type == "legacy" and table_type.lower() == "streaming_table" and backup_config.backup_catalog:
                    if not backup_config.backup_legacy_backing_tables:
                        self.logger.info(
                            f"Skipping backup for legacy dlt backing table as per configuration: {source_table}",
                            extra={"run_id": self.run_id, "operation": "backup"},
                        )
                        end_time = datetime.now(timezone.utc)
                        duration = (end_time - start_time).total_seconds()
                        return []
                    self.logger.info(
                        f"Starting backup legacy dlt backing table: {source_table} -> {backup_table}",
                        extra={"run_id": self.run_id, "operation": "backup"},
                    )

                    # Perform backup using deep clone and unset parentTableId property
                    step1_query = f"""CREATE OR REPLACE TABLE {backup_table}
                                    DEEP CLONE {actual_source_table};
                                    """
                    step2_query = f"""ALTER TABLE {backup_table}
                                    UNSET TBLPROPERTIES (spark.sql.internal.pipelines.parentTableId)"""
                elif dlt_type == "dpm" and table_type.lower() == "streaming_table":
                    self.logger.info(
                        f"Starting adding dpm dlt backing table to share: {source_table}",
                        extra={"run_id": self.run_id, "operation": "backup"},
                    )
                    if self.db_ops.is_table_in_share(
                        dpm_backing_table_share_name, actual_source_table
                    ):
                        self.logger.info(
                            f"DPM backing table {actual_source_table} is already in share {dpm_backing_table_share_name}, skipping.",
                            extra={"run_id": self.run_id, "operation": "backup"},
                        )
                        end_time = datetime.now(timezone.utc)
                        duration = (end_time - start_time).total_seconds()
                        return [
                            RunResult(
                                operation_type="backup",
                                catalog_name=source_catalog,
                                schema_name=schema_name,
                                object_name=table_name,
                                object_type="table",
                                status="success",
                                start_time=start_time.isoformat(),
                                end_time=end_time.isoformat(),
                                duration_seconds=duration,
                                details={
                                    "backup_table": backup_table,
                                    "backup_schema_name": backup_schema_name,
                                    "source_table": actual_source_table,
                                    "dlt_type": dlt_type,
                                    "step1_query": step1_query,
                                    "step2_query": step2_query,
                                    "backup_schema_prefix": backup_config.backup_schema_prefix,
                                    "dlt_flag": dlt_flag,
                                },
                                attempt_number=attempt,
                                max_attempts=max_attempts,
                            )
                        ]
                    # Prequisite: the executing user must be metastore admin or owner of the pipeline
                    # Commented code block below: Explicitly grant access to dpm backing table before adding to share not required, as metastore admin by default has SELECT access to backing table
                    # current_user = self.db_ops.get_current_user()
                    # step1_query = f"""GRANT SELECT ON TABLE {actual_source_table}
                    #                 TO `{current_user}`;
                    #                 """
                    # Add backing table to share with original table name
                    step1_query = f"""ALTER SHARE {dpm_backing_table_share_name}
                                    ADD TABLE {actual_source_table} AS {schema_name}.{table_name};"""
                else:
                    self.logger.info(
                        f"Skipping backup for {table_type}: {source_table}",
                        extra={"run_id": self.run_id, "operation": "backup"},
                    )
                    return []
            else:
                self.logger.info(
                    f"Skipping backup for non-DLT table: {source_table}",
                    extra={"run_id": self.run_id, "operation": "backup"},
                )
                return []

            # Use custom retry decorator with logging
            @retry_with_logging(table_config.retry, self.logger)
            def backup_operation(step1_query: str, step2_query: str):
                if step1_query:
                    self.logger.debug(
                        f"Executing step1 query: {step1_query}",
                        extra={"run_id": self.run_id, "operation": "backup"},
                    )
                    self.spark.sql(step1_query)
                if step2_query:
                    self.logger.debug(
                        f"Executing step2 query: {step2_query}",
                        extra={"run_id": self.run_id, "operation": "backup"},
                    )
                    self.spark.sql(step2_query)
                return True

            result, last_exception, attempt, max_attempts = backup_operation(
                step1_query, step2_query
            )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"Backup completed successfully: {source_table} -> {backup_table} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "backup"},
                )

                return [
                    RunResult(
                        operation_type="backup",
                        catalog_name=source_catalog,
                        schema_name=schema_name,
                        object_name=table_name,
                        object_type="table",
                        status="success",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        details={
                            "backup_table": backup_table,
                            "backup_schema_name": backup_schema_name,
                            "source_table": actual_source_table,
                            "dlt_type": dlt_type,
                            "step1_query": step1_query,
                            "step2_query": step2_query,
                            "backup_schema_prefix": backup_config.backup_schema_prefix,
                            "dlt_flag": dlt_flag,
                        },
                        attempt_number=attempt,
                        max_attempts=max_attempts,
                    )
                ]

            error_msg = (
                f"Backup failed after {max_attempts} attempts: "
                f"{source_table} -> {backup_table}"
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "backup"},
            )

            return [
                RunResult(
                    operation_type="backup",
                    catalog_name=source_catalog,
                    schema_name=schema_name,
                    object_name=table_name,
                    object_type="table",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                    details={
                        "backup_table": backup_table,
                        "backup_schema_name": backup_schema_name,
                        "source_table": actual_source_table,
                        "dlt_type": dlt_type,
                        "step1_query": step1_query,
                        "step2_query": step2_query,
                        "backup_schema_prefix": backup_config.backup_schema_prefix,
                        "dlt_flag": dlt_flag,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            ]

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Wrap in BackupError for better error categorization
            if not isinstance(e, BackupError):
                e = BackupError(f"Backup operation failed: {str(e)}")

            error_msg = f"Failed to backup table {source_table}: {str(e)}"
            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "backup"},
            )

            return [
                RunResult(
                    operation_type="backup",
                    catalog_name=source_catalog,
                    schema_name=schema_name,
                    object_name=table_name,
                    object_type="table",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                    details={
                        "backup_table": backup_table,
                        "backup_schema_name": backup_schema_name,
                        "source_table": actual_source_table,
                        "dlt_type": dlt_type,
                        "step1_query": step1_query,
                        "step2_query": step2_query,
                        "backup_schema_prefix": backup_config.backup_schema_prefix,
                        "dlt_flag": dlt_flag,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            ]
