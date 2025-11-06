"""
Replication provider implementation for data replication system.

This module handles replication operations with support for deep clone,
streaming tables, materialized views, and intermediate catalogs.
"""

from datetime import datetime, timezone
from typing import List

# from delta.tables import DeltaTable
from ..config.models import RunResult, TableConfig, VolumeConfig
from ..exceptions import ReplicationError, TableNotFoundError
from ..utils import retry_with_logging
from .base_provider import BaseProvider


class ReplicationProvider(BaseProvider):
    """Provider for replication operations using deep clone and insert overwrite."""

    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return "replication"

    def is_operation_enabled(self) -> bool:
        """Check if the replication operation is enabled in the configuration."""
        return (
            self.catalog_config.replication_config
            and self.catalog_config.replication_config.enabled
        )

    def process_table(self, schema_name: str, table_name: str) -> RunResult:
        """Process a single table for replication."""
        return self._replicate_table(schema_name, table_name)

    def process_volume(self, schema_name: str, volume_name: str) -> RunResult:
        """Process a single volume for replication."""
        return self._replicate_volume(schema_name, volume_name)

    def setup_operation_catalogs(self) -> str:
        """Setup replication-specific catalogs."""
        replication_config = self.catalog_config.replication_config
        if replication_config.create_target_catalog:
            self.logger.info(
                f"""Creating target catalog: {self.catalog_config.catalog_name} at location: {replication_config.target_catalog_location}"""
            )
            self.db_ops.create_catalog_if_not_exists(
                self.catalog_config.catalog_name,
                replication_config.target_catalog_location,
            )
        # Create intermediate catalog if needed
        if (
            replication_config.create_intermediate_catalog
            and replication_config.intermediate_catalog
        ):
            self.logger.info(
                f"""Creating intermediate catalog: {replication_config.intermediate_catalog} at location: {replication_config.intermediate_catalog_location}"""
            )
            self.db_ops.create_catalog_if_not_exists(
                replication_config.intermediate_catalog,
                replication_config.intermediate_catalog_location,
            )

        # Create source catalog from share if needed
        if replication_config.create_shared_catalog:
            provider_name = self.db_ops.get_provider_name(
                self.source_databricks_config.sharing_identifier
            )
            self.logger.info(
                f"""Creating source catalog from share: {replication_config.source_catalog} using share name: {replication_config.share_name}"""
            )
            self.db_ops.create_catalog_using_share_if_not_exists(
                replication_config.source_catalog,
                provider_name,
                replication_config.share_name,
            )

        return replication_config.source_catalog

    def process_schema_concurrently(
        self,
        schema_name: str,
        table_list: List[TableConfig],
        volume_list: List[VolumeConfig],
    ) -> List[RunResult]:
        """Override to add replication-specific schema setup."""
        replication_config = self.catalog_config.replication_config

        # Create intermediate schema if needed
        if replication_config.intermediate_catalog:
            self.db_ops.create_schema_if_not_exists(
                replication_config.intermediate_catalog, schema_name
            )

        # Create target schema if needed
        self.db_ops.create_schema_if_not_exists(
            self.catalog_config.catalog_name, schema_name
        )

        return super().process_schema_concurrently(schema_name, table_list, volume_list)

    def _replicate_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> RunResult:
        """
        Replicate a single table using deep clone or insert overwrite.

        Args:
            schema_name: Schema name
            table_name: Table name to replicate

        Returns:
            RunResult object for the replication operation
        """
        start_time = datetime.now(timezone.utc)
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_table = f"`{source_catalog}`.`{schema_name}`.`{table_name}`"
        target_table = f"`{target_catalog}`.`{schema_name}`.`{table_name}`"

        step1_query = None
        step2_query = None
        dlt_flag = None
        attempt = 1
        max_attempts = self.retry.max_attempts
        actual_target_table = target_table
        source_table_type = None

        try:
            self.logger.info(
                f"Starting replication: {source_table} -> {target_table}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            # Check if source table exists
            if not self.spark.catalog.tableExists(source_table):
                raise TableNotFoundError(f"Source table does not exist: {source_table}")

            # Get source table type to determine replication strategy
            source_table_type = self.db_ops.get_table_type(source_table)
            is_external = source_table_type.upper() == "EXTERNAL"

            try:
                table_details = self.db_ops.get_table_details(target_table)
                actual_target_table = table_details["table_name"]
                dlt_flag = table_details["is_dlt"]
                pipeline_id = table_details["pipeline_id"]
            except TableNotFoundError as exc:
                table_details = self.db_ops.get_table_details(source_table)
                if table_details["is_dlt"]:
                    raise TableNotFoundError(
                        f"""
                        Target table {target_table} does not exist. Cannot replicate DLT table without existing target.
                        """
                    ) from exc
                dlt_flag = False
                pipeline_id = None
                actual_target_table = target_table

            if self.spark.catalog.tableExists(actual_target_table):
                # Validate schema match between source and target
                if self.db_ops.get_table_fields(
                    source_table
                ) != self.db_ops.get_table_fields(actual_target_table):
                    if replication_config.enforce_schema:
                        raise ReplicationError(
                            f"Schema mismatch between table {source_table} "
                            f"and target table {target_table}"
                        )
                    self.logger.warning(
                        f"Schema mismatch detected between table {source_table} "
                        f"and target table {target_table}, but proceeding due to "
                        f"enforce_schema=False",
                        extra={"run_id": self.run_id, "operation": "replication"},
                    )

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def replication_operation(query: str):
                self.spark.sql(query)
                return True

            # Determine replication strategy based on table type and config
            if is_external:
                # External tables: always use direct replication (ignore intermediate catalog)
                if replication_config.intermediate_catalog:
                    self.logger.info(
                        "External table detected, intermediate catalog will be ignored.",
                        extra={"run_id": self.run_id, "operation": "replication"},
                    )
                (
                    result,
                    last_exception,
                    attempt,
                    max_attempts,
                    step1_query,
                    step2_query,
                ) = self._replicate_external_table(
                    source_table,
                    actual_target_table,
                    replication_operation,
                )
            elif replication_config.intermediate_catalog:
                # Two-step replication via intermediate catalog
                (
                    result,
                    last_exception,
                    attempt,
                    max_attempts,
                    step1_query,
                    step2_query,
                ) = self._replicate_via_intermediate(
                    source_table,
                    actual_target_table,
                    schema_name,
                    table_name,
                    pipeline_id,
                    replication_operation,
                )
            else:
                # Direct replication
                (
                    result,
                    last_exception,
                    attempt,
                    max_attempts,
                    step1_query,
                    step2_query,
                ) = self._replicate_direct(
                    source_table,
                    actual_target_table,
                    pipeline_id,
                    replication_operation,
                )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"Replication completed successfully: {source_table} -> {target_table} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )

                return RunResult(
                    operation_type="replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=table_name,
                    object_type="table",
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    details={
                        "target_table": actual_target_table,
                        "source_table": source_table,
                        "table_type": source_table_type,
                        "dlt_flag": dlt_flag,
                        "intermediate_catalog": replication_config.intermediate_catalog,
                        "step1_query": step1_query,
                        "step2_query": step2_query,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )

            error_msg = (
                f"Replication failed after {max_attempts} attempts: "
                f"{source_table} -> {target_table}"
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            return RunResult(
                operation_type="replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                table_name=table_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                error_message=error_msg,
                details={
                    "target_table": actual_target_table,
                    "source_table": source_table,
                    "table_type": source_table_type,
                    "dlt_flag": dlt_flag,
                    "intermediate_catalog": replication_config.intermediate_catalog,
                    "step1_query": step1_query,
                    "step2_query": step2_query,
                },
                attempt_number=attempt,
                max_attempts=max_attempts,
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Wrap in ReplicationError for better error categorization
            if not isinstance(e, ReplicationError):
                e = ReplicationError(f"Replication operation failed: {str(e)}")

            error_msg = f"Failed to replicate table {source_table}: {str(e)}"
            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
                exc_info=True,
            )

            return RunResult(
                operation_type="replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                table_name=table_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                error_message=error_msg,
                details={
                    "target_table": actual_target_table,
                    "source_table": source_table,
                    "table_type": source_table_type,
                    "dlt_flag": dlt_flag,
                    "intermediate_catalog": replication_config.intermediate_catalog,
                    "step1_query": step1_query,
                    "step2_query": step2_query,
                },
                attempt_number=attempt,
                max_attempts=max_attempts,
            )

    def _replicate_volume(self, schema_name: str, volume_name: str) -> RunResult:
        """
        Replicate a single volume.

        Args:
            schema_name: Schema name
            volume_name: Volume name to replicate

        Returns:
            RunResult object for the replication operation
        """
        start_time = datetime.now(timezone.utc)
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_volume = f"{source_catalog}.{schema_name}.{volume_name}"
        target_volume = f"{target_catalog}.{schema_name}.{volume_name}"

        attempt = 1
        max_attempts = self.retry.max_attempts
        source_volume_type = None

        try:
            self.logger.info(
                f"Starting volume replication: {source_volume} -> {target_volume}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            # Check if source volume exists
            if not self.db_ops.refresh_volume_metadata(source_volume):
                raise ReplicationError(f"Source volume does not exist: {source_volume}")

            # Get source volume type to determine replication strategy
            source_volume_type = self.db_ops.get_volume_type(source_volume)
            is_external = (
                source_volume_type and source_volume_type.upper() == "EXTERNAL"
            )

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def volume_replication_operation(query: str):
                self.spark.sql(query)
                return True

            # Determine replication strategy based on volume type
            if is_external:
                # External volumes: use external location mapping
                replication_query = self._build_external_volume_query(
                    source_volume, target_volume
                )
            else:
                # Managed volumes: use CREATE VOLUME AS DEEP CLONE
                replication_query = f"CREATE VOLUME IF NOT EXISTS {target_volume} AS DEEP CLONE {source_volume}"

            # Execute the replication
            result, last_exception, attempt, max_attempts = (
                volume_replication_operation(replication_query)
            )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"Volume replication completed successfully: {source_volume} -> {target_volume} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )

                return RunResult(
                    operation_type="replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=volume_name,
                    object_type="volume",
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    details={
                        "target_volume": target_volume,
                        "source_volume": source_volume,
                        "volume_type": source_volume_type,
                        "replication_query": replication_query,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )

            error_msg = (
                f"Volume replication failed after {max_attempts} attempts: "
                f"{source_volume} -> {target_volume}"
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            return RunResult(
                operation_type="replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                object_name=volume_name,
                object_type="volume",
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                error_message=error_msg,
                details={
                    "target_volume": target_volume,
                    "source_volume": source_volume,
                    "volume_type": source_volume_type,
                },
                attempt_number=attempt,
                max_attempts=max_attempts,
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Wrap in ReplicationError for better error categorization
            if not isinstance(e, ReplicationError):
                e = ReplicationError(f"Volume replication operation failed: {str(e)}")

            error_msg = f"Failed to replicate volume {source_volume}: {str(e)}"
            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
                exc_info=True,
            )

            return RunResult(
                operation_type="replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                object_name=volume_name,
                object_type="volume",
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                error_message=error_msg,
                details={
                    "target_volume": target_volume,
                    "source_volume": source_volume,
                    "volume_type": source_volume_type,
                },
                attempt_number=attempt,
                max_attempts=max_attempts,
            )

    def _build_external_volume_query(
        self, source_volume: str, target_volume: str
    ) -> str:
        """
        Build query for external volume replication using location mapping.

        Args:
            source_volume: Source volume name
            target_volume: Target volume name

        Returns:
            SQL query to create external volume
        """
        # Get source volume location
        try:
            source_location = (
                self.spark.sql(f"DESCRIBE VOLUME {source_volume}")
                .filter("info_name = 'Volume Path'")
                .collect()[0]["info_value"]
            )
        except Exception:
            raise ReplicationError(
                f"Cannot get location for source volume: {source_volume}"
            )

        if not self.external_location_mapping:
            raise ReplicationError(
                "external_location_mapping is required for external volume replication"
            )

        # Find matching source external location
        source_external_location = None
        target_external_location = None

        for src_location, tgt_location in self.external_location_mapping.items():
            if source_location.startswith(src_location):
                source_external_location = src_location
                target_external_location = tgt_location
                break

        if not source_external_location:
            raise ReplicationError(
                f"No external location mapping found for source volume location: {source_location}"
            )

        # Construct target location
        relative_path = source_location[len(source_external_location) :].lstrip("/")
        target_location = (
            f"{target_external_location.rstrip('/')}/{relative_path}"
            if relative_path
            else target_external_location
        )

        return (
            f"CREATE VOLUME IF NOT EXISTS {target_volume} LOCATION '{target_location}'"
        )

    def _replicate_via_intermediate(
        self,
        source_table: str,
        target_table: str,
        schema_name: str,
        table_name: str,
        pipeline_id: str,
        replication_operation,
    ) -> tuple:
        """Replicate table via intermediate catalog."""
        replication_config = self.catalog_config.replication_config
        intermediate_table = (
            f"{replication_config.intermediate_catalog}.{schema_name}.{table_name}"
        )

        # Step 1: Deep clone to intermediate
        step1_query = (
            f"CREATE OR REPLACE TABLE {intermediate_table} DEEP CLONE {source_table}"
        )

        result1, last_exception, attempt, max_attempts = replication_operation(
            step1_query
        )
        if not result1:
            return (
                result1,
                last_exception,
                attempt,
                max_attempts,
                step1_query,
                None,
            )

        # Use deep clone
        step2_query = self._build_deep_clone_query(
            source_table, target_table, pipeline_id
        )

        return (
            *replication_operation(step2_query),
            step1_query,
            step2_query,
        )

    def _replicate_direct(
        self,
        source_table: str,
        target_table: str,
        pipeline_id: str,
        replication_operation,
    ) -> tuple:
        """Replicate table directly to target."""

        # Use deep clone
        step1_query = self._build_deep_clone_query(
            source_table, target_table, pipeline_id
        )

        return *replication_operation(step1_query), step1_query, None

    def _replicate_external_table(
        self,
        source_table: str,
        target_table: str,
        replication_operation,
    ) -> tuple:
        """
        Replicate external table using external location mapping and file copy.

        Steps:
        1. Get source table storage location
        2. Map source external location to target external location
        3. Construct target storage location
        4. If copy_files is enabled, deep clone source table to target location as delta.`{target_location}`
        5. Drop target table if exists and create from target location
        """
        replication_config = self.catalog_config.replication_config

        # Step 1: Get source table storage location
        source_table_details = self.db_ops.describe_table_detail(source_table)
        source_location = source_table_details.get("location")

        if not source_location:
            raise ReplicationError(
                f"Source table {source_table} does not have a storage location"
            )

        # Step 2: Determine external location mapping
        if not self.external_location_mapping:
            raise ReplicationError(
                "external_location_mapping is required for external table replication"
            )

        # Find matching source external location
        source_external_location = None
        target_external_location = None

        for (
            src_location,
            tgt_location,
        ) in self.external_location_mapping.items():
            if source_location.startswith(src_location):
                source_external_location = src_location
                target_external_location = tgt_location
                break

        if not source_external_location:
            raise ReplicationError(
                f"No external location mapping found for source location: {source_location}"
            )

        # Step 3: Construct target storage location
        relative_path = source_location[len(source_external_location) :].lstrip("/")
        target_location = (
            f"{target_external_location.rstrip('/')}/{relative_path}"
            if relative_path
            else target_external_location
        )

        self.logger.debug(
            f"External table replication: {source_table} -> {target_location}",
            extra={"run_id": self.run_id, "operation": "replication"},
        )

        step1_query = None
        step2_query = None

        # Step 4: Deep clone to target location if copy_files is enabled
        if replication_config.copy_files:
            step1_query = f"""
            CREATE OR REPLACE TABLE delta.`{target_location}` DEEP CLONE {source_table}
            """

            # Execute the deep clone
            result1, last_exception, attempt, max_attempts = replication_operation(
                step1_query
            )
            if not result1:
                return (
                    result1,
                    last_exception,
                    attempt,
                    max_attempts,
                    step1_query,
                    None,
                )

        # Step 5: Drop target table if exists and create from target location
        drop_table_query = f"DROP TABLE IF EXISTS {target_table}"
        result2, last_exception, attempt, max_attempts = replication_operation(
            drop_table_query
        )
        if not result2:
            return (
                result2,
                last_exception,
                attempt,
                max_attempts,
                step1_query,
                step2_query,
            )

        # Create target table from target location
        step2_query = f"""
        CREATE TABLE {target_table}
        USING DELTA
        LOCATION '{target_location}'
        """

        return (
            *replication_operation(step2_query),
            step1_query,
            step2_query,
        )

    def _build_insert_overwrite_query(
        self, source_table: str, target_table: str
    ) -> str:
        """Build insert overwrite query based on enforce_schema setting."""
        replication_config = self.catalog_config.replication_config

        if replication_config.enforce_schema:
            # Use SELECT * (all fields)
            return f"INSERT OVERWRITE {target_table} SELECT * FROM {source_table}"
        else:
            # Get common fields between source and target
            common_fields = self.db_ops.get_common_fields(source_table, target_table)
            if common_fields:
                field_list = "`" + "`,`".join(common_fields) + "`"
                return f"INSERT OVERWRITE {target_table} ({field_list}) SELECT {field_list} FROM {source_table}"

            # Fallback to SELECT * if no common fields found
            return f"INSERT OVERWRITE {target_table} SELECT * FROM {source_table}"

    def _build_deep_clone_query(
        self, source_table: str, target_table: str, pipeline_id: str = None
    ) -> str:
        """Build deep clone query."""

        sql = f"CREATE OR REPLACE TABLE {target_table} DEEP CLONE {source_table} "

        if pipeline_id:
            # For dlt streaming tables/materialized views, use CREATE OR REPLACE TABLE with pipelineId property
            return f"{sql} TBLPROPERTIES ('pipelines.pipelineId'='{pipeline_id}')"
        else:
            # For regular tables, just return the deep clone query
            return sql
