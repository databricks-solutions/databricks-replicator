"""
Replication provider implementation for data replication system.

This module handles replication operations with support for deep clone,
streaming tables, materialized views, and intermediate catalogs.
"""

from datetime import datetime, timezone
from typing import List

from data_replication.databricks_operations import DatabricksOperations

# from delta.tables import DeltaTable
from ..config.models import RunResult, TableConfig, UCObjectType, VolumeConfig
from ..exceptions import ReplicationError, TableNotFoundError
from ..utils import (
    filter_common_maps,
    get_workspace_url_from_host,
    merge_maps,
    retry_with_logging,
    create_spark_session,
    validate_spark_session,
)
from .base_provider import BaseProvider


class ReplicationProvider(BaseProvider):
    """Provider for uc and data replication operations using deep clone and autoloader."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.source_spark = None
        self.source_dbops = None
        self.target_spark = None
        self.target_dbops = None

        # default driving spark is target spark. Create source spark for uc replication or if create_shared_catalog is True but source_databricks_connect_config.sharing_identifier is not provided
        if (
            self.catalog_config.uc_object_types
            and len(self.catalog_config.uc_object_types) > 0
        ) or (
            self.catalog_config.replication_config
            and self.catalog_config.replication_config.create_shared_catalog
            and not self.source_databricks_config.sharing_identifier
        ):
            source_host = self.source_databricks_config.host
            source_auth_type = self.source_databricks_config.auth_type
            source_secret_config = self.source_databricks_config.token
            source_cluster_id = self.source_databricks_config.cluster_id
            self.source_spark = create_spark_session(
                host=source_host,
                secret_config=source_secret_config,
                cluster_id=source_cluster_id,
                workspace_client=self.workspace_client,
                auth_type=source_auth_type,
            )
            validate_spark_session(
                self.source_spark, get_workspace_url_from_host(source_host)
            )
            self.source_dbops = DatabricksOperations(self.source_spark, self.logger)

        # for uc replication, set default driving spark to source spark and dbops to source dbops. Create separate target spark and dbops.
        if (
            self.catalog_config.uc_object_types
            and len(self.catalog_config.uc_object_types) > 0
        ):
            # set target spark and dbops to current spark and dbops
            self.target_spark = self.spark
            self.target_dbops = self.db_ops
            self.spark = self.source_spark
            self.db_ops = self.source_dbops

    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return "replication"

    def is_operation_enabled(self) -> bool:
        """Check if the replication operation is enabled in the configuration."""
        return (
            self.catalog_config.replication_config
            and self.catalog_config.replication_config.enabled
        )

    def process_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> List[RunResult]:
        """Process a single table for replication."""
        results = []
        if self.catalog_config.table_types and len(self.catalog_config.table_types) > 0:
            result = self._replicate_table(schema_name, table_name)
            results.append(result)

        if self.catalog_config.uc_object_types:
            if (
                UCObjectType.TABLE_TAG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                result = self._replicate_table_tags(
                    schema_name,
                    table_name,
                )
                results.extend(result)
            if (
                UCObjectType.COLUMN_TAG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                result = self._replicate_column_tags(
                    schema_name,
                    table_name,
                )
                results.extend(result)
            if (
                UCObjectType.COLUMN_COMMENT in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                result = self._replicate_column_comments(
                    schema_name,
                    table_name,
                )
                results.extend(result)

        if results:
            self.audit_logger.log_results(results)
        return results

    def process_volume(self, schema_name: str, volume_name: str) -> RunResult:
        """Process a single volume for replication."""
        results = []
        # # Check for volume replication first
        # if (
        #     self.catalog_config.volume_types
        #     and len(self.catalog_config.volume_types) > 0
        # ):
        #     result = self._replicate_volume(schema_name, volume_name)
        #     results.append(result)

        # Check for volume tag replication
        if (
            self.catalog_config.uc_object_types
            and len(self.catalog_config.uc_object_types) > 0
            and (
                UCObjectType.VOLUME_TAG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            )
        ):
            result = self._replicate_volume_tags(
                schema_name,
                volume_name,
            )
            results.extend(result)
        if results:
            self.audit_logger.log_results(results)
        return results

    def setup_operation_catalogs(self) -> str:
        """Setup replication-specific catalogs."""
        replication_config = self.catalog_config.replication_config
        if not self.catalog_config.uc_object_types:
            # Create target catalog if needed
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
                sharing_identifier = self.source_databricks_config.sharing_identifier
                if not sharing_identifier:
                    sharing_identifier = self.source_dbops.get_metastore_id()
                provider_name = self.db_ops.get_provider_name(sharing_identifier)
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
        if not self.catalog_config.uc_object_types:
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
                    raise TableNotFoundError(f"""
                        Target table {target_table} does not exist. Cannot replicate DLT table without existing target.
                        """) from exc
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
                self.logger.debug(
                    f"Executing replication query: {query}",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
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

    # def _replicate_volume(self, schema_name: str, volume_name: str) -> List[RunResult]:
    #     """
    #     Replicate a single volume.

    #     Args:
    #         schema_name: Schema name
    #         volume_name: Volume name to replicate

    #     Returns:
    #         RunResult object for the replication operation
    #     """
    #     return None  # Implementation of volume replication logic goes here

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
        except Exception as exc:
            raise ReplicationError(
                f"Cannot get location for source volume: {source_volume}"
            ) from exc

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

    def _replicate_table_tags(
        self,
        schema_name: str,
        table_name: str,
    ) -> List[RunResult]:
        """
        Replicate table tags from source to target table.

        Args:
            schema_name: Schema name
            table_name: Table name to replicate tags for

        Returns:
            RunResult object for the tag replication operation
        """
        start_time = datetime.now(timezone.utc)
        run_results = []
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        object_type = "table_tag"
        source_table = f"`{source_catalog}`.`{schema_name}`.`{table_name}`"
        target_table = f"`{target_catalog}`.`{schema_name}`.`{table_name}`"
        max_attempts = self.retry.max_attempts

        if self.source_spark.catalog.tableExists(
            source_table
        ) and self.target_spark.catalog.tableExists(target_table):
            # Get target and source table tags
            target_tag_names, target_tag_maps = self.target_dbops.get_table_tags(
                target_catalog, schema_name, table_name
            )
            _, source_tag_maps = self.source_dbops.get_table_tags(
                source_catalog, schema_name, table_name
            )

            # Execute tag replication using helper method
            run_result = self._replicate_tags(
                object_type=object_type,
                source_tag_maps_list=source_tag_maps,
                target_tag_names_list=target_tag_names,
                target_tag_maps_list=target_tag_maps,
                overwrite_tags=replication_config.overwrite_tags,
                source_catalog=source_catalog,
                target_catalog=target_catalog,
                schema_name=schema_name,
                table_name=table_name,
            )
            run_results.append(run_result)
        else:
            self.logger.warning(
                f"Source or target table does not exist for tag replication: `{target_catalog}`.`{schema_name}`.`{table_name}`",
                extra={"run_id": self.run_id, "operation": "replication"},
            )
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=table_name,
                    object_type="table_tag",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message="Source or Target table does not exist",
                    details={
                        "source_object": source_table,
                        "target_object": target_table,
                    },
                    attempt_number=1,
                    max_attempts=max_attempts,
                )
            )
        return run_results

    def _replicate_column_tags(
        self,
        schema_name: str,
        table_name: str,
    ) -> List[RunResult]:
        """
        Replicate column tags from source to target table.

        Args:
            schema_name: Schema name
            table_name: Table name to replicate column tags for
        """
        start_time = datetime.now(timezone.utc)
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_table = f"`{source_catalog}`.`{schema_name}`.`{table_name}`"
        target_table = f"`{target_catalog}`.`{schema_name}`.`{table_name}`"
        object_type = "column_tag"

        attempt = 1
        max_attempts = self.retry.max_attempts
        last_exception = None

        run_results = []

        try:
            if not self.source_spark.catalog.tableExists(
                source_table
            ) or not self.target_spark.catalog.tableExists(target_table):
                self.logger.warning(
                    f"Source or target table does not exist for column tag replication: {source_table} -> {target_table}",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                run_results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=target_catalog,
                        schema_name=schema_name,
                        object_name=table_name,
                        object_type="column_tag",
                        status="failed",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        error_message="Source or Target table does not exist",
                        details={
                            "source_object": source_table,
                            "target_object": target_table,
                        },
                        attempt_number=attempt,
                        max_attempts=max_attempts,
                    )
                )
                return run_results
            # Get source and target column tags
            source_df = self.source_dbops.get_column_tags_df(
                source_catalog, schema_name, table_name
            ).selectExpr(
                "column_name",
                "tag_names_list as source_tag_names_list",
                "tag_maps_list as source_tag_maps_list",
            )
            target_df = self.target_dbops.get_column_tags_df(
                target_catalog, schema_name, table_name
            ).selectExpr(
                "column_name",
                "tag_names_list as target_tag_names_list",
                "tag_maps_list as target_tag_maps_list",
            )
            column_with_tags_in_source = 0
            column_with_tags_in_target = 0
            df = None
            if not source_df.isEmpty() and not target_df.isEmpty():
                column_with_tags_in_source = source_df.count()
                column_with_tags_in_target = target_df.count()
                # Copy source dataframe to target using list comprehension
                source_df_new = self.target_spark.createDataFrame(
                    [
                        (
                            row["column_name"],
                            row["source_tag_names_list"],
                            row["source_tag_maps_list"],
                        )
                        for row in source_df.collect()
                    ],
                    [
                        "column_name",
                        "source_tag_names_list",
                        "source_tag_maps_list",
                    ],
                )
                df = source_df_new.join(
                    target_df, on=["column_name"], how="fullouter"
                ).selectExpr(
                    "column_name",
                    "source_tag_names_list",
                    "source_tag_maps_list",
                    "target_tag_names_list",
                    "target_tag_maps_list",
                )
            elif not source_df.isEmpty() and target_df.isEmpty():
                column_with_tags_in_source = source_df.count()
                df = source_df.selectExpr(
                    "column_name",
                    "source_tag_names_list",
                    "source_tag_maps_list",
                    "array() as target_tag_names_list",
                    "array() as target_tag_maps_list",
                )
            elif source_df.isEmpty() and not target_df.isEmpty():
                column_with_tags_in_target = target_df.count()
                df = target_df.selectExpr(
                    "column_name",
                    "array() as source_tag_names_list",
                    "array() as source_tag_maps_list",
                    "target_tag_names_list",
                    "target_tag_maps_list",
                )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            if not df:
                self.logger.info(
                    f"No columns with tags found for table: {source_table} -> {target_table} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
                run_results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=target_catalog,
                        schema_name=schema_name,
                        object_name=table_name,
                        object_type="column_tag",
                        status="success",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        details={
                            "source_object": source_table,
                            "target_object": target_table,
                            "overwrite_tags": replication_config.overwrite_tags,
                            "columns_with_tags_in_source": column_with_tags_in_source,
                            "columns_with_tags_in_target": column_with_tags_in_target,
                        },
                        attempt_number=attempt,
                        max_attempts=max_attempts,
                    )
                )
                return run_results
            if (
                column_with_tags_in_source == 0
                and not replication_config.overwrite_tags
            ):
                self.logger.info(
                    f"No columns with tags found in source for table: {source_table} -> {target_table} "
                    f"and overwrite_tags is disabled, skipping column tag replication "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
                run_results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=target_catalog,
                        schema_name=schema_name,
                        object_name=table_name,
                        object_type="column_tag",
                        status="success",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        details={
                            "source_object": source_table,
                            "target_object": target_table,
                            "overwrite_tags": replication_config.overwrite_tags,
                            "columns_with_tags_in_source": column_with_tags_in_source,
                            "columns_with_tags_in_target": column_with_tags_in_target,
                        },
                        attempt_number=attempt,
                        max_attempts=max_attempts,
                    )
                )
                return run_results

            for row in df.collect():
                column_name = row["column_name"]
                source_tag_maps_list = row["source_tag_maps_list"]
                target_tag_names_list = row["target_tag_names_list"]
                target_tag_maps_list = row["target_tag_maps_list"]

                run_result = self._replicate_tags(
                    object_type=object_type,
                    source_tag_maps_list=source_tag_maps_list,
                    target_tag_names_list=target_tag_names_list,
                    target_tag_maps_list=target_tag_maps_list,
                    overwrite_tags=replication_config.overwrite_tags,
                    source_catalog=source_catalog,
                    target_catalog=target_catalog,
                    schema_name=schema_name,
                    table_name=table_name,
                    column_name=column_name,
                )
                run_results.append(run_result)

            return run_results
        except Exception as e:
            last_exception = e

        # Handle failure case
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        error_msg = f"Column tag replication failed {source_table} -> {target_table}"
        if last_exception:
            error_msg += f" | Last error: {str(last_exception)}"

        self.logger.error(
            error_msg,
            extra={"run_id": self.run_id, "operation": "replication"},
        )

        run_results.append(
            RunResult(
                operation_type="uc_replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                object_name=table_name,
                object_type="column_tag",
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                error_message=str(last_exception)
                if last_exception
                else "Unknown error",
                details={
                    "source_object": source_table,
                    "target_object": target_table,
                    "overwrite_tags": replication_config.overwrite_tags,
                    "columns_with_tags_in_source": column_with_tags_in_source,
                    "columns_with_tags_in_target": column_with_tags_in_target,
                },
                attempt_number=attempt,
                max_attempts=max_attempts,
            )
        )
        return run_results

    def _replicate_volume_tags(
        self,
        schema_name: str,
        volume_name: str,
    ) -> list[RunResult]:
        """
        Replicate volume tags from source to target volume.

        Args:
            schema_name: Schema name
            volume_name: Volume name to replicate tags for

        Returns:
            RunResult object for the tag replication operation
        """
        start_time = datetime.now(timezone.utc)
        run_results = []
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        object_type = "volume_tag"
        source_volume = f"`{source_catalog}`.`{schema_name}`.`{volume_name}`"
        target_volume = f"`{target_catalog}`.`{schema_name}`.`{volume_name}`"
        max_attempts = self.retry.max_attempts

        if not self.source_dbops.if_volume_exists(
            source_volume
        ) or not self.target_dbops.if_volume_exists(target_volume):
            self.logger.warning(
                f"Source or target volume does not exist for tag replication: {source_volume} -> {target_volume}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=volume_name,
                    object_type="volume_tag",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message="Source or Target volume does not exist",
                    details={
                        "source_object": source_volume,
                        "target_object": target_volume,
                    },
                    attempt_number=1,
                    max_attempts=max_attempts,
                )
            )
            return run_results

        # Get target and source table tags
        target_tag_names, target_tag_maps = self.target_dbops.get_volume_tags(
            target_catalog, schema_name, volume_name
        )
        _, source_tag_maps = self.source_dbops.get_volume_tags(
            source_catalog, schema_name, volume_name
        )

        # Execute tag replication using helper method
        run_result = self._replicate_tags(
            object_type=object_type,
            source_tag_maps_list=source_tag_maps,
            target_tag_names_list=target_tag_names,
            target_tag_maps_list=target_tag_maps,
            overwrite_tags=replication_config.overwrite_tags,
            source_catalog=source_catalog,
            target_catalog=target_catalog,
            schema_name=schema_name,
            volume_name=volume_name,
        )
        run_results.append(run_result)
        return run_results

    def _replicate_column_comments(
        self,
        schema_name: str,
        table_name: str,
    ) -> List[RunResult]:
        """
        Replicate column comments from source to target table.

        Args:
            schema_name: Schema name
            table_name: Table name to replicate comments for
        """

        # Use custom retry decorator with logging
        @retry_with_logging(self.retry, self.logger)
        def column_comment_replication_operation(query: str):
            self.logger.debug(
                f"Executing column comment replication query: {query}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )
            self.target_spark.sql(query)
            return True

        run_results = []
        start_time = datetime.now(timezone.utc)
        query = ""
        attempt = 1
        max_attempts = self.retry.max_attempts
        last_exception = None
        result = True

        try:
            replication_config = self.catalog_config.replication_config
            source_catalog = replication_config.source_catalog
            target_catalog = self.catalog_config.catalog_name
            source_table = f"`{source_catalog}`.`{schema_name}`.`{table_name}`"
            target_table = f"`{target_catalog}`.`{schema_name}`.`{table_name}`"
            object_type = "column_comment"

            if not self.source_spark.catalog.tableExists(
                source_table
            ) or not self.target_spark.catalog.tableExists(target_table):
                self.logger.warning(
                    f"Source or target table does not exist for comment replication: `{target_catalog}`.`{schema_name}`.`{table_name}`",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()

                run_results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=target_catalog,
                        schema_name=schema_name,
                        object_name=table_name,
                        object_type="column_comment",
                        status="failed",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        error_message="Source or Target table does not exist",
                        details={
                            "source_object": source_table,
                            "target_object": target_table,
                        },
                        attempt_number=1,
                        max_attempts=max_attempts,
                    )
                )
                return run_results

            # Get source and target column comments
            source_comment_maps_list = self.source_dbops.get_column_comments(
                source_catalog, schema_name, table_name
            )
            target_comment_maps_list = self.target_dbops.get_column_comments(
                target_catalog, schema_name, table_name
            )

            (
                uncommon_source_comment_maps_list,
                uncommon_target_comment_maps_list,
            ) = filter_common_maps(source_comment_maps_list, target_comment_maps_list)

            if (
                not uncommon_source_comment_maps_list
                and not uncommon_target_comment_maps_list
            ):
                self.logger.info(
                    f"No uncommon comment found for: {source_table} -> {target_table} "
                    f"Skipping comment replication for this {object_type}",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
                run_results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=target_catalog,
                        schema_name=schema_name,
                        object_name=table_name,
                        object_type=object_type,
                        status="success",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        details={
                            "source_object": source_table,
                            "target_object": target_table,
                            "overwrite_comments": replication_config.overwrite_comments,
                            "skipped": True,
                        },
                        attempt_number=attempt,
                        max_attempts=max_attempts,
                    )
                )
                return run_results

            merged_comment_maps = merge_maps(
                uncommon_source_comment_maps_list,
                uncommon_target_comment_maps_list,
                replication_config.overwrite_comments,
            )

            table_type = self.source_dbops.get_table_type(source_table)

            comment_list = [
                f"`{k}` COMMENT '{v}'"
                for k, v in merged_comment_maps.items()
                if v is not None
            ]
            comment_str = ",".join(comment_list).replace("\\", "\\\\").replace("'", "'")

            if len(comment_list) > 0:
                if (
                    table_type.lower() == "view"
                    or table_type.lower() == "streaming_table"
                ):
                    filtered_comment_maps = {
                        k: v for k, v in merged_comment_maps.items() if v is not None
                    }
                    for name, comment in filtered_comment_maps.items():
                        comment_str = comment.replace("\\", "\\\\").replace("'", "''")
                        query = f"""
                        COMMENT ON COLUMN {target_table}.`{name}` IS '{comment_str}'
                        """
                        if table_type.lower() == "streaming_table":
                            query = f"""
                            ALTER STREAMING TABLE {target_table} ALTER COLUMN `{name}` COMMENT '{comment_str}'
                            """
                        # Execute the replication
                        result, last_exception, attempt, max_attempts = (
                            column_comment_replication_operation(query)
                        )
                elif table_type.lower() in ["managed", "external"]:
                    query = f"""
                    ALTER TABLE {target_table} ALTER COLUMN {comment_str}
                    """

                    # Execute the replication
                    result, last_exception, attempt, max_attempts = (
                        column_comment_replication_operation(query)
                    )
                else:
                    self.logger.warning(
                        f"Unsupported table type for column comment replication: {table_type} for table {source_table}",
                        extra={"run_id": self.run_id, "operation": "replication"},
                    )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"{object_type} replication completed successfully: {source_table} -> {target_table} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )

                run_results.append(
                    RunResult(
                        operation_type="uc_replication",
                        catalog_name=target_catalog,
                        schema_name=schema_name,
                        object_name=table_name,
                        object_type=object_type,
                        status="success",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        details={
                            "source_object": source_table,
                            "target_object": target_table,
                            "overwrite_comments": replication_config.overwrite_comments,
                            "columns_with_comments": len(comment_list)
                            if comment_list
                            else 0,
                        },
                        attempt_number=attempt,
                        max_attempts=max_attempts,
                    )
                )
                return run_results
        except Exception as e:
            last_exception = e

        # Handle failure case
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        error_msg = (
            f"Column comment replication failed {source_table} -> {target_table}"
        )
        if last_exception:
            error_msg += f" | Last error: {str(last_exception)}"

        self.logger.error(
            error_msg,
            extra={"run_id": self.run_id, "operation": "replication"},
        )

        run_results.append(
            RunResult(
                operation_type="uc_replication",
                catalog_name=target_catalog,
                schema_name=schema_name,
                object_name=table_name,
                object_type="column_comment",
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                error_message=str(last_exception)
                if last_exception
                else "Unknown error",
                details={
                    "source_object": source_table,
                    "target_object": target_table,
                    "overwrite_comments": replication_config.overwrite_comments,
                },
                attempt_number=1,
                max_attempts=max_attempts,
            )
        )
        return run_results
