"""
Replication provider implementation for data replication system.

This module handles replication operations with support for deep clone,
streaming tables, materialized views, and intermediate catalogs.
"""

from datetime import datetime, timezone
from typing import List

from data_replication.databricks_operations import DatabricksOperations

# from delta.tables import DeltaTable
from ..config.models import (
    RunResult,
    SchemaConfig,
    TableConfig,
    TableType,
    UCObjectType,
    VolumeConfig,
)
from ..exceptions import ReplicationError, TableNotFoundError
from ..utils import (
    filter_common_maps,
    get_workspace_url_from_host,
    map_external_location,
    merge_maps,
    retry_with_logging,
    create_spark_session,
    validate_spark_session,
    create_workspace_client,
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
        self.target_workspace_client = create_workspace_client(
            host=self.target_databricks_config.host,
            secret_config=self.target_databricks_config.token,
            workspace_client=self.workspace_client,
            auth_type=self.target_databricks_config.auth_type,
        )
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
            self.source_workspace_client = create_workspace_client(
                host=self.source_databricks_config.host,
                secret_config=self.source_databricks_config.token,
                workspace_client=self.workspace_client,
                auth_type=self.source_databricks_config.auth_type,
            )
            self.source_dbops = DatabricksOperations(
                self.source_spark, self.logger, self.source_workspace_client
            )

        # for uc replication, set default driving spark to source spark and dbops to source dbops. Create separate target spark and dbops.
        if (
            self.catalog_config.uc_object_types
            and len(self.catalog_config.uc_object_types) > 0
        ):
            # set target spark and dbops to current spark and dbops
            self.target_spark = self.spark
            self.target_dbops = DatabricksOperations(
                self.target_spark, self.logger, self.target_workspace_client
            )
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
                if replication_config.backup_catalog:
                    self.db_ops.create_catalog_using_share_if_not_exists(
                        replication_config.backup_catalog,
                        provider_name,
                        replication_config.backup_share_name,
                    )

            if replication_config.volume_config:
                # create file ingestion logging table if not exists
                self._create_file_ingestion_logging_table(
                    replication_config.volume_config
                )

        return replication_config.source_catalog

    def process_schema_concurrently(
        self,
        schema_config: SchemaConfig,
    ) -> List[RunResult]:
        """Override to add replication-specific schema setup."""
        replication_config = schema_config.replication_config
        if not self.catalog_config.uc_object_types:
            # Create intermediate schema if needed
            if replication_config.intermediate_catalog:
                self.db_ops.create_schema_if_not_exists(
                    replication_config.intermediate_catalog, schema_config.schema_name
                )

            # Create target schema if needed
            self.db_ops.create_schema_if_not_exists(
                self.catalog_config.catalog_name, schema_config.schema_name
            )

        return super().process_schema_concurrently(schema_config)

    def process_table(
        self,
        schema_config: SchemaConfig,
        table_config: TableConfig,
    ) -> List[RunResult]:
        """Process a single table for replication."""
        results = []
        schema_name = schema_config.schema_name
        if schema_config.table_types and len(schema_config.table_types) > 0:
            result = self._replicate_table(schema_name, table_config)
            results.append(result)

        if self.catalog_config.uc_object_types:
            if (
                UCObjectType.TABLE_TAG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                result = self._replicate_table_tags(
                    schema_name,
                    table_config,
                )
                results.extend(result)
            if (
                UCObjectType.COLUMN_TAG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                result = self._replicate_column_tags(
                    schema_name,
                    table_config,
                )
                results.extend(result)
            if (
                UCObjectType.COLUMN_COMMENT in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                result = self._replicate_column_comments(
                    schema_name,
                    table_config,
                )
                results.extend(result)

        if results:
            self.audit_logger.log_results(results)
        return results

    def process_volume(
        self, schema_config: SchemaConfig, volume_config: str
    ) -> List[RunResult]:
        """Process a single volume for replication."""
        results = []
        schema_name = schema_config.schema_name
        # Check for volume replication first
        if (
            self.catalog_config.volume_types
            and len(self.catalog_config.volume_types) > 0
        ):
            result = self._replicate_volume(schema_name, volume_config)
            results.extend(result)
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
                volume_config,
            )
            results.extend(result)
        if results:
            self.audit_logger.log_results(results)
        return results

    def _replicate_table(
        self,
        schema_name: str,
        table_config: TableConfig,
    ) -> RunResult:
        """
        Replicate a single table using deep clone or insert overwrite.

        Args:
            schema_config: SchemaConfig object for the schema
            table_config: TableConfig object for the table to replicate

        Returns:
            RunResult object for the replication operation
        """
        start_time = datetime.now(timezone.utc)
        table_name = table_config.table_name
        replication_config = table_config.replication_config
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
            # Check if source table exists
            if not self.spark.catalog.tableExists(source_table):
                raise TableNotFoundError(f"Source table does not exist: {source_table}")

            # Get source table type to determine replication strategy
            source_table_type = self.db_ops.get_table_type(source_table)
            if source_table_type.upper() == TableType.STREAMING_TABLE.upper():
                backup_catalog = replication_config.backup_catalog
                source_table = f"`{backup_catalog}`.`{schema_name}`.`{table_name}`"

            self.logger.info(
                f"Starting replication: {source_table} -> {target_table}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            is_external = source_table_type.upper() == TableType.EXTERNAL.upper()

            try:
                table_details = self.db_ops.get_table_details(target_table)
                actual_target_table = table_details["table_name"]
                dlt_flag = table_details["is_dlt"]
                pipeline_id = table_details["pipeline_id"]
            except TableNotFoundError as exc:
                table_details = self.db_ops.get_table_details(source_table)
                if table_details["is_dlt"]:
                    msg = f"Target DLT table {target_table} must exist before replicating."
                    self.logger.error(
                        msg,
                        extra={"run_id": self.run_id, "operation": "replication"},
                    )
                    raise TableNotFoundError(msg) from exc
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
            if is_external and not replication_config.replicate_as_managed:
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
                    replication_config,
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
                    replication_config,
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
                    replication_config,
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
                object_name=table_name,
                object_type="table",
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
                object_name=table_name,
                object_type="table",
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

    def _create_file_ingestion_logging_table(self, volume_replication_config) -> None:
        """Create file ingestion logging table if not exists."""
        # create detail ingestion logging catalog and schema if not exists
        if volume_replication_config.create_file_ingestion_logging_catalog:
            self.db_ops.create_catalog_if_not_exists(
                volume_replication_config.file_ingestion_logging_catalog,
                volume_replication_config.file_ingestion_logging_catalog_location,
            )

        self.db_ops.create_schema_if_not_exists(
            volume_replication_config.file_ingestion_logging_catalog,
            volume_replication_config.file_ingestion_logging_schema,
        )
        detail_ingestion_logging_table = f"`{volume_replication_config.file_ingestion_logging_catalog}`.`{volume_replication_config.file_ingestion_logging_schema}`.`{volume_replication_config.file_ingestion_logging_table}`"

        # create detail ingestion logging table if not exists
        self.logger.info(
            f"Creating detail ingestion logging table: {detail_ingestion_logging_table}"
        )
        # create detail ingestion logging table if not exists
        self.spark.sql(f"""
            create table if not exists {detail_ingestion_logging_table} (
            run_id string,
            source_path String,
            target_path string,
            ingestion_time timestamp,
            length bigint,
            file_modification_time timestamp,
            status string,
            error_msg string,
            batch_id string
        )
        """)
        self.logger.info(
            f"File ingestion details are logged in: {detail_ingestion_logging_table}"
        )

    def _replicate_volume(
        self, schema_name: str, volume_config: VolumeConfig
    ) -> List[RunResult]:
        """
        Replicate a single volume.

        Args:
            schema_name: Schema name
            volume_config: Volume configuration

        Returns:
            RunResult object for the replication operation
        """

        start_time = datetime.now(timezone.utc)
        volume_name = volume_config.volume_name
        replication_config = volume_config.replication_config
        volume_replication_config = replication_config.volume_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_volume = f"`{source_catalog}`.`{schema_name}`.`{volume_name}`"
        target_volume = f"`{target_catalog}`.`{schema_name}`.`{volume_name}`"
        source_path = f"/Volumes/{source_catalog}/{schema_name}/{volume_name}"
        target_path = f"/Volumes/{target_catalog}/{schema_name}/{volume_name}"
        checkpoint_path = f"{target_path}/_checkpoints"
        detail_ingestion_logging_table = f"`{volume_replication_config.file_ingestion_logging_catalog}`.`{volume_replication_config.file_ingestion_logging_schema}`.`{volume_replication_config.file_ingestion_logging_table}`"

        checkpoint_subfolder = (
            volume_replication_config.folder_path.strip("/")
            if volume_replication_config.folder_path
            else "root"
        )
        if volume_replication_config.folder_path:
            source_path = (
                f"{source_path}/{volume_replication_config.folder_path.strip('/')}/"
            )
            target_path = (
                f"{target_path}/{volume_replication_config.folder_path.strip('/')}/"
            )
            checkpoint_path = f"{checkpoint_path}/{checkpoint_subfolder}/"

        # Prepare autoloader read options
        read_options_always = {
            "cloudFiles.format": "binaryFile",
        }
        read_options = read_options_always
        if volume_replication_config.autoloader_options:
            read_options = {
                **volume_replication_config.autoloader_options,
                **read_options_always,
            }

        # Extract variables that will be used in the foreachBatch function to avoid serialization issues
        run_id = self.run_id

        attempt = 1
        max_attempts = self.retry.max_attempts
        volume_type = None
        error_count = 0

        details = {
            "source_path": source_path,
            "target_path": target_path,
            "checkpoint_path": checkpoint_path,
            "delete_and_reload": volume_replication_config.delete_and_reload,
            "error_count": error_count,
            "autoloader_options": read_options,
        }
        try:
            # Check if source table exists
            if not self.db_ops.if_volume_exists(source_volume):
                raise TableNotFoundError(
                    f"Source volume does not exist: {source_volume}"
                )

            # Get volume type
            volume_type = self.db_ops.get_volume_type(source_volume)
            details["volume_type"] = volume_type

            self.logger.info(
                f"Starting replication: {source_path} -> {target_path} at checkpoint: {checkpoint_path}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            if (
                volume_replication_config.delete_checkpoint
                or volume_replication_config.delete_and_reload
            ):
                try:
                    self.logger.info(
                        f"Deleting checkpoint directory: {checkpoint_path}"
                    )
                    self.target_workspace_client.dbutils.fs.rm(checkpoint_path, True)
                    self.logger.info(
                        f"Directory {checkpoint_path} removed successfully"
                    )
                except Exception as e:
                    self.logger.warning(
                        f"An error occurred when trying to remove directory: {checkpoint_path}: {str(e)}"
                    )
            if volume_replication_config.delete_and_reload:
                try:
                    self.logger.info(f"Deleting target directory: {target_path}")
                    self.target_workspace_client.dbutils.fs.rm(target_path, True)
                    self.logger.info(f"Directory {target_path} removed successfully")
                except Exception as e:
                    self.logger.warning(
                        f"An error occurred when trying to remove directory: {target_path}: {str(e)}"
                    )

            if volume_replication_config.streaming_timeout_seconds:
                self.spark.conf.set(
                    "spark.databricks.execution.timeout",
                    volume_replication_config.streaming_timeout_seconds,
                )

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def replication_operation(
                source_path: str,
                target_path: str,
                run_id: str,
                checkpoint_path: str,
                logging_table: str,
                max_workers: int,
                read_options: dict,
            ):
                try:
                    df = (
                        self.spark.readStream.format("cloudFiles")
                        .options(**read_options)
                        .load(source_path)
                        .select("path", "length", "modificationTime")
                    )

                    def copy_files(batch_df, batch_id):
                        import os
                        from concurrent.futures import ThreadPoolExecutor, as_completed

                        def process_file(file_row):
                            try:
                                result = 0
                                error_msg = ""
                                status = "success"
                                src_file_path = file_row["path"]
                                if src_file_path.startswith("dbfs:"):
                                    src_file_path = src_file_path.replace(
                                        "dbfs:", "", 1
                                    )

                                rel_path = os.path.relpath(
                                    src_file_path, source_path
                                ).lstrip("/")
                                dst_file_path = f"{target_path}/{rel_path}"

                                dst_dir = os.path.dirname(dst_file_path)
                                os.makedirs(dst_dir, exist_ok=True)

                                cmd = f'cp "{src_file_path}" "{dst_file_path}"'
                                result = os.system(cmd)
                                if result != 0:
                                    status = "error"
                                    error_msg = f"Copy failed with exit code {result}"

                            except Exception as e:
                                status = "error"
                                error_msg = str(e).replace("'", "''")

                            return (
                                src_file_path,
                                dst_file_path,
                                status,
                                error_msg,
                                file_row["length"],
                                file_row["modificationTime"],
                            )

                        # Use threading to process files
                        with ThreadPoolExecutor(max_workers=max_workers) as executor:
                            futures = [
                                executor.submit(process_file, row)
                                for row in batch_df.collect()
                            ]

                            for future in as_completed(futures):
                                try:
                                    (
                                        src_file,
                                        dst_file,
                                        status,
                                        error,
                                        length,
                                        mod_time,
                                    ) = future.result()

                                    # Log each result
                                    sql = f"""INSERT INTO {logging_table}
                                        VALUES ('{run_id}', '{src_file}', '{dst_file}',
                                               current_timestamp, {length}, '{mod_time}',
                                               '{status}', '{error}', '{batch_id}')"""
                                    batch_df.sparkSession.sql(sql)
                                except Exception:
                                    pass

                    query = (
                        df.writeStream.foreachBatch(copy_files)
                        .option("checkpointLocation", checkpoint_path)
                        .trigger(availableNow=True)
                        .start()
                    )
                except Exception as e:
                    raise ReplicationError(
                        f"Volume replication failed: {str(e)}"
                    ) from e

                query_result = None
                query_result = query.awaitTermination(
                    volume_replication_config.streaming_timeout_seconds
                )
                if not query_result:
                    raise ReplicationError(
                        f"Volume replication streaming query timed out after {volume_replication_config.streaming_timeout_seconds} seconds"
                    )
                return True

            # Direct replication
            (
                result,
                last_exception,
                attempt,
                max_attempts,
            ) = replication_operation(
                source_path=source_path,
                target_path=target_path,
                run_id=run_id,
                checkpoint_path=checkpoint_path,
                logging_table=detail_ingestion_logging_table,
                max_workers=volume_replication_config.max_concurrent_copies,
                read_options=read_options,
            )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # get error count from detail ingestion logging table
            details["error_count"] = (
                self.spark.sql(
                    f"select count(1) from {detail_ingestion_logging_table} where status = 'error' and run_id = '{self.run_id}'"
                ).collect()[0][0]
                or 0
            )
            details["total_count"] = (
                self.spark.sql(
                    f"select count(1) from {detail_ingestion_logging_table} where run_id = '{self.run_id}'"
                ).collect()[0][0]
                or 0
            )

            if result:
                success_rate = (
                    (details["total_count"] - details["error_count"])
                    / details["total_count"]
                    if details["total_count"] > 0
                    else 0
                )
                self.logger.info(
                    f"Replication completed successfully: {source_path} -> {target_path} "
                    f"(success: {details['total_count'] - details['error_count']}/{details['total_count']}) "
                    f"success_rate: {success_rate:.2%} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )

                return [
                    RunResult(
                        operation_type="replication",
                        catalog_name=target_catalog,
                        schema_name=schema_name,
                        object_name=volume_name,
                        object_type="volume",
                        status="success",
                        start_time=start_time.isoformat(),
                        end_time=end_time.isoformat(),
                        duration_seconds=duration,
                        details=details,
                        attempt_number=attempt,
                        max_attempts=max_attempts,
                    )
                ]

            error_msg = (
                f"Replication failed after {max_attempts} attempts: "
                f"{source_volume} -> {target_volume}"
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            return [
                RunResult(
                    operation_type="replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    volume_name=volume_name,
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    error_message=error_msg,
                    details=details,
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            ]

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Wrap in ReplicationError for better error categorization
            if not isinstance(e, ReplicationError):
                e = ReplicationError(f"Replication operation failed: {str(e)}")

            error_msg = f"Failed to replicate volume {source_volume}: {str(e)}"
            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "replication"},
                exc_info=True,
            )

            return [
                RunResult(
                    operation_type="replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    volume_name=volume_name,
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                    details=details,
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            ]

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

        # Map external location using utility function
        target_location = map_external_location(
            source_location, self.external_location_mapping
        )

        if not target_location:
            raise ReplicationError(
                f"No external location mapping found for source volume location: {source_location}"
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
        replication_config,
    ) -> tuple:
        """Replicate table via intermediate catalog."""
        intermediate_table = (
            f"{replication_config.intermediate_catalog}.{schema_name}.{table_name}"
        )

        # Step 1: Deep clone to intermediate
        step1_query = self._build_deep_clone_query(
            source_table, intermediate_table, None, replication_config
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
            source_table, target_table, pipeline_id, replication_config
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
        replication_config,
    ) -> tuple:
        """Replicate table directly to target."""

        # Use deep clone
        step1_query = self._build_deep_clone_query(
            source_table, target_table, pipeline_id, replication_config
        )

        return *replication_operation(step1_query), step1_query, None

    def _replicate_external_table(
        self,
        source_table: str,
        target_table: str,
        replication_operation,
        replication_config,
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

        # Step 3: Map external location using utility function
        target_location = map_external_location(
            source_location, self.external_location_mapping
        )

        if not target_location:
            raise ReplicationError(
                f"No external location mapping found for source location: {source_location}"
            )

        self.logger.debug(
            f"External table replication: {source_table} -> {target_location}",
            extra={"run_id": self.run_id, "operation": "replication"},
        )

        step1_query = None
        step2_query = None

        # Step 4: Deep clone to target location if copy_files is enabled
        if replication_config.copy_files:
            step1_query = self._build_deep_clone_query(
            source_table, f"delta.`{target_location}`", None, replication_config
        )

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
        self, source_table: str, target_table: str, pipeline_id: str = None, replication_config=None
    ) -> str:
        """Build deep clone query."""

        sql = f"CREATE OR REPLACE TABLE {target_table} DEEP CLONE {source_table} "

        if pipeline_id:
            # For dlt streaming tables/materialized views, use CREATE OR REPLACE TABLE with pipelineId property
            return f"{sql} TBLPROPERTIES ('pipelines.pipelineId'='{pipeline_id}')"

        # For regular tables, just return the deep clone query
        return sql

    def _replicate_table_tags(
        self,
        schema_name: str,
        table_config: TableConfig,
    ) -> List[RunResult]:
        """
        Replicate table tags from source to target table.

        Args:
            schema_name: Schema name
            table_config: TableConfig object containing table details

        Returns:
            RunResult object for the tag replication operation
        """
        start_time = datetime.now(timezone.utc)
        run_results = []
        table_name = table_config.table_name
        replication_config = table_config.replication_config
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
        table_config: TableConfig,
    ) -> List[RunResult]:
        """
        Replicate column tags from source to target table.

        Args:
            schema_name: Schema name
            table_config: TableConfig object containing table details
        """
        start_time = datetime.now(timezone.utc)
        table_name = table_config.table_name
        replication_config = table_config.replication_config
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
        volume_config: VolumeConfig,
    ) -> list[RunResult]:
        """
        Replicate volume tags from source to target volume.

        Args:
            schema_name: Schema name
            volume_config: VolumeConfig object containing volume details

        Returns:
            RunResult object for the tag replication operation
        """
        start_time = datetime.now(timezone.utc)
        run_results = []
        volume_name = volume_config.volume_name
        replication_config = volume_config.replication_config
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
        table_config: TableConfig,
    ) -> List[RunResult]:
        """
        Replicate column comments from source to target table.

        Args:
            schema_name: Schema name
            table_config: TableConfig object containing table details
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
            table_name = table_config.table_name
            replication_config = table_config.replication_config
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
