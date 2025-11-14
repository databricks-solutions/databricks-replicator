"""
Base provider class for data replication operations.

This module provides shared functionality that can be reused across different
provider types like backup, replication, and reconciliation.
"""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed
from datetime import datetime, timezone
from typing import List, Optional, Tuple

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from pyspark.sql.utils import AnalysisException

from data_replication.utils import retry_with_logging, merge_maps

from ..audit.audit_logger import AuditLogger
from ..audit.logger import DataReplicationLogger
from ..config.models import (
    DatabricksConnectConfig,
    RetryConfig,
    RunResult,
    TableConfig,
    TargetCatalogConfig,
    UCObjectType,
    VolumeConfig,
)
from ..databricks_operations import DatabricksOperations
from ..exceptions import (
    BackupError,
    DataReplicationError,
    ReconciliationError,
    ReplicationError,
    SparkSessionError,
)


class BaseProvider(ABC):
    """Base provider class with shared functionality for data replication operations."""

    def __init__(
        self,
        spark: DatabricksSession,
        logger: DataReplicationLogger,
        db_ops: DatabricksOperations,
        workspace_client: WorkspaceClient,
        run_id: str,
        catalog_config: TargetCatalogConfig,
        source_databricks_config: DatabricksConnectConfig,
        target_databricks_config: DatabricksConnectConfig,
        retry: Optional[RetryConfig] = None,
        max_workers: int = 2,
        timeout_seconds: int = 1800,
        external_location_mapping: Optional[dict] = None,
        audit_logger: Optional[AuditLogger] = None,
    ):
        """
        Initialize the base provider.

        Args:
            spark: Spark session for databricks workspace
            logger: Logger instance for audit logging
            db_ops: Databricks operations helper
            run_id: Unique run identifier
            catalog_config: Target catalog configuration
            retry: Retry configuration
            max_workers: Maximum number of concurrent workers
            timeout_seconds: Timeout for operations
        """
        self.spark = spark
        self.logger = logger
        self.db_ops = db_ops
        self.workspace_client = workspace_client
        self.run_id = run_id
        self.catalog_config = catalog_config
        self.source_databricks_config = source_databricks_config
        self.target_databricks_config = target_databricks_config
        self.retry = (
            retry if retry else RetryConfig(max_attempts=1, retry_delay_seconds=5)
        )
        self.max_workers = max_workers
        self.timeout_seconds = timeout_seconds
        self.external_location_mapping = external_location_mapping
        self.audit_logger = audit_logger
        self.catalog_name: Optional[str] = None
        self.source_spark = None
        self.source_dbops = None
        self.target_spark = None
        self.target_dbops = None

    @abstractmethod
    def setup_operation_catalogs(self):
        """
        Setup operation-specific catalogs. Override in subclasses as needed.
        Default implementation does nothing.
        """

    def _handle_exception(
        self,
        e: Exception,
        context: str,
        catalog_name: str,
        schema_name: str = "",
        object_name: str = "",
        object_type: str = "table",
        start_time: datetime = None,
    ) -> RunResult:
        """
        Handle exceptions consistently across all operations.

        Args:
            e: The exception that occurred
            context: Context description (e.g., "processing table", "processing schema")
            catalog_name: Catalog name
            schema_name: Schema name (optional)
            object_name: Object name (optional)
            object_type: Object type (optional)
            start_time: Operation start time

        Returns:
            RunResult with failed status
        """
        if isinstance(e, (FuturesTimeoutError, TimeoutError)):
            error_msg = f"Timeout {context} {catalog_name}.{schema_name}.{object_name} after {self.timeout_seconds}s"
            self.logger.error(error_msg)
        elif isinstance(
            e,
            (
                DataReplicationError,
                SparkSessionError,
                AnalysisException,
                BackupError,
                ReplicationError,
                ReconciliationError,
            ),
        ):
            error_msg = f"Failed to {context} {catalog_name}.{schema_name}.{object_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)
        else:
            error_msg = f"Unexpected error {context} {catalog_name}.{schema_name}.{object_name}: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

        return self._create_failed_result(
            catalog_name, schema_name, object_name, object_type, error_msg, start_time
        )

    @abstractmethod
    def process_table(self, schema_name: str, table_name: str) -> RunResult:
        """
        Process a single table.
        Must be implemented by subclasses.

        Args:
            schema_name: Schema name
            table_name: Table name

        Returns:
            RunResult object for the operation
        """

    def process_volume(self, schema_name: str, volume_name: str) -> RunResult:
        """
        Process a single volume.
        Default implementation returns a success result.
        Override in subclasses that need volume processing.

        Args:
            schema_name: Schema name
            volume_name: Volume name

        Returns:
            RunResult object for the operation
        """
        # Default implementation - volumes not supported by this provider
        start_time = datetime.now(timezone.utc)
        return RunResult(
            operation_type=self.get_operation_name(),
            catalog_name=self.catalog_config.catalog_name,
            schema_name=schema_name,
            object_name=volume_name,
            object_type="volume",
            status="success",
            start_time=start_time.isoformat(),
            end_time=datetime.now(timezone.utc).isoformat(),
        )

    @abstractmethod
    def get_operation_name(self) -> str:
        """
        Get the name of the operation for logging purposes.
        Must be implemented by subclasses.

        Returns:
            String name of the operation (e.g., "backup", "replication")
        """

    @abstractmethod
    def is_operation_enabled(self) -> bool:
        """
        Check if the operation is enabled in the configuration.
        Must be implemented by subclasses.

        Returns:
            True if operation is enabled, False otherwise
        """

    def process_catalog(self) -> List[RunResult]:
        """
        Process all tables in a catalog based on configuration.
        Template method that provides common catalog processing pattern.

        Returns:
            List of RunResult objects for each operation
        """
        if not self.is_operation_enabled():
            self.logger.info(
                f"{self.get_operation_name().title()} is disabled for catalog: {self.catalog_config.catalog_name}"
            )
            return []

        results = []
        start_time = datetime.now(timezone.utc)

        try:
            # Setup operation-specific catalogs (implemented by subclasses)
            self.catalog_name = self.setup_operation_catalogs()

            self.logger.info(
                f"Starting {self.get_operation_name()} catalog: {self.catalog_name}",
                extra={
                    "run_id": self.run_id,
                    "operation": self.get_operation_name(),
                },
            )

            # Replicate catalog tags if configured
            if self.catalog_config.uc_object_types and (
                UCObjectType.CATALOG_TAG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                uc_object_types_catalog = self.catalog_config.uc_object_types.copy()
                run_result = self._replicate_catalog_tags()
                results.extend(run_result)
                if UCObjectType.ALL not in uc_object_types_catalog:
                    uc_object_types_catalog.remove(UCObjectType.CATALOG_TAG)
                # immediately return if no other object types to process
                if len(uc_object_types_catalog) == 0:
                    return results

            # Get schemas to process
            schema_list = self._get_schemas()
            self.logger.info(
                f"Starting {self.get_operation_name()} schemas: {schema_list}"
            )

            for schema_name, table_list, volume_list in schema_list:
                self.logger.info(
                    f"Starting {self.get_operation_name()} schema: {schema_name}",
                    extra={
                        "run_id": self.run_id,
                        "operation": self.get_operation_name(),
                    },
                )
                # Replicate schema tags if configured
                if self.catalog_config.uc_object_types and (
                    UCObjectType.SCHEMA_TAG in self.catalog_config.uc_object_types
                    or UCObjectType.ALL in self.catalog_config.uc_object_types
                ):
                    uc_object_types_schema = uc_object_types_catalog.copy()
                    run_result = self._replicate_schema_tags(schema_name)
                    results.extend(run_result)
                    if UCObjectType.ALL not in uc_object_types_catalog:                    
                        uc_object_types_schema.remove(UCObjectType.SCHEMA_TAG)
                    # continue to next schema if no other object types to process
                    if len(uc_object_types_schema) == 0:
                        continue

                schema_results = self.process_schema_concurrently(
                    schema_name, table_list, volume_list
                )
                results.extend(schema_results)

                # Log results after processing each schema
                self._log_schema_results(schema_results, schema_name)

        except Exception as e:
            result = self._handle_exception(
                e,
                f"{self.get_operation_name()} catalog",
                self.catalog_config.catalog_name,
                "",
                "",
                "catalog",
                start_time,
            )
            results.append(result)

        return results

    def process_schema_concurrently(
        self,
        schema_name: str,
        table_list: List[TableConfig],
        volume_list: List[VolumeConfig],
    ) -> List[RunResult]:
        """
        Process all tables first, then volumes in a schema using ThreadPoolExecutor.

        Tables are processed concurrently first, and once all tables are complete,
        volumes are processed concurrently.

        Args:
            schema_name: Schema name to process
            table_list: List of table configurations to process
            volume_list: List of volume configurations to process

        Returns:
            List of RunResult objects for each table and volume operation
        """
        results: List[RunResult] = []
        catalog_name = self.catalog_config.catalog_name
        start_time = datetime.now(timezone.utc)

        try:
            total_objects = 0
            tables = []
            volumes = []
            # Get all tables and volumes in the schema
            if (
                self.catalog_config.uc_object_types
                and (
                    UCObjectType.TABLE_TAG in self.catalog_config.uc_object_types
                    or UCObjectType.ALL in self.catalog_config.uc_object_types
                    or UCObjectType.COLUMN_TAG in self.catalog_config.uc_object_types
                    or UCObjectType.COLUMN_COMMENT in self.catalog_config.uc_object_types
                )
            ) or self.catalog_config.table_types:
                tables = self._get_tables(schema_name, table_list)
            if (
                self.catalog_config.uc_object_types
                and (
                    UCObjectType.VOLUME_TAG in self.catalog_config.uc_object_types
                    or UCObjectType.ALL in self.catalog_config.uc_object_types
                )
            ) or self.catalog_config.volume_types:
                volumes = self._get_volumes(schema_name, volume_list)

            total_objects = len(tables) + len(volumes)
            if total_objects == 0:
                self.logger.info(
                    f"No objects found in schema {self.catalog_name}.{schema_name}"
                )
                return results

            # Process all tables first
            if tables:
                table_results = self._process_tables(schema_name, tables, catalog_name, start_time)
                results.extend(table_results)

            # Process all volumes after tables are complete
            if volumes:
                volume_results = self._process_volumes(schema_name, volumes, catalog_name, start_time)
                results.extend(volume_results)

        except Exception as e:
            result = self._handle_exception(
                e,
                "processing schema",
                catalog_name,
                schema_name,
                "",
                "schema",
                start_time,
            )
            results.append(result)

        return results

    def _process_tables(
        self, 
        schema_name: str, 
        tables: List[str], 
        catalog_name: str, 
        start_time: datetime
    ) -> List[RunResult]:
        """
        Process all tables in a schema using ThreadPoolExecutor.

        Args:
            schema_name: Schema name to process
            tables: List of table names to process
            catalog_name: Catalog name for error handling
            start_time: Operation start time for error handling

        Returns:
            List of RunResult objects for each table operation
        """
        results: List[RunResult] = []
        
        self.logger.info(
            f"starting {self.get_operation_name()} of {len(tables)} tables in schema {self.catalog_name}.{schema_name} using {self.max_workers} workers"
        )
        self.logger.info(f"Tables: {tables}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit table processing jobs
            future_to_table = {
                executor.submit(
                    self.process_table, schema_name, table_name
                ): table_name
                for table_name in tables
            }

            # Collect table results
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = future.result(timeout=self.timeout_seconds)

                    # Handle case where process_table returns a list of results
                    if isinstance(result, list):
                        results.extend(result)
                        # Check if any result in the list failed
                        for single_result in result:
                            if single_result.status != "success":
                                self.logger.error(
                                    f"Failed to process table "
                                    f"{catalog_name}.{schema_name}.{table_name}: "
                                    f"{single_result.error_message}"
                                )
                    else:
                        results.append(result)
                        if result.status != "success":
                            self.logger.error(
                                f"Failed to process table "
                                f"{catalog_name}.{schema_name}.{table_name}: "
                                f"{result.error_message}"
                            )

                except Exception as e:
                    result = self._handle_exception(
                        e,
                        "processing table",
                        catalog_name,
                        schema_name,
                        table_name,
                        "table",
                        start_time,
                    )
                    results.append(result)

        self.logger.info(
            f"All tables processed in schema {self.catalog_name}.{schema_name}"
        )
        
        return results

    def _process_volumes(
        self, 
        schema_name: str, 
        volumes: List[str], 
        catalog_name: str, 
        start_time: datetime
    ) -> List[RunResult]:
        """
        Process all volumes in a schema using ThreadPoolExecutor.

        Args:
            schema_name: Schema name to process
            volumes: List of volume names to process
            catalog_name: Catalog name for error handling
            start_time: Operation start time for error handling

        Returns:
            List of RunResult objects for each volume operation
        """
        results: List[RunResult] = []
        
        self.logger.info(
            f"starting {self.get_operation_name()} of {len(volumes)} volumes in schema {self.catalog_name}.{schema_name} using {self.max_workers} workers"
        )
        self.logger.info(f"Volumes: {volumes}")

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit volume processing jobs
            future_to_volume = {
                executor.submit(
                    self.process_volume, schema_name, volume_name
                ): volume_name
                for volume_name in volumes
            }

            # Collect volume results
            for future in as_completed(future_to_volume):
                volume_name = future_to_volume[future]
                try:
                    result = future.result(timeout=self.timeout_seconds)

                    # Handle case where process_volume returns a list of results
                    if isinstance(result, list):
                        results.extend(result)
                        # Check if any result in the list failed
                        for single_result in result:
                            if single_result.status != "success":
                                self.logger.error(
                                    f"Failed to process volume "
                                    f"{catalog_name}.{schema_name}.{volume_name}: "
                                    f"{single_result.error_message}"
                                )
                    else:
                        results.append(result)
                        if result.status != "success":
                            self.logger.error(
                                f"Failed to process volume "
                                f"{catalog_name}.{schema_name}.{volume_name}: "
                                f"{result.error_message}"
                            )

                except Exception as e:
                    result = self._handle_exception(
                        e,
                        "processing volume",
                        catalog_name,
                        schema_name,
                        volume_name,
                        "volume",
                        start_time,
                    )
                    results.append(result)

        self.logger.info(
            f"All volumes processed in schema {self.catalog_name}.{schema_name}"
        )
        
        return results

    def _create_failed_result(
        self,
        catalog_name: str,
        schema_name: str,
        object_name: Optional[str] = None,
        object_type: Optional[str] = None,
        error_msg: str = "",
        start_time: Optional[datetime] = None,
    ) -> RunResult:
        """
        Create a failed RunResult object with consistent structure.

        Args:
            catalog_name: Catalog name
            schema_name: Schema name
            object_name: Optional object name
            object_type: Optional object type
            error_msg: Error message
            start_time: Operation start time

        Returns:
            RunResult object with failed status
        """
        if start_time is None:
            start_time = datetime.now(timezone.utc)

        return RunResult(
            operation_type=self.get_operation_name(),
            catalog_name=catalog_name,
            schema_name=schema_name,
            object_name=object_name,
            object_type=object_type,
            status="failed",
            start_time=start_time.isoformat(),
            end_time=datetime.now(timezone.utc).isoformat(),
            error_message=error_msg,
        )

    def _get_schemas(self) -> List[Tuple[str, List[TableConfig], List[VolumeConfig]]]:
        """
        Get list of schemas to process based on configuration.

        Returns:
            List of tuples containing schema names, table lists, and volume lists to process
        """

        exclude_schema_names = set()
        # Apply exclude_schemas filter if configured
        if self.catalog_config.exclude_schemas:
            exclude_schema_names = {
                schema.schema_name for schema in self.catalog_config.exclude_schemas
            }

        if self.catalog_config.target_schemas:
            # Use explicitly configured schemas
            target_schemas = [
                (
                    schema.schema_name,
                    schema.tables or [],
                    schema.volumes or [],
                )
                for schema in self.catalog_config.target_schemas
                if self.db_ops.refresh_schema_metadata(
                    f"{self.catalog_name}.{schema.schema_name}"
                )
                and self.spark.catalog.databaseExists(
                    f"{self.catalog_name}.{schema.schema_name}"
                )
            ]

            target_schemas = [
                (schema_name, tables, volumes)
                for schema_name, tables, volumes in target_schemas
                if schema_name not in exclude_schema_names
            ]

            return target_schemas

        if self.catalog_config.schema_filter_expression:
            # Use schema filter expression
            schema_list = self.db_ops.get_schemas_by_filter(
                self.catalog_name,
                self.catalog_config.schema_filter_expression,
            )
        else:
            # Process all schemas
            schema_list = self.db_ops.get_all_schemas(self.catalog_name)

        # Apply exclude_schemas filter if configured
        schema_list = [
            schema for schema in schema_list if schema not in exclude_schema_names
        ]

        return [(item, [], []) for item in schema_list]

    def _get_tables(self, schema_name: str, table_list: List[TableConfig]) -> List[str]:
        """
        Get list of tables to process in a schema based on configuration.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_list: List of table configurations to process in the schema

        Returns:
            List of table names to process
        """
        # Find exclusions for this schema from configuration
        exclude_names = set()
        if self.catalog_config.target_schemas:
            for schema_config in self.catalog_config.target_schemas:
                if schema_config.schema_name == schema_name:
                    if schema_config.exclude_tables:
                        exclude_names = {
                            table.table_name for table in schema_config.exclude_tables
                        }
                    break

        if table_list:
            # Use explicitly configured tables
            tables = [item.table_name for item in table_list]
        else:
            # Process all tables in the schema
            tables = self.db_ops.get_tables_in_schema(self.catalog_name, schema_name)

        # Apply exclusions first
        tables = [table for table in tables if table not in exclude_names]

        # Then filter by table types
        return self.db_ops.filter_tables_by_type(
            self.catalog_name,
            schema_name,
            tables,
            self.catalog_config.table_types,
        )

    def _get_volumes(
        self, schema_name: str, volume_list: List[VolumeConfig]
    ) -> List[str]:
        """
        Get list of volumes to process in a schema based on configuration.

        Args:
            schema_name: Name of the schema
            volume_list: List of volume configurations to process in the schema

        Returns:
            List of volume names to process
        """
        # Find exclusions for this schema from configuration
        exclude_names = set()
        if self.catalog_config.target_schemas:
            for schema_config in self.catalog_config.target_schemas:
                if schema_config.schema_name == schema_name:
                    if schema_config.exclude_volumes:
                        exclude_names = {
                            volume.volume_name
                            for volume in schema_config.exclude_volumes
                        }
                    break

        if volume_list:
            # Use explicitly configured volumes
            volumes = [item.volume_name for item in volume_list]
        else:
            # Process all volumes in the schema
            volumes = self.db_ops.get_volumes_in_schema(self.catalog_name, schema_name)

        # Apply exclusions first
        volumes = [volume for volume in volumes if volume not in exclude_names]

        # Then filter by volume types
        return self.db_ops.filter_volumes_by_type(
            self.catalog_name,
            schema_name,
            volumes,
            self.catalog_config.volume_types,
        )

    def _log_schema_results(self, schema_results: List[RunResult], schema_name: str) -> None:
        """
        Log results after processing each schema to audit table.
        
        Args:
            schema_results: List of RunResult objects from schema processing
            schema_name: Name of the processed schema
        """
        if not schema_results:
            return
            
        # Log each individual result to the audit table if audit logger is available
        if self.audit_logger:
            import json
            for result in schema_results:
                try:
                    details_str = None
                    if result.details:
                        details_str = json.dumps(result.details)

                    # Parse string timestamps back to datetime objects
                    start_dt = datetime.fromisoformat(result.start_time.replace("Z", "+00:00"))
                    end_dt = datetime.fromisoformat(result.end_time.replace("Z", "+00:00"))
                    duration = (end_dt - start_dt).total_seconds()

                    self.audit_logger.log_operation(
                        operation_type=result.operation_type,
                        catalog_name=result.catalog_name,
                        schema_name=result.schema_name or "",
                        object_name=result.object_name or "",
                        object_type=result.object_type or "",
                        status=result.status,
                        start_time=start_dt,
                        end_time=end_dt,
                        duration_seconds=duration,
                        error_message=result.error_message,
                        details=details_str,
                        attempt_number=getattr(result, "attempt_number", 1),
                        max_attempts=getattr(result, "max_attempts", 1),
                    )
                except Exception as audit_error:
                    table_info = f"{result.catalog_name}.{result.schema_name}.{result.object_name}"
                    self.logger.warning(
                        f"Failed to write audit log for {table_info}: {str(audit_error)}"
                    )
        
        # Log summary info to regular logger
        successful = sum(1 for r in schema_results if r.status == "success")
        total = len(schema_results)
        
        self.logger.info(
            f"Completed {self.get_operation_name()} for schema {self.catalog_name}.{schema_name}: "
            f"{successful}/{total} operations successful"
        )

    def _create_tagging_operation(self):
        """Create a tagging operation function with retry and logging."""

        @retry_with_logging(self.retry, self.logger)
        def tagging_operation(
            unset_query: str,
            set_query: str,
            target_spark: DatabricksSession,
        ):
            if unset_query:
                self.logger.debug(
                    f"Executing tag unset query: {unset_query}",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
                target_spark.sql(unset_query)
            if set_query:
                self.logger.debug(
                    f"Executing tag set query: {set_query}",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )
                target_spark.sql(set_query)
            return True

        return tagging_operation

    def _build_tag_sql(
        self,
        tag_names_list: list,
        tag_maps: dict,
        catalog_name: str,
        schema_name: str = None,
        table_name: str = None,
        column_name: str = None,
        volume_name: str = None,
    ) -> tuple:
        """Build unset and set SQL for tags."""
        unset_sql = None
        set_sql = None

        # Build unset SQL if tags exist
        if tag_names_list:
            tag_names_str = ",".join([f"'{tag}'" for tag in tag_names_list])
            if column_name:
                unset_sql = (
                    f"ALTER TABLE `{catalog_name}`.`{schema_name}`.`{table_name}` "
                    f"ALTER COLUMN `{column_name}` UNSET TAGS ( {tag_names_str} )"
                )
            elif table_name:
                unset_sql = (
                    f"ALTER TABLE `{catalog_name}`.`{schema_name}`.`{table_name}` "
                    f"UNSET TAGS ( {tag_names_str} )"
                )
            elif volume_name:
                unset_sql = (
                    f"ALTER VOLUME `{catalog_name}`.`{schema_name}`.`{volume_name}` "
                    f"UNSET TAGS ( {tag_names_str} )"
                )
            elif schema_name:
                unset_sql = (
                    f"ALTER SCHEMA `{catalog_name}`.`{schema_name}` "
                    f"UNSET TAGS ( {tag_names_str} )"
                )
            else:
                unset_sql = (
                    f"ALTER CATALOG `{catalog_name}` UNSET TAGS ( {tag_names_str} )"
                )

        # Build set SQL if there are tags to apply
        if tag_maps:
            tag_maps_str = ",".join(
                [f"'{key}' = '{value}'" for key, value in tag_maps.items()]
            )
            if column_name:
                set_sql = (
                    f"ALTER TABLE `{catalog_name}`.`{schema_name}`.`{table_name}` "
                    f"ALTER COLUMN `{column_name}` SET TAGS ( {tag_maps_str} )"
                )
            elif table_name:
                set_sql = (
                    f"ALTER TABLE `{catalog_name}`.`{schema_name}`.`{table_name}` "
                    f"SET TAGS ( {tag_maps_str} )"
                )
            elif volume_name:
                set_sql = (
                    f"ALTER VOLUME `{catalog_name}`.`{schema_name}`.`{volume_name}` "
                    f"SET TAGS ( {tag_maps_str} )"
                )
            elif schema_name:
                set_sql = (
                    f"ALTER SCHEMA `{catalog_name}`.`{schema_name}` "
                    f"SET TAGS ( {tag_maps_str} )"
                )
            else:
                set_sql = f"ALTER CATALOG `{catalog_name}` SET TAGS ( {tag_maps_str} )"

        return unset_sql, set_sql

    def _replicate_tags(
        self,
        object_type: str,
        source_tag_maps_list: list,
        target_tag_names_list: list,
        target_tag_maps_list: list,
        overwrite_tags: bool,
        source_catalog: str,
        target_catalog: str,
        schema_name: str = None,
        table_name: str = None,
        column_name: str = None,
        volume_name: str = None,
    ) -> RunResult:
        """
        Execute tag replication operation for a single table or column.

        Returns:
            RunResult object for the tag replication operation
        """
        try:
            start_time = datetime.now(timezone.utc)
            object_name = target_catalog
            source_object = f"`{source_catalog}`"
            target_object = f"`{target_catalog}`"
            if schema_name:
                source_object += f".`{schema_name}`"
                target_object += f".`{schema_name}`"
                object_name = schema_name
                if table_name:
                    source_object += f".`{table_name}`"
                    target_object += f".`{table_name}`"
                    object_name = table_name
                    if column_name:
                        source_object += f".`{column_name}`"
                        target_object += f".`{column_name}`"
                if volume_name:
                    source_object += f".`{volume_name}`"
                    target_object += f".`{volume_name}`"
                    object_name = volume_name

            self.logger.info(
                f"Starting {object_type} replication: {source_object} -> {target_object}",
                extra={"run_id": self.run_id, "operation": "replication"},
            )

            attempt = 1
            max_attempts = self.retry.max_attempts
            last_exception = None
            # Create tagging operation with retry and logging
            tagging_operation = self._create_tagging_operation()

            # Merge tags using helper method
            merged_tag_maps = merge_maps(
                source_tag_maps_list, target_tag_maps_list, overwrite_tags
            )

            # Build SQL using helper method
            target_tag_names_to_unset = target_tag_names_list if overwrite_tags else []
            unset_sql, set_sql = self._build_tag_sql(
                target_tag_names_to_unset,
                merged_tag_maps,
                target_catalog,
                schema_name,
                table_name,
                column_name,
                volume_name,
            )

            # Execute tagging operation
            result, last_exception, attempt, max_attempts = tagging_operation(
                unset_sql, set_sql, self.target_spark
            )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if result:
                self.logger.info(
                    f"{object_type} replication completed successfully: {source_object} -> {target_object} "
                    f"({duration:.2f}s)",
                    extra={"run_id": self.run_id, "operation": "replication"},
                )

                return RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=object_name,
                    object_type=object_type,
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    details={
                        "source_object": source_object,
                        "target_object": target_object,
                        "overwrite_tags": overwrite_tags,
                        "tags_applied": len(merged_tag_maps) if merged_tag_maps else 0,
                        "unset_sql": unset_sql,
                        "set_sql": set_sql,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )

        except Exception as e:
            last_exception = e

        # Handle failure case
        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        error_msg = (
            f"{object_type} replication failed after {max_attempts} attempts: "
            f"{source_object} -> {target_object}"
        )
        if last_exception:
            error_msg += f" | Last error: {str(last_exception)}"

        self.logger.error(
            error_msg,
            extra={"run_id": self.run_id, "operation": "replication"},
        )

        return RunResult(
            operation_type="uc_replication",
            catalog_name=target_catalog,
            schema_name=schema_name,
            object_name=object_name,
            object_type=object_type,
            status="failed",
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration_seconds=duration,
            error_message=str(last_exception) if last_exception else "Unknown error",
            details={
                "source_object": source_object,
                "target_object": target_object,
                "overwrite_tags": overwrite_tags,
            },
            attempt_number=attempt,
            max_attempts=max_attempts,
        )

    def _replicate_catalog_tags(
        self,
    ) -> list[RunResult]:
        """
        Replicate catalog tags from source to target catalog.

        Returns:
            RunResult object for the tag replication operation
        """
        run_results = []
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        object_type = "catalog_tag"
        attempt = 1
        max_attempts = self.retry.max_attempts

        if not self.source_dbops.if_catalog_exists(
            source_catalog
        ) or not self.target_dbops.if_catalog_exists(target_catalog):
            self.logger.warning(
                f"Source or target catalog {target_catalog} does not exist. Skipping catalog tag replication."
            )
            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name="",
                    object_name="",
                    object_type=object_type,
                    status="failed",
                    start_time=datetime.now(timezone.utc).isoformat(),
                    end_time=datetime.now(timezone.utc).isoformat(),
                    duration_seconds=0.0,
                    error_message=f"Source or target catalog {target_catalog} does not exist.",
                    details={},
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            )
            return run_results

        # Get target and source schema tags
        target_tag_names, target_tag_maps = self.target_dbops.get_catalog_tags(
            target_catalog
        )
        _, source_tag_maps = self.source_dbops.get_catalog_tags(source_catalog)

        # Execute tag replication using helper method
        run_result = self._replicate_tags(
            object_type=object_type,
            source_tag_maps_list=source_tag_maps,
            target_tag_names_list=target_tag_names,
            target_tag_maps_list=target_tag_maps,
            overwrite_tags=replication_config.overwrite_tags,
            source_catalog=source_catalog,
            target_catalog=target_catalog,
        )
        run_results.append(run_result)
        return run_results

    def _replicate_schema_tags(
        self,
        schema_name: str,
    ) -> list[RunResult]:
        """
        Replicate schema tags from source to target schema.

        Args:
            schema_name: Schema name
        Returns:
            RunResult object for the tag replication operation
        """

        run_results = []

        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        object_type = "schema_tag"
        attempt = 1
        max_attempts = self.retry.max_attempts

        if not self.source_dbops.if_schema_exists(
            source_catalog, schema_name
        ) or not self.target_dbops.if_schema_exists(target_catalog, schema_name):
            self.logger.warning(
                f"Source or target schema {target_catalog}.{schema_name} does not exist. Skipping schema tag replication."
            )
            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name="",
                    object_type=object_type,
                    status="failed",
                    start_time=datetime.now(timezone.utc).isoformat(),
                    end_time=datetime.now(timezone.utc).isoformat(),
                    duration_seconds=0.0,
                    error_message=f"Source or target schema {target_catalog}.{schema_name} does not exist.",
                    details={},
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            )
            return run_results

        # Get target and source schema tags
        target_tag_names, target_tag_maps = self.target_dbops.get_schema_tags(
            target_catalog, schema_name
        )
        _, source_tag_maps = self.source_dbops.get_schema_tags(
            source_catalog, schema_name
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
        )

        run_results.append(run_result)
        return run_results
