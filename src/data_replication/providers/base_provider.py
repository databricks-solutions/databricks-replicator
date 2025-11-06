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
from pyspark.sql.utils import AnalysisException

from ..audit.logger import DataReplicationLogger
from ..config.models import (
    RetryConfig,
    RunResult,
    TableConfig,
    VolumeConfig,
    TargetCatalogConfig,
    DatabricksConnectConfig,
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
        run_id: str,
        catalog_config: TargetCatalogConfig,
        source_databricks_config: DatabricksConnectConfig,
        target_databricks_config: DatabricksConnectConfig,
        retry: Optional[RetryConfig] = None,
        max_workers: int = 2,
        timeout_seconds: int = 1800,
        external_location_mapping: Optional[dict] = None,
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
        self.catalog_name: Optional[str] = None

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
                schema_results = self.process_schema_concurrently(
                    schema_name, table_list, volume_list
                )
                results.extend(schema_results)

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
            if self.catalog_config.table_types:
                tables = self._get_tables(schema_name, table_list)
                if len(tables) == 0:
                    self.logger.info(
                        f"No tables found in schema {self.catalog_name}.{schema_name}"
                    )
            if self.catalog_config.volume_types:
                volumes = self._get_volumes(schema_name, volume_list)
                if len(volumes) == 0:
                    self.logger.info(
                        f"No volumes found in schema {self.catalog_name}.{schema_name}"
                    )

            total_objects = len(tables) + len(volumes)
            if total_objects == 0:
                self.logger.info(
                    f"No objects found in schema {self.catalog_name}.{schema_name}"
                )
                return results

            # Process all tables first
            if tables:
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
                            results.append(result)

                            if result.status == "success":
                                self.logger.info(
                                    f"Successfully processed table "
                                    f"{catalog_name}.{schema_name}.{table_name}"
                                )
                            else:
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

            # Process all volumes after tables are complete
            if volumes:
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
                            results.append(result)

                            if result.status == "success":
                                self.logger.info(
                                    f"Successfully processed volume "
                                    f"{catalog_name}.{schema_name}.{volume_name}"
                                )
                            else:
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

        if self.catalog_config.target_schemas:
            # Use explicitly configured schemas
            return [
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

        if self.catalog_config.schema_filter_expression:
            # Use schema filter expression
            schema_list = self.db_ops.get_schemas_by_filter(
                self.catalog_name,
                self.catalog_config.schema_filter_expression,
            )
        else:
            # Process all schemas
            schema_list = self.db_ops.get_all_schemas(self.catalog_name)

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
        # Return empty list if no volume types are configured
        if not self.catalog_config.volume_types:
            return []

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
