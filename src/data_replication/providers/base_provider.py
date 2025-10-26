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

            for schema_name, table_list in schema_list:
                self.logger.info(
                    f"Starting {self.get_operation_name()} schema: {schema_name}",
                    extra={
                        "run_id": self.run_id,
                        "operation": self.get_operation_name(),
                    },
                )
                schema_results = self.process_schema_concurrently(
                    schema_name, table_list
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
        self, schema_name: str, table_list: List[TableConfig]
    ) -> List[RunResult]:
        """
        Process all tables in a schema concurrently using ThreadPoolExecutor.

        Args:
            schema_name: Schema name to process
            table_list: List of table configurations to process

        Returns:
            List of RunResult objects for each table operation
        """
        results: List[RunResult] = []
        catalog_name = self.catalog_config.catalog_name
        start_time = datetime.now(timezone.utc)

        try:
            # Get all tables in the schema
            tables = self._get_tables(schema_name, table_list)

            if not tables:
                self.logger.info(
                    f"No tables found in schema {self.catalog_name}.{schema_name}"
                )
                return results

            self.logger.info(
                f"Starting concurrent {self.get_operation_name()} of {len(tables)} tables "
                f"in schema {self.catalog_name}.{schema_name} using {self.max_workers} workers"
            )

            self.logger.info(f"Tables {tables}")

            # Process tables concurrently within the schema
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                # Submit jobs for all tables in the schema
                future_to_table = {
                    executor.submit(
                        self.process_table, schema_name, table_name
                    ): table_name
                    for table_name in tables
                }

                # Collect results
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

    def _get_schemas(self) -> List[Tuple[str, List[TableConfig]]]:
        """
        Get list of schemas to process based on configuration.

        Args:
            catalog_name: Optional catalog name to override config catalog

        Returns:
            List of tuples containing schema names and table lists to process
        """

        if self.catalog_config.target_schemas:
            # Use explicitly configured schemas
            return [
                (
                    schema.schema_name,
                    schema.tables or [],
                )
                for schema in self.catalog_config.target_schemas
                if self.db_ops.refresh_schema_metadata(f"{self.catalog_name}.{schema.schema_name}")
                and self.spark.catalog.databaseExists(f"{self.catalog_name}.{schema.schema_name}")
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

        return [(item, []) for item in schema_list]

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
