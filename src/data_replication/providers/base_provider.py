"""
Base provider class for data replication operations.

This module provides shared functionality that can be reused across different
provider types like backup, replication, and reconciliation.
"""

from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FuturesTimeoutError
from concurrent.futures import as_completed
from copy import deepcopy
from datetime import datetime, timezone
from typing import List, Optional
import random

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import EnablePredictiveOptimization

from pyspark.sql.utils import AnalysisException

from data_replication.utils import (
    filter_common_maps,
    merge_models_recursive,
    recursive_substitute,
    retry_with_logging,
    merge_maps,
    map_cloud_url,
)

from ..audit.audit_logger import AuditLogger
from ..audit.logger import DataReplicationLogger
from ..config.models import (
    DatabricksConnectConfig,
    RetryConfig,
    RunResult,
    SchemaConfig,
    TableConfig,
    TableType,
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
from ..constants import (
    DICT_FOR_CREATION_CATALOG,
    DICT_FOR_CREATION_SCHEMA,
    DICT_FOR_UPDATE_CATALOG,
    DICT_FOR_UPDATE_SCHEMA,
    SKIP_PROCESSED_TABLES,
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
        cloud_url_mapping: Optional[dict] = None,
        audit_logger: Optional[AuditLogger] = None,
        completed_run_results: Optional[List[RunResult]] = None,
    ):
        """
        Initialize the base provider.

        Args:
            spark: Spark session for databricks workspace
            logger: Logger instance for audit logging
            db_ops: Databricks operations helper
            run_id: Unique run identifier
            catalog_config: Target catalog configuration
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
        self.max_workers = self.catalog_config.concurrency.max_workers
        self.timeout_seconds = self.catalog_config.concurrency.timeout_seconds
        self.cloud_url_mapping = cloud_url_mapping
        self.audit_logger = audit_logger
        self.catalog_name: Optional[str] = None
        self.source_spark = None
        self.source_dbops = None
        self.target_spark = None
        self.target_dbops = None
        self.source_workspace_client = None
        self.target_workspace_client = None
        self.processed_objects: List[str] = []
        self.completed_objects: List[str] = [
            f"{result.catalog_name}.{result.schema_name}.{result.object_name}"
            for result in completed_run_results or []
        ]
        self.shared_tables: List[str] = []

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
    ) -> List[RunResult]:
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
            self.logger.error(error_msg)
        else:
            error_msg = f"Unexpected error {context} {catalog_name}.{schema_name}.{object_name}: {str(e)}"
            self.logger.error(error_msg)

        return self._create_failed_result(
            catalog_name, schema_name, object_name, object_type, error_msg, start_time
        )

    @abstractmethod
    def process_table(
        self, schema_config: SchemaConfig, table_config: TableConfig
    ) -> List[RunResult]:
        """
        Process a single table.
        Must be implemented by subclasses.

        Args:
            schema_config: Schema configuration
            table_config: Table configuration

        Returns:
            RunResult object for the operation
        """

    def process_volume(
        self, schema_config: SchemaConfig, volume_config: VolumeConfig
    ) -> List[RunResult]:
        """
        Process a single volume.
        Default implementation returns a success result.
        Override in subclasses that need volume processing.

        Args:
            schema_config: SchemaConfig object for the schema
            volume_config: Volume configuration

        Returns:
            RunResult object for the operation
        """
        return None

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
        schema_table_list = []
        schema_volume_list = []

        try:
            # Setup operation-specific catalogs (implemented by subclasses)
            self.catalog_name = self.setup_operation_catalogs()
            uc_object_types_catalog_processed = (
                self.catalog_config.uc_object_types.copy()
                if self.catalog_config.uc_object_types
                else []
            )
            self.logger.info(
                f"Starting {self.get_operation_name()} catalog: {self.catalog_name}",
                extra={
                    "run_id": self.run_id,
                    "operation": self.get_operation_name(),
                },
            )

            # Replicate catalog if configured
            if self.catalog_config.uc_object_types and (
                UCObjectType.CATALOG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                run_result = self._uc_replicate_catalog()
                results.extend(run_result)
                catalog_run_result = []
                if run_result:
                    catalog_run_result.extend(run_result)
                    self.audit_logger.log_results(catalog_run_result)
                if UCObjectType.ALL not in self.catalog_config.uc_object_types:
                    uc_object_types_catalog_processed.remove(UCObjectType.CATALOG)
                # immediately return if no other object types to process
                if len(uc_object_types_catalog_processed) == 0:
                    return results

            # Replicate catalog tags if configured
            if self.catalog_config.uc_object_types and (
                UCObjectType.CATALOG_TAG in self.catalog_config.uc_object_types
                or UCObjectType.ALL in self.catalog_config.uc_object_types
            ):
                run_result = self._uc_replicate_catalog_tags()
                results.extend(run_result)
                catalog_run_result = []
                if run_result:
                    catalog_run_result.extend(run_result)
                    self.audit_logger.log_results(catalog_run_result)
                if UCObjectType.ALL not in self.catalog_config.uc_object_types:
                    uc_object_types_catalog_processed.remove(UCObjectType.CATALOG_TAG)
                # immediately return if no other object types to process
                if len(uc_object_types_catalog_processed) == 0:
                    return results

            # Handle schema-table filter expression if configured or sampling is enabled
            if self.catalog_config.schema_table_filter_expression or (
                self.catalog_config.reconciliation_config
                and self.catalog_config.reconciliation_config.enable_sampling
                and self.get_operation_name() == "reconciliation"
            ):
                # construct schema_table_filter_expression from config
                schema_table_filter_expression = (
                    self._build_schema_table_filter_expression()
                )
                self.logger.debug(
                    f"Constructed schema_table_filter_expression: {schema_table_filter_expression}"
                )
                # Get schema-table combinations to process based on filter expression
                schema_table_combinations = self._get_schema_tables(
                    schema_table_filter_expression=schema_table_filter_expression
                )
                # Apply sampling if enabled
                if (
                    self.catalog_config.reconciliation_config
                    and self.catalog_config.reconciliation_config.enable_sampling
                ):
                    total_tables = len(schema_table_combinations)
                    sample_size = min(
                        self.catalog_config.reconciliation_config.no_of_sampling_tables,
                        total_tables,
                    )
                    self.logger.info(
                        f"Sampling {sample_size} tables out of {total_tables} total tables for reconciliation"
                    )
                    schema_table_combinations = random.sample(
                        schema_table_combinations, sample_size
                    )
                # Build schema-level configs from schema-table combinations
                # do not overwrite existing target_schemas config if already provided
                schema_configs = {}
                schema_configs_update = {}
                for item in schema_table_combinations:
                    schema_name = item["schema_name"]
                    table_name = item["table_name"]
                    if schema_name not in schema_configs:
                        if getattr(self.catalog_config, "target_schemas", None):
                            schema_config = next(
                                (
                                    schema
                                    for schema in self.catalog_config.target_schemas
                                    if schema.schema_name == schema_name
                                ),
                                SchemaConfig(schema_name=schema_name, tables=[]),
                            )
                        else:
                            schema_config = SchemaConfig(
                                schema_name=schema_name, tables=[]
                            )
                        schema_configs[schema_name] = schema_config
                        schema_configs_update[schema_name] = schema_config.model_copy()
                        tables = []
                        schema_configs_update[schema_name].tables = tables
                    if getattr(schema_configs[schema_name], "tables", None):
                        table_config = next(
                            (
                                table
                                for table in schema_configs[schema_name].tables
                                if table.table_name == table_name
                            ),
                            TableConfig(table_name=table_name),
                        )
                    else:
                        table_config = TableConfig(table_name=table_name)
                    schema_configs_update[schema_name].tables.append(table_config)

                # Update the catalog config target_schemas with the filtered schemas and tables
                self.catalog_config.target_schemas = list(
                    schema_configs_update.values()
                )

            # Get schemas to process
            schema_configs = self._get_schemas()
            schema_list = [schema.schema_name for schema in schema_configs]
            self.logger.info(
                f"Starting {self.get_operation_name()} schemas: {schema_list}"
            )
            # Merge schema-level configs into catalog-level config.
            if schema_configs:
                for i, schema_config in enumerate(schema_configs):
                    schema_configs[i] = merge_models_recursive(
                        deepcopy(self.catalog_config), schema_config
                    )

            for schema_config in schema_configs:
                self.logger.info(
                    f"Starting {self.get_operation_name()} schema: {schema_config.schema_name}",
                    extra={
                        "run_id": self.run_id,
                        "operation": self.get_operation_name(),
                    },
                )
                # Substitute schema name in schema config
                schema_config = recursive_substitute(
                    schema_config, schema_config.schema_name, "{{schema_name}}"
                )

                uc_object_types_schema_processed = (
                    uc_object_types_catalog_processed.copy()
                )

                # Replicate schema if configured
                if self.catalog_config.uc_object_types and (
                    UCObjectType.SCHEMA in self.catalog_config.uc_object_types
                    or UCObjectType.ALL in self.catalog_config.uc_object_types
                ):
                    run_result = self._uc_replicate_schema(schema_config)
                    results.extend(run_result)
                    schema_run_result = []
                    if run_result:
                        schema_run_result.extend(run_result)
                        self.audit_logger.log_results(schema_run_result)
                    if UCObjectType.ALL not in self.catalog_config.uc_object_types:
                        uc_object_types_schema_processed.remove(UCObjectType.SCHEMA)
                    # continue to next schema if no other object types to process
                    if len(uc_object_types_schema_processed) == 0:
                        continue

                # Replicate schema tags if configured
                if self.catalog_config.uc_object_types and (
                    UCObjectType.SCHEMA_TAG in self.catalog_config.uc_object_types
                    or UCObjectType.ALL in self.catalog_config.uc_object_types
                ):
                    run_result = self._uc_replicate_schema_tags(schema_config)
                    results.extend(run_result)
                    schema_run_result = []
                    if run_result:
                        schema_run_result.extend(run_result)
                        self.audit_logger.log_results(schema_run_result)
                    if UCObjectType.ALL not in self.catalog_config.uc_object_types:
                        uc_object_types_schema_processed.remove(UCObjectType.SCHEMA_TAG)
                    # continue to next schema if no other object types to process
                    if len(uc_object_types_schema_processed) == 0:
                        continue

                # Process all tables and volumes in the schema
                schema_results = []
                schema_tables = []
                schema_volumes = []
                schema_results, schema_tables, schema_volumes = self.process_schema(
                    schema_config
                )
                results.extend(schema_results)
                schema_table_list.extend(schema_tables)
                schema_volume_list.extend(schema_volumes)

                if self.catalog_config.concurrency.process_schemas_in_serial:
                    if schema_results:
                        # Log summary info to regular logger
                        successful = sum(
                            1
                            for r in schema_results
                            if schema_results and r.status == "success"
                        )
                        skipped = sum(
                            1
                            for r in schema_results
                            if schema_results and r.status == "skipped"
                        )
                        failed = sum(
                            1
                            for r in schema_results
                            if schema_results and r.status == "failed"
                        )
                        total = len(schema_results) if schema_results else 0

                        self.logger.info(
                            f"Completed {self.get_operation_name()} for schema {self.catalog_name}.{schema_config.schema_name}: "
                            f"{successful}/{total} operations successful, {skipped}/{total} operations skipped, {failed}/{total} operations failed"
                        )

            if not self.catalog_config.concurrency.process_schemas_in_serial:
                catalog_run_result = []
                if schema_table_list:
                    catalog_run_result = self._process_schema_tables(
                        schema_table_list, self.catalog_name, start_time
                    )
                    if catalog_run_result:
                        results.extend(catalog_run_result)
                        # Log summary info to regular logger
                        successful = sum(
                            1
                            for r in catalog_run_result
                            if catalog_run_result and r.status == "success"
                        )
                        skipped = sum(
                            1
                            for r in catalog_run_result
                            if catalog_run_result and r.status == "skipped"
                        )
                        failed = sum(
                            1
                            for r in catalog_run_result
                            if catalog_run_result and r.status == "failed"
                        )
                        total = len(catalog_run_result) if catalog_run_result else 0

                        self.logger.info(
                            f"Completed table {self.get_operation_name()} for catalog {self.catalog_name}: "
                            f"{successful}/{total} operations successful, {skipped}/{total} operations skipped, {failed}/{total} operations failed"
                        )

                if schema_volume_list:
                    catalog_run_result = self._process_schema_volumes(
                        schema_volume_list, self.catalog_name, start_time
                    )
                    if catalog_run_result:
                        results.extend(catalog_run_result)
                        # Log summary info to regular logger
                        successful = sum(
                            1
                            for r in catalog_run_result
                            if catalog_run_result and r.status == "success"
                        )
                        total = len(catalog_run_result) if catalog_run_result else 0

                        self.logger.info(
                            f"Completed volume {self.get_operation_name()} for catalog {self.catalog_name}: "
                            f"{successful}/{total} operations successful"
                        )

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

    def process_schema(
        self,
        schema_config: SchemaConfig,
    ):
        """
        Process all tables first, then volumes in a schema using ThreadPoolExecutor.

        Tables are processed concurrently first, and once all tables are complete,
        volumes are processed concurrently.

        Args:
            schema_config: Schema configuration to process
        Returns:
            List of RunResult objects for each table and volume operation
        """
        results: List[RunResult] = []
        catalog_name = self.catalog_config.catalog_name
        start_time = datetime.now(timezone.utc)
        self.logger.debug(f"Processing with config: {self.catalog_config}")
        schema_table_list = []
        schema_volume_list = []
        try:
            total_objects = 0
            table_configs = []
            volume_configs = []
            # Get all tables and volumes in the schema
            if (
                self.catalog_config.uc_object_types
                and (
                    any(
                        t in self.catalog_config.uc_object_types
                        for t in [
                            UCObjectType.TABLE_TAG,
                            UCObjectType.ALL,
                            UCObjectType.COLUMN_TAG,
                            UCObjectType.COLUMN_COMMENT,
                            UCObjectType.VIEW,
                            UCObjectType.TABLE,
                            UCObjectType.TABLE_COMMENT,
                            UCObjectType.MATERIALIZED_VIEW,
                            UCObjectType.STREAMING_TABLE,
                        ]
                    )
                )
            ) or self.catalog_config.table_types:
                table_configs = self._get_tables(schema_config, schema_config.tables)
            if (
                self.catalog_config.uc_object_types
                and (
                    any(
                        t in self.catalog_config.uc_object_types
                        for t in [
                            UCObjectType.VOLUME_TAG,
                            UCObjectType.VOLUME,
                            UCObjectType.ALL,
                        ]
                    )
                )
            ) or self.catalog_config.volume_types:
                volume_configs = self._get_volumes(schema_config, schema_config.volumes)

            total_objects = len(table_configs) + len(volume_configs)
            if total_objects == 0:
                self.logger.info(
                    f"No objects found in schema {self.catalog_name}.{schema_config.schema_name}. Skipping."
                )
                return results, schema_table_list, schema_volume_list

            # Process all tables first
            if table_configs:
                # Merge table-level configs into schema-level config.
                for i, table_config in enumerate(table_configs):
                    table_configs[i] = merge_models_recursive(
                        deepcopy(schema_config), table_config
                    )
                    schema_table_list.append(
                        {
                            "schema_config": schema_config,
                            "table_config": table_configs[i],
                        }
                    )

                if self.catalog_config.concurrency.process_schemas_in_serial:
                    table_results = self._process_tables(
                        schema_config, table_configs, catalog_name, start_time
                    )
                    results.extend(table_results)

            # Process all volumes after tables are complete
            if volume_configs:
                # Merge volume-level configs into schema-level config.
                for i, volume_config in enumerate(volume_configs):
                    volume_configs[i] = merge_models_recursive(
                        deepcopy(schema_config), volume_config
                    )
                    schema_volume_list.append(
                        {
                            "schema_config": schema_config,
                            "volume_config": volume_configs[i],
                        }
                    )
                if self.catalog_config.concurrency.process_schemas_in_serial:
                    volume_results = self._process_volumes(
                        schema_config, volume_configs, catalog_name, start_time
                    )
                    results.extend(volume_results)
        except Exception as e:
            result = self._handle_exception(
                e,
                "processing schema",
                catalog_name,
                schema_config.schema_name,
                "",
                "schema",
                start_time,
            )
            results.extend(result)
        return results, schema_table_list, schema_volume_list

    def _process_schema_tables(
        self,
        schema_table_list: List[dict],
        catalog_name: str,
        start_time: datetime,
    ) -> List[RunResult]:
        """
        Process all tables in a schema using ThreadPoolExecutor.

        Args:
            schema_table_list: List of dictionaries with 'schema_config' and 'table_config' keys
            catalog_name: Catalog name for error handling
            start_time: Operation start time for error handling
        Returns:
            List of RunResult objects for each table operation
        """
        results: List[RunResult] = []
        concurrency = self.catalog_config.concurrency
        self.logger.info(
            f"starting {self.get_operation_name()} of {len(schema_table_list)} tables in catalog {self.catalog_name} using {concurrency.max_workers} workers"
        )
        self.logger.debug(
            f"Tables: {[f'{item['schema_config'].schema_name}.{item['table_config'].table_name}' for item in schema_table_list]}"
        )

        with ThreadPoolExecutor(max_workers=concurrency.max_workers) as executor:
            # Submit table processing jobs
            future_to_table = {
                executor.submit(
                    self.process_table, item["schema_config"], item["table_config"]
                ): (item["schema_config"].schema_name, item["table_config"].table_name)
                for item in schema_table_list
            }

            # Collect table results
            for future in as_completed(future_to_table):
                schema_name, table_name = future_to_table[future]
                try:
                    result = []
                    result = future.result(timeout=self.timeout_seconds)

                    results.extend(result)
                    # Check if any result in the list failed
                    for single_result in result:
                        if single_result.status == "failed":
                            self.logger.error(
                                f"{single_result.status} to {single_result.operation_type} {single_result.object_type} "
                                f"{catalog_name}.{schema_name}.{table_name}: "
                                f"{single_result.error_message}"
                            )
                        if single_result.status == "skipped":
                            self.logger.warning(
                                f"{single_result.status} {single_result.operation_type} {single_result.object_type} "
                                f"{catalog_name}.{schema_name}.{table_name}: "
                                f"{single_result.error_message}"
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
                    results.extend(result)
                finally:
                    self.processed_objects.append(f"{schema_name}.{table_name}")

        executor.shutdown(wait=True)

        self.logger.info(f"All tables processed in catalog {self.catalog_name}")
        return results

    def _process_schema_volumes(
        self,
        schema_volume_list: List[dict],
        catalog_name: str,
        start_time: datetime,
    ) -> List[RunResult]:
        """
        Process all volumes in a schema using ThreadPoolExecutor.

        Args:
            schema_volume_list: List of dictionaries with 'schema_config' and 'volume_config' keys
            catalog_name: Catalog name for error handling
            start_time: Operation start time for error handling
        Returns:
            List of RunResult objects for each volume operation
        results: List[RunResult] = []
        """
        results: List[RunResult] = []
        concurrency = self.catalog_config.concurrency
        self.logger.info(
            f"starting {self.get_operation_name()} of {len(schema_volume_list)} volumes in catalog {self.catalog_name} using {concurrency.max_workers} workers"
        )
        self.logger.debug(
            f"Volumes: {[f'{item['schema_config'].schema_name}.{item['volume_config'].volume_name}' for item in schema_volume_list]}"
        )

        with ThreadPoolExecutor(max_workers=concurrency.max_workers) as executor:
            # Submit volume processing jobs
            future_to_volume = {
                executor.submit(
                    self.process_volume, item["schema_config"], item["volume_config"]
                ): (
                    item["schema_config"].schema_name,
                    item["volume_config"].volume_name,
                )
                for item in schema_volume_list
            }

            # Collect volume results
            for future in as_completed(future_to_volume):
                schema_name, volume_name = future_to_volume[future]
                try:
                    result = []
                    result = future.result(timeout=self.timeout_seconds)

                    results.extend(result)
                    # Check if any result in the list failed
                    for single_result in result:
                        if single_result.status != "success":
                            self.logger.error(
                                f"Failed to process volume "
                                f"{catalog_name}.{schema_name}.{volume_name}: "
                                f"{single_result.error_message}"
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
                    results.extend(result)
                finally:
                    self.processed_objects.append(f"{schema_name}.{volume_name}")

        executor.shutdown(wait=True)

        self.logger.info(f"All volumes processed in catalog {self.catalog_name}")
        return results

    def _process_tables(
        self,
        schema_config: SchemaConfig,
        table_configs: List[TableConfig],
        catalog_name: str,
        start_time: datetime,
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
        tables = [table_config.table_name for table_config in table_configs]
        schema_name = schema_config.schema_name
        concurrency = schema_config.concurrency
        self.logger.info(
            f"starting {self.get_operation_name()} of {len(tables)} tables in schema {self.catalog_name}.{schema_name} using {concurrency.max_workers} workers"
        )
        self.logger.info(f"Tables: {tables}")

        with ThreadPoolExecutor(max_workers=concurrency.max_workers) as executor:
            # Submit table processing jobs
            future_to_table = {
                executor.submit(
                    self.process_table, schema_config, table_config
                ): table_config.table_name
                for table_config in table_configs
            }

            # Collect table results
            for future in as_completed(future_to_table):
                table_name = future_to_table[future]
                try:
                    result = []
                    result = future.result(timeout=self.timeout_seconds)

                    results.extend(result)
                    # Check if any result in the list failed
                    for single_result in result:
                        if single_result.status == "failed":
                            self.logger.error(
                                f"{single_result.status} to {single_result.operation_type} {single_result.object_type} "
                                f"{catalog_name}.{schema_name}.{table_name}: "
                                f"{single_result.error_message}"
                            )
                        if single_result.status == "skipped":
                            self.logger.warning(
                                f"{single_result.status} {single_result.operation_type} {single_result.object_type} "
                                f"{catalog_name}.{schema_name}.{table_name}: "
                                f"{single_result.error_message}"
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
                    results.extend(result)
                finally:
                    self.processed_objects.append(f"{schema_name}.{table_name}")

        executor.shutdown(wait=True)

        self.logger.info(
            f"All tables processed in schema {self.catalog_name}.{schema_name}"
        )

        return results

    def _process_volumes(
        self,
        schema_config: SchemaConfig,
        volume_configs: List[VolumeConfig],
        catalog_name: str,
        start_time: datetime,
    ) -> List[RunResult]:
        """
        Process all volumes in a schema using ThreadPoolExecutor.

        Args:
            schema_name: Schema name to process
            volume_configs: List of volume configurations to process
            catalog_name: Catalog name for error handling
            start_time: Operation start time for error handling

        Returns:
            List of RunResult objects for each volume operation
        """
        results: List[RunResult] = []
        volumes = [volume_config.volume_name for volume_config in volume_configs]
        schema_name = schema_config.schema_name
        concurrency = schema_config.concurrency
        self.logger.info(
            f"starting {self.get_operation_name()} of {len(volumes)} volumes in schema {self.catalog_name}.{schema_name} using {concurrency.max_workers} workers"
        )
        self.logger.info(f"Volumes: {volumes}")

        with ThreadPoolExecutor(max_workers=concurrency.max_workers) as executor:
            # Submit volume processing jobs
            future_to_volume = {
                executor.submit(
                    self.process_volume, schema_config, volume_config
                ): volume_config.volume_name
                for volume_config in volume_configs
            }

            # Collect volume results
            for future in as_completed(future_to_volume):
                volume_name = future_to_volume[future]
                try:
                    result = []
                    result = future.result(timeout=self.timeout_seconds)
                    results.extend(result)
                    # Check if any result in the list failed
                    for single_result in result:
                        if single_result.status == "failed":
                            self.logger.error(
                                f"{single_result.status} to {single_result.operation_type} {single_result.object_type} "
                                f"{catalog_name}.{schema_name}.{volume_name}: "
                                f"{single_result.error_message}"
                            )
                        if single_result.status == "skipped":
                            self.logger.warning(
                                f"{single_result.status} {single_result.operation_type} {single_result.object_type} "
                                f"{catalog_name}.{schema_name}.{volume_name}: "
                                f"{single_result.error_message}"
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
                    results.extend(result)
                finally:
                    self.processed_objects.append(f"{schema_name}.{volume_name}")

        executor.shutdown(wait=True)

        self.logger.info(
            f"All volumes processed in schema {self.catalog_name}.{schema_name}"
        )

        return results

    def _adjust_table_types(self, table_types: List[TableType]) -> List[TableType]:
        """Adjust table types for replication/reconciliation operations."""
        adjusted_table_types = table_types.copy()
        # Add MANAGED table type when EXTERNAL is specified for replication/reconciliation as external tables are shown as MANAGED in shared catalogs
        if (
            adjusted_table_types
            and self.get_operation_name() in ("replication", "reconciliation")
            and TableType.EXTERNAL in adjusted_table_types
            and TableType.MANAGED not in adjusted_table_types
        ):
            adjusted_table_types.append(TableType.MANAGED)

        return adjusted_table_types

    def _table_type_filter_expression(
        self, table_types: List[TableType]
    ) -> Optional[str]:
        """Construct table type filter expression from table types."""
        if table_types and len(table_types) > 0:
            table_types = self._adjust_table_types(table_types)
            table_type_filters = [
                f"table_type = '{table_type.value.upper()}'"
                for table_type in table_types
            ]
            table_type_filter_expr = " or ".join(table_type_filters)
            return table_type_filter_expr
        return None

    def _build_schema_table_filter_expression(self) -> str:
        """construct schema_table_filter_expression from target_schemas if provided"""
        if self.catalog_config.target_schemas:
            schema_table_filters = []
            for schema in self.catalog_config.target_schemas:
                schema_name = schema.schema_name
                schema = recursive_substitute(schema, schema_name, "{{schema_name}}")
                if schema.tables:
                    table_names = [
                        f"table_name = '{table.table_name}'" for table in schema.tables
                    ]
                    table_filter_expr = " or ".join(table_names)
                    schema_table_filter = (
                        f"(table_schema = '{schema_name}' and ({table_filter_expr}))"
                    )
                elif schema.exclude_tables:
                    table_names = [
                        f"table_name != '{table.table_name}'"
                        for table in schema.exclude_tables
                    ]
                    table_filter_expr = " and ".join(table_names)
                    schema_table_filter = (
                        f"(table_schema = '{schema_name}' and ({table_filter_expr}))"
                    )
                elif schema.table_filter_expression:
                    schema_table_filter = f"(table_schema = '{schema_name}' and ({schema.table_filter_expression}))"
                else:
                    schema_table_filter = f"(table_schema = '{schema_name}')"

                table_type_filter_expr = None
                table_types = []

                if schema.table_types and len(schema.table_types) > 0:
                    table_types = schema.table_types
                elif (
                    self.catalog_config.table_types
                    and len(self.catalog_config.table_types) > 0
                ):
                    table_types = self.catalog_config.table_types
                # Add MANAGED table type when EXTERNAL is specified for replication/reconciliation as external tables are shown as MANAGED in shared catalogs
                table_type_filter_expr = self._table_type_filter_expression(table_types)
                if table_type_filter_expr:
                    schema_table_filter = (
                        f"({schema_table_filter} and ({table_type_filter_expr}))"
                    )
                schema_table_filters.append(schema_table_filter)
            schema_table_filter_expression = " or ".join(schema_table_filters)
            return schema_table_filter_expression

        if self.catalog_config.exclude_schemas:
            schema_names = [
                f"table_schema != '{schema.schema_name}'"
                for schema in self.catalog_config.exclude_schemas
            ]
            schema_table_filter_expression = " and ".join(schema_names)
            return schema_table_filter_expression

        if self.catalog_config.schema_table_filter_expression:
            # add table type filters to schema_table_filter_expression
            table_types = []
            table_type_filter_expr = None
            if (
                self.catalog_config.table_types
                and len(self.catalog_config.table_types) > 0
            ):
                table_types = self.catalog_config.table_types

            # Add MANAGED table type when EXTERNAL is specified for replication/reconciliation as external tables are shown as MANAGED in shared catalogs
            schema_table_filter_expression = (
                self.catalog_config.schema_table_filter_expression
            )
            table_type_filter_expr = self._table_type_filter_expression(table_types)
            if table_type_filter_expr:
                schema_table_filter_expression = f"({self.catalog_config.schema_table_filter_expression} and ({table_type_filter_expr}))"
        return schema_table_filter_expression

    def _create_failed_result(
        self,
        catalog_name: str,
        schema_name: str,
        object_name: Optional[str] = None,
        object_type: Optional[str] = None,
        error_msg: str = "",
        start_time: Optional[datetime] = None,
    ) -> List[RunResult]:
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

        return [
            RunResult(
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
        ]

    def _get_schemas(self) -> List[SchemaConfig]:
        """
        Get list of schemas to process based on configuration.

        Returns:
            List of SchemaConfig objects to process
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
                schema
                for schema in self.catalog_config.target_schemas
                if self.db_ops.refresh_schema_metadata(
                    f"{self.catalog_name}.{schema.schema_name}"
                )
                and self.spark.catalog.databaseExists(
                    f"{self.catalog_name}.{schema.schema_name}"
                )
            ]

            target_schemas = [
                schema
                for schema in target_schemas
                if schema.schema_name not in exclude_schema_names
            ]

            return target_schemas

        # Process all schemas
        if not self.catalog_config.schema_table_filter_expression:
            schema_list = self.db_ops.get_all_schemas(self.catalog_name)

        # Apply exclude_schemas filter if configured
        schema_list = [
            schema for schema in schema_list if schema not in exclude_schema_names
        ]

        return [SchemaConfig(schema_name=item) for item in schema_list]

    def _get_schema_tables(
        self, schema_table_filter_expression: Optional[str] = None
    ) -> List[dict]:
        """
        Get list of schema and table combinations when schema_table_filter_expression is configured.
        This method is triggered when catalog_config.schema_table_filter_expression is set.

        Returns:
            List of dictionaries with 'schema_name' and 'table_name' keys
        """

        # Use database operations to get schema-table combinations matching the filter expression
        schema_tables = self.db_ops.get_schema_tables_by_filter(
            self.catalog_name, schema_table_filter_expression
        )
        schema_tables_filtered = schema_tables
        # Skip already processed tables
        if SKIP_PROCESSED_TABLES:
            schema_tables_filtered = [
                item
                for item in schema_tables
                if f"{item['schema_name']}.{item['table_name']}"
                not in self.processed_objects
                and f"{self.catalog_config.catalog_name}.{item['schema_name']}.{item['table_name']}"
                not in self.completed_objects
            ]
        if len(schema_tables_filtered) != len(schema_tables):
            self.logger.info(
                f"Found {len(schema_tables)} and exclude {len(schema_tables) - len(schema_tables_filtered)} processed tables"
            )
        return schema_tables_filtered

    def _get_tables(
        self, schema_config: SchemaConfig, table_list: List[TableConfig]
    ) -> List[TableConfig]:
        """
        Get list of tables to process in a schema based on configuration.

        Args:
            catalog_name: Name of the catalog
            schema_config: Schema configuration object
            table_list: List of table configurations to process in the schema

        Returns:
            List of table configurations to process
        """
        # Find exclusions and table filter expression for this schema from configuration
        table_names = []
        tables = []
        schema_name = schema_config.schema_name
        exclude_names = set()
        table_filter_expression = None
        if schema_config.exclude_tables:
            exclude_names = {table.table_name for table in schema_config.exclude_tables}
        if schema_config.table_filter_expression:
            table_filter_expression = schema_config.table_filter_expression

        if table_list:
            # Use explicitly configured tables
            table_names = [item.table_name for item in table_list]
            tables = table_list
        elif table_filter_expression:
            # Use table filter expression
            table_names = self.db_ops.get_tables_by_filter(
                self.catalog_name,
                schema_name,
                table_filter_expression,
            )
            tables = [TableConfig(table_name=item) for item in table_names]
        elif not self.catalog_config.schema_table_filter_expression:
            # Process all tables in the schema
            table_names = self.db_ops.get_tables_in_schema(
                self.catalog_name, schema_name
            )
            tables = [TableConfig(table_name=item) for item in table_names]

        # Apply exclusions first
        table_names = [table for table in table_names if table not in exclude_names]

        table_names_unprocessed = table_names
        # Skip already processed tables
        if SKIP_PROCESSED_TABLES:
            table_names_unprocessed = [
                table
                for table in table_names
                if f"{schema_name}.{table}" not in self.processed_objects
                and f"{self.catalog_config.catalog_name}.{schema_name}.{table}"
                not in self.completed_objects
            ]
        if len(table_names_unprocessed) != len(table_names):
            self.logger.info(
                f"Exclude {len(table_names) - len(table_names_unprocessed)} processed tables from {len(table_names)} tables"
            )

        table_names_unshared = table_names_unprocessed
        if self.shared_tables:
            # Exclude tables that are already in shared tables
            table_names_unshared = [
                table
                for table in table_names_unprocessed
                if f"{self.catalog_name}.{schema_name}.{table}"
                not in self.shared_tables
            ]
            if len(table_names_unshared) != len(table_names_unprocessed):
                self.logger.info(
                    f"Exclude {len(table_names_unprocessed) - len(table_names_unshared)} already shared tables from {len(table_names_unprocessed)} tables"
                )

        filtered_table_names = table_names_unshared
        # ignore table type filtering for backup operation as it will only handle dlt tables
        if self.get_operation_name() != "backup":
            table_types = []
            # get table type filters from schema configuration
            if schema_config.table_types and len(schema_config.table_types) > 0:
                # Exclude MATERIALIZED_VIEW and VIEW for replication operation as they are not supported
                if self.get_operation_name() in ("replication"):
                    self.logger.warning(
                        "Excluding MATERIALIZED_VIEW and VIEW table types for replication operation as they are not supported."
                    )
                    schema_config.table_types.remove(TableType.MATERIALIZED_VIEW)
                    schema_config.table_types.remove(TableType.VIEW)
                # Exclude VIEW for reconciliation operation as they are not supported
                if self.get_operation_name() in ("reconciliation"):
                    self.logger.warning(
                        "Excluding VIEW table type for reconciliation operation as it is not supported."
                    )
                    schema_config.table_types.remove(TableType.VIEW)
                table_types = [
                    table_type.value.lower() for table_type in schema_config.table_types
                ]
            # create table type filters based on uc_object_types
            if schema_config.uc_object_types:
                table_types_set = set()
                # if UCObjectType.ALL in schema_config.uc_object_types:
                #     table_types_set.update(
                #         [
                #             "managed",
                #             "external",
                #             "streaming_table",
                #             "view",
                #             "materialized_view",
                #         ]
                #     )
                # if (
                #     UCObjectType.TABLE_TAG in schema_config.uc_object_types
                #     or UCObjectType.COLUMN_TAG in schema_config.uc_object_types
                #     or UCObjectType.COLUMN_COMMENT in schema_config.uc_object_types
                # ):
                #     table_types_set.update(
                #         [
                #             "managed",
                #             "external",
                #             "streaming_table",
                #             "view",
                #             "materialized_view",
                #         ]
                #     )
                if UCObjectType.TABLE_COMMENT in schema_config.uc_object_types:
                    table_types_set.update(["managed", "external", "view"])
                if UCObjectType.TABLE in schema_config.uc_object_types:
                    table_types_set.update(["managed", "external"])
                if UCObjectType.VIEW in schema_config.uc_object_types:
                    table_types_set.update(["view"])
                if UCObjectType.MATERIALIZED_VIEW in schema_config.uc_object_types:
                    table_types_set.update(["materialized_view"])
                if UCObjectType.STREAMING_TABLE in schema_config.uc_object_types:
                    table_types_set.update(["streaming_table"])
                if table_types_set:
                    table_types = list(table_types_set)

            filtered_table_names = self.db_ops.filter_tables_by_type(
                self.catalog_name,
                schema_name,
                table_names_unshared,
                table_types,
                schema_config.concurrency.parallel_table_filter,
            )
            if len(filtered_table_names) != len(table_names_unshared):
                self.logger.info(
                    f"Exclude {len(table_names_unshared) - len(filtered_table_names)} table types unmatched tables from {len(table_names_unshared)} tables"
                )
        # Then filter by table types
        return [table for table in tables if table.table_name in filtered_table_names]

    def _get_volumes(
        self, schema_config: SchemaConfig, volume_list: List[VolumeConfig]
    ) -> List[VolumeConfig]:
        """
        Get list of volumes to process in a schema based on configuration.

        Args:
            schema_config: Schema configuration object
            volume_list: List of volume configurations to process in the schema

        Returns:
            List of volume configurations to process
        """
        # Find exclusions for this schema from configuration
        exclude_names = set()
        schema_name = schema_config.schema_name
        if schema_config.exclude_volumes:
            exclude_names = {
                volume.volume_name for volume in schema_config.exclude_volumes
            }

        if volume_list:
            # Use explicitly configured volumes
            volume_names = [item.volume_name for item in volume_list]
            volumes = volume_list
        else:
            # Process all volumes in the schema
            volume_names = self.db_ops.get_volumes_in_schema(
                self.catalog_name, schema_name
            )
            volumes = [VolumeConfig(volume_name=item) for item in volume_names]

        # Apply exclusions first
        volume_names = [
            volume for volume in volume_names if volume not in exclude_names
        ]

        volume_types = []
        # get volume type filters from schema configuration
        if schema_config.volume_types and len(schema_config.volume_types) > 0:
            volume_types = [
                volume_type.value.lower() for volume_type in schema_config.volume_types
            ]
        # create volume type filters based on uc_object_types
        if schema_config.uc_object_types and (
            UCObjectType.VOLUME in schema_config.uc_object_types
            or UCObjectType.VOLUME_TAG in schema_config.uc_object_types
            or UCObjectType.ALL in schema_config.uc_object_types
        ):
            volume_types.extend(["managed", "external"])

        filtered_volume_names = self.db_ops.filter_volumes_by_type(
            self.catalog_name,
            schema_name,
            volume_names,
            volume_types,
        )
        if len(filtered_volume_names) != len(volume_names):
            self.logger.info(
                f"Exclude {len(volume_names) - len(filtered_volume_names)} volume types unmatched volumes from {len(volume_names)} volumes"
            )
        # Then filter by volume types
        return [
            volume for volume in volumes if volume.volume_name in filtered_volume_names
        ]

    def _create_tagging_operation(self, retry: RetryConfig):
        """Create a tagging operation function with retry and logging."""

        @retry_with_logging(retry, self.logger)
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
        retry: RetryConfig = None,
    ) -> RunResult:
        """
        Execute tag replication operation for a single table or column.

        Returns:
            RunResult object for the tag replication operation
        """
        try:
            start_time = datetime.now(timezone.utc)
            object_name = ""
            source_object = f"`{source_catalog}`"
            target_object = f"`{target_catalog}`"
            object_name = target_catalog
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
                extra={"run_id": self.run_id, "operation": "uc_replication"},
            )

            attempt = 1
            max_attempts = retry.max_attempts
            last_exception = None
            # Create tagging operation with retry and logging
            tagging_operation = self._create_tagging_operation(retry)

            (
                uncommon_source_tag_maps_list,
                uncommon_target_tag_maps_list,
            ) = filter_common_maps(source_tag_maps_list, target_tag_maps_list)

            if not uncommon_source_tag_maps_list and not uncommon_target_tag_maps_list:
                self.logger.info(
                    f"No uncommon tags found for: {source_object} -> {target_object} "
                    f"Skipping tag replication for this {object_type}",
                    extra={"run_id": self.run_id, "operation": "uc_replication"},
                )
                end_time = datetime.now(timezone.utc)
                duration = (end_time - start_time).total_seconds()
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
                        "skipped": True,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
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
                    extra={"run_id": self.run_id, "operation": "uc_replication"},
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
            extra={"run_id": self.run_id, "operation": "uc_replication"},
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

    def _uc_replicate_catalog_tags(
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
        max_attempts = self.catalog_config.retry.max_attempts

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
            retry=self.catalog_config.retry,
        )
        run_results.append(run_result)
        return run_results

    def _uc_replicate_schema_tags(
        self,
        schema_config: SchemaConfig,
    ) -> list[RunResult]:
        """
        Replicate schema tags from source to target schema.

        Args:
            schema_config: SchemaConfig object for the schema to replicate tags for
        Returns:
            RunResult object for the tag replication operation
        """

        run_results = []

        replication_config = schema_config.replication_config
        schema_name = schema_config.schema_name
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        object_type = "schema_tag"
        attempt = 1
        max_attempts = schema_config.retry.max_attempts

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
            retry=schema_config.retry,
        )

        run_results.append(run_result)
        return run_results

    def _uc_replicate_catalog(self) -> List[RunResult]:
        """
        Replicate catalog from source to target using workspace client.
        If storage location exists, uses external_location mapping to determine target path.

        Returns:
            List[RunResult]: Results for the catalog replication operation
        """
        start_time = datetime.now(timezone.utc)
        run_results = []
        replication_config = self.catalog_config.replication_config
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name

        attempt = 1
        max_attempts = self.catalog_config.retry.max_attempts
        last_exception = None

        dict_for_creation = DICT_FOR_CREATION_CATALOG.copy()
        dict_for_update = DICT_FOR_UPDATE_CATALOG.copy()

        try:
            self.logger.info(
                f"Starting catalog metadata replication: {source_catalog} -> {target_catalog}",
                extra={"run_id": self.run_id, "operation": "uc_replication"},
            )

            # Get source catalog info using source dbops
            source_catalog_info = self.source_dbops.get_catalog(source_catalog)

            dict_for_creation = {
                k: getattr(source_catalog_info, k, None)
                for k, v in source_catalog_info.as_dict().items()
                if k in dict_for_creation.keys()
            }

            dict_for_update = {
                k: getattr(source_catalog_info, k, None)
                for k, v in source_catalog_info.as_dict().items()
                if k in dict_for_update.keys()
            }

            # Determine target storage root using cloud_url_mapping if applicable
            target_storage_root = None
            source_storage_root = getattr(source_catalog_info, "storage_root", None)
            # Check if replicate_as_managed is enabled
            if getattr(replication_config, "replicate_as_managed", False):
                self.logger.info(
                    "Creating catalog as managed due to replicate_as_managed=true.",
                    extra={
                        "run_id": self.run_id,
                        "operation": "uc_replication",
                    },
                )
            else:
                if source_storage_root:
                    if self.cloud_url_mapping:
                        # Map external location using utility function
                        target_storage_root = map_cloud_url(
                            source_storage_root, self.cloud_url_mapping
                        )

                        if target_storage_root is None:
                            raise ReplicationError(
                                f"No external location mapping found for source catalog storage root: {source_storage_root}. "
                                f"Cannot replicate external catalog without proper mapping. "
                                f"Set replicate_as_managed=true to create as managed catalog instead."
                            )
                    else:
                        raise ReplicationError(
                            f"Source catalog {source_catalog} has storage root: {source_storage_root} "
                            f"but cloud_url_mapping is not configured. "
                            f"Cannot replicate external catalog without proper mapping. "
                            f"Set replicate_as_managed=true to create as managed catalog instead."
                        )

            dict_for_creation["storage_root"] = target_storage_root
            dict_for_creation["name"] = target_catalog
            # Check if target catalog already exists
            catalog_exists = False
            target_catalog_info = None
            try:
                target_catalog_info = self.target_dbops.get_catalog(target_catalog)
                catalog_exists = True
                self.logger.info(
                    f"Target catalog {target_catalog} already exists, will update properties",
                    extra={"run_id": self.run_id, "operation": "uc_replication"},
                )
            except Exception:
                # Catalog doesn't exist, we'll create it
                pass

            if not catalog_exists:
                # Create catalog using workspace client
                _ = self.target_dbops.create_catalog(dict_for_creation)

                if not replication_config.replicate_enable_predictive_optimization:
                    self.logger.info(
                        f"Disabling predictive optimization for newly created catalog {target_catalog}",
                        extra={"run_id": self.run_id, "operation": "uc_replication"},
                    )
                    _ = self.target_dbops.update_catalog(
                        {
                            "name": target_catalog,
                            "enable_predictive_optimization": EnablePredictiveOptimization.DISABLE,
                        }
                    )
            else:
                dict_for_update_target = {
                    k: getattr(target_catalog_info, k, None)
                    for k, v in target_catalog_info.as_dict().items()
                    if k in dict_for_update.keys()
                }
                dict_for_update["name"] = target_catalog
                if not replication_config.replicate_enable_predictive_optimization:
                    self.logger.info(
                        f"Disabling predictive optimization for existing catalog {target_catalog}",
                        extra={"run_id": self.run_id, "operation": "uc_replication"},
                    )
                    dict_for_update["enable_predictive_optimization"] = (
                        EnablePredictiveOptimization.DISABLE
                    )

                if dict_for_update != dict_for_update_target:
                    _ = self.target_dbops.update_catalog(dict_for_update)

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            self.logger.info(
                f"Catalog metadata replication completed successfully: {source_catalog} -> {target_catalog} ({duration:.2f}s)",
                extra={"run_id": self.run_id, "operation": "uc_replication"},
            )

            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=None,
                    object_name=target_catalog,
                    object_type="catalog",
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    details={
                        "source_catalog": source_catalog,
                        "target_catalog": target_catalog,
                        "source_storage_root": source_storage_root,
                        "target_storage_root": target_storage_root,
                        "catalog_existed": catalog_exists,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            last_exception = e

            if not isinstance(e, ReplicationError):
                e = ReplicationError(
                    f"Catalog metadata replication operation failed: {str(e)}"
                )

            error_msg = f"Failed to replicate catalog metadata {source_catalog} -> {target_catalog}: {str(e)}"
            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "uc_replication"},
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=None,
                    object_name=target_catalog,
                    object_type="catalog",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                    details={
                        "source_catalog": source_catalog,
                        "target_catalog": target_catalog,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            )

        return run_results

    def _uc_replicate_schema(self, schema_config: SchemaConfig) -> List[RunResult]:
        """
        Replicate schema from source to target using workspace client.

        Args:
            schema_config: SchemaConfig object for the schema to replicate

        Returns:
            List[RunResult]: Results for the schema replication operation
        """
        start_time = datetime.now(timezone.utc)
        run_results = []
        replication_config = schema_config.replication_config
        schema_name = schema_config.schema_name
        source_catalog = replication_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_schema_full_name = f"{source_catalog}.{schema_name}"
        target_schema_full_name = f"{target_catalog}.{schema_name}"

        attempt = 1
        max_attempts = schema_config.retry.max_attempts
        last_exception = None

        dict_for_creation = DICT_FOR_CREATION_SCHEMA

        dict_for_update = DICT_FOR_UPDATE_SCHEMA

        try:
            self.logger.info(
                f"Starting schema metadata replication: {source_schema_full_name} -> {target_schema_full_name}",
                extra={"run_id": self.run_id, "operation": "uc_replication"},
            )

            # Get source schema info using source dbops
            source_schema_info = self.source_dbops.get_schema(source_schema_full_name)

            dict_for_creation = {
                k: getattr(source_schema_info, k, None)
                for k, v in source_schema_info.as_dict().items()
                if k in dict_for_creation.keys()
            }
            # Ensure catalog_name and name are set correctly for target
            dict_for_creation["catalog_name"] = target_catalog
            dict_for_creation["name"] = schema_name

            dict_for_update = {
                k: getattr(source_schema_info, k, None)
                for k, v in source_schema_info.as_dict().items()
                if k in dict_for_update.keys()
            }
            # Ensure full_name is set correctly for target
            dict_for_update["full_name"] = target_schema_full_name

            # Determine target storage root using cloud_url_mapping if applicable
            target_storage_root = None
            source_storage_root = getattr(source_schema_info, "storage_root", None)
            # Check if replicate_as_managed is enabled
            if getattr(replication_config, "replicate_as_managed", False):
                self.logger.info(
                    "Creating schema as managed due to replicate_as_managed=true.",
                    extra={
                        "run_id": self.run_id,
                        "operation": "uc_replication",
                    },
                )
            else:
                if source_storage_root:
                    if self.cloud_url_mapping:
                        # Map external location using utility function
                        target_storage_root = map_cloud_url(
                            source_storage_root, self.cloud_url_mapping
                        )

                        if target_storage_root is None:
                            raise ReplicationError(
                                f"No external location mapping found for source schema storage root: {source_storage_root}. "
                                f"Cannot replicate external schema without proper mapping. "
                                f"Set replicate_as_managed=true to create as managed schema instead."
                            )
                    else:
                        raise ReplicationError(
                            f"Source schema {source_schema_full_name} has storage root: {source_storage_root} "
                            f"but cloud_url_mapping is not configured. "
                            f"Cannot replicate external schema without proper mapping. "
                            f"Set replicate_as_managed=true to create as managed schema instead."
                        )

            dict_for_creation["storage_root"] = target_storage_root

            # Check if target schema already exists
            schema_exists = False
            target_schema_info = None
            try:
                target_schema_info = self.target_dbops.get_schema(
                    target_schema_full_name
                )
                schema_exists = True
                self.logger.info(
                    f"Target schema {target_schema_full_name} already exists, will update properties",
                    extra={"run_id": self.run_id, "operation": "uc_replication"},
                )
            except Exception:
                # Schema doesn't exist, we'll create it
                pass

            if not schema_exists:
                # Create schema using workspace client
                _ = self.target_dbops.create_schema(dict_for_creation)

                if not replication_config.replicate_enable_predictive_optimization:
                    self.logger.info(
                        f"Disabling predictive optimization for newly created schema {target_schema_full_name}",
                        extra={"run_id": self.run_id, "operation": "uc_replication"},
                    )
                    _ = self.target_dbops.update_schema(
                        {
                            "full_name": target_schema_full_name,
                            "enable_predictive_optimization": EnablePredictiveOptimization.DISABLE,
                        }
                    )
            else:
                dict_for_update_target = {
                    k: getattr(target_schema_info, k, None)
                    for k, v in target_schema_info.as_dict().items()
                    if k in dict_for_update.keys()
                }

                if not replication_config.replicate_enable_predictive_optimization:
                    self.logger.info(
                        f"Disabling predictive optimization for existing schema {target_schema_full_name}",
                        extra={"run_id": self.run_id, "operation": "uc_replication"},
                    )
                    dict_for_update["enable_predictive_optimization"] = (
                        EnablePredictiveOptimization.DISABLE
                    )

                if dict_for_update != dict_for_update_target:
                    _ = self.target_dbops.update_schema(dict_for_update)

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            self.logger.info(
                f"Schema metadata replication completed successfully: {source_schema_full_name} -> {target_schema_full_name} ({duration:.2f}s)",
                extra={"run_id": self.run_id, "operation": "uc_replication"},
            )

            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=schema_name,
                    object_type="schema",
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    details={
                        "source_schema": source_schema_full_name,
                        "target_schema": target_schema_full_name,
                        "source_storage_root": source_storage_root,
                        "target_storage_root": target_storage_root,
                        "schema_existed": schema_exists,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()
            last_exception = e

            if not isinstance(e, ReplicationError):
                e = ReplicationError(
                    f"Schema metadata replication operation failed: {str(e)}"
                )

            error_msg = f"Failed to replicate schema metadata {source_schema_full_name} -> {target_schema_full_name}: {str(e)}"
            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "uc_replication"},
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            run_results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=schema_name,
                    object_type="schema",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                    details={
                        "source_schema": source_schema_full_name,
                        "target_schema": target_schema_full_name,
                    },
                    attempt_number=attempt,
                    max_attempts=max_attempts,
                )
            )

        return run_results
