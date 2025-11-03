"""
Provider factory for creating and managing data replication operations.

This module provides a factory class that creates provider instances and manages
the execution of backup, replication, and reconciliation operations.
"""

import json
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING, List, Optional, Type

from databricks.connect import DatabricksSession

from ..audit.audit_logger import AuditLogger
from ..audit.logger import DataReplicationLogger
from ..config.models import (
    ReplicationSystemConfig,
    RunResult,
    RunSummary,
    TargetCatalogConfig
)
from ..databricks_operations import DatabricksOperations

if TYPE_CHECKING:
    from .base_provider import BaseProvider


class ProviderFactory:
    """
    Factory class for creating and managing data replication operations.

    Creates provider instances for different operation types (backup, replication,
    reconciliation) and manages their execution with audit logging capabilities.
    """

    # Valid operation types
    VALID_OPERATIONS = {"backup", "replication", "reconciliation"}

    def __init__(
        self,
        operation_type: str,
        config: ReplicationSystemConfig,
        spark: DatabricksSession,
        logging_spark: DatabricksSession,
        logger: DataReplicationLogger,
        run_id: Optional[str] = None,
    ):
        """
        Initialize the provider factory.

        Args:
            operation_type: Type of operation ("backup", "replication", "reconciliation")
            config: System configuration
            spark: Spark session for database operations
            logging_spark: Spark session for logging
            logger: Logger instance
            run_id: Optional run identifier
        """
        if operation_type not in self.VALID_OPERATIONS:
            raise ValueError(
                f"Invalid operation type: {operation_type}. "
                f"Must be one of: {list(self.VALID_OPERATIONS)}"
            )

        self._operation_type = operation_type
        self.config = config
        self.spark = spark
        self.logging_spark = logging_spark
        self.logger = logger
        self.run_id = run_id or str(uuid.uuid4())
        self.db_ops = DatabricksOperations(spark)
        self.db_ops_logging = DatabricksOperations(logging_spark)

        # Initialize AuditLogger if audit table is configured
        self.audit_logger: Optional[AuditLogger] = None
        if self.config.audit_config and self.config.audit_config.audit_table:
            # Convert config to dict for logging
            config_dict = (
                self.config.model_dump()
                if hasattr(self.config, "model_dump")
                else self.config.dict()
            )
            self.audit_logger = AuditLogger(
                spark=self.logging_spark,
                db_ops=self.db_ops_logging,
                logger=self.logger,
                run_id=self.run_id,
                create_audit_catalog=self.config.audit_config.create_audit_catalog,
                audit_table=self.config.audit_config.audit_table,
                audit_catalog_location=self.config.audit_config.audit_catalog_location,
                config_details=config_dict,
            )

    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return self._operation_type

    def is_catalog_enabled(self, catalog: TargetCatalogConfig) -> bool:
        """Check if the operation is enabled for the given catalog."""
        if self._operation_type == "backup":
            return bool(catalog.backup_config and catalog.backup_config.enabled)
        elif self._operation_type == "replication":
            return bool(
                catalog.replication_config and catalog.replication_config.enabled
            )
        elif self._operation_type == "reconciliation":
            return bool(
                catalog.reconciliation_config and catalog.reconciliation_config.enabled
            )
        return False

    def create_provider(
        self,
        catalog: TargetCatalogConfig
    ) -> "BaseProvider":
        """Create a provider instance for the given catalog."""
        # Lazy import to avoid circular imports
        from .base_provider import BaseProvider

        provider_class: Type[BaseProvider]
        if self._operation_type == "backup":
            from .backup_provider import BackupProvider

            provider_class = BackupProvider
        elif self._operation_type == "replication":
            from .replication_provider import ReplicationProvider

            provider_class = ReplicationProvider
        elif self._operation_type == "reconciliation":
            from .reconciliation_provider import ReconciliationProvider

            provider_class = ReconciliationProvider
        else:
            raise ValueError(f"Unknown operation type: {self._operation_type}")

        max_workers = 2  # default
        timeout_seconds = 300  # default
        if self.config.concurrency:
            max_workers = self.config.concurrency.max_workers
            timeout_seconds = self.config.concurrency.timeout_seconds

        # Ensure we have a retry config (providers expect it to be non-None)
        retry_config = self.config.retry
        if retry_config is None:
            # Use default retry config if none provided
            from ..config.models import RetryConfig

            retry_config = RetryConfig()

        return provider_class(
            self.spark,
            self.logger,
            self.db_ops,
            self.run_id,
            catalog,
            self.config.source_databricks_connect_config,
            self.config.target_databricks_connect_config,
            retry_config,
            max_workers,
            timeout_seconds,
            self.config.external_location_mapping,
        )

    def log_run_result(self, result: RunResult) -> None:
        """
        Log a RunResult to the configured audit table.

        Args:
            result: RunResult object to log
        """
        try:
            details_str = None
            if result.details:
                details_str = json.dumps(result.details)

            # Parse string timestamps back to datetime objects
            start_dt = datetime.fromisoformat(result.start_time.replace("Z", "+00:00"))
            end_dt = datetime.fromisoformat(result.end_time.replace("Z", "+00:00"))
            duration = (end_dt - start_dt).total_seconds()

            # Log to standard logger
            table_info = (
                f"{result.catalog_name}.{result.schema_name}.{result.object_name}"
            )
            self.logger.info(
                f"Operation {result.operation_type} {result.status} for {table_info}"
            )

            # Log to audit table if AuditLogger is available
            if self.audit_logger:
                try:
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
                    self.logger.warning(
                        f"Failed to write audit log for {table_info}: {str(audit_error)}"
                    )

        except Exception as e:
            self.logger.error(f"Failed to log run result: {str(e)}", exc_info=True)

    def log_run_results(self, results: List[RunResult]) -> None:
        """
        Log multiple RunResult objects to all configured audit tables.

        Args:
            results: List of RunResult objects to log
        """
        for result in results:
            self.log_run_result(result)

    def create_summary(
        self,
        start_time: datetime,
        results: List[RunResult],
        operation_type: str = "operation",
        error_message: Optional[str] = None,
    ) -> RunSummary:
        """
        Create a run summary object.

        Args:
            start_time: Operation start time
            results: List of operation results
            operation_type: Type of operation (backup, replication, etc.)
            error_message: Optional error message
        """
        # Suppress unused argument warning for error_message
        _ = error_message

        end_time = datetime.now(timezone.utc)
        duration = (end_time - start_time).total_seconds()

        # Calculate summary statistics
        successful_operations = sum(1 for r in results if r.status == "success")
        failed_operations = sum(1 for r in results if r.status == "failed")
        total_operations = len(results)

        status = "completed" if failed_operations == 0 else "completed_with_failures"

        # Count unique catalogs, schemas, and tables
        catalogs = set(r.catalog_name for r in results if r.catalog_name)
        schemas = set(
            f"{r.catalog_name}.{r.schema_name}" for r in results if r.schema_name
        )
        objects = set(
            f"{r.catalog_name}.{r.schema_name}.{r.object_name}"
            for r in results
            if r.object_name
        )

        success_rate = (
            (successful_operations / total_operations * 100)
            if total_operations > 0
            else 0
        )
        summary_text = (
            f"{operation_type.title()} operation completed in {duration:.1f}s. "
            f"Processed {len(catalogs)} catalogs, {len(schemas)} schemas, "
            f"{len(objects)} objects. Success rate: {successful_operations}/"
            f"{total_operations} ({success_rate:.1f}%)"
        )

        return RunSummary(
            run_id=self.run_id,
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            duration=duration,
            status=status,
            total_catalogs=len(catalogs),
            total_schemas=len(schemas),
            total_tables=len(objects),
            successful_operations=successful_operations,
            failed_operations=failed_operations,
            summary=summary_text,
        )

    def log_run_summary(
        self, summary: RunSummary, operation_type: str = "operation"
    ) -> None:
        """
        Log run summary to audit tables.

        Args:
            summary: Run summary to log
            operation_type: Type of operation for audit logging
        """
        try:
            # Log to standard logger
            self.logger.info(f"{operation_type.title()} summary: {summary.summary}")

            # Log to audit table if AuditLogger is available
            if self.audit_logger:
                try:
                    # Parse timestamps
                    start_dt = datetime.fromisoformat(
                        summary.start_time.replace("Z", "+00:00")
                    )
                    end_dt = None
                    if summary.end_time:
                        end_dt = datetime.fromisoformat(
                            summary.end_time.replace("Z", "+00:00")
                        )
                    else:
                        end_dt = datetime.now(timezone.utc)

                    # Create summary details as JSON
                    summary_details = {
                        "total_catalogs": summary.total_catalogs,
                        "total_schemas": summary.total_schemas,
                        "total_tables": summary.total_tables,
                        "successful_operations": summary.successful_operations,
                        "failed_operations": summary.failed_operations,
                        "summary_text": summary.summary,
                    }

                    self.audit_logger.log_operation(
                        operation_type=f"{operation_type}_summary",
                        catalog_name="ALL",
                        schema_name="ALL",
                        object_name="ALL",
                        object_type="ALL",
                        status=summary.status,
                        start_time=start_dt,
                        end_time=end_dt,
                        duration_seconds=summary.duration or 0.0,
                        error_message=None,
                        details=json.dumps(summary_details),
                        attempt_number=1,
                        max_attempts=1,
                    )
                except Exception as audit_error:
                    self.logger.warning(
                        f"Failed to write audit log for {operation_type} summary: {str(audit_error)}"
                    )

        except Exception as e:
            self.logger.error(f"Failed to log run summary: {str(e)}", exc_info=True)

    def run_operations(self) -> RunSummary:
        """
        Run operations for all configured catalogs.

        Returns:
            RunSummary with operation results
        """
        start_time = datetime.now(timezone.utc)
        operation_name = self.get_operation_name()

        self.logger.info(
            f"Starting {operation_name} operations (run_id: {self.run_id})"
        )

        try:
            # Get catalogs that need this operation
            enabled_catalogs = [
                catalog
                for catalog in self.config.target_catalogs
                if self.is_catalog_enabled(catalog)
            ]

            if not enabled_catalogs:
                self.logger.info(f"No catalogs configured for {operation_name}")
                return self.create_summary(start_time, [], operation_name)

            results = self._run_catalog_operations(enabled_catalogs)

            summary = self.create_summary(start_time, results, operation_name)

            # Log individual results
            self.log_run_results(results)
            # Log final summary
            self.log_run_summary(summary, operation_name)

            return summary

        except Exception as e:
            error_msg = f"{operation_name.title()} operation failed: {str(e)}"
            self.logger.error(error_msg, exc_info=True)

            summary = self.create_summary(start_time, [], operation_name, error_msg)
            self.log_run_summary(summary, operation_name)

            return summary

        finally:
            try:
                if self.spark:
                    self.spark.stop()  # type: ignore
            except AttributeError:
                pass

    def _run_catalog_operations(
        self, catalogs: List[TargetCatalogConfig]
    ) -> List[RunResult]:
        """
        Run operations for the given catalogs.

        Args:
            catalogs: List of catalogs to process

        Returns:
            List of RunResult objects for all operations
        """
        results = []
        operation_name = self.get_operation_name()
        max_workers = (
            self.config.concurrency.max_workers if self.config.concurrency else 2
        )

        self.logger.info(
            f"Running {operation_name} with {max_workers} concurrent workers per schema"
        )

        # Process catalogs sequentially, but use concurrency within each schema
        for catalog in catalogs:
            try:
                provider = self.create_provider(catalog)
                catalog_results = provider.process_catalog()
                results.extend(catalog_results)

                successful = sum(1 for r in catalog_results if r.status == "success")
                total = len(catalog_results)

                self.logger.info(
                    f"Completed {operation_name} for catalog {catalog.catalog_name}: "
                    f"{successful}/{total} operations successful"
                )

            except Exception as e:
                error_msg = f"{operation_name.title()} failed for catalog {catalog.catalog_name}: {str(e)}"
                self.logger.error(error_msg, exc_info=True)

                # Create failure result
                now = datetime.now(timezone.utc)
                result = RunResult(
                    operation_type=operation_name,
                    catalog_name=catalog.catalog_name,
                    status="failed",
                    start_time=now.isoformat(),
                    end_time=now.isoformat(),
                    error_message=error_msg,
                )
                results.append(result)

        return results

    # Convenience methods for specific operations
    def run_backup_operations(self) -> RunSummary:
        """Run backup operations for all configured catalogs."""
        if self._operation_type != "backup":
            raise ValueError("This factory is not configured for backup operations")
        return self.run_operations()

    def run_replication_operations(self) -> RunSummary:
        """Run replication operations for all configured catalogs."""
        if self._operation_type != "replication":
            raise ValueError(
                "This factory is not configured for replication operations"
            )
        return self.run_operations()

    def run_reconciliation_operations(self) -> RunSummary:
        """Run reconciliation operations for all configured catalogs."""
        if self._operation_type != "reconciliation":
            raise ValueError(
                "This factory is not configured for reconciliation operations"
            )
        return self.run_operations()

    # Factory methods for creating specific operation factories
    @classmethod
    def create_backup_factory(cls, *args, **kwargs) -> "ProviderFactory":
        """Factory method to create a backup provider factory."""
        return cls("backup", *args, **kwargs)

    @classmethod
    def create_replication_factory(cls, *args, **kwargs) -> "ProviderFactory":
        """Factory method to create a replication provider factory."""
        return cls("replication", *args, **kwargs)

    @classmethod
    def create_reconciliation_factory(cls, *args, **kwargs) -> "ProviderFactory":
        """Factory method to create a reconciliation provider factory."""
        return cls("reconciliation", *args, **kwargs)
