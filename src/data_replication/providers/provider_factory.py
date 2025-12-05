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
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    AwsIamRoleRequest,
    AzureManagedIdentityRequest,
)

from ..audit.audit_logger import AuditLogger
from ..audit.logger import DataReplicationLogger
from ..config.models import (
    ReplicationSystemConfig,
    RunResult,
    RunSummary,
    TargetCatalogConfig,
    UCObjectType,
)
from ..constants import (
    DICT_FOR_CREATION_STORAGE_CREDENTIAL,
    DICT_FOR_UPDATE_STORAGE_CREDENTIAL,
    DICT_FOR_CREATION_EXTERNAL_LOCATION,
    DICT_FOR_UPDATE_EXTERNAL_LOCATION,
)
from ..databricks_operations import DatabricksOperations
from ..utils import (
    create_workspace_client,
    map_cloud_url,
)

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
        workspace_client: WorkspaceClient,
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
        self.workspace_client = workspace_client
        self.logger = logger
        self.run_id = run_id or str(uuid.uuid4())
        self.db_ops = DatabricksOperations(spark, logger)
        self.db_ops_logging = DatabricksOperations(logging_spark, logger)
        self.completed_run_results: List[RunResult] = []

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

        # Create source and target workspace client for storage credential and external location replication
        if self._operation_type == "replication" and (
            self.config.uc_object_types
            and (
                UCObjectType.STORAGE_CREDENTIAL in self.config.uc_object_types
                or UCObjectType.EXTERNAL_LOCATION in self.config.uc_object_types
                or UCObjectType.ALL in self.config.uc_object_types
            )
        ):
            self.source_workspace_client = create_workspace_client(
                host=self.config.source_databricks_connect_config.host,
                secret_config=self.config.source_databricks_connect_config.token,
                workspace_client=self.workspace_client,
                auth_type=self.config.source_databricks_connect_config.auth_type,
            )
            self.source_db_ops = DatabricksOperations(
                spark=None,
                logger=self.logger,
                workspace_client=self.source_workspace_client,
            )
            self.target_workspace_client = create_workspace_client(
                host=self.config.target_databricks_connect_config.host,
                secret_config=self.config.target_databricks_connect_config.token,
                workspace_client=self.workspace_client,
                auth_type=self.config.target_databricks_connect_config.auth_type,
            )
            self.target_db_ops = DatabricksOperations(
                spark=None,
                logger=self.logger,
                workspace_client=self.target_workspace_client,
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

    def create_provider(self, catalog: TargetCatalogConfig) -> "BaseProvider":
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


        return provider_class(
            self.spark,
            self.logger,
            self.db_ops,
            self.workspace_client,
            self.run_id,
            catalog,
            self.config.source_databricks_connect_config,
            self.config.target_databricks_connect_config,
            self.config.cloud_url_mapping,
            self.audit_logger,
            self.completed_run_results,
        )

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
        successful_operations = sum(
            1 for r in results if results and r.status == "success"
        )
        failed_operations = sum(1 for r in results if results and r.status == "failed")
        total_operations = len(results) if results else 0

        status = "completed" if failed_operations == 0 else "completed_with_failures"

        # Count unique catalogs, schemas, and tables
        catalogs = set(r.catalog_name for r in results if results and r.catalog_name)
        schemas = set(
            f"{r.catalog_name}.{r.schema_name}"
            for r in results
            if results and r.schema_name
        )
        objects = set(
            f"{r.catalog_name}.{r.schema_name}.{r.object_name}"
            for r in results
            if results and r.object_name
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
                        "total_objects": summary.total_tables,
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
            self.logger.error(f"Failed to log run summary: {str(e)}")

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

            # Check if storage credential replication is needed (only for replication operations)
            # Only check system-level uc_object_types
            if (
                self._operation_type == "replication"
                and self.config.uc_object_types
                and (
                    UCObjectType.STORAGE_CREDENTIAL in self.config.uc_object_types
                    or UCObjectType.ALL in self.config.uc_object_types
                )
            ):
                storage_credential_results = self._replicate_storage_credentials()
                self.completed_run_results.extend(storage_credential_results)
                if storage_credential_results:
                    self.audit_logger.log_results(storage_credential_results)
                if UCObjectType.ALL not in self.config.uc_object_types:
                    self.config.uc_object_types.remove(UCObjectType.STORAGE_CREDENTIAL)
                # immediately return if no other object types to process
                if len(self.config.uc_object_types) == 0:
                    summary = self.create_summary(
                        start_time, self.completed_run_results, operation_name
                    )
                    # Log final summary
                    self.log_run_summary(summary, operation_name)
                    return summary

            # Check if external location replication is needed (only for replication operations)
            # Only check system-level uc_object_types
            if (
                self._operation_type == "replication"
                and self.config.uc_object_types
                and (
                    UCObjectType.EXTERNAL_LOCATION in self.config.uc_object_types
                    or UCObjectType.ALL in self.config.uc_object_types
                )
            ):
                external_location_results = self._replicate_external_locations()
                self.completed_run_results.extend(external_location_results)
                if external_location_results:
                    self.audit_logger.log_results(external_location_results)
                if UCObjectType.ALL not in self.config.uc_object_types:
                    self.config.uc_object_types.remove(UCObjectType.EXTERNAL_LOCATION)
                # immediately return if no other object types to process
                if len(self.config.uc_object_types) == 0:
                    summary = self.create_summary(
                        start_time, self.completed_run_results, operation_name
                    )
                    # Log final summary
                    self.log_run_summary(summary, operation_name)
                    return summary

            # Get catalogs that need this operation
            enabled_catalogs = [
                catalog
                for catalog in self.config.target_catalogs
                if self.is_catalog_enabled(catalog)
            ]

            if not enabled_catalogs:
                self.logger.info(f"No catalogs configured for {operation_name}")
                return self.create_summary(start_time, self.completed_run_results, operation_name)

            self._run_catalog_operations(enabled_catalogs)


            summary = self.create_summary(start_time, self.completed_run_results, operation_name)

            # Log final summary
            self.log_run_summary(summary, operation_name)

            return summary

        except Exception as e:
            error_msg = f"{operation_name.title()} operation failed: {str(e)}"
            self.logger.error(error_msg)

            summary = self.create_summary(start_time, [], operation_name, error_msg)
            self.log_run_summary(summary, operation_name)

            return summary

    def _replicate_storage_credentials(self) -> List[RunResult]:
        """
        Replicate storage credentials before catalog operations.

        Returns:
            List of RunResult objects for storage credential operations
        """
        results = []

        # Check if storage credential config is provided
        if (
            not self.config.storage_credential_config
            or not self.config.storage_credential_config.mapping
        ):
            self.logger.info(
                "No storage credential configuration provided. Skipping storage credential replication."
            )
            return results

        try:
            self.logger.info(
                f"Replicating {len(self.config.storage_credential_config.mapping.keys())} storage credentials"
            )

            # Replicate each storage credential
            for (
                source_credential_name,
                target_principal_id,
            ) in self.config.storage_credential_config.mapping.items():
                start_time = datetime.now(timezone.utc)

                try:
                    target_credential_name = (
                        source_credential_name  # Use same name for target
                    )

                    self.logger.info(
                        f"Starting storage credential replication: {source_credential_name} -> {target_credential_name}"
                    )

                    # Get source storage credential info
                    source_credential_info = self.source_db_ops.get_storage_credential(
                        source_credential_name
                    )

                    # Build creation dict from source credential following same pattern as catalog replication
                    dict_for_creation = {
                        k: getattr(source_credential_info, k, None)
                        for k, v in source_credential_info.as_dict().items()
                        if k in DICT_FOR_CREATION_STORAGE_CREDENTIAL.keys()
                    }
                    dict_for_creation["name"] = target_credential_name
                    dict_for_creation["skip_validation"] = (
                        False  # Always validate on target
                    )

                    # Build update dict from source credential
                    dict_for_update = {
                        k: getattr(source_credential_info, k, None)
                        for k, v in source_credential_info.as_dict().items()
                        if k in DICT_FOR_UPDATE_STORAGE_CREDENTIAL.keys()
                    }
                    dict_for_update["name"] = target_credential_name

                    # Set the appropriate credential field based on target cloud type
                    if (
                        self.config.storage_credential_config.target_credential_type
                        == "aws"
                    ):
                        credential_request = AwsIamRoleRequest(
                            role_arn=target_principal_id
                        )
                        dict_for_creation["aws_iam_role"] = credential_request
                        dict_for_update["aws_iam_role"] = credential_request

                    if self.config.storage_credential_config.target_credential_type in (
                        "azure_access_connector",
                        "azure_managed_identity",
                    ):
                        credential_request = AzureManagedIdentityRequest(
                            access_connector_id=target_principal_id
                        )
                        dict_for_creation["azure_managed_identity"] = credential_request
                        dict_for_update["azure_managed_identity"] = credential_request

                    # Check if target storage credential already exists
                    credential_exists = False
                    target_credential_info = None
                    try:
                        target_credential_info = (
                            self.target_db_ops.get_storage_credential(
                                target_credential_name
                            )
                        )
                        credential_exists = True
                        self.logger.info(
                            f"Target storage credential {target_credential_name} already exists, will update properties"
                        )
                    except Exception:
                        # Credential doesn't exist, we'll create it
                        pass

                    if not credential_exists:
                        # Create storage credential
                        _ = self.target_db_ops.create_storage_credential(
                            dict_for_creation
                        )
                        self.logger.info(
                            f"Created storage credential {target_credential_name}"
                        )
                    else:
                        # Update existing storage credential - compare target and source dicts like catalog pattern
                        dict_for_update_target = {
                            k: getattr(target_credential_info, k, None)
                            for k, v in target_credential_info.as_dict().items()
                            if k in DICT_FOR_UPDATE_STORAGE_CREDENTIAL.keys()
                        }

                        if dict_for_update != dict_for_update_target:
                            _ = self.target_db_ops.update_storage_credential(
                                dict_for_update
                            )
                            self.logger.info(
                                f"Updated storage credential {target_credential_name}"
                            )
                        else:
                            self.logger.info(
                                f"Storage credential {target_credential_name} already up to date"
                            )

                    end_time = datetime.now(timezone.utc)
                    duration = (end_time - start_time).total_seconds()

                    self.logger.info(
                        f"Storage credential replication completed successfully: {source_credential_name} -> {target_credential_name} ({duration:.2f}s)"
                    )

                    results.append(
                        RunResult(
                            operation_type="uc_replication",
                            catalog_name=None,  # Storage credentials are not catalog-specific
                            schema_name=None,
                            object_name=target_credential_name,
                            object_type="storage_credential",
                            status="success",
                            start_time=start_time.isoformat(),
                            end_time=end_time.isoformat(),
                            duration_seconds=duration,
                            details={
                                "source_credential": source_credential_name,
                                "target_credential": target_credential_name,
                                "target_credential_type": self.config.storage_credential_config.target_credential_type,
                                "target_principal_id": target_principal_id,
                                "credential_existed": credential_exists,
                            },
                        )
                    )

                except Exception as e:
                    end_time = datetime.now(timezone.utc)
                    duration = (end_time - start_time).total_seconds()

                    error_msg = f"Failed to replicate storage credential {source_credential_name}: {str(e)}"
                    self.logger.error(error_msg)

                    results.append(
                        RunResult(
                            operation_type="uc_replication",
                            catalog_name=None,
                            schema_name=None,
                            object_name=source_credential_name,
                            object_type="storage_credential",
                            status="failed",
                            start_time=start_time.isoformat(),
                            end_time=end_time.isoformat(),
                            duration_seconds=duration,
                            error_message=error_msg,
                            details={
                                "source_credential": source_credential_name,
                                "target_credential": target_credential_name,
                                "target_credential_type": self.config.storage_credential_config.target_credential_type,
                                "target_principal_id": target_principal_id,
                            },
                        )
                    )

        except Exception as e:
            error_msg = f"Storage credential replication failed: {str(e)}"
            duration = (end_time - start_time).total_seconds()
            last_exception = e

            self.logger.error(
                error_msg, extra={"run_id": self.run_id, "operation": "uc_replication"}
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=None,
                    schema_name=None,
                    object_name=None,
                    object_type="storage_credential",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                )
            )
        return results

    def _replicate_external_locations(self) -> List[RunResult]:
        """
        Replicate external locations before catalog operations.

        Returns:
            List of RunResult objects for external location operations
        """
        results = []
        start_time = datetime.now(timezone.utc)
        end_time = start_time
        last_exception = None

        # Check if cloud_url_mapping is provided
        if not self.config.cloud_url_mapping:
            self.logger.info(
                "No cloud_url_mapping provided. Skipping external location replication."
            )
            return results

        try:
            # Get all external locations from source if specific locations aren't configured
            if (
                self.config.external_location_config
                and self.config.external_location_config.external_locations
            ):
                external_locations_to_replicate = (
                    self.config.external_location_config.external_locations
                )
                self.logger.info(
                    f"Replicating {len(external_locations_to_replicate)} configured external locations"
                )
            else:
                # Get all external locations from source and filter by cloud_url_mapping
                all_source_locations = self.source_db_ops.list_external_locations()
                external_locations_to_replicate = []

                for location in all_source_locations:
                    location_url = getattr(location, "url", None)
                    target_url = map_cloud_url(location_url, self.config.cloud_url_mapping)
                    if target_url:
                        external_locations_to_replicate.append(location.name)

                self.logger.info(
                    f"Found {len(external_locations_to_replicate)} external locations matching cloud_url_mapping"
                )

            # Replicate each external location
            for source_location_name in external_locations_to_replicate:
                start_time = datetime.now(timezone.utc)

                try:
                    target_location_name = (
                        source_location_name  # Use same name for target
                    )

                    self.logger.info(
                        f"Starting external location replication: {source_location_name} -> {target_location_name}"
                    )

                    # Get source external location info
                    source_location_info = self.source_db_ops.get_external_location(
                        source_location_name
                    )

                    # Build creation dict from source location following same pattern as catalog replication
                    dict_for_creation = {
                        k: getattr(source_location_info, k, None)
                        for k, v in source_location_info.as_dict().items()
                        if k in DICT_FOR_CREATION_EXTERNAL_LOCATION.keys()
                    }

                    # Build update dict from source location
                    dict_for_update = {
                        k: getattr(source_location_info, k, None)
                        for k, v in source_location_info.as_dict().items()
                        if k in DICT_FOR_UPDATE_EXTERNAL_LOCATION.keys()
                    }

                    # Map the URL using cloud_url_mapping
                    source_url = getattr(source_location_info, "url", None)
                    if source_url:
                        target_url = map_cloud_url(
                            source_url, self.config.cloud_url_mapping
                        )
                        if not target_url:
                            raise ValueError(
                                f"No URL mapping found for external location {source_location_name} with URL {source_url}"
                            )
                    else:
                        raise ValueError(
                            f"Source external location {source_location_name} has no URL"
                        )

                    dict_for_creation["name"] = target_location_name
                    dict_for_creation["url"] = target_url
                    dict_for_creation["skip_validation"] = (
                        False  # Always validate on target
                    )

                    dict_for_update["name"] = target_location_name
                    dict_for_update["url"] = target_url

                    # Check if target external location already exists
                    location_exists = False
                    target_location_info = None
                    try:
                        target_location_info = self.target_db_ops.get_external_location(
                            target_location_name
                        )
                        location_exists = True
                        self.logger.info(
                            f"Target external location {target_location_name} already exists, will update properties"
                        )
                    except Exception:
                        # Location doesn't exist, we'll create it
                        pass

                    if not location_exists:
                        # Create external location
                        _ = self.target_db_ops.create_external_location(
                            dict_for_creation
                        )
                        self.logger.info(
                            f"Created external location {target_location_name}"
                        )
                    else:
                        # Update existing external location - compare target and source dicts like catalog pattern
                        dict_for_update_target = {
                            k: getattr(target_location_info, k, None)
                            for k, v in target_location_info.as_dict().items()
                            if k in DICT_FOR_UPDATE_EXTERNAL_LOCATION.keys()
                        }

                        if dict_for_update != dict_for_update_target:
                            _ = self.target_db_ops.update_external_location(
                                dict_for_update
                            )
                            self.logger.info(
                                f"Updated external location {target_location_name}"
                            )
                        else:
                            self.logger.info(
                                f"External location {target_location_name} already up to date"
                            )

                    end_time = datetime.now(timezone.utc)
                    duration = (end_time - start_time).total_seconds()

                    self.logger.info(
                        f"External location replication completed successfully: {source_location_name} -> {target_location_name} ({duration:.2f}s)"
                    )

                    results.append(
                        RunResult(
                            operation_type="uc_replication",
                            catalog_name=None,  # External locations are not catalog-specific
                            schema_name=None,
                            object_name=target_location_name,
                            object_type="external_location",
                            status="success",
                            start_time=start_time.isoformat(),
                            end_time=end_time.isoformat(),
                            duration_seconds=duration,
                            details={
                                "source_location": source_location_name,
                                "target_location": target_location_name,
                                "source_url": source_url,
                                "target_url": target_url,
                                "location_existed": location_exists,
                            },
                        )
                    )

                except Exception as e:
                    end_time = datetime.now(timezone.utc)
                    duration = (end_time - start_time).total_seconds()

                    error_msg = f"Failed to replicate external location {source_location_name}: {str(e)}"
                    self.logger.error(error_msg)

                    results.append(
                        RunResult(
                            operation_type="uc_replication",
                            catalog_name=None,
                            schema_name=None,
                            object_name=source_location_name,
                            object_type="external_location",
                            status="failed",
                            start_time=start_time.isoformat(),
                            end_time=end_time.isoformat(),
                            duration_seconds=duration,
                            error_message=error_msg,
                            details={
                                "source_location": source_location_name,
                                "target_location": source_location_name,
                            },
                        )
                    )

        except Exception as e:
            error_msg = f"External location replication failed: {str(e)}"
            duration = (end_time - start_time).total_seconds()
            last_exception = e

            self.logger.error(
                error_msg, extra={"run_id": self.run_id, "operation": "uc_replication"}
            )
            if last_exception:
                error_msg += f" | Last error: {str(last_exception)}"

            results.append(
                RunResult(
                    operation_type="uc_replication",
                    catalog_name=None,
                    schema_name=None,
                    object_name=None,
                    object_type="external_location",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    duration_seconds=duration,
                    error_message=error_msg,
                )
            )
        return results

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

        # Process catalogs sequentially
        for catalog in catalogs:
            try:
                provider = self.create_provider(catalog)
                catalog_results = provider.process_catalog()
                results.extend(catalog_results)
                self.completed_run_results.extend(catalog_results)                

                successful = sum(
                    1
                    for r in catalog_results
                    if catalog_results and r.status == "success"
                )
                total = len(catalog_results) if catalog_results else 0

                self.logger.info(
                    f"Completed {operation_name} for catalog {catalog.catalog_name}: "
                    f"{successful}/{total} operations successful"
                )

            except Exception as e:
                error_msg = f"{operation_name.title()} failed for catalog {catalog.catalog_name}: {str(e)}"
                self.logger.error(error_msg)

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
