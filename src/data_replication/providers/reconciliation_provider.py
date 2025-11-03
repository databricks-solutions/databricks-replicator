"""
Reconciliation provider implementation for data replication system.

This module handles reconciliation operations including schema checks,
row count validation, and missing data detection between source and target tables.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List

from ..config.models import RunResult
from ..exceptions import ReconciliationError, TableNotFoundError
from ..utils import retry_with_logging
from .base_provider import BaseProvider


class ReconciliationProvider(BaseProvider):
    """Provider for reconciliation operations between source and target tables."""

    def get_operation_name(self) -> str:
        """Get the name of the operation for logging purposes."""
        return "reconciliation"

    def is_operation_enabled(self) -> bool:
        """Check if the reconciliation operation is enabled in the configuration."""
        return (
            self.catalog_config.reconciliation_config
            and self.catalog_config.reconciliation_config.enabled
        )

    def process_table(self, schema_name: str, table_name: str) -> RunResult:
        """Process a single table for reconciliation."""
        return self._reconcile_table(schema_name, table_name)

    def _get_filtered_table_reference(self, table_name: str, is_source: bool) -> str:
        """Get table reference with optional filter applied."""
        reconciliation_config = self.catalog_config.reconciliation_config

        if is_source and reconciliation_config.source_filter_expression:
            return f"(SELECT * FROM {table_name} WHERE {reconciliation_config.source_filter_expression})"
        elif not is_source and reconciliation_config.target_filter_expression:
            return f"(SELECT * FROM {table_name} WHERE {reconciliation_config.target_filter_expression})"
        else:
            return table_name

    def setup_operation_catalogs(self) -> str:
        """Setup reconciliation-specific catalogs."""
        reconciliation_config = self.catalog_config.reconciliation_config
        if reconciliation_config.create_recon_catalog:
            self.logger.info(
                f"""Creating recon result catalog: {reconciliation_config.recon_outputs_catalog} at location: {reconciliation_config.recon_catalog_location}"""
            )
            self.db_ops.create_catalog_if_not_exists(
                reconciliation_config.recon_outputs_catalog,
                reconciliation_config.recon_catalog_location,
            )

        # Create source catalog from share if needed
        if reconciliation_config.create_shared_catalog:
            provider_name = self.db_ops.get_provider_name(
                self.source_databricks_config.sharing_identifier
            )
            self.logger.info(
                f"""Creating source catalog from share: {reconciliation_config.source_catalog} using share name: {reconciliation_config.share_name}"""
            )
            self.db_ops.create_catalog_using_share_if_not_exists(
                reconciliation_config.source_catalog,
                provider_name,
                reconciliation_config.share_name,
            )
        return reconciliation_config.source_catalog

    def process_schema_concurrently(
        self, schema_name: str, table_list: List
    ) -> List[RunResult]:
        """Override to add reconciliation-specific schema setup."""
        reconciliation_config = self.catalog_config.reconciliation_config

        # Ensure reconciliation_results schema exists for consolidated tables
        self.db_ops.create_schema_if_not_exists(
            reconciliation_config.recon_outputs_catalog,
            reconciliation_config.recon_outputs_schema,
        )

        return super().process_schema_concurrently(schema_name, table_list)

    def _reconcile_table(
        self,
        schema_name: str,
        table_name: str,
    ) -> RunResult:
        """
        Reconcile a single table between source and target.

        Args:
            schema_name: Schema name
            table_name: Table name to reconcile

        Returns:
            RunResult object for the reconciliation operation
        """
        start_time = datetime.now(timezone.utc)
        reconciliation_config = self.catalog_config.reconciliation_config
        source_catalog = reconciliation_config.source_catalog
        target_catalog = self.catalog_config.catalog_name
        source_table = f"{source_catalog}.{schema_name}.{table_name}"
        target_table = f"{target_catalog}.{schema_name}.{table_name}"

        self.logger.info(
            f"Starting reconciliation: {source_table} vs {target_table}",
            extra={"run_id": self.run_id, "operation": "reconciliation"},
        )

        source_table_type = None
        try:
            # Refresh target table metadata
            self.db_ops.refresh_table_metadata(target_table)

            # Check if source table exists
            if not self.spark.catalog.tableExists(source_table):
                raise TableNotFoundError(f"Source table does not exist: {source_table}")

            # Get source table type for audit logging
            source_table_type = self.db_ops.get_table_type(source_table)

            reconciliation_results = {}
            failed_checks = []
            skipped_checks = []

            # Use custom retry decorator with logging
            @retry_with_logging(self.retry, self.logger)
            def reconciliation_operation(query: str):
                return self.spark.sql(query)

            # Flag to track if we should continue with remaining checks
            continue_checks = True

            # Schema check
            if reconciliation_config.schema_check:
                schema_start_time = datetime.now(timezone.utc)
                self.logger.info(
                    f"Starting schema check for {source_table} vs {target_table}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )
                schema_result = self._perform_schema_check(
                    source_table,
                    target_table,
                    reconciliation_operation,
                )
                schema_end_time = datetime.now(timezone.utc)
                schema_duration = (schema_end_time - schema_start_time).total_seconds()

                self.logger.info(
                    f"Schema check completed for {source_table} vs {target_table} "
                    f"({schema_duration:.2f}s) - {'PASSED' if schema_result['passed'] else 'FAILED'}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )

                reconciliation_results["schema_check"] = schema_result
                if not schema_result["passed"]:
                    failed_checks.append("schema_check")
                    continue_checks = False
                    self.logger.warning(
                        f"Schema check failed for {source_table} vs {target_table}. Skipping remaining checks.",
                        extra={
                            "run_id": self.run_id,
                            "operation": "reconciliation",
                        },
                    )

            # Row count check
            if reconciliation_config.row_count_check and continue_checks:
                row_count_start_time = datetime.now(timezone.utc)
                self.logger.info(
                    f"Starting row count check for {source_table} vs {target_table}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )
                row_count_result = self._perform_row_count_check(
                    source_table,
                    target_table,
                    reconciliation_operation,
                )
                row_count_end_time = datetime.now(timezone.utc)
                row_count_duration = (
                    row_count_end_time - row_count_start_time
                ).total_seconds()

                self.logger.info(
                    f"Row count check completed for {source_table} vs {target_table} "
                    f"({row_count_duration:.2f}s) - {'PASSED' if row_count_result['passed'] else 'FAILED'}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )

                reconciliation_results["row_count_check"] = row_count_result
                if not row_count_result["passed"]:
                    failed_checks.append("row_count_check")
                    continue_checks = False
                    self.logger.warning(
                        f"Row count check failed for {source_table} vs {target_table}. Skipping remaining checks.",
                        extra={
                            "run_id": self.run_id,
                            "operation": "reconciliation",
                        },
                    )
            elif reconciliation_config.row_count_check and not continue_checks:
                skipped_checks.append("row_count_check")
                reconciliation_results["row_count_check"] = {
                    "passed": None,
                    "status": "skipped",
                    "reason": "Previous check failed",
                }

            # Missing data check
            if reconciliation_config.missing_data_check and continue_checks:
                missing_data_start_time = datetime.now(timezone.utc)
                self.logger.info(
                    f"Starting missing data check for {source_table} vs {target_table}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )
                missing_data_result = self._perform_missing_data_check(
                    source_table,
                    target_table,
                    reconciliation_operation,
                )
                missing_data_end_time = datetime.now(timezone.utc)
                missing_data_duration = (
                    missing_data_end_time - missing_data_start_time
                ).total_seconds()

                self.logger.info(
                    f"Missing data check completed for {source_table} vs {target_table} "
                    f"({missing_data_duration:.2f}s) - {'PASSED' if missing_data_result['passed'] else 'FAILED'}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )

                reconciliation_results["missing_data_check"] = missing_data_result
                if not missing_data_result["passed"]:
                    failed_checks.append("missing_data_check")
            elif reconciliation_config.missing_data_check and not continue_checks:
                skipped_checks.append("missing_data_check")
                reconciliation_results["missing_data_check"] = {
                    "passed": None,
                    "status": "skipped",
                    "reason": "Previous check failed",
                }

            # Log skipped checks
            if skipped_checks:
                self.logger.info(
                    f"Skipped checks for {source_table} vs {target_table}: {skipped_checks}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )

            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            if not failed_checks:
                self.logger.info(
                    f"Reconciliation passed all checks: {source_table} vs {target_table} "
                    f"({duration:.2f}s)",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )

                # Collect check durations for audit logging
                check_durations = {}
                if (
                    reconciliation_config.schema_check
                    and "schema_check" in reconciliation_results
                ):
                    check_durations["schema_check_duration_seconds"] = schema_duration
                if (
                    reconciliation_config.row_count_check
                    and "row_count_check" in reconciliation_results
                ):
                    check_durations["row_count_check_duration_seconds"] = (
                        row_count_duration
                    )
                if (
                    reconciliation_config.missing_data_check
                    and "missing_data_check" in reconciliation_results
                ):
                    check_durations["missing_data_check_duration_seconds"] = (
                        missing_data_duration
                    )

                return RunResult(
                    operation_type="reconciliation",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=table_name,
                    object_type="table",
                    status="success",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    details={
                        "source_table": source_table,
                        "target_table": target_table,
                        "table_type": source_table_type,
                        "reconciliation_results": reconciliation_results,
                        "failed_checks": failed_checks,
                        "skipped_checks": skipped_checks,
                        "check_durations": check_durations,
                    },
                )
            else:
                error_msg = f"Reconciliation failed checks: {failed_checks}"
                self.logger.error(
                    f"Reconciliation failed: {source_table} vs {target_table} - {error_msg}",
                    extra={
                        "run_id": self.run_id,
                        "operation": "reconciliation",
                    },
                )

                # Collect check durations for audit logging (for failed case)
                check_durations = {}
                if (
                    reconciliation_config.schema_check
                    and "schema_check" in reconciliation_results
                ):
                    check_durations["schema_check_duration_seconds"] = locals().get(
                        "schema_duration", 0.0
                    )
                if (
                    reconciliation_config.row_count_check
                    and "row_count_check" in reconciliation_results
                ):
                    check_durations["row_count_check_duration_seconds"] = locals().get(
                        "row_count_duration", 0.0
                    )
                if (
                    reconciliation_config.missing_data_check
                    and "missing_data_check" in reconciliation_results
                ):
                    check_durations["missing_data_check_duration_seconds"] = (
                        locals().get("missing_data_duration", 0.0)
                    )

                return RunResult(
                    operation_type="reconciliation",
                    catalog_name=target_catalog,
                    schema_name=schema_name,
                    object_name=table_name,
                    object_type="table",
                    status="failed",
                    start_time=start_time.isoformat(),
                    end_time=end_time.isoformat(),
                    error_message=error_msg,
                    details={
                        "source_table": source_table,
                        "target_table": target_table,
                        "table_type": source_table_type,
                        "reconciliation_results": reconciliation_results,
                        "failed_checks": failed_checks,
                        "skipped_checks": skipped_checks,
                        "check_durations": check_durations,
                    },
                )

        except Exception as e:
            end_time = datetime.now(timezone.utc)
            duration = (end_time - start_time).total_seconds()

            # Wrap in ReconciliationError for better error categorization
            if not isinstance(e, ReconciliationError):
                e = ReconciliationError(f"Reconciliation operation failed: {str(e)}")

            error_msg = f"Failed to reconcile table {source_table}: {str(e)}"
            self.logger.error(
                error_msg,
                extra={"run_id": self.run_id, "operation": "reconciliation"},
                exc_info=True,
            )

            return RunResult(
                operation_type="reconciliation",
                catalog_name=target_catalog,
                schema_name=schema_name,
                table_name=table_name,
                status="failed",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                duration_seconds=duration,
                error_message=error_msg,
                details={
                    "source_table": source_table,
                    "target_table": target_table,
                    "table_type": source_table_type,
                },
            )

    def _perform_schema_check(
        self,
        source_table: str,
        target_table: str,
        reconciliation_operation,
    ) -> Dict[str, Any]:
        """Perform schema comparison between source and target tables."""
        try:
            # Use consolidated schema comparison table
            reconciliation_config = self.catalog_config.reconciliation_config
            schema_comparison_table = f"{reconciliation_config.recon_outputs_catalog}.{reconciliation_config.recon_outputs_schema}.recon_schema_comparison"

            source_catalog = source_table.split(".")[0]
            target_catalog = target_table.split(".")[0]
            source_schema = source_table.split(".")[1]
            target_schema = target_table.split(".")[1]
            source_tbl = source_table.split(".")[2]
            target_tbl = target_table.split(".")[2]

            # Build exclude columns filter
            exclude_columns = reconciliation_config.exclude_columns or []
            exclude_filter = ""
            if exclude_columns:
                exclude_list = "', '".join(exclude_columns)
                exclude_filter = f"AND column_name NOT IN ('{exclude_list}')"

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {schema_comparison_table} (
                run_id STRING,
                target_catalog STRING,
                target_schema STRING,
                target_table STRING,
                source_catalog STRING,
                source_schema STRING,
                source_table STRING,
                column_name STRING,
                source_data_type STRING,
                target_data_type STRING,
                source_nullable STRING,
                target_nullable STRING,
                comparison_result STRING,
                check_timestamp TIMESTAMP
            ) USING DELTA
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                create_table_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Failed to create schema comparison table: {last_exception}",
                    "output_table": schema_comparison_table,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                }

            # Insert only unmatched records
            schema_insert_query = f"""
            INSERT INTO {schema_comparison_table}
            WITH source_schema_info AS (
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM {source_catalog}.information_schema.columns
                WHERE table_name = '{source_tbl}'
                  AND table_schema = '{source_schema}'
                  AND table_catalog = '{source_catalog}'
                  {exclude_filter}
            ),
            target_schema_info AS (
                SELECT
                    column_name,
                    data_type,
                    is_nullable,
                    column_default,
                    ordinal_position
                FROM {target_catalog}.information_schema.columns
                WHERE table_name = '{target_tbl}'
                  AND table_schema = '{target_schema}'
                  AND table_catalog = '{target_catalog}'
                  {exclude_filter}
            ),
            schema_diff AS (
                SELECT
                    COALESCE(s.column_name, t.column_name) as column_name,
                    s.data_type as source_data_type,
                    t.data_type as target_data_type,
                    s.is_nullable as source_nullable,
                    t.is_nullable as target_nullable,
                    CASE
                        WHEN s.column_name IS NULL THEN 'missing_in_source'
                        WHEN t.column_name IS NULL THEN 'missing_in_target'
                        WHEN s.data_type != t.data_type THEN 'type_mismatch'
                        WHEN s.is_nullable != t.is_nullable THEN 'nullable_mismatch'
                        ELSE 'match'
                    END as comparison_result
                FROM source_schema_info s
                FULL OUTER JOIN target_schema_info t ON s.column_name = t.column_name
            )
            SELECT
                '{self.run_id}' as run_id,
                '{target_catalog}' as target_catalog,
                '{target_schema}' as target_schema,
                '{target_tbl}' as target_table,
                '{source_catalog}' as source_catalog,
                '{source_schema}' as source_schema,
                '{source_tbl}' as source_table,
                column_name,
                source_data_type,
                target_data_type,
                source_nullable,
                target_nullable,
                comparison_result,
                current_timestamp() as check_timestamp
            FROM schema_diff
            WHERE comparison_result != 'match'
            ORDER BY column_name
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                schema_insert_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Schema check insert failed: {last_exception}",
                    "output_table": schema_comparison_table,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                }

            # Check if there are any mismatches for this specific table
            mismatch_count_df = self.spark.sql(
                f"""
                SELECT COUNT(*) as mismatch_count
                FROM {schema_comparison_table}
                WHERE run_id = '{self.run_id}'
                  AND target_catalog = '{target_catalog}'
                  AND target_schema = '{target_schema}'
                  AND target_table = '{target_tbl}'
                  AND source_catalog = '{source_catalog}'
                  AND source_schema = '{source_schema}'
                  AND source_table = '{source_tbl}'
            """
            )

            mismatch_count = mismatch_count_df.collect()[0]["mismatch_count"]

            return {
                "passed": mismatch_count == 0,
                "mismatch_count": mismatch_count,
                "output_table": schema_comparison_table,
                "details": "Schema check completed successfully",
                "attempt": attempt,
                "max_attempts": max_attempts,
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Schema check failed: {str(e)}",
                "output_table": schema_comparison_table,
                "attempt": attempt,
                "max_attempts": max_attempts,
            }

    def _perform_row_count_check(
        self,
        source_table: str,
        target_table: str,
        reconciliation_operation,
    ) -> Dict[str, Any]:
        """Perform row count comparison between source and target tables."""
        try:
            reconciliation_config = self.catalog_config.reconciliation_config

            # Get filtered table references
            source_table_ref = self._get_filtered_table_reference(source_table, True)
            target_table_ref = self._get_filtered_table_reference(target_table, False)

            # Get row counts directly without creating a table
            row_count_query = f"""
            WITH source_count AS (
                SELECT COUNT(*) as source_row_count FROM {source_table_ref}
            ),
            target_count AS (
                SELECT COUNT(*) as target_row_count FROM {target_table_ref}
            )
            SELECT
                source_row_count,
                target_row_count,
                target_row_count - source_row_count as row_count_diff,
                CASE
                    WHEN source_row_count = target_row_count THEN 'match'
                    ELSE 'mismatch'
                END as comparison_result
            FROM source_count, target_count
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                row_count_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Row count check query failed: {last_exception}",
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                }

            # Get the comparison result directly
            comparison_result = result.collect()[0]

            return {
                "passed": comparison_result["comparison_result"] == "match",
                "source_count": comparison_result["source_row_count"],
                "target_count": comparison_result["target_row_count"],
                "difference": comparison_result["row_count_diff"],
                "source_filter": reconciliation_config.source_filter_expression,
                "target_filter": reconciliation_config.target_filter_expression,
                "details": "Row count check completed successfully",
                "attempt": attempt,
                "max_attempts": max_attempts,
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Row count check failed: {str(e)}",
                "attempt": attempt,
                "max_attempts": max_attempts,
            }

    def _perform_missing_data_check(
        self,
        source_table: str,
        target_table: str,
        reconciliation_operation,
    ) -> Dict[str, Any]:
        """Perform missing data check between source and target tables."""
        try:
            # Use consolidated missing data comparison table
            reconciliation_config = self.catalog_config.reconciliation_config
            missing_data_table = f"{reconciliation_config.recon_outputs_catalog}.{reconciliation_config.recon_outputs_schema}.recon_missing_data_comparison"

            source_catalog = source_table.split(".")[0]
            target_catalog = target_table.split(".")[0]
            source_schema = source_table.split(".")[1]
            target_schema = target_table.split(".")[1]
            source_tbl = source_table.split(".")[2]
            target_tbl = target_table.split(".")[2]

            # Get common columns between source and target
            common_fields = self.db_ops.get_common_fields(source_table, target_table)

            # Exclude specified columns from reconciliation
            exclude_columns = reconciliation_config.exclude_columns or []
            if exclude_columns:
                common_fields = [
                    field for field in common_fields if field not in exclude_columns
                ]

            if not common_fields:
                return {
                    "passed": False,
                    "error": "No common fields found between source and target tables after exclusions",
                    "output_table": missing_data_table,
                }

            # Create table if it doesn't exist
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {missing_data_table} (
                run_id STRING,
                target_catalog STRING,
                target_schema STRING,
                target_table STRING,
                source_catalog STRING,
                source_schema STRING,
                source_table STRING,
                issue_type STRING,
                data MAP<STRING, STRING>,
                check_timestamp TIMESTAMP
            ) USING DELTA
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                create_table_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Failed to create missing data comparison table: {last_exception}",
                    "output_table": missing_data_table,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                }

            # Get filtered table references
            source_table_ref = self._get_filtered_table_reference(source_table, True)
            target_table_ref = self._get_filtered_table_reference(target_table, False)

            # Create a hash-based comparison for data content and insert only mismatched records
            field_list = "`" + "`,`".join(common_fields) + "`"
            field_map = ", ".join(
                [f"'{field}', CAST(`{field}` AS STRING)" for field in common_fields]
            )

            missing_data_insert_query = f"""
            INSERT INTO {missing_data_table}
            WITH source_hashes AS (
                SELECT
                    hash({field_list}) as row_hash,
                    map({field_map}) as data_map,
                    {field_list}
                FROM {source_table_ref}
            ),
            target_hashes AS (
                SELECT
                    hash({field_list}) as row_hash,
                    map({field_map}) as data_map,
                    {field_list}
                FROM {target_table_ref}
            ),
            missing_in_target AS (
                SELECT
                    'missing_in_target' as issue_type,
                    s.data_map as data
                FROM source_hashes s
                LEFT JOIN target_hashes t ON s.row_hash = t.row_hash
                WHERE t.row_hash IS NULL
            ),
            missing_in_source AS (
                SELECT
                    'missing_in_source' as issue_type,
                    t.data_map as data
                FROM target_hashes t
                LEFT JOIN source_hashes s ON t.row_hash = s.row_hash
                WHERE s.row_hash IS NULL
            )
            SELECT
                '{self.run_id}' as run_id,
                '{target_catalog}' as target_catalog,
                '{target_schema}' as target_schema,
                '{target_tbl}' as target_table,
                '{source_catalog}' as source_catalog,
                '{source_schema}' as source_schema,
                '{source_tbl}' as source_table,
                issue_type,
                data,
                current_timestamp() as check_timestamp
            FROM missing_in_target
            UNION ALL
            SELECT
                '{self.run_id}' as run_id,
                '{target_catalog}' as target_catalog,
                '{target_schema}' as target_schema,
                '{target_tbl}' as target_table,
                '{source_catalog}' as source_catalog,
                '{source_schema}' as source_schema,
                '{source_tbl}' as source_table,
                issue_type,
                data,
                current_timestamp() as check_timestamp
            FROM missing_in_source
            """

            result, last_exception, attempt, max_attempts = reconciliation_operation(
                missing_data_insert_query
            )

            if not result:
                return {
                    "passed": False,
                    "error": f"Missing data check insert failed: {last_exception}",
                    "output_table": missing_data_table,
                    "attempt": attempt,
                    "max_attempts": max_attempts,
                }

            # Get summary of missing data for this specific table
            summary_df = self.spark.sql(
                f"""
                SELECT
                    issue_type,
                    COUNT(*) as row_count
                FROM {missing_data_table}
                WHERE run_id = '{self.run_id}'
                  AND target_catalog = '{target_catalog}'
                  AND target_schema = '{target_schema}'
                  AND target_table = '{target_tbl}'
                  AND source_catalog = '{source_catalog}'
                  AND source_schema = '{source_schema}'
                  AND source_table = '{source_tbl}'
                GROUP BY issue_type
            """
            )

            comparison_results = summary_df.collect()
            total_missing = sum(row["row_count"] for row in comparison_results)

            missing_breakdown = {
                row["issue_type"]: row["row_count"] for row in comparison_results
            }

            return {
                "passed": total_missing == 0,
                "total_missing_rows": total_missing,
                "missing_breakdown": missing_breakdown,
                "common_fields_count": len(common_fields),
                "output_table": missing_data_table,
                "details": "Missing data check completed successfully",
                "attempt": attempt,
                "max_attempts": max_attempts,
            }

        except Exception as e:
            return {
                "passed": False,
                "error": f"Missing data check failed: {str(e)}",
                "output_table": missing_data_table,
                "attempt": attempt,
                "max_attempts": max_attempts,
            }
