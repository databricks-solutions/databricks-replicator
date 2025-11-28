"""
Audit logger for data replication operations.

This module provides audit logging functionality that can be used across
different components to log operations to audit tables.
"""

import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from databricks.connect import DatabricksSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


from data_replication.audit.logger import DataReplicationLogger
from data_replication.databricks_operations import DatabricksOperations
from data_replication.utils import get_spark_current_user

from ..config.models import (
    RunResult,
)


class AuditLogger:
    """Audit logger for logging operations to database tables."""

    def __init__(
        self,
        spark: DatabricksSession,
        db_ops: DatabricksOperations,
        logger: DataReplicationLogger,
        run_id: str,
        create_audit_catalog: bool,
        audit_table: str,
        audit_catalog_location: Optional[str] = None,
        config_details: Optional[Dict[str, Any]] = None,
    ):
        """
        Initialize the audit logger and create audit table.

        Args:
            spark: Spark session for database operations
            logger: Logger instance for standard logging
            run_id: Unique run identifier
            audit_table: Full audit table name (catalog.schema.table)
            config_details: Full configuration object to be logged
        """
        self.spark = spark
        self.db_ops = db_ops
        self.logger = logger
        self.run_id = run_id
        self.create_audit_table = create_audit_catalog
        self.audit_table = audit_table
        self.audit_catalog_location = audit_catalog_location
        self.config_details_json = (
            json.dumps(config_details, default=str) if config_details else None
        )

        # Get current execution user using Spark SQL
        self.execution_user = get_spark_current_user(self.spark)

        # Create audit table during instantiation
        self._create_audit_table()

    def _create_audit_table(self) -> None:
        """
        Create audit log table for operations.

        Args:
            audit_table: Full audit table name (catalog.schema.table)
        """

        # Create audit catalog and schema
        audit_parts = self.audit_table.split(".")
        if len(audit_parts) >= 2:
            audit_catalog = audit_parts[0]
            audit_schema = audit_parts[1]

            if self.create_audit_table:
                try:
                    self.db_ops.create_catalog_if_not_exists(
                        audit_catalog, self.audit_catalog_location
                    )
                except Exception as e:
                    self.logger.error(f"Failed to create audit catalog {audit_catalog}")
                    raise e
            self.db_ops.create_schema_if_not_exists(audit_catalog, audit_schema)
        else:
            # Fallback to standard logging if audit table logging fails
            self.logger.warning(f"Invalid audit table name: {self.audit_table}. ")
            raise ValueError(
                f"Invalid audit table name: {self.audit_table}. "
                "Expected format: catalog.schema.table"
            )

        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {self.audit_table} (
            run_id STRING,
            logging_time TIMESTAMP,
            operation_type STRING,
            catalog_name STRING,
            schema_name STRING,
            object_name STRING,
            object_type STRING,
            status STRING,
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            duration_seconds DOUBLE,
            error_message STRING,
            details STRING,
            attempt_number INT,
            max_attempts INT,
            config_details STRING,
            execution_user STRING
        ) USING DELTA
        """
        self.logger.debug(f"Creating audit table with SQL: {create_table_sql}")
        self.spark.sql(create_table_sql)

    def log_results(self, results: List[RunResult]) -> None:
        """
        Log results in bulk to audit table using Spark DataFrame operations.

        Args:
            results: List of RunResult objects from operations
        """
        if not results:
            return

        audit_data = []
        current_time = datetime.now(timezone.utc)

        for result in results:
            try:
                details_str = None
                if result.details:
                    details_str = json.dumps(result.details)

                # Parse string timestamps back to datetime objects
                start_dt = datetime.fromisoformat(
                    result.start_time.replace("Z", "+00:00")
                )
                end_dt = datetime.fromisoformat(result.end_time.replace("Z", "+00:00"))
                duration = (end_dt - start_dt).total_seconds()

                audit_record = (
                    self.run_id,
                    current_time,
                    result.operation_type,
                    result.catalog_name,
                    result.schema_name or "",
                    result.object_name or "",
                    result.object_type or "",
                    result.status,
                    start_dt,
                    end_dt,
                    duration,
                    result.error_message,
                    details_str,
                    getattr(result, "attempt_number", 1),
                    getattr(result, "max_attempts", 1),
                    self.config_details_json,
                    self.execution_user,
                )
                audit_data.append(audit_record)

            except Exception as audit_error:
                table_info = (
                    f"{result.catalog_name}.{result.schema_name}.{result.object_name}"
                )
                self.logger.warning(
                    f"Failed to prepare audit log for {table_info}: {str(audit_error)}"
                )

        if not audit_data:
            self.logger.warning("No valid audit records to log")
            return

        # Define schema for bulk insert
        schema = StructType(
            [
                StructField("run_id", StringType(), True),
                StructField("logging_time", TimestampType(), True),
                StructField("operation_type", StringType(), True),
                StructField("catalog_name", StringType(), True),
                StructField("schema_name", StringType(), True),
                StructField("object_name", StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration_seconds", DoubleType(), True),
                StructField("error_message", StringType(), True),
                StructField("details", StringType(), True),
                StructField("attempt_number", IntegerType(), True),
                StructField("max_attempts", IntegerType(), True),
                StructField("config_details", StringType(), True),
                StructField("execution_user", StringType(), True),
            ]
        )

        try:
            audit_df = self.spark.createDataFrame(audit_data, schema)
            audit_df.write.mode("append").saveAsTable(self.audit_table)
            self.logger.debug(
                f"Successfully logged {len(audit_data)} records to audit table {self.audit_table}"
            )
        except Exception as e:
            self.logger.error(
                f"Failed to bulk log to audit table {self.audit_table}: {str(e)}"
            )
            raise e

    def log_operation(
        self,
        operation_type: str,
        catalog_name: str,
        schema_name: str,
        object_name: str,
        object_type: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        duration_seconds: float,
        error_message: Optional[str] = None,
        details: Optional[str] = None,
        attempt_number: int = 1,
        max_attempts: int = 1,
    ) -> None:
        """
        Log operation to audit table.

        Args:
            operation_type: Type of operation (backup, restore, etc.)
            catalog_name: Catalog name
            schema_name: Schema name
            object_name: Object name (table, view, etc.)
            object_type: Type of object (table, view, etc.)
            status: Operation status
            start_time: Operation start time
            end_time: Operation end time
            duration_seconds: Operation duration in seconds
            error_message: Error message if failed
            details: Additional details about the operation
            attempt_number: Current attempt number
            max_attempts: Total number of attempts
        """
        audit_data = [
            (
                self.run_id,
                datetime.now(timezone.utc),
                operation_type,
                catalog_name,
                schema_name,
                object_name,
                object_type,
                status,
                start_time,
                end_time,
                duration_seconds,
                error_message,
                details,
                attempt_number,
                max_attempts,
                self.config_details_json,
                self.execution_user,
            )
        ]

        # Define schema explicitly to avoid type inference issues
        schema = StructType(
            [
                StructField("run_id", StringType(), True),
                StructField("logging_time", TimestampType(), True),
                StructField("operation_type", StringType(), True),
                StructField("catalog_name", StringType(), True),
                StructField("schema_name", StringType(), True),
                StructField("object_name", StringType(), True),
                StructField("object_type", StringType(), True),
                StructField("status", StringType(), True),
                StructField("start_time", TimestampType(), True),
                StructField("end_time", TimestampType(), True),
                StructField("duration_seconds", DoubleType(), True),
                StructField("error_message", StringType(), True),
                StructField("details", StringType(), True),
                StructField("attempt_number", IntegerType(), True),
                StructField("max_attempts", IntegerType(), True),
                StructField("config_details", StringType(), True),
                StructField("execution_user", StringType(), True),
            ]
        )

        try:
            audit_df = self.spark.createDataFrame(audit_data, schema)
            audit_df.write.mode("append").saveAsTable(self.audit_table)
        except Exception as e:
            # Fallback to standard logging if audit table logging fails
            self.logger.warning(
                f"Failed to log to audit table {self.audit_table}: {str(e)}"
            )
