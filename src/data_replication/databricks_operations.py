"""
Databricks operations utility for common database DB operations.

This module provides utilities for interacting with Databricks catalogs,
schemas, and tables.
"""

from typing import Iterator, List, Optional
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    ExternalLocationInfo,
    SchemaInfo,
    StorageCredentialInfo,
    VolumeInfo,
)
from pyspark.sql.functions import col

from data_replication.audit.logger import DataReplicationLogger
from data_replication.config.models import (
    RetryConfig,
)
from data_replication.exceptions import TableNotFoundError
from data_replication.utils import (
    retry_with_logging,
    get_spark_current_user,
    get_spark_workspace_url,
)


class DatabricksOperations:
    """Utility class for Databricks operations."""

    def __init__(
        self,
        spark: DatabricksSession,
        logger: Optional[DataReplicationLogger] = None,
        workspace_client: Optional[WorkspaceClient] = None,
    ):
        """
        Initialize Databricks operations.

        Args:
            spark: DatabricksSession instance
            logger: Optional logger for SQL statement logging
            workspace_client: Optional WorkspaceClient for catalog operations
        """
        self.spark = spark
        self.logger = logger
        self.workspace_client = workspace_client

    def _execute_sql(self, sql_query: str, operation_context: str = ""):
        """
        Execute SQL with debug logging.

        Args:
            sql_query: The SQL query to execute
            operation_context: Context description for the operation

        Returns:
            Result of the SQL execution
        """
        if self.logger:
            current_user = get_spark_current_user(self.spark)
            workspace_url = get_spark_workspace_url(self.spark)
            self.logger.debug(f"Workspace: {workspace_url}, User: {current_user}")
            self.logger.debug(
                f"Executing SQL{f' ({operation_context})' if operation_context else ''}: {sql_query}"
            )

        start_time = time.time()
        try:
            result = self.spark.sql(sql_query)
            execution_time = time.time() - start_time

            if self.logger:
                self.logger.debug(
                    f"SQL execution completed{f' ({operation_context})' if operation_context else ''} "
                    f"in {execution_time:.3f}s"
                )

            return result
        except Exception as e:
            execution_time = time.time() - start_time

            if self.logger:
                self.logger.debug(
                    f"SQL execution failed{f' ({operation_context})' if operation_context else ''} "
                    f"after {execution_time:.3f}s: {str(e)}"
                )
            raise

    def execute_query(self, sql_query: str, operation_context: str = ""):
        """
        Execute SQL query.

        Args:
            sql_query: The SQL query to execute
            operation_context: Context description for the operation

        Returns:
            Result of the SQL execution
        """
        return self._execute_sql(sql_query, operation_context)

    def get_current_user(self) -> str:
        """
        Get the current user executing the Spark session.

        Returns:
            Current user as a string
        """
        try:
            current_user = self.spark.sql("SELECT current_user()").collect()[0][0]
            return current_user
        except Exception as e:
            raise Exception(f"Failed to get current user: {str(e)}") from e

    def get_metastore_id(self) -> str:
        """
        Get the metastore ID of the current Databricks workspace.

        Returns:
            Metastore ID as a string
        """
        try:
            metastore_id = self.spark.sql("SELECT current_metastore()").collect()[0][0]
            return metastore_id
        except Exception as e:
            raise Exception(f"Failed to get metastore ID: {str(e)}") from e

    def if_catalog_exists(self, catalog_name: str) -> bool:
        """
        Check if catalog exists.

        Args:
            catalog_name: Name of the catalog to check
        Returns:
            True if catalog exists, False otherwise
        """
        try:
            catalogs_df = self.spark.sql(f"SHOW CATALOGS LIKE '{catalog_name}'")
            return not catalogs_df.isEmpty()
        except Exception:
            return False

    def if_schema_exists(self, catalog_name: str, schema_name: str) -> bool:
        """
        Check if schema exists.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema to check
        Returns:
            True if catalog exists, False otherwise
        """
        schema_name = f"`{catalog_name}`.`{schema_name}`"
        return self.spark.catalog.databaseExists(schema_name)

    def create_catalog_if_not_exists(
        self, catalog_name: str, catalog_location: str
    ) -> None:
        """
        Create catalog if it doesn't exist.

        Args:
            catalog_name: Name of the catalog to create
            catalog_location: Location of the catalog
        """
        try:
            if catalog_location:
                self._execute_sql(
                    f"CREATE CATALOG IF NOT EXISTS `{catalog_name}` MANAGED LOCATION '{catalog_location}'",
                    f"create catalog {catalog_name}",
                )
            else:
                self._execute_sql(
                    f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`",
                    f"create catalog {catalog_name}",
                )
        except Exception as e:
            raise Exception(f"""Failed to create catalog: {str(e)}""") from e

    def create_catalog_using_share_if_not_exists(
        self, catalog_name: str, provider_name: str, share_name: str
    ) -> None:
        """
        Create catalog if it doesn't exist.

        Args:
            catalog_name: Name of the catalog to create
            provider_name: Name of the provider
            share_name: Name of the share
        """
        try:
            self._execute_sql(
                f"CREATE CATALOG IF NOT EXISTS `{catalog_name}` USING SHARE `{provider_name}`.`{share_name}`",
                f"create catalog {catalog_name} using share",
            )
        except Exception as e:
            raise Exception(f"""
                            Failed to create catalog: {str(e)} using share `{provider_name}`.`{share_name}`
            """) from e

    def create_schema_if_not_exists(self, catalog_name: str, schema_name: str) -> None:
        """
        Create schema if it doesn't exist.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema to create
        """
        try:
            full_schema = f"`{catalog_name}`.`{schema_name}`"
            self._execute_sql(
                f"CREATE SCHEMA IF NOT EXISTS {full_schema}",
                f"create schema {catalog_name}.{schema_name}",
            )
        except Exception as e:
            raise Exception(f"""Failed to create schema: {str(e)}""") from e

    def get_tables_in_schema(self, catalog_name: str, schema_name: str) -> List[str]:
        """
        Get all tables in a schema

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema

        Returns:
            List of table names
        """
        try:
            full_schema = f"`{catalog_name}`.`{schema_name}`"

            # Get visible tables using SHOW TABLES (excludes internal tables)
            show_tables_df = self.spark.sql(f"SHOW TABLES IN {full_schema}").filter(
                "isTemporary == false"
            )
            return [row.tableName for row in show_tables_df.collect()]

        except Exception:
            # Schema might not exist or be accessible
            return []

    def filter_tables_by_type(
        self,
        catalog_name: str,
        schema_name: str,
        table_names: List[str],
        table_types: List[str],
        parallel_table_filter: int = 8,
    ) -> List[str]:
        """
        Filter a list of table names to only include selected types using multithreading.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_names: List of table names to filter
            table_types: List of table types to filter by
            parallel_table_filter: Maximum number of threads to use for parallel processing

        Returns:
            List of table names that are of the selected types
        """

        if table_types is None or len(table_types) == 0 or len(table_names) == 0:
            return []

        def check_table_type(table_name: str) -> tuple[str, bool]:
            """Check if table type is in allowed types"""
            try:
                full_table_name = f"`{catalog_name}`.`{schema_name}`.`{table_name}`"
                table_type = self.get_table_type(full_table_name).lower()
                return table_name, table_type in table_types
            except Exception:
                # If we can't determine the type, exclude the table
                return table_name, False

        filtered_tables = []

        # Use ThreadPoolExecutor for parallel processing
        with ThreadPoolExecutor(max_workers=parallel_table_filter) as executor:
            # Submit all table type checks
            future_to_table = {
                executor.submit(check_table_type, table_name): table_name
                for table_name in table_names
            }

            # Collect results as they complete
            for future in as_completed(future_to_table):
                table_name, is_included = future.result(timeout=60)
                if is_included:
                    filtered_tables.append(table_name)
        executor.shutdown(wait=True)
        return filtered_tables

    def get_all_schemas(self, catalog_name: str) -> List[str]:
        """
        Get all schemas in a catalog.

        Args:
            catalog_name: Name of the catalog

        Returns:
            List of schema names
        """
        try:
            schemas_df = self.spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").filter(
                'databaseName != "information_schema"'
            )
            return [row.databaseName for row in schemas_df.collect()]
        except Exception:
            # Catalog might not exist or be accessible
            return []

    @retry_with_logging(retry_config=RetryConfig(max_attempts=2, retry_delay_seconds=2))
    def refresh_schema_metadata(self, schema_name: str) -> bool:
        """
        Check if a schema exists.

        Args:
            schema_name: Full schema name (catalog.schema)

        Returns:
            True if schema exists, False otherwise
        """
        # Retry to refresh schema metadata in delta share catalog
        return self.spark.catalog.databaseExists(f"`{schema_name}`")

    def get_schemas_by_filter(
        self, catalog_name: str, filter_expression: str
    ) -> List[str]:
        """
        Get schemas matching a filter expression.

        Args:
            catalog_name: Name of the catalog
            filter_expression: SQL filter expression

        Returns:
            List of schema names matching the filter
        """
        try:
            # Get all schemas first
            schemas_df = (
                self.spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`")
                .filter('databaseName != "information_schema"')
                .withColumnRenamed("databaseName", "schema_name")
            )

            # Apply filter expression
            filtered_df = schemas_df.filter(filter_expression)

            return [row.schema_name for row in filtered_df.collect()]
        except Exception as e:
            print(f"Warning: Could not filter schemas in `{catalog_name}`: {e}")
            return []

    def get_tables_by_filter(
        self, catalog_name: str, schema_name: str, filter_expression: str
    ) -> List[str]:
        """
        Get tables matching a filter expression.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            filter_expression: SQL filter expression

        Returns:
            List of table names matching the filter
        """
        try:
            full_schema = f"`{catalog_name}`.`{schema_name}`"

            # Get all tables first
            tables_df = (
                self.spark.sql(f"SHOW TABLES IN {full_schema}")
                .filter("isTemporary == false")
                .withColumnRenamed("tableName", "table_name")
                .withColumnRenamed("database", "schema_name")
            )

            # Apply filter expression
            filtered_df = tables_df.filter(filter_expression)

            return [row.table_name for row in filtered_df.collect()]
        except Exception as e:
            print(
                f"Warning: Could not filter tables in `{catalog_name}`.`{schema_name}`: {e}"
            )
            return []

    def get_schema_tables_by_filter(
        self, catalog_name: str, schema_table_filter_expression: str
    ) -> List[dict]:
        """
        Get schema and table combinations matching a filter expression from information_schema.tables.

        Args:
            catalog_name: Name of the catalog
            schema_table_filter_expression: SQL filter expression to apply on schema_name and table_name

        Returns:
            List of dictionaries with 'schema_name' and 'table_name' keys
        """
        try:
            if not schema_table_filter_expression:
                self.logger.debug(
                    "No schema-table filter expression provided, returning empty list"
                )
                return []
            # Query the information_schema.tables view with the provided filter
            query = f"""
                SELECT table_schema as schema_name, table_name
                FROM {catalog_name}.information_schema.tables
                WHERE {schema_table_filter_expression}
            """

            self.logger.debug(f"Executing schema-table filter query: {query}")

            # Execute the query using Spark
            result_df = self.spark.sql(query).filter(
                "table_name not like '__materialization_mat%' and table_name not like 'event_log_%'"
            )
            schema_table_combinations = result_df.collect()

            # Convert to list of dictionaries
            schema_tables = [
                {"schema_name": row["schema_name"], "table_name": row["table_name"]}
                for row in schema_table_combinations
            ]

            return schema_tables

        except Exception as e:
            self.logger.error(
                f"Failed to get schema-table combinations using filter expression in catalog {catalog_name}: {str(e)}"
            )
            return []

    def describe_table_detail(self, table_name: str) -> dict:
        """
        Get detailed information about a table.

        Args:
            table_name: Full table name (catalog.schema.table)

        Returns:
            Dictionary containing table details
        """
        try:
            details = (
                self.spark.sql(f"DESCRIBE DETAIL {table_name}").collect()[0].asDict()
            )
            return details

        except Exception:
            details_str = (
                self.spark.sql(f"DESCRIBE EXTENDED {table_name}")
                .filter(col("col_name") == "Table Properties")
                .select("data_type")
                .collect()[0][0]
            )
            properties = {}
            result_clean = details_str.replace("[", "").replace("]", "")
            properties = dict(
                item.split("=") for item in result_clean.split(",") if "=" in item
            )
            return {"properties": properties}

    @retry_with_logging(retry_config=RetryConfig(max_attempts=2, retry_delay_seconds=2))
    def refresh_table_metadata(self, table_name: str) -> bool:
        """
        Check if a table exists.

        Args:
            table_name: Full table name (catalog.schema.table)

        Returns:
            True if table exists, False otherwise
        """
        # Retry to refresh table metadata in delta share catalog
        return self.spark.catalog.tableExists(table_name)

    def get_table_type(self, table_name) -> str:
        """
        get the type of a table from shared catalog or normal catalog
        """
        pipeline_id = None
        table_type = None
        # Refresh table metadata before checking if it exists
        self.refresh_table_metadata(table_name)

        if self.spark.catalog.tableExists(table_name):
            # First check if it's a view using DESCRIBE EXTENDED to avoid DESCRIBE DETAIL error
            try:
                table_type = (
                    self.spark.sql(f"DESCRIBE EXTENDED {table_name}")
                    .filter(
                        (col("col_name") == "Type")
                        & (
                            (col("data_type").contains("MANAGED"))
                            | (col("data_type").contains("EXTERNAL"))
                            | (col("data_type").contains("STREAMING_TABLE"))
                            | (col("data_type").contains("MATERIALIZED_VIEW"))
                            | (col("data_type").contains("VIEW"))
                        )
                    )
                    .select("data_type")
                    .collect()[0][0]
                )
                if table_type != "MANAGED":
                    return table_type
            except Exception:
                pass
            # when it's Managed, check if it's STREAMING_TABLE as it may be a backup table
            pipeline_id = (
                self.spark.sql(f"DESCRIBE DETAIL {table_name}")
                .collect()[0]
                .asDict()["properties"]
                .get("pipelines.pipelineId", None)
            )
            if pipeline_id:
                table_type = "STREAMING_TABLE"
                return table_type

            # when it's Managed, check if it's EXTERNAL
            # as it may be an external table when delta shared
            location = (
                self.spark.sql(f"DESCRIBE DETAIL {table_name}")
                .collect()[0]
                .asDict()["location"]
            )
            if location and "/tables/" in location:
                table_type = "MANAGED"
                return table_type

            return "EXTERNAL"

        raise TableNotFoundError(f"Table {table_name} does not exist")

    def drop_table_if_exists(self, table_name: str) -> None:
        """
        Drop table if it exists.

        Args:
            table_name: Full table name (catalog.schema.table)
        """
        try:
            self._execute_sql(
                f"DROP TABLE IF EXISTS {table_name}", f"drop table {table_name}"
            )
        except Exception as e:
            raise Exception(f"""Failed to drop table: {str(e)}""") from e

    def get_pipeline_id(self, table_name: str):
        """
        get pipleline id from table properties

        Args:
            table_name: table full name (catalog.schema.table)
        Returns:
            pipeline id if exists else None
            parent table id if exists else None
        """

        table_details = self.describe_table_detail(table_name)

        return table_details["properties"].get(
            "pipelines.pipelineId", None
        ), table_details["properties"].get(
            "spark.sql.internal.pipelines.parentTableId", None
        )

    def get_table_details(self, table_name: str) -> dict:
        """
        Get actual table name, whether it's a DLT table, and pipeline ID.
        Args:
            table_name: Full table name (catalog.schema.table)
        Returns:
        Dictionary with keys:
            - table_name: Actual table name to use for deep clone
            - is_dlt: Boolean indicating if it's a DLT table
            - pipeline_id: Pipeline ID if applicable, else None
        """
        if self.spark.catalog.tableExists(table_name):
            pipeline_id, parent_table_id = self.get_pipeline_id(table_name)
            if pipeline_id:
                dlt_type = None
                # Handle streaming table or materialized view
                actual_table_name, dlt_type = self._get_internal_table_name(
                    table_name, pipeline_id
                )
                return {
                    "table_name": actual_table_name.lower(),
                    "is_dlt": True,
                    "dlt_type": dlt_type,
                    "pipeline_id": pipeline_id,
                    "parent_table_id": parent_table_id,
                }

            # If not a DLT table, just return the original table name and "delta"
            return {
                "table_name": table_name.lower(),
                "is_dlt": False,
                "dlt_type": None,
                "pipeline_id": None,
                "parent_table_id": None,
            }
        else:
            raise TableNotFoundError(f"Table {table_name} does not exist")

    def _if_internal_table_exists(self, table_name: str) -> bool:
        """
        Check if internal table exists.

        Args:
            table_name: Full internal table name (catalog.schema.table)
            dlt_type: Type of DLT table ("dpm" or "legacy")
        Returns:
            True if internal table exists, False otherwise
        """
        try:
            return self.spark.catalog.tableExists(table_name)
        except Exception as e:
            if "UnauthorizedAccessException".upper() in str(e).upper():
                return True
            raise e

    def _get_internal_table_name(self, table_name: str, pipeline_id: str):
        """
        Get the internal table name for streaming tables or materialized views.

        Args:
            table_name: Original table name
            pipeline_id: Pipeline ID for the table

        Returns:
            Internal table name for deep clone
        """

        # Extract table name from full table name
        table_name_only = table_name.split(".")[-1].replace("`", "")

        # Construct internal table name
        pipeline_id_underscores = pipeline_id.replace("-", "_")
        internal_table_name = (
            f"__materialization_mat_{pipeline_id_underscores}_{table_name_only}_1"
        )

        # Construct internal schema name
        internal_schema_name = f"__dlt_materialization_schema_{pipeline_id_underscores}"

        # Get catalog from original table name
        catalog_name = table_name.split(".")[0].replace("`", "")

        # Check possible locations for the internal table 1 - dpm dlt backing table location
        schema_name = table_name.split(".")[1].replace("`", "")
        full_internal_table_name = (
            f"`{catalog_name}`.`{schema_name}`.`{internal_table_name}`"
        )
        if self._if_internal_table_exists(full_internal_table_name):
            return full_internal_table_name, "dpm"

        # Check possible locations for the internal table 2 - legacy dlt backing table location 1
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{table_name_only}`"
        )
        if self._if_internal_table_exists(full_internal_table_name):
            return full_internal_table_name, "legacy"

        # Check possible locations for the internal table 3 - legacy dlt backing table location 2
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{internal_table_name}`"
        )
        if self._if_internal_table_exists(full_internal_table_name):
            return full_internal_table_name, "legacy"

        # Check possible locations for the internal table 4 - legacy dlt backing table location 3
        internal_table_name = f"__materialization_mat_{table_name_only}_1"
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{internal_table_name}`"
        )
        if self._if_internal_table_exists(full_internal_table_name):
            return full_internal_table_name, "legacy"

        raise Exception(
            f"Could not find internal table for {table_name} with pipeline ID {pipeline_id}"
        )

    def get_common_fields(self, source_table: str, target_table: str) -> List[str]:
        """
        Get common fields between source and target tables.

        Args:
            source_table: Full source table name (catalog.schema.table)
            target_table: Full target table name (catalog.schema.table)

        Returns:
            List of common field names
        """
        source_fields = set(self.get_table_fields(source_table))
        target_fields = set(self.get_table_fields(target_table))
        return list(source_fields & target_fields)

    def get_table_fields(self, table_name: str) -> List[str]:
        """
        Get field names of a table.

        Args:
            table_name: Full table name (catalog.schema.table)

        Returns:
            List of field names
        """
        try:
            df = self.spark.table(table_name)
            return df.columns
        except Exception as e:
            print(f"Warning: Could not get fields for table {table_name}: {e}")
            return []

    def get_recipient_name(self, sharing_identifier: str) -> str:
        """
        Get delta share recipient name.

        Returns:
            recipient_name if exists else None
        """
        try:
            # check if the recipient exists
            select_sql = f"""SELECT recipient_name FROM
                    system.information_schema.recipients
                    WHERE authentication_type = 'DATABRICKS'
                    AND data_recipient_global_metastore_id = '{sharing_identifier}'
                    """
            df = self.spark.sql(select_sql)
            # return recipient name if exists
            if not df.isEmpty():
                return df.collect()[0][0]
            return None

        except Exception as e:
            raise Exception(f"""Failed to get recipient name: {str(e)}""") from e

    def create_recipient(self, sharing_identifier: str, recipient_name: str) -> str:
        """
        Create delta share recipient.

        Args:
            sharing_identifier: ID of the recipient to grant permissions to
            recipient_name: Name of the recipient to grant permissions to

        Returns:
            Name of the created or existing recipient

        Raises:
            Exception: If recipient creation fails
        """
        try:
            # return recipient name if exists
            existing_recipient = self.get_recipient_name(sharing_identifier)
            if existing_recipient:
                return existing_recipient

            create_query = f"CREATE RECIPIENT IF NOT EXISTS `{recipient_name}` USING ID '{sharing_identifier}'"
            self._execute_sql(create_query, f"create recipient {recipient_name}")
            return recipient_name

        except Exception as e:
            raise Exception(
                f"""Failed to create recipient `{recipient_name}` for '{sharing_identifier}': {str(e)}"""
            ) from e

    def create_delta_share(self, share_name: str, recipient_name: str) -> None:
        """
        Create delta share and grant permissions to recipient.

        Args:
            share_name: Name of the share to create
            recipient_name: Name of the recipient to grant permissions to

        Raises:
            Exception: If share creation or permission granting fails
        """
        try:
            # Create the share
            create_share_query = f"CREATE SHARE IF NOT EXISTS `{share_name}`"
            self._execute_sql(create_share_query, f"create share {share_name}")

            # Grant SELECT and READ_VOLUME permissions to recipient
            grant_select_query = (
                f"GRANT SELECT ON SHARE `{share_name}` TO RECIPIENT `{recipient_name}`"
            )

            self._execute_sql(
                grant_select_query, f"grant permissions on share {share_name}"
            )

        except Exception as e:
            raise Exception(
                f"""Failed to create delta share `{share_name}` or grant permissions to `{recipient_name}`: {str(e)}"""
            ) from e

    def is_schema_in_share(
        self, share_name: str, catalog_name: str, schema_name: str
    ) -> bool:
        """
        Check if schema is already in the delta share.

        Args:
            share_name: Name of the share
            catalog_name: Name of the catalog containing the schema
            schema_name: Name of the schema to check

        Returns:
            True if schema is in share, False otherwise
        """
        try:
            # Default schema is always included in shares
            if schema_name == "default":
                return True
            show_share_query = f"SHOW ALL IN SHARE `{share_name}`"
            full_schema_name = f"{catalog_name}.{schema_name}"
            result = (
                self.spark.sql(show_share_query)
                .filter(f'''
                        `shared_object` = "{full_schema_name}"
                        and `type` = "SCHEMA"''')
                .count()
            )

            if result > 0:
                return True
            return False

        except Exception as e:
            print(
                f"""Warning: Could not check if schema `{catalog_name}.{schema_name}` is in share `{share_name}`: {e}"""
            )
            return False

    def is_table_in_share(self, share_name: str, table_name: str) -> bool:
        """
        Check if table is already in the delta share.

        Args:
            share_name: Name of the share
            catalog_name: Name of the catalog containing the schema
            table_name: Name of the table to check

        Returns:
            True if table is in share, False otherwise
        """
        try:
            full_table_name = ""
            show_share_query = f"SHOW ALL IN SHARE `{share_name}`"
            full_table_name = table_name.replace("`", "")
            result = (
                self.spark.sql(show_share_query)
                .filter(f'''
                        `shared_object` = "{full_table_name}"
                        and `type` = "TABLE"''')
                .count()
            )

            if result > 0:
                return True
            return False

        except Exception as e:
            print(
                f"""Warning: Could not check if table {full_table_name} is in share `{share_name}`: {e}"""
            )
            return False

    def add_schema_to_share(
        self, share_name: str, catalog_name: str, schema_name: str
    ) -> None:
        """
        Add schema to delta share if not already present.

        Args:
            share_name: Name of the share
            catalog_name: Name of the catalog containing the schema
            schema_name: Name of the schema to add to the share

        Raises:
            Exception: If adding schema to share fails
        """
        try:
            # Check if schema is already in the share
            if self.is_schema_in_share(share_name, catalog_name, schema_name):
                return

            full_schema_name = f"`{catalog_name}`.`{schema_name}`"
            add_schema_query = (
                f"ALTER SHARE `{share_name}` ADD SCHEMA {full_schema_name}"
            )
            self._execute_sql(
                add_schema_query,
                f"add schema {catalog_name}.{schema_name} to share {share_name}",
            )

        except Exception as e:
            raise Exception(
                f"""Failed to add schema `{catalog_name}.{schema_name}` to share `{share_name}`: {str(e)}"""
            ) from e

    def get_shared_tables(self, share_name: str) -> List[str]:
        """
        Get all tables in a delta share.

        Args:
            share_name: Name of the share
        Returns:
            List of table names in the share
        """
        try:
            query = f"select concat(catalog_name, '.', shared_as_schema, '.', shared_as_table) as table_name from system.information_schema.table_share_usage where share_name = '{share_name}'"
            result_df = self.spark.sql(query)
            return [row.table_name for row in result_df.collect()]

        except Exception as e:
            raise Exception(
                f"""Failed to get shared tables in share `{share_name}`: {str(e)}"""
            ) from e

    def get_provider_name(self, sharing_identifier: str) -> str:
        """
        Get delta share provider name.

        Returns:
            provider_name if exists else None
        """
        try:
            # check if the provider exists
            select_sql = f"""SELECT provider_name FROM
                    system.information_schema.providers
                    WHERE authentication_type = 'DATABRICKS'
                    AND data_provider_global_metastore_id = '{sharing_identifier}'
                    """
            df = self.spark.sql(select_sql)
            # return provider name if exists
            if not df.isEmpty():
                return df.collect()[0][0]
            return None

        except Exception as e:
            raise Exception(f"""Failed to get provider name: {str(e)}""") from e

    def if_volume_exists(self, volume_name: str) -> bool:
        """
        Check if volume exists.

        Args:
            volume_name: Full volume name (catalog.schema.volume)
        Returns:
            True if vollume exists, False otherwise
        """
        try:
            # Check if volume exists by attempting to describe it
            self.spark.sql(f"DESCRIBE VOLUME {volume_name}")
            return True
        except Exception:
            return False

    def get_volumes_in_schema(self, catalog_name: str, schema_name: str) -> List[str]:
        """
        Get all volumes in a schema.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema

        Returns:
            List of volume names
        """
        try:
            full_schema = f"`{catalog_name}`.`{schema_name}`"

            # Get visible volumes using SHOW VOLUMES
            show_volumes_df = self.spark.sql(f"SHOW VOLUMES IN {full_schema}")
            return [row.volume_name for row in show_volumes_df.collect()]

        except Exception:
            # Schema might not exist or be accessible
            return []

    def filter_volumes_by_type(
        self,
        catalog_name: str,
        schema_name: str,
        volume_names: List[str],
        volume_types: List[str],
    ) -> List[str]:
        """
        Filter a list of volume names to only include selected types.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            volume_names: List of volume names to filter
            volume_types: List of volume types to filter by

        Returns:
            List of volume names that are of the selected types
        """

        if volume_types is None or len(volume_types) == 0:
            return []
        return [
            volume
            for volume in volume_names
            if self.get_volume_type(
                f"`{catalog_name}`.`{schema_name}`.`{volume}`"
            ).lower()
            in [vol_type.lower() for vol_type in volume_types]
        ]

    def get_volume_type(self, volume_name: str) -> str:
        """
        Get the type of a volume.

        Args:
            volume_name: Full volume name (catalog.schema.volume)

        Returns:
            Volume type as string
        """
        try:
            self.refresh_volume_metadata(volume_name)
            # Use DESCRIBE VOLUME to get volume information
            describe_df = self.spark.sql(f"DESCRIBE VOLUME {volume_name}")

            # Look for the volume type in the describe output
            volume_type = describe_df.select("volume_type").collect()[0][0]
            if volume_type:
                return volume_type.upper()
            return None

        except Exception as e:
            print(f"Warning: Could not determine volume type for {volume_name}: {e}")
            return None

    @retry_with_logging(retry_config=RetryConfig(max_attempts=2, retry_delay_seconds=2))
    def refresh_volume_metadata(self, volume_name: str) -> bool:
        """
        Check if a volume exists.

        Args:
            volume_name: Full volume name (catalog.schema.volume)

        Returns:
            True if volume exists, False otherwise
        """
        try:
            # Check if volume exists by attempting to describe it
            self.spark.sql(f"DESCRIBE VOLUME {volume_name}")
            return True
        except Exception:
            return False

    def get_catalog_tags(self, catalog_name):
        """
        Get tags associated with a catalog.
        """
        tag_names_list = None
        tag_maps_list = None
        df = self.spark.sql(f"""
            select collect_list(b.tag_name) as tag_names_list, collect_list(map(b.tag_name,trim(b.tag_value))) as tag_maps_list 
            from system.information_schema.catalogs as a inner join system.information_schema.catalog_tags as b 
            on a.catalog_name=b.catalog_name
            where a.catalog_name = '{catalog_name}'
            group by a.catalog_name
            """)
        if not df.isEmpty():
            tag_names_list = df.collect()[0]["tag_names_list"]
            tag_maps_list = df.collect()[0]["tag_maps_list"]

        return tag_names_list, tag_maps_list

    def get_schema_tags(self, catalog_name, schema_name):
        """
        Get tags associated with a schema.
        """
        tag_names_list = None
        tag_maps_list = None
        df = self.spark.sql(f"""
            select collect_list(b.tag_name) as tag_names_list, collect_list(map(b.tag_name,trim(b.tag_value))) as tag_maps_list 
            from system.information_schema.schemata as a inner join system.information_schema.schema_tags as b 
            on a.catalog_name=b.catalog_name and a.schema_name=b.schema_name
            where a.schema_name = '{schema_name}' and a.catalog_name = '{catalog_name}'
            group by a.catalog_name, a.schema_name
            """)
        if not df.isEmpty():
            tag_names_list = df.collect()[0]["tag_names_list"]
            tag_maps_list = df.collect()[0]["tag_maps_list"]

        return tag_names_list, tag_maps_list

    def get_table_tags(self, catalog_name, schema_name, table_name):
        """
        Get tags associated with a table.
        """
        tag_names_list = None
        tag_maps_list = None
        df = self.spark.sql(f"""
            select collect_list(b.tag_name) as tag_names_list, collect_list(map(b.tag_name,trim(b.tag_value))) as tag_maps_list 
            from system.information_schema.tables as a inner join system.information_schema.table_tags as b 
            on a.table_catalog=b.catalog_name and a.table_schema=b.schema_name and a.table_name=b.table_name
            where a.table_name = '{table_name}' and a.table_schema = '{schema_name}' and a.table_catalog = '{catalog_name}'
            group by a.table_catalog, a.table_schema, a.table_name
            """)
        if not df.isEmpty():
            tag_names_list = df.collect()[0]["tag_names_list"]
            tag_maps_list = df.collect()[0]["tag_maps_list"]

        return tag_names_list, tag_maps_list

    def get_volume_tags(self, catalog_name, schema_name, volume_name):
        """
        Get tags associated with a volume.
        """
        tag_names_list = None
        tag_maps_list = None
        df = self.spark.sql(f"""
            select collect_list(b.tag_name) as tag_names_list, collect_list(map(b.tag_name,trim(b.tag_value))) as tag_maps_list 
            from system.information_schema.volumes as a inner join system.information_schema.volume_tags as b 
            on a.volume_catalog=b.catalog_name and a.volume_schema=b.schema_name and a.volume_name=b.volume_name
            where a.volume_name = '{volume_name}' and a.volume_schema = '{schema_name}' and a.volume_catalog = '{catalog_name}'
            group by a.volume_catalog, a.volume_schema, a.volume_name
            """)
        if not df.isEmpty():
            tag_names_list = df.collect()[0]["tag_names_list"]
            tag_maps_list = df.collect()[0]["tag_maps_list"]

        return tag_names_list, tag_maps_list

    def get_column_tags_df(self, catalog_name, schema_name, table_name):
        """
        Get tags associated with columns of a table.
        """

        df = self.spark.sql(f"""
            select a.table_catalog, a.table_schema, a.table_name,a.column_name, 
            collect_list(b.tag_name) as tag_names_list, collect_list(map(b.tag_name,trim(b.tag_value))) as tag_maps_list 
            from system.information_schema.columns as a inner join system.information_schema.column_tags as b 
            on a.table_catalog=b.catalog_name and a.table_schema=b.schema_name and a.table_name=b.table_name and a.column_name=b.column_name
            where a.table_name = '{table_name}' and a.table_schema = '{schema_name}' and a.table_catalog = '{catalog_name}'
            group by a.table_catalog, a.table_schema, a.table_name,a.column_name
            """)

        return df

    def get_column_comments(self, catalog_name, schema_name, table_name):
        """
        Get comments associated with columns of a table.
        """
        comment_maps_list = self.spark.sql(f"""
            select collect_list(map(column_name,trim(coalesce(comment, ''))))as comment_maps_list from system.information_schema.columns
            where table_catalog = '{catalog_name}' and table_schema = '{schema_name}' and table_name = '{table_name}'
            group by table_catalog, table_catalog, table_name""").collect()[0][0]
        return comment_maps_list

    def get_table_comments(self, catalog_name, schema_name, table_name):
        """
        Get table comments
        """
        table_comment = self.spark.sql(f"""
            select comment from system.information_schema.tables
            where table_catalog = '{catalog_name}' and table_schema = '{schema_name}' and table_name = '{table_name}'
            """).collect()[0][0]

        return table_comment

    def get_catalog(self, catalog_name: str) -> CatalogInfo:
        """
        Get source catalog info using workspace client.

        Args:
            catalog_name: Name of the catalog to get

        Returns:
            CatalogInfo containing catalog information

        Raises:
            Exception: If getting catalog fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for catalog operations")

        try:
            source_catalog_info = self.workspace_client.catalogs.get(catalog_name)
            return source_catalog_info
        except Exception as e:
            raise Exception(
                f"Failed to get source catalog {catalog_name}: {str(e)}"
            ) from e

    def create_catalog(self, catalog_config: dict) -> CatalogInfo:
        """
        Create catalog using workspace client.

        Args:
            catalog_config: Dictionary containing catalog creation parameters

        Returns:
            CatalogInfo containing created catalog information

        Raises:
            Exception: If catalog creation fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for catalog operations")

        try:
            created_catalog = self.workspace_client.catalogs.create(**catalog_config)
            return created_catalog
        except Exception as e:
            raise Exception(f"Failed to create catalog: {str(e)}") from e

    def update_catalog(self, catalog_config: dict) -> CatalogInfo:
        """
        Update catalog using workspace client.

        Args:
            catalog_config: Dictionary containing catalog update parameters

        Returns:
            CatalogInfo containing updated catalog information

        Raises:
            Exception: If catalog update fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for catalog operations")

        try:
            updated_catalog = self.workspace_client.catalogs.update(**catalog_config)
            return updated_catalog
        except Exception as e:
            raise Exception(f"Failed to update catalog: {str(e)}") from e

    def get_schema(self, full_name: str) -> SchemaInfo:
        """
        Get schema information using workspace client.

        Args:
            full_name: Full name of the schema (catalog.schema)

        Returns:
            SchemaInfo containing schema information

        Raises:
            Exception: If getting schema fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for schema operations")

        try:
            schema_info = self.workspace_client.schemas.get(full_name)
            return schema_info
        except Exception as e:
            raise Exception(f"Failed to get schema {full_name}: {str(e)}") from e

    def create_schema(self, schema_config: dict) -> SchemaInfo:
        """
        Create schema using workspace client.

        Args:
            schema_config: Dictionary containing schema creation parameters

        Returns:
            SchemaInfo containing created schema information

        Raises:
            Exception: If schema creation fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for schema operations")

        try:
            created_schema = self.workspace_client.schemas.create(**schema_config)
            return created_schema
        except Exception as e:
            raise Exception(f"Failed to create schema: {str(e)}") from e

    def update_schema(self, schema_config: dict) -> SchemaInfo:
        """
        Update schema using workspace client.

        Args:
            schema_config: Dictionary containing schema update parameters

        Returns:
            SchemaInfo containing updated schema information

        Raises:
            Exception: If schema update fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for schema operations")

        try:
            updated_schema = self.workspace_client.schemas.update(**schema_config)
            return updated_schema
        except Exception as e:
            raise Exception(f"Failed to update schema: {str(e)}") from e

    def get_volume(self, full_name: str) -> VolumeInfo:
        """
        Get volume information using workspace client.

        Args:
            full_name: Full name of the volume (catalog.schema.volume)

        Returns:
            VolumeInfo containing volume information

        Raises:
            Exception: If getting volume fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for volume operations")

        try:
            volume_info = self.workspace_client.volumes.read(full_name)
            return volume_info
        except Exception as e:
            raise Exception(f"Failed to get volume {full_name}: {str(e)}") from e

    def create_volume(self, volume_config: dict) -> VolumeInfo:
        """
        Create volume using workspace client.

        Args:
            volume_config: Dictionary containing volume creation parameters

        Returns:
            VolumeInfo containing created volume information

        Raises:
            Exception: If volume creation fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for volume operations")

        try:
            created_volume = self.workspace_client.volumes.create(**volume_config)
            return created_volume
        except Exception as e:
            raise Exception(f"Failed to create volume: {str(e)}") from e

    def update_volume(self, volume_config: dict) -> VolumeInfo:
        """
        Update volume using workspace client.

        Args:
            volume_config: Dictionary containing volume update parameters

        Returns:
            VolumeInfo containing updated volume information

        Raises:
            Exception: If volume update fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for volume operations")

        try:
            updated_volume = self.workspace_client.volumes.update(**volume_config)
            return updated_volume
        except Exception as e:
            raise Exception(f"Failed to update volume: {str(e)}") from e

    def get_storage_credential(self, credential_name: str) -> StorageCredentialInfo:
        """
        Get source storage credential info using workspace client.

        Args:
            credential_name: Name of the storage credential to get

        Returns:
            StorageCredentialInfo containing storage credential information

        Raises:
            Exception: If getting catalog fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception("WorkspaceClient is required for catalog operations")

        try:
            source_storage_credential_info = (
                self.workspace_client.storage_credentials.get(credential_name)
            )
            return source_storage_credential_info
        except Exception as e:
            raise Exception(
                f"Failed to get source storage credential {credential_name}: {str(e)}"
            ) from e

    def create_storage_credential(
        self, storage_credential_config: dict
    ) -> StorageCredentialInfo:
        """
        Create storage credential using workspace client.

        Args:
            storage_credential_config: Dictionary containing storage credential creation parameters

        Returns:
            StorageCredentialInfo containing created storage credential information

        Raises:
            Exception: If storage credential creation fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception(
                "WorkspaceClient is required for storage credential operations"
            )

        try:
            created_storage_credential = (
                self.workspace_client.storage_credentials.create(
                    **storage_credential_config
                )
            )
            return created_storage_credential
        except Exception as e:
            raise Exception(f"Failed to create storage credential: {str(e)}") from e

    def update_storage_credential(
        self, storage_credential_config: dict
    ) -> StorageCredentialInfo:
        """
        Update storage credential using workspace client.

        Args:
            storage_credential_config: Dictionary containing storage credential update parameters

        Returns:
            StorageCredentialInfo containing updated storage credential information

        Raises:
            Exception: If storage credential update fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception(
                "WorkspaceClient is required for storage credential operations"
            )

        try:
            updated_storage_credential = (
                self.workspace_client.storage_credentials.update(
                    **storage_credential_config
                )
            )
            return updated_storage_credential
        except Exception as e:
            raise Exception(f"Failed to update storage credential: {str(e)}") from e

    def list_external_locations(self) -> Iterator[ExternalLocationInfo]:
        """
        List all external locations using workspace client.

        Returns:
            List of ExternalLocationInfo objects

        Raises:
            Exception: If listing external locations fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception(
                "WorkspaceClient is required for external location operations"
            )

        try:
            external_locations = list(self.workspace_client.external_locations.list())
            return external_locations
        except Exception as e:
            raise Exception(f"Failed to list external locations: {str(e)}") from e

    def get_external_location(self, location_name: str) -> ExternalLocationInfo:
        """
        Get external location info using workspace client.

        Args:
            location_name: Name of the external location to get

        Returns:
            ExternalLocationInfo containing external location information

        Raises:
            Exception: If getting external location fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception(
                "WorkspaceClient is required for external location operations"
            )

        try:
            external_location_info = self.workspace_client.external_locations.get(
                location_name
            )
            return external_location_info
        except Exception as e:
            raise Exception(
                f"Failed to get external location {location_name}: {str(e)}"
            ) from e

    def create_external_location(
        self, external_location_config: dict
    ) -> ExternalLocationInfo:
        """
        Create external location using workspace client.

        Args:
            external_location_config: Dictionary containing external location creation parameters

        Returns:
            ExternalLocationInfo containing created external location information

        Raises:
            Exception: If external location creation fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception(
                "WorkspaceClient is required for external location operations"
            )

        try:
            created_external_location = self.workspace_client.external_locations.create(
                **external_location_config
            )
            return created_external_location
        except Exception as e:
            raise Exception(f"Failed to create external location: {str(e)}") from e

    def update_external_location(
        self, external_location_config: dict
    ) -> ExternalLocationInfo:
        """
        Update external location using workspace client.

        Args:
            external_location_config: Dictionary containing external location update parameters

        Returns:
            ExternalLocationInfo containing updated external location information

        Raises:
            Exception: If external location update fails or workspace_client is None
        """
        if not self.workspace_client:
            raise Exception(
                "WorkspaceClient is required for external location operations"
            )

        try:
            updated_external_location = self.workspace_client.external_locations.update(
                **external_location_config
            )
            return updated_external_location
        except Exception as e:
            raise Exception(f"Failed to update external location: {str(e)}") from e

    def show_create_table_ddl(self, table_name: str) -> str:
        """
        Get the CREATE TABLE DDL statement for a given table.

        Args:
            table_name: Full table name (catalog.schema.table)
        Returns:
            CREATE TABLE DDL statement as a string
        """
        try:
            ddl_df = self.spark.sql(f"SHOW CREATE TABLE {table_name}")
            ddl_statement = "\n".join(row["createtab_stmt"] for row in ddl_df.collect())
            return ddl_statement
        except Exception as e:
            raise Exception(
                f"Failed to get CREATE TABLE DDL for {table_name}: {str(e)}"
            ) from e
