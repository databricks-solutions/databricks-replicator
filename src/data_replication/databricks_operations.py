"""
Databricks operations utility for common database DB operations.

This module provides utilities for interacting with Databricks catalogs,
schemas, and tables.
"""

from typing import List, Tuple

from databricks.connect import DatabricksSession
from pyspark.sql.functions import col

from data_replication.config.models import RetryConfig, TableType, VolumeType
from data_replication.exceptions import TableNotFoundError
from data_replication.utils import retry_with_logging


class DatabricksOperations:
    """Utility class for Databricks operations."""

    def __init__(self, spark: DatabricksSession):
        """
        Initialize Databricks operations.

        Args:
            spark: DatabricksSession instance
        """
        self.spark = spark

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
                self.spark.sql(
                    f"CREATE CATALOG IF NOT EXISTS `{catalog_name}` MANAGED LOCATION '{catalog_location}'"
                )
            else:
                self.spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog_name}`")
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
            self.spark.sql(
                f"CREATE CATALOG IF NOT EXISTS `{catalog_name}` USING SHARE `{provider_name}`.`{share_name}`"
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
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS {full_schema}")
        except Exception as e:
            raise Exception(f"""Failed to create schema: {str(e)}""") from e

    def get_tables_in_schema(self, catalog_name: str, schema_name: str) -> List[str]:
        """
        Get all tables in a schema, including STREAMING_TABLE and MANAGED table types.

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
        table_types: List[TableType],
    ) -> List[str]:
        """
        Filter a list of table names to only include selected types.

        Args:
            catalog_name: Name of the catalog
            schema_name: Name of the schema
            table_names: List of table names to filter

        Returns:
            List of table names that are of the selected types
        """

        return [
            table
            for table in table_names
            if self.get_table_type(
                f"`{catalog_name}`.`{schema_name}`.`{table}`"
            ).lower()
            in [type.lower() for type in table_types]
        ]

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

    @retry_with_logging(retry_config=RetryConfig(retries=5, delay=3))
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
            schemas_df = self.spark.sql(f"SHOW SCHEMAS IN `{catalog_name}`").filter(
                'databaseName != "information_schema"'
            )

            # Apply filter expression
            filtered_df = schemas_df.filter(filter_expression)

            return [row.databaseName for row in filtered_df.collect()]
        except Exception as e:
            print(f"Warning: Could not filter schemas in `{catalog_name}`: {e}")
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

    @retry_with_logging(retry_config=RetryConfig(retries=5, delay=3))
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

            # when it's Managed, check if it's STREAMING_TABLE
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
            self.spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        except Exception as e:
            raise Exception(f"""Failed to drop table: {str(e)}""") from e

    def get_pipeline_id(self, table_name: str) -> str:
        """
        get pipleline id from table properties

        Args:
            table_name: table full name (catalog.schema.table)
        Returns:
            pipeline id if exists else None
        """

        table_details = self.describe_table_detail(table_name)

        return table_details["properties"].get("pipelines.pipelineId", None)

    def get_table_details(self, table_name: str) -> Tuple[str, bool]:
        if self.spark.catalog.tableExists(table_name):
            pipeline_id = self.get_pipeline_id(table_name)
            if pipeline_id:
                # Handle streaming table or materialized view
                actual_table_name = self._get_internal_table_name(
                    table_name, pipeline_id
                )
                return {
                    "table_name": actual_table_name.lower(),
                    "is_dlt": True,
                    "pipeline_id": pipeline_id,
                }

            # If not a DLT table, just return the original table name and "delta"
            return {
                "table_name": table_name.lower(),
                "is_dlt": False,
                "pipeline_id": None,
            }
        else:
            raise TableNotFoundError(f"Table {table_name} does not exist")

    def _get_internal_table_name(self, table_name: str, pipeline_id: str) -> str:
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

        # Check possible locations for the internal table 1
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{table_name_only}`"
        )
        # print(full_internal_table_name)
        if self.spark.catalog.tableExists(full_internal_table_name):
            return full_internal_table_name

        # Check possible locations for the internal table 2
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{internal_table_name}`"
        )
        # print(full_internal_table_name)
        if self.spark.catalog.tableExists(full_internal_table_name):
            return full_internal_table_name

        # Check possible locations for the internal table 3
        schema_name = table_name.split(".")[1].replace("`", "")
        full_internal_table_name = (
            f"`{catalog_name}`.`{schema_name}`.`{internal_table_name}`"
        )
        # print(full_internal_table_name)
        if self.spark.catalog.tableExists(full_internal_table_name):
            return full_internal_table_name

        # Check possible locations for the internal table 4
        internal_table_name = f"__materialization_mat_{table_name_only}_1"
        full_internal_table_name = (
            f"`__databricks_internal`.`{internal_schema_name}`.`{internal_table_name}`"
        )
        # print(full_internal_table_name)
        if self.spark.catalog.tableExists(full_internal_table_name):
            return full_internal_table_name

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
            self.spark.sql(create_query)
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
            self.spark.sql(create_share_query)

            # Grant SELECT and READ_VOLUME permissions to recipient
            grant_select_query = (
                f"GRANT SELECT ON SHARE `{share_name}` TO RECIPIENT `{recipient_name}`"
            )

            self.spark.sql(grant_select_query)

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

            full_schema_name = f"`{catalog_name}`.`{schema_name}`"
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
            self.spark.sql(add_schema_query)

        except Exception as e:
            raise Exception(
                f"""Failed to add schema `{catalog_name}.{schema_name}` to share `{share_name}`: {str(e)}"""
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
        volume_types: List[VolumeType],
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

    @retry_with_logging(retry_config=RetryConfig(retries=5, delay=3))
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
