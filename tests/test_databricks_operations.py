"""Tests for databricks_operations.py — thin WorkspaceClient/Spark wrappers.

Focuses on:
  1. WorkspaceClient-required guard raises clear errors when workspace_client is None
  2. Happy paths delegate correctly to the underlying client
  3. Failure paths wrap exceptions with contextual messages
  4. SQL-based helpers behave deterministically given mocked DataFrames
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from data_replication.databricks_operations import DatabricksOperations

# ---------- Fixtures ----------


@pytest.fixture
def wc_ops():
    """DatabricksOperations with a MagicMock workspace client (no spark)."""
    wc = MagicMock()
    return DatabricksOperations(spark=None, workspace_client=wc), wc


@pytest.fixture
def spark_ops():
    """DatabricksOperations with a MagicMock spark (no workspace client)."""
    spark = MagicMock()
    return DatabricksOperations(spark=spark), spark


# ---------- WorkspaceClient guard ----------


@pytest.mark.parametrize(
    "method,args",
    [
        ("get_catalog", ("c",)),
        ("create_catalog", ({"name": "c"},)),
        ("update_catalog", ({"name": "c"},)),
        ("get_schema", ("c.s",)),
        ("create_schema", ({"name": "s"},)),
        ("update_schema", ({"full_name": "c.s"},)),
        ("get_volume", ("c.s.v",)),
        ("create_volume", ({"name": "v"},)),
        ("update_volume", ({"full_name": "c.s.v"},)),
        ("get_storage_credential", ("cred",)),
        ("create_storage_credential", ({"name": "cred"},)),
        ("update_storage_credential", ({"name": "cred"},)),
        ("list_external_locations", ()),
        ("get_external_location", ("loc",)),
        ("create_external_location", ({"name": "loc"},)),
        ("update_external_location", ({"name": "loc"},)),
    ],
)
def test_methods_require_workspace_client(method, args):
    ops = DatabricksOperations(spark=None, workspace_client=None)
    with pytest.raises(Exception, match="WorkspaceClient is required"):
        getattr(ops, method)(*args)


# ---------- Happy paths ----------


class TestCatalogWrappers:
    def test_get_catalog_delegates(self, wc_ops):
        ops, wc = wc_ops
        wc.catalogs.get.return_value = "catalog_info"
        assert ops.get_catalog("c") == "catalog_info"
        wc.catalogs.get.assert_called_once_with("c")

    def test_create_catalog_spreads_kwargs(self, wc_ops):
        ops, wc = wc_ops
        wc.catalogs.create.return_value = "created"
        assert ops.create_catalog({"name": "c", "comment": "x"}) == "created"
        wc.catalogs.create.assert_called_once_with(name="c", comment="x")

    def test_update_catalog_spreads_kwargs(self, wc_ops):
        ops, wc = wc_ops
        wc.catalogs.update.return_value = "updated"
        assert ops.update_catalog({"name": "c"}) == "updated"
        wc.catalogs.update.assert_called_once_with(name="c")

    def test_get_catalog_wraps_errors(self, wc_ops):
        ops, wc = wc_ops
        wc.catalogs.get.side_effect = RuntimeError("boom")
        with pytest.raises(Exception, match="Failed to get source catalog cat"):
            ops.get_catalog("cat")


class TestSchemaWrappers:
    def test_get_schema(self, wc_ops):
        ops, wc = wc_ops
        wc.schemas.get.return_value = "schema_info"
        assert ops.get_schema("c.s") == "schema_info"
        wc.schemas.get.assert_called_once_with("c.s")

    def test_create_schema_error(self, wc_ops):
        ops, wc = wc_ops
        wc.schemas.create.side_effect = RuntimeError("nope")
        with pytest.raises(Exception, match="Failed to create schema"):
            ops.create_schema({"name": "s"})


class TestVolumeWrappers:
    def test_get_volume_uses_read(self, wc_ops):
        ops, wc = wc_ops
        wc.volumes.read.return_value = "volume_info"
        assert ops.get_volume("c.s.v") == "volume_info"
        wc.volumes.read.assert_called_once_with("c.s.v")

    def test_update_volume_error(self, wc_ops):
        ops, wc = wc_ops
        wc.volumes.update.side_effect = RuntimeError("bad")
        with pytest.raises(Exception, match="Failed to update volume"):
            ops.update_volume({"full_name": "c.s.v"})


class TestStorageCredentialWrappers:
    def test_get(self, wc_ops):
        ops, wc = wc_ops
        wc.storage_credentials.get.return_value = "ok"
        assert ops.get_storage_credential("n") == "ok"

    def test_create_error_message(self, wc_ops):
        ops, wc = wc_ops
        wc.storage_credentials.create.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to create storage credential"):
            ops.create_storage_credential({"name": "n"})


class TestExternalLocationWrappers:
    def test_list_returns_list(self, wc_ops):
        ops, wc = wc_ops
        wc.external_locations.list.return_value = iter(["a", "b"])
        assert ops.list_external_locations() == ["a", "b"]

    def test_get_wraps_errors(self, wc_ops):
        ops, wc = wc_ops
        wc.external_locations.get.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to get"):
            ops.get_external_location("loc")


# ---------- SQL-based helpers ----------


class TestIfCatalogExists:
    def test_returns_true_when_rows(self, spark_ops):
        ops, spark = spark_ops
        df = MagicMock()
        df.isEmpty.return_value = False
        spark.sql.return_value = df
        assert ops.if_catalog_exists("cat") is True
        spark.sql.assert_called_once_with("SHOW CATALOGS LIKE 'cat'")

    def test_returns_false_when_empty(self, spark_ops):
        ops, spark = spark_ops
        df = MagicMock()
        df.isEmpty.return_value = True
        spark.sql.return_value = df
        assert ops.if_catalog_exists("cat") is False

    def test_swallows_exceptions(self, spark_ops):
        ops, spark = spark_ops
        spark.sql.side_effect = RuntimeError("oops")
        assert ops.if_catalog_exists("cat") is False


class TestGetCurrentUser:
    def test_happy(self, spark_ops):
        ops, spark = spark_ops
        row = [["alice"]]
        df = MagicMock()
        df.collect.return_value = row
        spark.sql.return_value = df
        assert ops.get_current_user() == "alice"

    def test_wraps_errors(self, spark_ops):
        ops, spark = spark_ops
        spark.sql.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to get current user"):
            ops.get_current_user()


class TestGetAllSchemas:
    def test_filters_information_schema(self, spark_ops):
        ops, spark = spark_ops
        row1 = MagicMock()
        row1.databaseName = "public"
        row2 = MagicMock()
        row2.databaseName = "other"
        df = MagicMock()
        df.filter.return_value = df
        df.collect.return_value = [row1, row2]
        spark.sql.return_value = df
        assert ops.get_all_schemas("cat") == ["public", "other"]
        df.filter.assert_called_once_with('databaseName != "information_schema"')

    def test_returns_empty_on_error(self, spark_ops):
        ops, spark = spark_ops
        spark.sql.side_effect = RuntimeError("x")
        assert ops.get_all_schemas("cat") == []


class TestGetTablesInSchema:
    def test_returns_table_names(self, spark_ops):
        ops, spark = spark_ops
        row1 = MagicMock()
        row1.tableName = "t1"
        row2 = MagicMock()
        row2.tableName = "t2"
        df = MagicMock()
        df.filter.return_value = df
        df.collect.return_value = [row1, row2]
        spark.sql.return_value = df
        assert ops.get_tables_in_schema("c", "s") == ["t1", "t2"]

    def test_returns_empty_on_error(self, spark_ops):
        ops, spark = spark_ops
        spark.sql.side_effect = RuntimeError("x")
        assert ops.get_tables_in_schema("c", "s") == []


class TestCreateCatalogIfNotExists:
    def test_with_location(self, spark_ops):
        ops, spark = spark_ops
        spark.sql.return_value = MagicMock()
        ops.create_catalog_if_not_exists("cat", "s3://x/")
        sql = spark.sql.call_args.args[0]
        assert "CREATE CATALOG IF NOT EXISTS `cat` MANAGED LOCATION 's3://x/'" in sql

    def test_without_location(self, spark_ops):
        ops, spark = spark_ops
        spark.sql.return_value = MagicMock()
        ops.create_catalog_if_not_exists("cat", None)
        sql = spark.sql.call_args.args[0]
        assert "CREATE CATALOG IF NOT EXISTS `cat`" in sql
        assert "MANAGED LOCATION" not in sql

    def test_wraps_errors(self, spark_ops):
        ops, spark = spark_ops
        spark.sql.side_effect = RuntimeError("bad")
        with pytest.raises(Exception, match="Failed to create catalog"):
            ops.create_catalog_if_not_exists("cat", None)
