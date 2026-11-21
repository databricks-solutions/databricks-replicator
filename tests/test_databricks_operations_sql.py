"""Tests for the Spark-SQL helpers in databricks_operations.py.

Complements tests/test_databricks_operations.py (WorkspaceClient wrappers).
All Spark and WorkspaceClient interactions are mocked; no workspace access.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType
from databricks.sdk.service.tags import TagPolicy, Value

from data_replication.databricks_operations import DatabricksOperations
from data_replication.exceptions import TableNotFoundError


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """retry_with_logging sleeps between attempts; make retries instant."""
    monkeypatch.setattr("data_replication.utils.time.sleep", lambda *_: None)


def _df(rows=None, empty=None, count=None):
    """Minimal DataFrame stand-in: chainable filter/select, collect/isEmpty/count."""
    rows = rows or []
    df = MagicMock()
    df.collect.return_value = rows
    df.isEmpty.return_value = (not rows) if empty is None else empty
    df.count.return_value = len(rows) if count is None else count
    for m in ("filter", "select", "withColumnRenamed", "selectExpr"):
        getattr(df, m).return_value = df
    return df


@pytest.fixture
def ops():
    spark = MagicMock()
    wc = MagicMock()
    return DatabricksOperations(spark=spark, workspace_client=wc), spark, wc


def _sql_calls(spark):
    return [c.args[0] for c in spark.sql.call_args_list]


# ---------- _execute_sql / execute_query ----------


class TestExecuteSql:
    def test_returns_spark_result_without_logger(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = "RESULT"
        assert o.execute_query("SELECT 1") == "RESULT"
        spark.sql.assert_called_once_with("SELECT 1")

    def test_logs_context_and_user_with_logger(self, monkeypatch):
        spark = MagicMock()
        logger = MagicMock()
        monkeypatch.setattr(
            "data_replication.databricks_operations.get_spark_current_user",
            lambda s: "alice",
        )
        monkeypatch.setattr(
            "data_replication.databricks_operations.get_spark_workspace_url",
            lambda s: "ws.example.com",
        )
        o = DatabricksOperations(spark=spark, logger=logger)
        o._execute_sql("SELECT 1", "ctx")
        msgs = " ".join(c.args[0] for c in logger.debug.call_args_list)
        assert "ws.example.com" in msgs and "alice" in msgs
        assert "Executing SQL (ctx): SELECT 1" in msgs
        assert "SQL execution completed (ctx)" in msgs

    def test_reraises_and_logs_failure(self, monkeypatch):
        spark = MagicMock()
        spark.sql.side_effect = RuntimeError("boom")
        logger = MagicMock()
        monkeypatch.setattr(
            "data_replication.databricks_operations.get_spark_current_user",
            lambda s: "u",
        )
        monkeypatch.setattr(
            "data_replication.databricks_operations.get_spark_workspace_url",
            lambda s: "w",
        )
        o = DatabricksOperations(spark=spark, logger=logger)
        with pytest.raises(RuntimeError, match="boom"):
            o._execute_sql("SELECT 1")
        assert any(
            "SQL execution failed" in c.args[0] for c in logger.debug.call_args_list
        )


# ---------- simple lookups ----------


class TestLookups:
    def test_get_metastore_id(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([["aws:us-west-2:abc"]])
        assert o.get_metastore_id() == "aws:us-west-2:abc"
        spark.sql.assert_called_once_with("SELECT current_metastore()")

    def test_get_metastore_id_wraps_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("down")
        with pytest.raises(Exception, match="Failed to get metastore ID: down"):
            o.get_metastore_id()

    def test_if_schema_exists_quotes_name(self, ops):
        o, spark, _ = ops
        spark.catalog.databaseExists.return_value = True
        assert o.if_schema_exists("c", "s") is True
        spark.catalog.databaseExists.assert_called_once_with("`c`.`s`")

    def test_refresh_schema_metadata_returns_retry_tuple(self, ops):
        o, spark, _ = ops
        spark.catalog.databaseExists.return_value = True
        result, exc, attempt, max_attempts = o.refresh_schema_metadata("c.s")
        assert (result, exc, attempt, max_attempts) == (True, None, 1, 2)
        spark.catalog.databaseExists.assert_called_once_with("`c.s`")


# ---------- create helpers ----------


class TestCreateHelpers:
    def test_create_catalog_using_share(self, ops):
        o, spark, _ = ops
        o.create_catalog_using_share_if_not_exists("cat", "prov", "share")
        assert _sql_calls(spark) == [
            "CREATE CATALOG IF NOT EXISTS `cat` USING SHARE `prov`.`share`"
        ]

    def test_create_catalog_using_share_wraps_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("no provider")
        with pytest.raises(Exception, match="no provider"):
            o.create_catalog_using_share_if_not_exists("cat", "prov", "share")

    def test_create_schema(self, ops):
        o, spark, _ = ops
        o.create_schema_if_not_exists("c", "s")
        assert _sql_calls(spark) == ["CREATE SCHEMA IF NOT EXISTS `c`.`s`"]

    def test_create_schema_wraps_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("denied")
        with pytest.raises(Exception, match="Failed to create schema: denied"):
            o.create_schema_if_not_exists("c", "s")

    def test_drop_table(self, ops):
        o, spark, _ = ops
        o.drop_table_if_exists("`c`.`s`.`t`")
        assert _sql_calls(spark) == ["DROP TABLE IF EXISTS `c`.`s`.`t`"]

    def test_drop_table_wraps_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("locked")
        with pytest.raises(Exception, match="Failed to drop table: locked"):
            o.drop_table_if_exists("t")


# ---------- filter_tables_by_type ----------


class TestFilterTablesByType:
    def test_no_types_returns_input(self, ops):
        o, _, _ = ops
        assert o.filter_tables_by_type("c", "s", ["a", "b"], []) == ["a", "b"]
        assert o.filter_tables_by_type("c", "s", ["a"], None) == ["a"]
        assert o.filter_tables_by_type("c", "s", [], ["managed"]) == []

    def test_sequential(self, ops, monkeypatch):
        o, _, _ = ops
        types = {"`c`.`s`.`a`": "MANAGED", "`c`.`s`.`b`": "VIEW"}
        monkeypatch.setattr(o, "get_table_type", lambda name: types[name])
        assert o.filter_tables_by_type(
            "c", "s", ["a", "b"], ["managed"], parallel_table_filter=1
        ) == ["a"]

    def test_parallel(self, ops, monkeypatch):
        o, _, _ = ops
        types = {
            "`c`.`s`.`a`": "MANAGED",
            "`c`.`s`.`b`": "VIEW",
            "`c`.`s`.`d`": "EXTERNAL",
        }
        monkeypatch.setattr(o, "get_table_type", lambda name: types[name])
        out = o.filter_tables_by_type(
            "c", "s", ["a", "b", "d"], ["managed", "external"], parallel_table_filter=4
        )
        assert sorted(out) == ["a", "d"]


# ---------- filter helpers ----------


class TestFilterQueries:
    def test_get_schemas_by_filter(self, ops):
        o, spark, _ = ops
        df = _df([SimpleNamespace(schema_name="s1")])
        spark.sql.return_value = df
        assert o.get_schemas_by_filter("c", "schema_name like 's%'") == ["s1"]
        spark.sql.assert_called_once_with("SHOW SCHEMAS IN `c`")
        df.filter.assert_any_call("schema_name like 's%'")

    def test_get_schemas_by_filter_error_returns_empty(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        assert o.get_schemas_by_filter("c", "1=1") == []

    def test_get_tables_by_filter(self, ops):
        o, spark, _ = ops
        df = _df([SimpleNamespace(table_name="t1")])
        spark.sql.return_value = df
        assert o.get_tables_by_filter("c", "s", "table_name='t1'") == ["t1"]
        spark.sql.assert_called_once_with("SHOW TABLES IN `c`.`s`")
        df.filter.assert_any_call("table_name='t1'")

    def test_get_tables_by_filter_error_returns_empty(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        assert o.get_tables_by_filter("c", "s", "1=1") == []

    def test_get_schema_tables_by_filter(self, ops):
        o, spark, _ = ops
        o.logger = MagicMock()
        spark.sql.return_value = _df([{"schema_name": "s", "table_name": "t"}])
        out = o.get_schema_tables_by_filter("c", "table_schema = 's'")
        assert out == [{"schema_name": "s", "table_name": "t"}]
        q = spark.sql.call_args.args[0]
        assert "FROM c.information_schema.tables" in q
        assert "WHERE table_schema = 's'" in q

    def test_get_schema_tables_by_filter_no_expression(self, ops):
        o, spark, _ = ops
        o.logger = MagicMock()
        assert o.get_schema_tables_by_filter("c", None) == []
        spark.sql.assert_not_called()

    def test_get_schema_tables_by_filter_error(self, ops):
        o, spark, _ = ops
        o.logger = MagicMock()
        spark.sql.side_effect = RuntimeError("bad filter")
        assert o.get_schema_tables_by_filter("c", "x") == []
        assert "bad filter" in o.logger.error.call_args.args[0]


# ---------- describe_table_detail / pipeline ids ----------


def _describe_extended(type_value, properties=None, has_props=True):
    """DESCRIBE EXTENDED mock whose filter() result depends on the col_name asked."""
    df = MagicMock()
    df.__getitem__.side_effect = lambda col: _Col(col)
    type_df = _df([["Type", type_value]] if type_value else [])
    props_df = _df([[properties]] if has_props else [], empty=not has_props)

    def _filter(cond):
        return type_df if cond.value == "Type" else props_df

    df.filter.side_effect = _filter
    return df


class _Col:
    def __init__(self, name):
        self.name = name

    def __eq__(self, other):
        return SimpleNamespace(value=other)


class TestDescribeTableDetail:
    def test_view_parses_properties(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _describe_extended(
            "VIEW",
            "[pipelines.pipelineId=pid,spark.sql.internal.pipelines.parentTableId=ptid]",
        )
        out = o.describe_table_detail("`c`.`s`.`v`")
        assert out == {
            "properties": {
                "pipelines.pipelineId": "pid",
                "spark.sql.internal.pipelines.parentTableId": "ptid",
            }
        }

    def test_view_without_properties_row(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _describe_extended("VIEW", has_props=False)
        assert o.describe_table_detail("v") == {"properties": {}}

    def test_view_with_empty_properties(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _describe_extended("MATERIALIZED_VIEW", "")
        assert o.describe_table_detail("mv") == {"properties": {}}

    def test_table_uses_describe_detail(self, ops):
        o, spark, _ = ops
        detail_row = MagicMock()
        detail_row.asDict.return_value = {"location": "s3://b/t", "properties": {}}
        detail_df = _df([detail_row])
        spark.sql.side_effect = [_describe_extended("MANAGED"), detail_df]
        out = o.describe_table_detail("`c`.`s`.`t`")
        assert out == {"location": "s3://b/t", "properties": {}}
        assert _sql_calls(spark) == [
            "DESCRIBE EXTENDED `c`.`s`.`t`",
            "DESCRIBE DETAIL `c`.`s`.`t`",
        ]

    def test_get_pipeline_id(self, ops, monkeypatch):
        o, _, _ = ops
        monkeypatch.setattr(
            o,
            "describe_table_detail",
            lambda t: {
                "properties": {
                    "pipelines.pipelineId": "pid",
                    "spark.sql.internal.pipelines.parentTableId": "ptid",
                }
            },
        )
        assert o.get_pipeline_id("t") == ("pid", "ptid")

    def test_get_pipeline_id_none(self, ops, monkeypatch):
        o, _, _ = ops
        monkeypatch.setattr(o, "describe_table_detail", lambda t: {"properties": {}})
        assert o.get_pipeline_id("t") == (None, None)


# ---------- get_table_type ----------


class TestGetTableType:
    def _table(self, table_type, pipeline_id=None, location=None):
        return SimpleNamespace(
            table_type=SimpleNamespace(value=table_type),
            pipeline_id=pipeline_id,
            storage_location=location,
        )

    def test_non_managed_returned_as_is(self, ops):
        o, spark, wc = ops
        spark.catalog.tableExists.return_value = True
        wc.tables.get.return_value = self._table("VIEW")
        assert o.get_table_type("`c`.`s`.`v`") == "VIEW"
        wc.tables.get.assert_called_once_with(full_name="c.s.v")

    def test_managed_with_pipeline_is_streaming_table(self, ops):
        o, spark, wc = ops
        spark.catalog.tableExists.return_value = True
        wc.tables.get.return_value = self._table("MANAGED", pipeline_id="pid")
        assert o.get_table_type("t") == "STREAMING_TABLE"

    def test_managed_in_tables_location(self, ops):
        o, spark, wc = ops
        spark.catalog.tableExists.return_value = True
        wc.tables.get.return_value = self._table(
            "MANAGED", location="s3://b/metastore/tables/abc"
        )
        assert o.get_table_type("t") == "MANAGED"

    def test_managed_outside_tables_location_is_external(self, ops):
        o, spark, wc = ops
        spark.catalog.tableExists.return_value = True
        wc.tables.get.return_value = self._table("MANAGED", location="s3://b/ext/t")
        assert o.get_table_type("t") == "EXTERNAL"

    def test_missing_table_raises(self, ops):
        o, spark, _ = ops
        spark.catalog.tableExists.return_value = False
        with pytest.raises(TableNotFoundError):
            o.get_table_type("t")


# ---------- get_table_details / internal table lookup ----------


class TestGetTableDetails:
    def test_regular_table(self, ops, monkeypatch):
        o, spark, _ = ops
        spark.catalog.tableExists.return_value = True
        monkeypatch.setattr(o, "get_pipeline_id", lambda t: (None, None))
        assert o.get_table_details("`C`.`s`.`T`") == {
            "table_name": "`c`.`s`.`t`",
            "is_dlt": False,
            "dlt_type": None,
            "pipeline_id": None,
            "parent_table_id": None,
        }

    def test_dlt_table_resolves_internal_name(self, ops, monkeypatch):
        o, spark, _ = ops
        spark.catalog.tableExists.return_value = True
        monkeypatch.setattr(o, "get_pipeline_id", lambda t: ("p-1", "ptid"))
        monkeypatch.setattr(
            o, "_get_internal_table_name", lambda t, p: ("`C`.`S`.`__mat`", "dpm")
        )
        out = o.get_table_details("`c`.`s`.`t`")
        assert out == {
            "table_name": "`c`.`s`.`__mat`",
            "is_dlt": True,
            "dlt_type": "dpm",
            "pipeline_id": "p-1",
            "parent_table_id": "ptid",
        }

    def test_missing_table_raises(self, ops):
        o, spark, _ = ops
        spark.catalog.tableExists.return_value = False
        with pytest.raises(TableNotFoundError):
            o.get_table_details("t")

    @pytest.mark.parametrize(
        "existing,expected",
        [
            (
                "`c`.`s`.`__materialization_mat_p_1_t_1`",
                ("`c`.`s`.`__materialization_mat_p_1_t_1`", "dpm"),
            ),
            (
                "`__databricks_internal`.`__dlt_materialization_schema_p_1`.`t`",
                (
                    "`__databricks_internal`.`__dlt_materialization_schema_p_1`.`t`",
                    "legacy",
                ),
            ),
            (
                "`__databricks_internal`.`__dlt_materialization_schema_p_1`"
                ".`__materialization_mat_p_1_t_1`",
                (
                    "`__databricks_internal`.`__dlt_materialization_schema_p_1`"
                    ".`__materialization_mat_p_1_t_1`",
                    "legacy",
                ),
            ),
            (
                "`__databricks_internal`.`__dlt_materialization_schema_p_1`"
                ".`__materialization_mat_t_1`",
                (
                    "`__databricks_internal`.`__dlt_materialization_schema_p_1`"
                    ".`__materialization_mat_t_1`",
                    "legacy",
                ),
            ),
        ],
    )
    def test_internal_table_name_lookup_order(self, ops, existing, expected):
        o, spark, _ = ops
        spark.catalog.tableExists.side_effect = lambda name: name == existing
        assert o._get_internal_table_name("`c`.`s`.`t`", "p-1") == expected

    def test_internal_table_not_found(self, ops):
        o, spark, _ = ops
        spark.catalog.tableExists.return_value = False
        with pytest.raises(Exception, match="Could not find internal table"):
            o._get_internal_table_name("`c`.`s`.`t`", "p-1")

    def test_internal_table_unauthorized_counts_as_exists(self, ops):
        o, spark, _ = ops
        spark.catalog.tableExists.side_effect = RuntimeError(
            "UnauthorizedAccessException: no"
        )
        assert o._if_internal_table_exists("x") is True

    def test_internal_table_other_error_raises(self, ops):
        o, spark, _ = ops
        spark.catalog.tableExists.side_effect = RuntimeError("other")
        with pytest.raises(RuntimeError, match="other"):
            o._if_internal_table_exists("x")


# ---------- fields ----------


class TestFields:
    def test_get_table_fields(self, ops):
        o, spark, _ = ops
        spark.table.return_value = SimpleNamespace(columns=["a", "b"])
        assert o.get_table_fields("t") == ["a", "b"]

    def test_get_table_fields_error_returns_empty(self, ops):
        o, spark, _ = ops
        spark.table.side_effect = RuntimeError("x")
        assert o.get_table_fields("t") == []

    def test_get_common_fields(self, ops, monkeypatch):
        o, _, _ = ops
        fields = {"src": ["a", "b", "c"], "tgt": ["b", "c", "d"]}
        monkeypatch.setattr(o, "get_table_fields", lambda t: fields[t])
        assert sorted(o.get_common_fields("src", "tgt")) == ["b", "c"]


# ---------- delta sharing ----------


class TestDeltaSharing:
    def test_get_recipient_name_found(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([["rcpt"]])
        assert o.get_recipient_name("aws:x:y") == "rcpt"
        assert "data_recipient_global_metastore_id = 'aws:x:y'" in _sql_calls(spark)[0]

    def test_get_recipient_name_missing(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([])
        assert o.get_recipient_name("id") is None

    def test_get_recipient_name_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to get recipient name"):
            o.get_recipient_name("id")

    def test_create_recipient_reuses_existing(self, ops, monkeypatch):
        o, spark, _ = ops
        monkeypatch.setattr(o, "get_recipient_name", lambda i: "existing")
        assert o.create_recipient("id", "new") == "existing"
        spark.sql.assert_not_called()

    def test_create_recipient_creates(self, ops, monkeypatch):
        o, spark, _ = ops
        monkeypatch.setattr(o, "get_recipient_name", lambda i: None)
        assert o.create_recipient("aws:id", "rcpt") == "rcpt"
        assert _sql_calls(spark) == [
            "CREATE RECIPIENT IF NOT EXISTS `rcpt` USING ID 'aws:id'"
        ]

    def test_create_recipient_error(self, ops, monkeypatch):
        o, spark, _ = ops
        monkeypatch.setattr(o, "get_recipient_name", lambda i: None)
        spark.sql.side_effect = RuntimeError("denied")
        with pytest.raises(Exception, match="Failed to create recipient `rcpt`"):
            o.create_recipient("id", "rcpt")

    def test_create_delta_share(self, ops):
        o, spark, _ = ops
        o.create_delta_share("sh", "rcpt")
        assert _sql_calls(spark) == [
            "CREATE SHARE IF NOT EXISTS `sh`",
            "GRANT SELECT ON SHARE `sh` TO RECIPIENT `rcpt`",
        ]

    def test_create_delta_share_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to create delta share `sh`"):
            o.create_delta_share("sh", "rcpt")

    def test_default_schema_always_in_share(self, ops):
        o, spark, _ = ops
        assert o.is_schema_in_share("sh", "c", "default") is True
        spark.sql.assert_not_called()

    @pytest.mark.parametrize("count,expected", [(1, True), (0, False)])
    def test_is_schema_in_share(self, ops, count, expected):
        o, spark, _ = ops
        spark.sql.return_value = _df(count=count)
        assert o.is_schema_in_share("sh", "c", "s") is expected
        assert _sql_calls(spark) == ["SHOW ALL IN SHARE `sh`"]

    def test_is_schema_in_share_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        assert o.is_schema_in_share("sh", "c", "s") is False

    @pytest.mark.parametrize("count,expected", [(2, True), (0, False)])
    def test_is_table_in_share(self, ops, count, expected):
        o, spark, _ = ops
        df = _df(count=count)
        spark.sql.return_value = df
        assert o.is_table_in_share("sh", "`c`.`s`.`t`") is expected
        assert '"c.s.t"' in df.filter.call_args.args[0]

    def test_is_table_in_share_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        assert o.is_table_in_share("sh", "t") is False

    def test_add_schema_to_share_skips_if_present(self, ops, monkeypatch):
        o, spark, _ = ops
        monkeypatch.setattr(o, "is_schema_in_share", lambda *a: True)
        o.add_schema_to_share("sh", "c", "s")
        spark.sql.assert_not_called()

    def test_add_schema_to_share(self, ops, monkeypatch):
        o, spark, _ = ops
        monkeypatch.setattr(o, "is_schema_in_share", lambda *a: False)
        o.add_schema_to_share("sh", "c", "s")
        assert _sql_calls(spark) == ["ALTER SHARE `sh` ADD SCHEMA `c`.`s`"]

    def test_add_schema_to_share_error(self, ops, monkeypatch):
        o, spark, _ = ops
        monkeypatch.setattr(o, "is_schema_in_share", lambda *a: False)
        spark.sql.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to add schema `c.s`"):
            o.add_schema_to_share("sh", "c", "s")

    def test_get_shared_tables(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([SimpleNamespace(table_name="c.s.t")])
        assert o.get_shared_tables("sh") == ["c.s.t"]
        assert "share_name = 'sh'" in _sql_calls(spark)[0]

    def test_get_shared_tables_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to get shared tables"):
            o.get_shared_tables("sh")

    def test_get_provider_name(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([["prov"]])
        assert o.get_provider_name("azure:x") == "prov"
        assert "data_provider_global_metastore_id = 'azure:x'" in _sql_calls(spark)[0]

    def test_get_provider_name_missing(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([])
        assert o.get_provider_name("id") is None

    def test_get_provider_name_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to get provider name"):
            o.get_provider_name("id")


# ---------- volumes ----------


class TestVolumes:
    def test_if_volume_exists(self, ops):
        o, spark, _ = ops
        assert o.if_volume_exists("c.s.v") is True
        spark.sql.assert_called_once_with("DESCRIBE VOLUME c.s.v")

    def test_if_volume_exists_false_on_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        assert o.if_volume_exists("c.s.v") is False

    def test_get_volumes_in_schema(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([SimpleNamespace(volume_name="v1")])
        assert o.get_volumes_in_schema("c", "s") == ["v1"]
        spark.sql.assert_called_once_with("SHOW VOLUMES IN `c`.`s`")

    def test_get_volumes_in_schema_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        assert o.get_volumes_in_schema("c", "s") == []

    def test_get_volume_type_uppercases(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([["managed"]])
        assert o.get_volume_type("c.s.v") == "MANAGED"

    def test_get_volume_type_none_when_empty(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([[None]])
        assert o.get_volume_type("c.s.v") is None

    def test_get_volume_type_error_returns_none(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("x")
        assert o.get_volume_type("c.s.v") is None

    def test_refresh_volume_metadata(self, ops):
        o, spark, _ = ops
        assert o.refresh_volume_metadata("v")[0] is True
        spark.sql.side_effect = RuntimeError("x")
        assert o.refresh_volume_metadata("v")[0] is False

    def test_filter_volumes_by_type(self, ops, monkeypatch):
        o, _, _ = ops
        types = {"`c`.`s`.`a`": "MANAGED", "`c`.`s`.`b`": "EXTERNAL"}
        monkeypatch.setattr(o, "get_volume_type", lambda n: types[n])
        assert o.filter_volumes_by_type("c", "s", ["a", "b"], ["external"]) == ["b"]

    def test_filter_volumes_no_types_returns_empty(self, ops):
        o, _, _ = ops
        assert o.filter_volumes_by_type("c", "s", ["a"], []) == []
        assert o.filter_volumes_by_type("c", "s", ["a"], None) == []


# ---------- tags / comments ----------


class TestTagsAndComments:
    @pytest.mark.parametrize(
        "method,args,where",
        [
            ("get_catalog_tags", ("c",), "a.catalog_name = 'c'"),
            ("get_schema_tags", ("c", "s"), "a.schema_name = 's'"),
            ("get_table_tags", ("c", "s", "t"), "a.table_name = 't'"),
            ("get_volume_tags", ("c", "s", "v"), "a.volume_name = 'v'"),
        ],
    )
    def test_tags_found(self, ops, method, args, where):
        o, spark, _ = ops
        row = {"tag_names_list": ["k"], "tag_maps_list": [{"k": "v"}]}
        spark.sql.return_value = _df([row])
        assert getattr(o, method)(*args) == (["k"], [{"k": "v"}])
        assert where in _sql_calls(spark)[0]

    @pytest.mark.parametrize(
        "method,args",
        [
            ("get_catalog_tags", ("c",)),
            ("get_schema_tags", ("c", "s")),
            ("get_table_tags", ("c", "s", "t")),
            ("get_volume_tags", ("c", "s", "v")),
        ],
    )
    def test_tags_none_when_empty(self, ops, method, args):
        o, spark, _ = ops
        spark.sql.return_value = _df([])
        assert getattr(o, method)(*args) == (None, None)

    def test_get_column_tags_df_returns_dataframe(self, ops):
        o, spark, _ = ops
        df = _df([])
        spark.sql.return_value = df
        assert o.get_column_tags_df("c", "s", "t") is df
        assert "a.table_name = 't'" in _sql_calls(spark)[0]

    def test_get_column_comments(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([[[{"a": "comment a"}]]])
        assert o.get_column_comments("c", "s", "t") == [{"a": "comment a"}]
        assert "table_name = 't'" in _sql_calls(spark)[0]

    def test_get_table_comments(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df([["my table"]])
        assert o.get_table_comments("c", "s", "t") == "my table"


# ---------- DDL / table info ----------


class TestDdlAndTableInfo:
    def test_show_create_table_joins_rows(self, ops):
        o, spark, _ = ops
        spark.sql.return_value = _df(
            [{"createtab_stmt": "CREATE TABLE t ("}, {"createtab_stmt": "a INT)"}]
        )
        assert o.show_create_table_ddl("t") == "CREATE TABLE t (\na INT)"
        spark.sql.assert_called_once_with("SHOW CREATE TABLE t")

    def test_show_create_table_wraps_error(self, ops):
        o, spark, _ = ops
        spark.sql.side_effect = RuntimeError("nope")
        with pytest.raises(Exception, match="Failed to get CREATE TABLE DDL for t"):
            o.show_create_table_ddl("t")

    def test_get_table_strips_backticks(self, ops):
        o, _, wc = ops
        wc.tables.get.return_value = "INFO"
        assert o.get_table("`c`.`s`.`t`") == "INFO"
        wc.tables.get.assert_called_once_with("c.s.t")

    def test_get_table_requires_client(self):
        o = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            o.get_table("t")

    def test_get_table_wraps_error_as_not_found(self, ops):
        o, _, wc = ops
        wc.tables.get.side_effect = RuntimeError("404")
        with pytest.raises(TableNotFoundError, match="404"):
            o.get_table("t")

    def test_get_view_definition(self, ops):
        o, _, wc = ops
        info = MagicMock()
        info.as_dict.return_value = {"view_definition": "SELECT 1"}
        wc.tables.get.return_value = info
        assert o.get_view_definition("v") == "SELECT 1"

    def test_get_view_definition_missing(self, ops):
        o, _, wc = ops
        info = MagicMock()
        info.as_dict.return_value = {}
        wc.tables.get.return_value = info
        assert o.get_view_definition("t") is None


# ---------- tag policies ----------


class TestTagPolicies:
    def test_build_tag_policy(self):
        tp = DatabricksOperations._build_tag_policy(
            {"tag_key": "k", "description": "d", "values": [{"name": "a"}, Value("b")]}
        )
        assert tp.tag_key == "k" and tp.description == "d"
        assert [v.name for v in tp.values] == ["a", "b"]

    def test_build_tag_policy_without_values(self):
        assert DatabricksOperations._build_tag_policy({"tag_key": "k"}).values is None

    def test_list_and_get(self, ops):
        o, _, wc = ops
        wc.tag_policies.list_tag_policies.return_value = iter(["p1", "p2"])
        assert o.list_tag_policies() == ["p1", "p2"]
        wc.tag_policies.get_tag_policy.return_value = "p"
        assert o.get_tag_policy("k") == "p"

    def test_create_and_update(self, ops):
        o, _, wc = ops
        o.create_tag_policy({"tag_key": "k"})
        assert isinstance(
            wc.tag_policies.create_tag_policy.call_args.kwargs["tag_policy"], TagPolicy
        )
        o.update_tag_policy({"tag_key": "k"}, update_mask="values")
        kwargs = wc.tag_policies.update_tag_policy.call_args.kwargs
        assert kwargs["tag_key"] == "k" and kwargs["update_mask"] == "values"

    @pytest.mark.parametrize(
        "method,args,api",
        [
            ("list_tag_policies", (), "list_tag_policies"),
            ("get_tag_policy", ("k",), "get_tag_policy"),
            ("create_tag_policy", ({"tag_key": "k"},), "create_tag_policy"),
            ("update_tag_policy", ({"tag_key": "k"},), "update_tag_policy"),
        ],
    )
    def test_errors_wrapped(self, ops, method, args, api):
        o, _, wc = ops
        getattr(wc.tag_policies, api).side_effect = RuntimeError("tp down")
        with pytest.raises(Exception, match="tp down"):
            getattr(o, method)(*args)

    @pytest.mark.parametrize(
        "method,args",
        [
            ("list_tag_policies", ()),
            ("get_tag_policy", ("k",)),
            ("create_tag_policy", ({"tag_key": "k"},)),
            ("update_tag_policy", ({"tag_key": "k"},)),
        ],
    )
    def test_require_workspace_client(self, method, args):
        o = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            getattr(o, method)(*args)


# ---------- service principals ----------


class TestServicePrincipals:
    def test_display_name_by_app_id(self, ops):
        o, _, wc = ops
        wc.service_principals.list.return_value = iter(
            [SimpleNamespace(display_name="sp-name")]
        )
        assert o.get_sp_display_name_by_app_id("app-1") == "sp-name"
        assert (
            wc.service_principals.list.call_args.kwargs["filter"]
            == "applicationId eq 'app-1'"
        )

    def test_app_id_by_display_name(self, ops):
        o, _, wc = ops
        wc.service_principals.list.return_value = iter(
            [SimpleNamespace(application_id="app-1")]
        )
        assert o.get_sp_app_id_by_display_name("sp") == "app-1"

    @pytest.mark.parametrize(
        "method", ["get_sp_display_name_by_app_id", "get_sp_app_id_by_display_name"]
    )
    def test_not_found_error_and_empty_input(self, ops, method):
        o, _, wc = ops
        wc.service_principals.list.return_value = iter([])
        assert getattr(o, method)("x") is None
        wc.service_principals.list.side_effect = RuntimeError("scim")
        assert getattr(o, method)("x") is None
        assert getattr(o, method)("") is None


# ---------- SQL warehouse ----------


class TestSqlWarehouse:
    def _cfg(self, **kw):
        cfg = {
            "name": "wh",
            "create_if_not_exists": True,
            "warehouse_type": SimpleNamespace(value="PRO"),
            "cluster_size": "Small",
        }
        cfg.update(kw)
        return cfg

    def test_reuses_existing_by_name(self, ops):
        o, _, wc = ops
        wc.warehouses.list.return_value = [SimpleNamespace(id="1", name="wh")]
        assert o.create_or_get_sql_warehouse(self._cfg()) == ("1", "wh")
        wc.warehouses.create.assert_not_called()

    def test_creates_when_missing(self, ops):
        o, _, wc = ops
        wc.warehouses.list.return_value = [SimpleNamespace(id="1", name="other")]
        wc.warehouses.create.return_value.result.return_value = SimpleNamespace(
            id="2", name="wh"
        )
        assert o.create_or_get_sql_warehouse(self._cfg()) == ("2", "wh")
        kwargs = wc.warehouses.create.call_args.kwargs
        assert kwargs["name"] == "wh"
        assert kwargs["cluster_size"] == "Small"
        assert kwargs["warehouse_type"] == CreateWarehouseRequestWarehouseType.PRO

    def test_generates_name_when_not_given(self, ops):
        o, _, wc = ops
        wc.warehouses.create.return_value.result.return_value = SimpleNamespace(
            id="3", name="gen"
        )
        o.create_or_get_sql_warehouse(self._cfg(name=None))
        assert wc.warehouses.create.call_args.kwargs["name"].startswith("replication-")
        wc.warehouses.list.assert_not_called()

    def test_wraps_errors(self, ops):
        o, _, wc = ops
        wc.warehouses.list.side_effect = RuntimeError("quota")
        with pytest.raises(Exception, match="Failed to create SQL warehouse: quota"):
            o.create_or_get_sql_warehouse(self._cfg())

    def test_requires_workspace_client(self):
        o = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            o.create_or_get_sql_warehouse({})


# ---------- remaining WorkspaceClient wrapper error paths ----------


@pytest.mark.parametrize(
    "method,args,api",
    [
        ("create_catalog", ({"name": "c"},), ("catalogs", "create")),
        ("update_catalog", ({"name": "c"},), ("catalogs", "update")),
        ("get_schema", ("c.s",), ("schemas", "get")),
        ("update_schema", ({"full_name": "c.s"},), ("schemas", "update")),
        ("get_volume", ("c.s.v",), ("volumes", "read")),
        ("create_volume", ({"name": "v"},), ("volumes", "create")),
        ("get_storage_credential", ("x",), ("storage_credentials", "get")),
        (
            "update_storage_credential",
            ({"name": "x"},),
            ("storage_credentials", "update"),
        ),
        ("list_external_locations", (), ("external_locations", "list")),
        (
            "create_external_location",
            ({"name": "l"},),
            ("external_locations", "create"),
        ),
        (
            "update_external_location",
            ({"name": "l"},),
            ("external_locations", "update"),
        ),
    ],
)
def test_wrapper_errors_include_cause(ops, method, args, api):
    o, _, wc = ops
    getattr(getattr(wc, api[0]), api[1]).side_effect = RuntimeError("api down")
    with pytest.raises(Exception, match="api down"):
        getattr(o, method)(*args)


@pytest.mark.parametrize(
    "method,args,api",
    [
        ("create_schema", ({"name": "s"},), ("schemas", "create")),
        ("update_schema", ({"full_name": "c.s"},), ("schemas", "update")),
        ("create_volume", ({"name": "v"},), ("volumes", "create")),
        ("update_volume", ({"full_name": "c.s.v"},), ("volumes", "update")),
        (
            "create_storage_credential",
            ({"name": "x"},),
            ("storage_credentials", "create"),
        ),
        (
            "update_storage_credential",
            ({"name": "x"},),
            ("storage_credentials", "update"),
        ),
        (
            "create_external_location",
            ({"name": "l"},),
            ("external_locations", "create"),
        ),
        (
            "update_external_location",
            ({"name": "l"},),
            ("external_locations", "update"),
        ),
    ],
)
def test_wrapper_happy_paths_spread_kwargs(ops, method, args, api):
    o, _, wc = ops
    fn = getattr(getattr(wc, api[0]), api[1])
    fn.return_value = "OK"
    assert getattr(o, method)(*args) == "OK"
    fn.assert_called_once_with(**args[0])
