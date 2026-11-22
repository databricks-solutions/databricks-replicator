"""ReplicationProvider: UC metadata replication (DDL, MV/ST, comments, tags, grants, volumes).

Offline: providers are built with ``__new__`` and all Spark/SDK objects are mocks.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import VolumeInfo, VolumeType
from databricks.sdk.service.sql import StatementState

from data_replication.exceptions import TableNotFoundError
from data_replication.providers import replication_provider as rp_module
from tests._replication_fakes import (
    df,
    make_provider,
    sql_calls,
    table_config,
    volume_config,
)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("data_replication.utils.time.sleep", lambda *_: None)
    monkeypatch.setattr(rp_module, "sleep", lambda *_: None)


def _uc_provider(**kw):
    p = make_provider(**kw)
    for spark in (p.spark, p.source_spark, p.target_spark):
        spark.catalog.tableExists.return_value = True
    return p


# ---------- _uc_replicate_ddl (tables and views) ----------

VIEW_DDL = "CREATE VIEW src.s.t (\n  a)\nAS SELECT a FROM src.s.base"
TABLE_DDL = (
    "CREATE TABLE src.s.t (\n  a INT)\nUSING delta\n"
    "LOCATION 'abfss://c@acct.dfs.core.windows.net/data/t'"
)


class TestUcReplicateDdl:
    def test_view_create_or_replace(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "VIEW"
        p.source_dbops.show_create_table_ddl.return_value = VIEW_DDL
        [r] = p._uc_replicate_ddl("s", table_config())
        assert r.status == "success" and r.object_type == "view"
        # The source catalog in the view body is rewritten to the target (#49).
        assert r.details["step1_query"] == (
            "CREATE OR REPLACE VIEW `tgt`.`s`.`t` (\n  a)\nAS SELECT a FROM tgt.s.base"
        )
        assert sql_calls(p.target_spark) == [r.details["step1_query"]]

    def test_view_if_not_exists(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "VIEW"
        p.source_dbops.show_create_table_ddl.return_value = VIEW_DDL
        [r] = p._uc_replicate_ddl("s", table_config(create_or_replace_view=False))
        assert r.details["step1_query"].startswith(
            "CREATE VIEW IF NOT EXISTS `tgt`.`s`.`t` ("
        )

    def test_managed_table_if_not_exists_by_default(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "MANAGED"
        p.source_dbops.show_create_table_ddl.return_value = (
            "CREATE TABLE src.s.t (a INT)"
        )
        [r] = p._uc_replicate_ddl("s", table_config())
        assert r.object_type == "table"
        assert (
            r.details["step1_query"]
            == "CREATE TABLE IF NOT EXISTS `tgt`.`s`.`t` (a INT)"
        )

    def test_managed_table_create_or_replace(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "MANAGED"
        p.source_dbops.show_create_table_ddl.return_value = (
            "CREATE TABLE src.s.t (a INT)"
        )
        [r] = p._uc_replicate_ddl("s", table_config(create_or_replace_table=True))
        assert (
            r.details["step1_query"] == "CREATE OR REPLACE TABLE `tgt`.`s`.`t` (a INT)"
        )

    def test_external_table_maps_location(self):
        p = _uc_provider(
            cloud_url_mapping={"abfss://c@acct.dfs.core.windows.net/": "s3://bucket/"}
        )
        p.db_ops.get_table_type.return_value = "EXTERNAL"
        p.source_dbops.show_create_table_ddl.return_value = TABLE_DDL
        [r] = p._uc_replicate_ddl("s", table_config())
        assert "LOCATION 's3://bucket/data/t'" in r.details["step1_query"]

    def test_external_replicated_as_managed_drops_location(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "EXTERNAL"
        p.source_dbops.show_create_table_ddl.return_value = TABLE_DDL
        [r] = p._uc_replicate_ddl("s", table_config(replicate_as_managed=True))
        assert "LOCATION" not in r.details["step1_query"]
        assert r.details["step1_query"].startswith("CREATE TABLE IF NOT EXISTS `tgt`")

    def test_unsupported_type_is_skipped(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "FOREIGN"
        [r] = p._uc_replicate_ddl("s", table_config())
        assert r.status == "skipped"
        assert r.error_message == "Create DDL is not supported for FOREIGN"
        p.target_spark.sql.assert_not_called()

    def test_missing_source_raises(self):
        p = _uc_provider()
        p.spark.catalog.tableExists.return_value = False
        with pytest.raises(TableNotFoundError):
            p._uc_replicate_ddl("s", table_config())

    def test_execution_failure_after_retries(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "MANAGED"
        p.source_dbops.show_create_table_ddl.return_value = "CREATE TABLE x (a INT)"
        p.target_spark.sql.side_effect = RuntimeError("PERMISSION_DENIED")
        [r] = p._uc_replicate_ddl("s", table_config(max_attempts=2))
        assert r.status == "failed"
        assert p.target_spark.sql.call_count == 2
        assert "PERMISSION_DENIED" in r.error_message
        assert (
            r.details["step1_query"]
            == "CREATE TABLE IF NOT EXISTS `tgt`.`s`.`t` (a INT)"
        )

    def test_show_create_failure_returns_failed_result(self):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = "VIEW"
        p.source_dbops.show_create_table_ddl.side_effect = RuntimeError("no access")
        [r] = p._uc_replicate_ddl("s", table_config())
        assert r.status == "failed"


# ---------- _uc_replicate_sql_st_mv ----------


def _resp(state, message=None):
    return SimpleNamespace(
        statement_id="stmt-1",
        status=SimpleNamespace(state=state, error=SimpleNamespace(message=message)),
    )


class TestUcReplicateSqlStMv:
    def _provider(
        self, table_type, ddl="CREATE MATERIALIZED VIEW src.s.t (a) AS SELECT 1"
    ):
        p = _uc_provider()
        p.db_ops.get_table_type.return_value = table_type
        p.db_ops.get_view_definition.return_value = "SELECT 1"
        p.source_dbops.show_create_table_ddl.return_value = ddl
        se = p.target_workspace_client.statement_execution
        se.execute_statement.return_value = _resp(StatementState.PENDING)
        se.get_statement.return_value = _resp(StatementState.SUCCEEDED)
        return p, se

    def test_materialized_view_polls_until_done(self):
        p, se = self._provider("MATERIALIZED_VIEW")
        [r] = p._uc_replicate_sql_st_mv("s", table_config())
        assert r.status == "success" and r.object_type == "materialized_view"
        assert r.details["step1_query"] == (
            "CREATE OR REPLACE MATERIALIZED VIEW `tgt`.`s`.`t` (a) AS SELECT 1"
        )
        kwargs = se.execute_statement.call_args.kwargs
        assert kwargs["warehouse_id"] == "wh-1"
        assert kwargs["statement"] == r.details["step1_query"]
        se.get_statement.assert_called_once_with("stmt-1")

    def test_materialized_view_if_not_exists(self):
        p, _ = self._provider("MATERIALIZED_VIEW")
        [r] = p._uc_replicate_sql_st_mv(
            "s", table_config(create_or_replace_materialized_view=False)
        )
        assert r.details["step1_query"].startswith(
            "CREATE MATERIALIZED VIEW IF NOT EXISTS `tgt`.`s`.`t`"
        )

    def test_streaming_table_maps_urls(self):
        ddl = "CREATE STREAMING TABLE src.s.t (a) AS SELECT * FROM STREAM read_files('abfss://c@a/x')"
        p, _ = self._provider("STREAMING_TABLE", ddl=ddl)
        p.cloud_url_mapping = {"abfss://c@a/": "s3://b/"}
        [r] = p._uc_replicate_sql_st_mv("s", table_config())
        assert r.status == "success" and r.object_type == "streaming_table"
        q = r.details["step1_query"]
        assert q.startswith("CREATE STREAMING TABLE IF NOT EXISTS `tgt`.`s`.`t`")
        assert "read_files('s3://b/x')" in q

    def test_streaming_table_create_or_replace(self):
        p, _ = self._provider(
            "STREAMING_TABLE", ddl="CREATE STREAMING TABLE x (a) AS SELECT 1"
        )
        [r] = p._uc_replicate_sql_st_mv(
            "s", table_config(create_or_replace_streaming_table=True)
        )
        assert r.details["step1_query"].startswith(
            "CREATE OR REPLACE STREAMING TABLE `tgt`.`s`.`t`"
        )

    def test_statement_failure_is_reported(self):
        p, se = self._provider("MATERIALIZED_VIEW")
        se.execute_statement.return_value = _resp(StatementState.FAILED, "bad sql")
        [r] = p._uc_replicate_sql_st_mv("s", table_config(max_attempts=1))
        assert r.status == "failed"
        assert "bad sql" in r.error_message

    def test_unsupported_type_skipped(self):
        p, se = self._provider("MANAGED")
        [r] = p._uc_replicate_sql_st_mv("s", table_config())
        assert r.status == "skipped"
        se.execute_statement.assert_not_called()

    def test_non_sql_definition_skipped(self):
        p, se = self._provider("STREAMING_TABLE")
        p.db_ops.get_view_definition.return_value = None
        [r] = p._uc_replicate_sql_st_mv("s", table_config())
        assert r.status == "skipped"
        assert "SQL definition not found" in r.error_message
        se.execute_statement.assert_not_called()

    def test_show_create_failure_returns_failed_result(self):
        p, _ = self._provider("MATERIALIZED_VIEW")
        p.source_dbops.show_create_table_ddl.side_effect = RuntimeError("denied")
        [r] = p._uc_replicate_sql_st_mv("s", table_config())
        assert r.status == "failed"


# ---------- table comments ----------


class TestTableComments:
    def _provider(self, table_type="MANAGED", src="new comment", tgt="old"):
        p = _uc_provider()
        p.source_dbops.get_table_type.return_value = table_type
        p.source_dbops.get_table_comments.return_value = src
        p.target_dbops.get_table_comments.return_value = tgt
        return p

    def test_table_comment_applied(self):
        p = self._provider()
        [r] = p._uc_replicate_table_comments("s", table_config())
        assert r.status == "success" and r.object_type == "table_comment"
        [q] = sql_calls(p.target_spark)
        assert "COMMENT ON TABLE `tgt`.`s`.`t` IS 'new comment'" in q
        p.source_dbops.get_table_comments.assert_called_once_with("src", "s", "t")
        p.target_dbops.get_table_comments.assert_called_once_with("tgt", "s", "t")

    def test_view_comment_applied(self):
        p = self._provider(table_type="VIEW")
        p._uc_replicate_table_comments("s", table_config())
        assert (
            "COMMENT ON VIEW `tgt`.`s`.`t` IS 'new comment'"
            in sql_calls(p.target_spark)[0]
        )

    def test_unchanged_comment_is_noop(self):
        p = self._provider(src="same", tgt="same")
        [r] = p._uc_replicate_table_comments("s", table_config())
        assert r.status == "success"
        p.target_spark.sql.assert_not_called()

    def test_both_empty_is_noop(self):
        p = self._provider(src=None, tgt="")
        [r] = p._uc_replicate_table_comments("s", table_config())
        assert r.status == "success"
        p.target_spark.sql.assert_not_called()

    def test_overwrite_disabled_is_noop(self):
        p = self._provider()
        [r] = p._uc_replicate_table_comments(
            "s", table_config(overwrite_comments=False)
        )
        assert r.status == "success"
        assert r.details["overwrite_comments"] is False
        p.target_spark.sql.assert_not_called()

    def test_dlt_types_skipped(self):
        p = self._provider(table_type="STREAMING_TABLE")
        [r] = p._uc_replicate_table_comments("s", table_config())
        assert r.status == "skipped"
        assert r.error_message == "table_comment is not supported for STREAMING_TABLE"

    def test_missing_target_fails(self):
        p = self._provider()
        p.target_spark.catalog.tableExists.return_value = False
        [r] = p._uc_replicate_table_comments("s", table_config())
        assert r.status == "failed"
        assert r.error_message == "Source or Target table does not exist"

    def test_execution_failure(self):
        p = self._provider()
        p.target_spark.sql.side_effect = RuntimeError("denied")
        [r] = p._uc_replicate_table_comments("s", table_config(max_attempts=1))
        assert r.status == "failed" and r.error_message == "denied"

    def test_lookup_exception(self):
        p = self._provider()
        p.source_dbops.get_table_type.side_effect = RuntimeError("lookup")
        [r] = p._uc_replicate_table_comments("s", table_config())
        assert r.status == "failed" and r.error_message == "lookup"

    def test_quote_in_comment_is_escaped(self):
        p = self._provider(src="owner's table")
        p._uc_replicate_table_comments("s", table_config())
        assert "IS 'owner\\'s table'" in sql_calls(p.target_spark)[0]


# ---------- column comments ----------


class TestColumnComments:
    def _provider(self, table_type="MANAGED", src=None, tgt=None):
        p = _uc_provider()
        p.source_dbops.get_table_type.return_value = table_type
        p.source_dbops.get_column_comments.return_value = (
            src if src is not None else [{"a": "col a"}, {"b": "col b"}]
        )
        p.target_dbops.get_column_comments.return_value = (
            tgt if tgt is not None else [{"a": "col a"}, {"b": ""}]
        )
        return p

    def test_managed_table_single_alter(self):
        p = self._provider()
        [r] = p._uc_replicate_column_comments("s", table_config())
        assert r.status == "success"
        assert r.details["columns_with_comments"] == 1
        [q] = sql_calls(p.target_spark)
        assert "ALTER TABLE `tgt`.`s`.`t` ALTER COLUMN `b` COMMENT 'col b'" in q

    def test_view_comment_per_column_escapes_quotes(self):
        p = self._provider(
            table_type="VIEW",
            src=[{"a": "it's a"}, {"b": "b\\c"}],
            tgt=[{"a": ""}, {"b": ""}],
        )
        [r] = p._uc_replicate_column_comments("s", table_config())
        assert r.status == "success"
        queries = sql_calls(p.target_spark)
        assert len(queries) == 2
        assert "COMMENT ON COLUMN `tgt`.`s`.`t`.`a` IS 'it\\'s a'" in queries[0]
        assert "IS 'b\\\\c'" in queries[1]

    def test_streaming_table_uses_alter_streaming_table(self):
        p = self._provider(table_type="STREAMING_TABLE")
        p._uc_replicate_column_comments("s", table_config())
        [q] = sql_calls(p.target_spark)
        assert (
            "ALTER STREAMING TABLE `tgt`.`s`.`t` ALTER COLUMN `b` COMMENT 'col b'" in q
        )

    def test_no_differences_is_noop(self):
        p = self._provider(src=[{"a": "x"}], tgt=[{"a": "x"}])
        [r] = p._uc_replicate_column_comments("s", table_config())
        assert r.status == "success"
        p.target_spark.sql.assert_not_called()

    def test_keep_target_comment_when_not_overwriting(self):
        p = self._provider(src=[{"a": "src"}], tgt=[{"a": "tgt"}])
        p._uc_replicate_column_comments("s", table_config(overwrite_comments=False))
        assert "COMMENT 'tgt'" in sql_calls(p.target_spark)[0]

    def test_unsupported_type_skipped(self):
        p = self._provider(table_type="MATERIALIZED_VIEW")
        [r] = p._uc_replicate_column_comments("s", table_config())
        assert r.status == "skipped"
        p.target_spark.sql.assert_not_called()

    def test_missing_table_fails(self):
        p = self._provider()
        p.source_spark.catalog.tableExists.return_value = False
        [r] = p._uc_replicate_column_comments("s", table_config())
        assert r.status == "failed"

    def test_execution_failure(self):
        p = self._provider()
        p.target_spark.sql.side_effect = RuntimeError("bad column")
        [r] = p._uc_replicate_column_comments("s", table_config(max_attempts=1))
        assert r.status == "failed" and r.error_message == "bad column"

    def test_lookup_exception(self):
        p = self._provider()
        p.source_dbops.get_column_comments.side_effect = RuntimeError("lookup")
        [r] = p._uc_replicate_column_comments("s", table_config())
        assert r.status == "failed" and r.error_message == "lookup"

    def test_managed_table_escapes_quotes(self):
        p = self._provider(src=[{"a": "it's"}], tgt=[{"a": ""}])
        p._uc_replicate_column_comments("s", table_config())
        assert "COMMENT 'it\\'s'" in sql_calls(p.target_spark)[0]


# ---------- tags ----------


class TestTableAndVolumeTags:
    def test_table_tags_delegate_to_replicate_tags(self):
        p = _uc_provider()
        p.target_dbops.get_table_tags.return_value = (["k"], [{"k": "old"}])
        p.source_dbops.get_table_tags.return_value = (["k"], [{"k": "new"}])
        p._replicate_tags = MagicMock(return_value="TAG_RESULT")
        assert p._uc_replicate_table_tags("s", table_config(overwrite_tags=False)) == [
            "TAG_RESULT"
        ]
        kw = p._replicate_tags.call_args.kwargs
        assert kw["object_type"] == "table_tag"
        assert kw["source_tag_maps_list"] == [{"k": "new"}]
        assert kw["target_tag_names_list"] == ["k"]
        assert kw["overwrite_tags"] is False
        assert (kw["source_catalog"], kw["target_catalog"], kw["table_name"]) == (
            "src",
            "tgt",
            "t",
        )

    def test_table_tags_missing_table(self):
        p = _uc_provider()
        p.target_spark.catalog.tableExists.return_value = False
        p._replicate_tags = MagicMock()
        [r] = p._uc_replicate_table_tags("s", table_config())
        assert r.status == "failed" and r.object_type == "table_tag"
        p._replicate_tags.assert_not_called()

    def test_volume_tags_delegate(self):
        p = _uc_provider()
        p.source_dbops.if_volume_exists.return_value = True
        p.target_dbops.if_volume_exists.return_value = True
        p.target_dbops.get_volume_tags.return_value = (None, None)
        p.source_dbops.get_volume_tags.return_value = (["k"], [{"k": "v"}])
        p._replicate_tags = MagicMock(return_value="VOL_TAG")
        assert p._uc_replicate_volume_tags("s", volume_config()) == ["VOL_TAG"]
        kw = p._replicate_tags.call_args.kwargs
        assert kw["object_type"] == "volume_tag" and kw["volume_name"] == "v"
        p.source_dbops.if_volume_exists.assert_called_once_with("`src`.`s`.`v`")

    def test_volume_tags_missing_volume(self):
        p = _uc_provider()
        p.source_dbops.if_volume_exists.return_value = True
        p.target_dbops.if_volume_exists.return_value = False
        [r] = p._uc_replicate_volume_tags("s", volume_config())
        assert r.status == "failed"
        assert r.error_message == "Source or Target volume does not exist"


class TestColumnTags:
    def _tags_df(self, rows, prefix):
        d = df(rows)
        d.selectExpr.return_value = d
        return d

    def _provider(self, src_rows, tgt_rows):
        p = _uc_provider()
        src = self._tags_df(src_rows, "source")
        tgt = self._tags_df(tgt_rows, "target")
        p.source_dbops.get_column_tags_df.return_value = src
        p.target_dbops.get_column_tags_df.return_value = tgt
        p._replicate_tags = MagicMock(side_effect=lambda **kw: kw["column_name"])
        return p, src, tgt

    def test_both_sides_joined_full_outer(self):
        merged_rows = [
            {
                "column_name": "a",
                "source_tag_maps_list": [{"k": "v"}],
                "target_tag_names_list": [],
                "target_tag_maps_list": [],
            },
            {
                "column_name": "b",
                "source_tag_maps_list": [],
                "target_tag_names_list": ["k"],
                "target_tag_maps_list": [{"k": "v"}],
            },
        ]
        src_row = {
            "column_name": "a",
            "source_tag_names_list": ["k"],
            "source_tag_maps_list": [{"k": "v"}],
        }
        p, _, tgt = self._provider([src_row], [{"column_name": "b"}])
        joined = df(merged_rows)
        p.target_spark.createDataFrame.return_value.join.return_value = joined
        out = p._uc_replicate_column_tags("s", table_config())
        assert out == ["a", "b"]
        rows_arg = p.target_spark.createDataFrame.call_args.args[0]
        assert rows_arg == [("a", ["k"], [{"k": "v"}])]
        p.target_spark.createDataFrame.return_value.join.assert_called_once_with(
            tgt, on=["column_name"], how="fullouter"
        )

    def test_source_only(self):
        row = {
            "column_name": "a",
            "source_tag_maps_list": [{"k": "v"}],
            "target_tag_names_list": [],
            "target_tag_maps_list": [],
        }
        p, src, _ = self._provider([row], [])
        assert p._uc_replicate_column_tags("s", table_config()) == ["a"]
        assert "array() as target_tag_names_list" in src.selectExpr.call_args.args

    def test_target_only_with_overwrite(self):
        row = {
            "column_name": "b",
            "source_tag_maps_list": [],
            "target_tag_names_list": ["k"],
            "target_tag_maps_list": [{"k": "v"}],
        }
        p, _, tgt = self._provider([], [row])
        assert p._uc_replicate_column_tags("s", table_config()) == ["b"]
        assert "array() as source_tag_names_list" in tgt.selectExpr.call_args.args

    def test_target_only_without_overwrite_is_noop(self):
        p, _, _ = self._provider([], [{"column_name": "b"}])
        [r] = p._uc_replicate_column_tags("s", table_config(overwrite_tags=False))
        assert r.status == "success"
        assert r.details["columns_with_tags_in_target"] == 1
        p._replicate_tags.assert_not_called()

    def test_no_tags_anywhere(self):
        p, _, _ = self._provider([], [])
        [r] = p._uc_replicate_column_tags("s", table_config())
        assert r.status == "success"
        assert r.details["columns_with_tags_in_source"] == 0
        p._replicate_tags.assert_not_called()

    def test_missing_table(self):
        p, _, _ = self._provider([], [])
        p.source_spark.catalog.tableExists.return_value = False
        [r] = p._uc_replicate_column_tags("s", table_config())
        assert r.status == "failed" and r.object_type == "column_tag"

    def test_exception(self):
        p, _, _ = self._provider([], [])
        p.source_dbops.get_column_tags_df.side_effect = RuntimeError("tags down")
        [r] = p._uc_replicate_column_tags("s", table_config())
        assert r.status == "failed" and r.error_message == "tags down"


# ---------- grants ----------


class TestGrants:
    def test_table_grants_delegate(self):
        p = _uc_provider()
        p._replicate_grants = MagicMock(return_value="GRANTS")
        assert p._uc_replicate_table_grants(
            "s", table_config(overwrite_grants=True)
        ) == ["GRANTS"]
        kw = p._replicate_grants.call_args.kwargs
        assert kw["securable_type"] == "TABLE"
        assert kw["source_full_name"] == "src.s.t"
        assert kw["target_full_name"] == "tgt.s.t"
        assert kw["overwrite_grants"] is True

    def test_table_grants_missing_table(self):
        p = _uc_provider()
        p.source_spark.catalog.tableExists.return_value = False
        p._replicate_grants = MagicMock()
        [r] = p._uc_replicate_table_grants("s", table_config())
        assert r.status == "failed" and r.object_type == "table_grant"
        p._replicate_grants.assert_not_called()

    def test_volume_grants_delegate(self):
        p = _uc_provider()
        p.source_dbops.if_volume_exists.return_value = True
        p.target_dbops.if_volume_exists.return_value = True
        p._replicate_grants = MagicMock(return_value="VGRANTS")
        assert p._uc_replicate_volume_grants("s", volume_config()) == ["VGRANTS"]
        kw = p._replicate_grants.call_args.kwargs
        assert kw["securable_type"] == "VOLUME"
        assert kw["source_full_name"] == "src.s.v"
        assert kw["overwrite_grants"] is False

    def test_volume_grants_missing_volume(self):
        p = _uc_provider()
        p.source_dbops.if_volume_exists.return_value = False
        [r] = p._uc_replicate_volume_grants("s", volume_config())
        assert r.status == "failed" and r.object_type == "volume_grant"


# ---------- _uc_replicate_volume ----------


def _volume_info(volume_type=VolumeType.MANAGED, location=None, comment="c"):
    return VolumeInfo(
        catalog_name="src",
        schema_name="s",
        name="v",
        full_name="src.s.v",
        volume_type=volume_type,
        storage_location=location,
        comment=comment,
    )


class TestUcReplicateVolume:
    def test_creates_managed_volume(self):
        p = _uc_provider()
        p.source_dbops.get_volume.return_value = _volume_info()
        p.target_dbops.get_volume.side_effect = RuntimeError("not found")
        [r] = p._uc_replicate_volume("s", volume_config())
        assert r.status == "success" and r.details["volume_existed"] is False
        created = p.target_dbops.create_volume.call_args.args[0]
        assert created["catalog_name"] == "tgt"
        assert created["schema_name"] == "s" and created["name"] == "v"
        assert created["volume_type"] == VolumeType.MANAGED
        assert created["storage_location"] is None
        assert created["comment"] == "c"

    def test_creates_external_volume_with_mapped_location(self):
        p = _uc_provider(cloud_url_mapping={"abfss://c@a/": "s3://b/"})
        p.source_dbops.get_volume.return_value = _volume_info(
            VolumeType.EXTERNAL, "abfss://c@a/vols/v"
        )
        p.target_dbops.get_volume.side_effect = RuntimeError("not found")
        [r] = p._uc_replicate_volume("s", volume_config())
        assert r.status == "success"
        assert r.details["target_storage_location"] == "s3://b/vols/v"
        created = p.target_dbops.create_volume.call_args.args[0]
        assert created["storage_location"] == "s3://b/vols/v"
        assert created["volume_type"] == VolumeType.EXTERNAL

    def test_external_replicated_as_managed(self):
        p = _uc_provider()
        p.source_dbops.get_volume.return_value = _volume_info(
            VolumeType.EXTERNAL, "abfss://c@a/vols/v"
        )
        p.target_dbops.get_volume.side_effect = RuntimeError("not found")
        [r] = p._uc_replicate_volume("s", volume_config(replicate_as_managed=True))
        assert r.status == "success"
        created = p.target_dbops.create_volume.call_args.args[0]
        assert created["volume_type"] == VolumeType.MANAGED
        assert created["storage_location"] is None

    def test_external_without_mapping_fails(self):
        p = _uc_provider()
        p.source_dbops.get_volume.return_value = _volume_info(
            VolumeType.EXTERNAL, "abfss://c@a/vols/v"
        )
        [r] = p._uc_replicate_volume("s", volume_config())
        assert r.status == "failed"
        assert "cloud_url_mapping is not configured" in r.error_message
        p.target_dbops.create_volume.assert_not_called()

    def test_external_with_unmatched_mapping_fails(self):
        p = _uc_provider(cloud_url_mapping={"gs://x/": "s3://b/"})
        p.source_dbops.get_volume.return_value = _volume_info(
            VolumeType.EXTERNAL, "abfss://c@a/vols/v"
        )
        [r] = p._uc_replicate_volume("s", volume_config())
        assert r.status == "failed"
        assert "No external location mapping found" in r.error_message

    def test_existing_volume_updated_when_comment_differs(self):
        p = _uc_provider()
        p.source_dbops.get_volume.return_value = _volume_info(comment="new")
        p.target_dbops.get_volume.return_value = VolumeInfo(
            full_name="tgt.s.v", comment="old", volume_type=VolumeType.MANAGED
        )
        [r] = p._uc_replicate_volume("s", volume_config())
        assert r.details["volume_existed"] is True
        p.target_dbops.update_volume.assert_called_once_with(
            {"full_name": "tgt.s.v", "comment": "new"}
        )
        p.target_dbops.create_volume.assert_not_called()

    def test_existing_volume_unchanged_not_updated(self):
        p = _uc_provider()
        p.source_dbops.get_volume.return_value = _volume_info(comment="same")
        p.target_dbops.get_volume.return_value = VolumeInfo(
            full_name="tgt.s.v", comment="same", volume_type=VolumeType.MANAGED
        )
        [r] = p._uc_replicate_volume("s", volume_config())
        assert r.status == "success"
        p.target_dbops.update_volume.assert_not_called()

    def test_create_failure(self):
        p = _uc_provider()
        p.source_dbops.get_volume.return_value = _volume_info()
        p.target_dbops.get_volume.side_effect = RuntimeError("not found")
        p.target_dbops.create_volume.side_effect = RuntimeError("quota")
        [r] = p._uc_replicate_volume("s", volume_config())
        assert r.status == "failed" and "quota" in r.error_message
