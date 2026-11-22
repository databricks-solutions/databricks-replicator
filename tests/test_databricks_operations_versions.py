"""Delta history / time-travel helpers in databricks_operations.py and sql_utils.py.

All Spark calls are mocked; no workspace access.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from data_replication.constants import DELTA_DATA_CHANGE_OPERATIONS
from data_replication.databricks_operations import DatabricksOperations
from data_replication.sql_utils import normalize_table_name, quote_table_name


@pytest.fixture
def ops():
    spark = MagicMock()
    return DatabricksOperations(spark=spark, workspace_client=MagicMock()), spark


def _rows(spark, rows):
    spark.sql.return_value.collect.return_value = rows


class TestQuoting:
    @pytest.mark.parametrize(
        "name",
        ["cat.sch.tbl", "`cat`.`sch`.`tbl`"],
    )
    def test_quote_table_name_is_idempotent_for_plain_and_quoted(self, name):
        assert quote_table_name(name) == "`cat`.`sch`.`tbl`"

    def test_quote_table_name_escapes_backticks(self):
        assert quote_table_name("cat.sch.we`ird") == "`cat`.`sch`.`we``ird`"

    def test_normalize_table_name(self):
        assert normalize_table_name("`SRC`.sales.`Orders`") == "src.sales.orders"


class TestGetLatestCloneCommit:
    def test_none_when_no_clone_commit(self, ops):
        o, spark = ops
        _rows(spark, [])
        assert o.get_latest_clone_commit("cat.sch.t") is None

    def test_parses_target_and_source_versions(self, ops):
        o, spark = ops
        _rows(
            spark,
            [
                {
                    "version": 5,
                    "operationParameters": {
                        "source": "src.sch.t",
                        "sourceVersion": "12",
                        "isShallow": "false",
                    },
                }
            ],
        )
        assert o.get_latest_clone_commit("cat.sch.t") == {
            "target_version": 5,
            "source_table": "src.sch.t",
            "source_version": 12,
        }

    def test_missing_source_version_is_none(self, ops):
        o, spark = ops
        _rows(spark, [{"version": 0, "operationParameters": {"source": "s.t.u"}}])
        clone = o.get_latest_clone_commit("cat.sch.t")
        assert clone["source_version"] is None
        assert clone["source_table"] == "s.t.u"

    def test_query_reads_clone_operation_from_quoted_history(self, ops):
        o, spark = ops
        _rows(spark, [])
        o.get_latest_clone_commit("cat.sch.t")
        query = spark.sql.call_args.args[0]
        assert "DESCRIBE HISTORY `cat`.`sch`.`t`" in query
        assert "operation = 'CLONE'" in query
        assert "ORDER BY version DESC LIMIT 1" in query


class TestHasDataWritesAfter:
    def test_true_when_a_data_commit_follows(self, ops):
        o, spark = ops
        _rows(spark, [{"n": 2}])
        assert o.has_data_writes_after("cat.sch.t", 4) is True
        query = spark.sql.call_args.args[0]
        assert "version > 4" in query
        for op in DELTA_DATA_CHANGE_OPERATIONS:
            assert f"'{op}'" in query

    def test_false_when_none(self, ops):
        o, spark = ops
        _rows(spark, [{"n": 0}])
        assert o.has_data_writes_after("cat.sch.t", 4) is False


class TestGetTimeTravelError:
    def test_none_when_version_readable(self, ops):
        o, spark = ops
        _rows(spark, [])
        assert o.get_time_travel_error("cat.sch.t", 3) is None
        assert "VERSION AS OF 3 LIMIT 1" in spark.sql.call_args.args[0]

    def test_first_line_of_error_returned(self, ops):
        o, spark = ops
        spark.sql.side_effect = RuntimeError("cannot read v3\nstack...")
        assert o.get_time_travel_error("cat.sch.t", 3) == "cannot read v3"

    def test_empty_error_message_still_reported(self, ops):
        o, spark = ops
        spark.sql.side_effect = RuntimeError("")
        assert o.get_time_travel_error("cat.sch.t", 3) == "RuntimeError('')"


class TestCommonFieldsAtVersion:
    def test_live_path_unchanged(self, ops, monkeypatch):
        o, spark = ops
        fields = {"s": ["a", "b"], "t": ["b", "c"]}
        monkeypatch.setattr(o, "get_table_fields", lambda t: fields[t])
        assert o.get_common_fields("s", "t") == ["b"]
        spark.sql.assert_not_called()

    def test_versioned_path_reads_schema_at_each_version(self, ops):
        o, spark = ops
        columns = {
            "VERSION AS OF 7 LIMIT 0": ["id", "name", "old"],
            "VERSION AS OF 4 LIMIT 0": ["id", "name", "new"],
        }

        def sql(query):
            df = MagicMock()
            df.columns = next(cols for key, cols in columns.items() if key in query)
            return df

        spark.sql.side_effect = sql
        common = o.get_common_fields(
            "src.s.t", "tgt.s.t", source_version=7, target_version=4
        )
        assert sorted(common) == ["id", "name"]
        queries = [c.args[0] for c in spark.sql.call_args_list]
        assert "FROM `src`.`s`.`t` VERSION AS OF 7 LIMIT 0" in queries[0]
        assert "FROM `tgt`.`s`.`t` VERSION AS OF 4 LIMIT 0" in queries[1]
