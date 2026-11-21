"""Tests for UC function replication (issue #38)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from data_replication.cli import args as cli_args
from data_replication.config.models import ReplicationConfig, RetryConfig, UCObjectType
from data_replication.databricks_operations import DatabricksOperations
from data_replication.function_ddl import (
    UnsupportedFunctionError,
    build_create_function_ddl,
)
from data_replication.providers.replication_provider import ReplicationProvider

TARGET = "`tgt`.`sch`.`fn`"


def _param(pos, name, full_type, default=None, comment=None):
    return {
        "ordinal_position": pos,
        "parameter_name": name,
        "data_type": full_type.split("(")[0].split("<")[0],
        "full_data_type": full_type,
        "parameter_default": default,
        "comment": comment,
        "is_result": "NO",
    }


def _col(pos, name, full_type, comment=None):
    return {
        "ordinal_position": pos,
        "column_name": name,
        "data_type": full_type,
        "full_data_type": full_type,
        "comment": comment,
    }


# ---------- DDL reconstruction ----------


class TestSqlScalar:
    def routine(self, **kw):
        row = {
            "routine_name": "fn",
            "routine_body": "SQL",
            "external_language": None,
            "data_type": "DECIMAL",
            "full_data_type": "DECIMAL(10,2)",
            "routine_definition": "x * rate",
            "is_deterministic": "YES",
            "sql_data_access": "CONTAINS SQL",
            "comment": "Apply rate",
        }
        row.update(kw)
        return row

    def test_full_ddl(self):
        ddl = build_create_function_ddl(
            self.routine(),
            [
                _param(2, "rate", "DOUBLE", default="0.5", comment="the rate"),
                _param(1, "x", "DECIMAL(10,2)"),
            ],
            [],
            TARGET,
        )
        assert ddl == (
            "CREATE FUNCTION IF NOT EXISTS `tgt`.`sch`.`fn`"
            "(`x` DECIMAL(10,2), `rate` DOUBLE DEFAULT 0.5 COMMENT 'the rate')\n"
            "RETURNS DECIMAL(10,2)\n"
            "LANGUAGE SQL\n"
            "DETERMINISTIC\n"
            "COMMENT 'Apply rate'\n"
            "CONTAINS SQL\n"
            "RETURN x * rate"
        )

    def test_create_or_replace(self):
        ddl = build_create_function_ddl(self.routine(), [], [], TARGET, create_or_replace=True)
        assert ddl.startswith("CREATE OR REPLACE FUNCTION `tgt`.`sch`.`fn`()")
        assert "IF NOT EXISTS" not in ddl

    def test_not_deterministic_and_reads_sql_data(self):
        ddl = build_create_function_ddl(
            self.routine(is_deterministic="NO", sql_data_access="READS SQL DATA"),
            [], [], TARGET,
        )
        assert "\nNOT DETERMINISTIC\n" in ddl
        assert "\nREADS SQL DATA\n" in ddl

    def test_unknown_determinism_and_unsupported_access_omitted(self):
        ddl = build_create_function_ddl(
            self.routine(is_deterministic=None, sql_data_access="MODIFIES SQL DATA", comment=None),
            [], [], TARGET,
        )
        assert "DETERMINISTIC" not in ddl
        assert "MODIFIES" not in ddl
        assert "COMMENT" not in ddl

    def test_comment_quotes_escaped(self):
        ddl = build_create_function_ddl(self.routine(comment="it's a \\ test"), [], [], TARGET)
        assert "COMMENT 'it\\'s a \\\\ test'" in ddl

    def test_falls_back_to_data_type(self):
        ddl = build_create_function_ddl(
            self.routine(full_data_type=None, data_type="STRING"), [], [], TARGET
        )
        assert "\nRETURNS STRING\n" in ddl

    def test_leading_return_not_duplicated(self):
        ddl = build_create_function_ddl(
            self.routine(routine_definition="RETURN 1"), [], [], TARGET
        )
        assert ddl.endswith("\nRETURN 1")
        ddl = build_create_function_ddl(
            self.routine(routine_definition="returned_col + 1"), [], [], TARGET
        )
        assert ddl.endswith("\nRETURN returned_col + 1")

    def test_complex_parameter_types_and_quoted_names(self):
        ddl = build_create_function_ddl(
            self.routine(),
            [_param(1, "we`ird", "ARRAY<STRUCT<a: INT, b: STRING>>")],
            [], TARGET,
        )
        assert "(`we``ird` ARRAY<STRUCT<a: INT, b: STRING>>)" in ddl

    def test_result_parameter_rows_ignored(self):
        result_row = _param(0, "ret", "INT")
        result_row["is_result"] = "YES"
        ddl = build_create_function_ddl(self.routine(), [result_row], [], TARGET)
        assert "`tgt`.`sch`.`fn`()" in ddl

    def test_uppercase_keys_accepted(self):
        routine = {k.upper(): v for k, v in self.routine().items()}
        ddl = build_create_function_ddl(routine, [], [], TARGET)
        assert "RETURNS DECIMAL(10,2)" in ddl

    def test_missing_body_raises(self):
        with pytest.raises(UnsupportedFunctionError):
            build_create_function_ddl(self.routine(routine_definition="  "), [], [], TARGET)


class TestSqlTableValued:
    def routine(self, **kw):
        row = {
            "routine_name": "fn",
            "routine_body": "SQL",
            "data_type": "TABLE",
            "full_data_type": "TABLE",
            "routine_definition": "SELECT id, name FROM src.sch.t WHERE id > min_id",
            "is_deterministic": "NO",
            "sql_data_access": "READS SQL DATA",
            "comment": None,
        }
        row.update(kw)
        return row

    def test_returns_table(self):
        ddl = build_create_function_ddl(
            self.routine(),
            [_param(1, "min_id", "BIGINT", default="0")],
            [_col(2, "name", "STRING", comment="display name"), _col(1, "id", "BIGINT")],
            TARGET,
        )
        assert ddl == (
            "CREATE FUNCTION IF NOT EXISTS `tgt`.`sch`.`fn`(`min_id` BIGINT DEFAULT 0)\n"
            "RETURNS TABLE (`id` BIGINT, `name` STRING COMMENT 'display name')\n"
            "LANGUAGE SQL\n"
            "NOT DETERMINISTIC\n"
            "READS SQL DATA\n"
            "RETURN SELECT id, name FROM src.sch.t WHERE id > min_id"
        )

    def test_table_without_result_columns_raises(self):
        with pytest.raises(UnsupportedFunctionError, match="result columns"):
            build_create_function_ddl(self.routine(), [], [], TARGET)


class TestPython:
    def routine(self, **kw):
        row = {
            "routine_name": "fn",
            "routine_body": "EXTERNAL",
            "external_language": "Python",
            "data_type": "STRING",
            "full_data_type": "STRING",
            "routine_definition": "\nreturn s.upper()\n",
            "is_deterministic": "YES",
            "sql_data_access": "NO SQL",
            "comment": "upper",
        }
        row.update(kw)
        return row

    def test_python_udf(self):
        ddl = build_create_function_ddl(
            self.routine(), [_param(1, "s", "STRING")], [], TARGET, create_or_replace=True
        )
        assert ddl == (
            "CREATE OR REPLACE FUNCTION `tgt`.`sch`.`fn`(`s` STRING)\n"
            "RETURNS STRING\n"
            "LANGUAGE PYTHON\n"
            "DETERMINISTIC\n"
            "COMMENT 'upper'\n"
            "AS $$\nreturn s.upper()\n$$"
        )

    def test_python_body_indentation_preserved(self):
        body = "def f(x):\n    return x\nreturn f(s)"
        ddl = build_create_function_ddl(self.routine(routine_definition=body), [], [], TARGET)
        assert ddl.endswith(f"AS $$\n{body}\n$$")

    def test_python_body_with_dollar_quotes_raises(self):
        with pytest.raises(UnsupportedFunctionError, match=r"\$\$"):
            build_create_function_ddl(
                self.routine(routine_definition="return '$$'"), [], [], TARGET
            )

    def test_unsupported_language_raises(self):
        with pytest.raises(UnsupportedFunctionError, match="Unsupported"):
            build_create_function_ddl(
                self.routine(external_language="SCALA"), [], [], TARGET
            )


# ---------- enum / CLI / config ----------


def test_enum_values():
    assert UCObjectType("function") is UCObjectType.FUNCTION
    assert UCObjectType("function_grant") is UCObjectType.FUNCTION_GRANT


def test_cli_parses_function_types():
    out = cli_args.parse_comma_delimited_enums(
        "function,function_grant", UCObjectType, "--uc-object-types"
    )
    assert out == [UCObjectType.FUNCTION, UCObjectType.FUNCTION_GRANT]
    assert cli_args.parse_comma_delimited_enums(
        "all", UCObjectType, "--uc-object-types"
    ) == [UCObjectType.ALL]


def test_create_or_replace_function_default():
    assert ReplicationConfig().create_or_replace_function is True
    assert ReplicationConfig(create_or_replace_function=False).create_or_replace_function is False


# ---------- DatabricksOperations ----------


class _Row(dict):
    def asDict(self):
        return dict(self)


def test_get_function_metadata_queries_information_schema():
    spark = MagicMock()
    results = {
        "routines": [_Row(routine_name="fn", routine_body="SQL")],
        "parameters": [_Row(parameter_name="x")],
        "routine_columns": [],
    }

    def fake_sql(query):
        for view, rows in results.items():
            if f"information_schema.{view} " in query:
                df = MagicMock()
                df.collect.return_value = rows
                return df
        raise AssertionError(query)

    spark.sql.side_effect = fake_sql
    ops = DatabricksOperations(spark=spark)
    meta = ops.get_function_metadata("src", "s'ch", "fn")
    assert meta == {
        "routine": {"routine_name": "fn", "routine_body": "SQL"},
        "parameters": [{"parameter_name": "x"}],
        "result_columns": [],
    }
    queries = [c.args[0] for c in spark.sql.call_args_list]
    assert all("`src`.information_schema." in q for q in queries)
    assert all("'s\\'ch'" in q for q in queries)


def test_get_function_metadata_not_found():
    spark = MagicMock()
    spark.sql.return_value.collect.return_value = []
    assert DatabricksOperations(spark=spark).get_function_metadata("c", "s", "f") is None


# ---------- ReplicationProvider ----------


@pytest.fixture
def provider():
    p = ReplicationProvider.__new__(ReplicationProvider)
    p.logger = MagicMock()
    p.run_id = "run"
    p.catalog_config = MagicMock()
    p.catalog_config.catalog_name = "tgt"
    p.source_dbops = MagicMock()
    p.target_dbops = MagicMock()
    p.target_spark = MagicMock()
    p.principal_mapping = {}
    return p


@pytest.fixture
def schema_config():
    sc = MagicMock()
    sc.schema_name = "sch"
    sc.replication_config = ReplicationConfig(source_catalog="src")
    sc.retry = RetryConfig(max_attempts=1, retry_delay_seconds=1)
    return sc


SCALAR_META = {
    "routine": {
        "routine_body": "SQL",
        "full_data_type": "INT",
        "routine_definition": "x + 1",
        "is_deterministic": "YES",
    },
    "parameters": [_param(1, "x", "INT")],
    "result_columns": [],
}


def test_replicate_functions_success(provider, schema_config):
    provider.source_dbops.get_functions_in_schema.return_value = ["fn"]
    provider.source_dbops.get_function_metadata.return_value = SCALAR_META
    results = provider._uc_replicate_functions(schema_config)
    assert [r.status for r in results] == ["success"]
    assert results[0].object_type == "function"
    ddl = provider.target_spark.sql.call_args.args[0]
    assert ddl.startswith("CREATE OR REPLACE FUNCTION `tgt`.`sch`.`fn`(`x` INT)")


def test_replicate_functions_if_not_exists(provider, schema_config):
    schema_config.replication_config = ReplicationConfig(
        source_catalog="src", create_or_replace_function=False
    )
    provider.source_dbops.get_functions_in_schema.return_value = ["fn"]
    provider.source_dbops.get_function_metadata.return_value = SCALAR_META
    provider._uc_replicate_functions(schema_config)
    assert "CREATE FUNCTION IF NOT EXISTS" in provider.target_spark.sql.call_args.args[0]


def test_replicate_functions_failure_and_skip(provider, schema_config):
    provider.source_dbops.get_functions_in_schema.return_value = ["bad", "odd"]
    odd = {"routine": {"routine_body": "EXTERNAL", "external_language": "SCALA",
                       "routine_definition": "x"}, "parameters": [], "result_columns": []}
    provider.source_dbops.get_function_metadata.side_effect = [SCALAR_META, odd]
    provider.target_spark.sql.side_effect = Exception("boom")
    results = provider._uc_replicate_functions(schema_config)
    assert [r.status for r in results] == ["failed", "skipped"]
    assert "boom" in results[0].error_message


def test_replicate_functions_listing_failure(provider, schema_config):
    provider.source_dbops.get_functions_in_schema.side_effect = Exception("no access")
    results = provider._uc_replicate_functions(schema_config)
    assert len(results) == 1 and results[0].status == "failed"
    assert "no access" in results[0].error_message


def test_replicate_function_grants(provider, schema_config):
    provider.source_dbops.get_functions_in_schema.return_value = ["fn", "missing"]
    provider.target_dbops.if_function_exists.side_effect = [True, False]
    provider._replicate_grants = MagicMock(return_value="grant-result")
    results = provider._uc_replicate_function_grants(schema_config)
    assert results[0] == "grant-result"
    assert results[1].status == "failed"
    kwargs = provider._replicate_grants.call_args.kwargs
    assert kwargs["securable_type"] == "FUNCTION"
    assert kwargs["object_type"] == "function_grant"
    assert kwargs["source_full_name"] == "src.sch.fn"
    assert kwargs["target_full_name"] == "tgt.sch.fn"


# ---------- orchestration order ----------


def _orchestration_provider(uc_types, calls):
    from data_replication.config.models import (
        ConcurrencyConfig,
        SchemaConfig,
        TargetCatalogConfig,
    )

    p = ReplicationProvider.__new__(ReplicationProvider)
    p.logger = MagicMock()
    p.run_id = "run"
    p.audit_logger = MagicMock()
    p.catalog_config = TargetCatalogConfig(
        catalog_name="tgt",
        uc_object_types=uc_types,
        replication_config=ReplicationConfig(enabled=True, source_catalog="src"),
        concurrency=ConcurrencyConfig(process_schemas_in_serial=True),
        retry=RetryConfig(),
    )
    p.setup_operation_catalogs = lambda: "src"
    p._get_schemas = lambda: [SchemaConfig(schema_name="sch")]

    def rec(name, value):
        def _f(*_a, **_k):
            calls.append(name)
            return value
        return _f

    p._uc_replicate_catalog = rec("catalog", [])
    p._uc_replicate_catalog_tags = rec("catalog_tag", [])
    p._uc_replicate_catalog_grants = rec("catalog_grant", [])
    p._uc_replicate_schema = rec("schema", [])
    p._uc_replicate_schema_tags = rec("schema_tag", [])
    p._uc_replicate_schema_grants = rec("schema_grant", [])
    p._uc_replicate_functions = rec("function", [])
    p._uc_replicate_function_grants = rec("function_grant", [])
    p.process_schema = rec("tables", ([], [], []))
    return p


def test_functions_run_after_schema_and_before_tables():
    calls = []
    p = _orchestration_provider([UCObjectType.ALL], calls)
    p.process_catalog()
    assert calls == [
        "catalog", "catalog_tag", "catalog_grant",
        "schema", "schema_tag", "schema_grant",
        "function", "function_grant", "tables",
    ]


def test_function_only_skips_tables():
    calls = []
    p = _orchestration_provider([UCObjectType.FUNCTION], calls)
    p.process_catalog()
    assert calls == ["function"]


def test_function_and_table_types_both_run():
    calls = []
    p = _orchestration_provider([UCObjectType.FUNCTION, UCObjectType.TABLE], calls)
    p.process_catalog()
    assert calls == ["function", "tables"]
