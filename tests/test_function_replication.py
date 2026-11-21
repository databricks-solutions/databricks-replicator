"""Tests for UC function replication (issue #38)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    ColumnTypeName,
    FunctionInfo,
    FunctionInfoRoutineBody,
    FunctionInfoSqlDataAccess,
    FunctionParameterInfo,
    FunctionParameterInfos,
)

from data_replication.cli import args as cli_args
from data_replication.config.models import ReplicationConfig, RetryConfig, UCObjectType
from data_replication.databricks_operations import DatabricksOperations
from data_replication.function_ddl import (
    UnsupportedFunctionError,
    build_create_function_ddl,
)
from data_replication.providers.replication_provider import ReplicationProvider

TARGET = "`tgt`.`sch`.`fn`"


def _param(pos, name, type_text, default=None, comment=None):
    return FunctionParameterInfo(
        name=name,
        type_text=type_text,
        type_name=ColumnTypeName.STRING,
        position=pos,
        parameter_default=default,
        comment=comment,
    )


def _params(*params):
    return FunctionParameterInfos(parameters=list(params))


def _fn(**kw):
    """A SQL scalar FunctionInfo; override any field via kwargs."""
    base = dict(
        name="fn",
        routine_body=FunctionInfoRoutineBody.SQL,
        data_type=ColumnTypeName.DECIMAL,
        full_data_type="DECIMAL(10,2)",
        routine_definition="x * rate",
        is_deterministic=True,
        sql_data_access=FunctionInfoSqlDataAccess.CONTAINS_SQL,
        comment="Apply rate",
    )
    base.update(kw)
    return FunctionInfo(**base)


# ---------- DDL reconstruction ----------


class TestSqlScalar:
    def test_full_ddl(self):
        fn = _fn(
            input_params=_params(
                _param(1, "rate", "double", default="0.5", comment="the rate"),
                _param(0, "x", "decimal(10,2)"),
            )
        )
        assert build_create_function_ddl(fn, TARGET) == (
            "CREATE FUNCTION IF NOT EXISTS `tgt`.`sch`.`fn`"
            "(`x` decimal(10,2), `rate` double DEFAULT 0.5 COMMENT 'the rate')\n"
            "RETURNS DECIMAL(10,2)\n"
            "LANGUAGE SQL\n"
            "DETERMINISTIC\n"
            "COMMENT 'Apply rate'\n"
            "CONTAINS SQL\n"
            "RETURN x * rate"
        )

    def test_create_or_replace(self):
        ddl = build_create_function_ddl(_fn(), TARGET, create_or_replace=True)
        assert ddl.startswith("CREATE OR REPLACE FUNCTION `tgt`.`sch`.`fn`()")
        assert "IF NOT EXISTS" not in ddl

    def test_not_deterministic_and_reads_sql_data(self):
        ddl = build_create_function_ddl(
            _fn(
                is_deterministic=False,
                sql_data_access=FunctionInfoSqlDataAccess.READS_SQL_DATA,
            ),
            TARGET,
        )
        assert "\nNOT DETERMINISTIC\n" in ddl
        assert "\nREADS SQL DATA\n" in ddl

    def test_unknown_determinism_and_no_sql_access_omitted(self):
        ddl = build_create_function_ddl(
            _fn(
                is_deterministic=None,
                sql_data_access=FunctionInfoSqlDataAccess.NO_SQL,
                comment=None,
            ),
            TARGET,
        )
        assert "DETERMINISTIC" not in ddl
        assert "NO SQL" not in ddl and "NO_SQL" not in ddl
        assert "COMMENT" not in ddl

    def test_comment_quotes_escaped(self):
        ddl = build_create_function_ddl(_fn(comment="it's a \\ test"), TARGET)
        assert "COMMENT 'it\\'s a \\\\ test'" in ddl

    def test_falls_back_to_data_type(self):
        ddl = build_create_function_ddl(
            _fn(full_data_type=None, data_type=ColumnTypeName.STRING), TARGET
        )
        assert "\nRETURNS STRING\n" in ddl

    def test_leading_return_not_duplicated(self):
        ddl = build_create_function_ddl(_fn(routine_definition="RETURN 1"), TARGET)
        assert ddl.endswith("\nRETURN 1")
        ddl = build_create_function_ddl(
            _fn(routine_definition="returned_col + 1"), TARGET
        )
        assert ddl.endswith("\nRETURN returned_col + 1")

    def test_complex_parameter_types_and_quoted_names(self):
        fn = _fn(input_params=_params(_param(0, "we`ird", "array<struct<a:int>>")))
        ddl = build_create_function_ddl(fn, TARGET)
        assert "(`we``ird` array<struct<a:int>>)" in ddl

    def test_missing_body_raises(self):
        with pytest.raises(UnsupportedFunctionError):
            build_create_function_ddl(_fn(routine_definition="  "), TARGET)

    def test_body_catalog_references_rewritten(self):
        fn = _fn(
            routine_definition=(
                "(SELECT max(rate) FROM src.sch.rates WHERE note <> 'src.sch.x')"
            )
        )
        ddl = build_create_function_ddl(fn, TARGET, catalog_map={"src": "tgt"})
        assert ddl.endswith(
            "RETURN (SELECT max(rate) FROM tgt.sch.rates WHERE note <> 'src.sch.x')"
        )

    def test_body_unchanged_without_catalog_map(self):
        fn = _fn(routine_definition="(SELECT 1 FROM src.sch.t)")
        assert build_create_function_ddl(fn, TARGET).endswith(
            "RETURN (SELECT 1 FROM src.sch.t)"
        )


class TestSqlTableValued:
    def fn(self, **kw):
        base = dict(
            routine_body=FunctionInfoRoutineBody.SQL,
            data_type=ColumnTypeName.TABLE_TYPE,
            full_data_type="(id BIGINT, name STRING)",
            routine_definition="SELECT id, name FROM src.sch.t WHERE id > min_id",
            is_deterministic=False,
            sql_data_access=FunctionInfoSqlDataAccess.READS_SQL_DATA,
            comment=None,
            input_params=_params(_param(0, "min_id", "bigint", default="0")),
            return_params=_params(
                _param(1, "name", "string", comment="display name"),
                _param(0, "id", "bigint"),
            ),
        )
        base.update(kw)
        return _fn(**base)

    def test_returns_table(self):
        assert build_create_function_ddl(
            self.fn(), TARGET, catalog_map={"src": "tgt"}
        ) == (
            "CREATE FUNCTION IF NOT EXISTS `tgt`.`sch`.`fn`(`min_id` bigint DEFAULT 0)\n"
            "RETURNS TABLE (`id` bigint, `name` string COMMENT 'display name')\n"
            "LANGUAGE SQL\n"
            "NOT DETERMINISTIC\n"
            "READS SQL DATA\n"
            "RETURN SELECT id, name FROM tgt.sch.t WHERE id > min_id"
        )

    def test_table_without_return_params_raises(self):
        with pytest.raises(UnsupportedFunctionError, match="return_params"):
            build_create_function_ddl(self.fn(return_params=None), TARGET)


class TestPython:
    def fn(self, **kw):
        base = dict(
            routine_body=FunctionInfoRoutineBody.EXTERNAL,
            external_language="Python",
            data_type=ColumnTypeName.STRING,
            full_data_type="STRING",
            routine_definition="\nreturn s.upper()\n",
            sql_data_access=FunctionInfoSqlDataAccess.NO_SQL,
            comment="upper",
            input_params=_params(_param(0, "s", "string")),
        )
        base.update(kw)
        return _fn(**base)

    def test_python_udf(self):
        ddl = build_create_function_ddl(self.fn(), TARGET, create_or_replace=True)
        assert ddl == (
            "CREATE OR REPLACE FUNCTION `tgt`.`sch`.`fn`(`s` string)\n"
            "RETURNS STRING\n"
            "LANGUAGE PYTHON\n"
            "DETERMINISTIC\n"
            "COMMENT 'upper'\n"
            "AS $$\nreturn s.upper()\n$$"
        )

    def test_python_body_indentation_preserved(self):
        body = "def f(x):\n    return x\nreturn f(s)"
        ddl = build_create_function_ddl(self.fn(routine_definition=body), TARGET)
        assert ddl.endswith(f"AS $$\n{body}\n$$")

    def test_python_body_not_catalog_rewritten(self):
        body = "return 'src.sch.t'"
        ddl = build_create_function_ddl(
            self.fn(routine_definition=body), TARGET, catalog_map={"src": "tgt"}
        )
        assert ddl.endswith(f"AS $$\n{body}\n$$")

    def test_python_body_with_dollar_quotes_raises(self):
        with pytest.raises(UnsupportedFunctionError, match=r"\$\$"):
            build_create_function_ddl(self.fn(routine_definition="return '$$'"), TARGET)

    def test_unsupported_language_raises(self):
        with pytest.raises(UnsupportedFunctionError, match="Unsupported"):
            build_create_function_ddl(self.fn(external_language="SCALA"), TARGET)


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
    assert (
        ReplicationConfig(create_or_replace_function=False).create_or_replace_function
        is False
    )


# ---------- DatabricksOperations (Databricks SDK) ----------


def test_get_functions_in_schema_uses_sdk_list():
    w = MagicMock()
    w.functions.list.return_value = [
        FunctionInfo(name="b"),
        FunctionInfo(name="a"),
        FunctionInfo(name=None),
    ]
    ops = DatabricksOperations(spark=MagicMock(), workspace_client=w)
    assert ops.get_functions_in_schema("src", "sch") == ["a", "b"]
    w.functions.list.assert_called_once_with("src", "sch")


def test_get_function_info_uses_sdk_get():
    w = MagicMock()
    info = FunctionInfo(name="fn")
    w.functions.get.return_value = info
    ops = DatabricksOperations(spark=MagicMock(), workspace_client=w)
    assert ops.get_function_info("src", "sch", "fn") is info
    w.functions.get.assert_called_once_with("src.sch.fn")


def test_get_function_info_not_found():
    w = MagicMock()
    w.functions.get.side_effect = NotFound("nope")
    ops = DatabricksOperations(spark=MagicMock(), workspace_client=w)
    assert ops.get_function_info("c", "s", "f") is None


def test_function_reads_require_workspace_client():
    ops = DatabricksOperations(spark=MagicMock())
    with pytest.raises(ValueError):
        ops.get_functions_in_schema("c", "s")
    with pytest.raises(ValueError):
        ops.get_function_info("c", "s", "f")


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


SCALAR_FN = _fn(
    full_data_type="INT",
    routine_definition="x + (SELECT count(*) FROM src.sch.t)",
    input_params=_params(_param(0, "x", "int")),
)


def test_replicate_functions_success(provider, schema_config):
    provider.source_dbops.get_functions_in_schema.return_value = ["fn"]
    provider.source_dbops.get_function_info.return_value = SCALAR_FN
    results = provider._uc_replicate_functions(schema_config)
    assert [r.status for r in results] == ["success"]
    assert results[0].object_type == "function"
    provider.source_dbops.get_function_info.assert_called_once_with("src", "sch", "fn")
    ddl = provider.target_spark.sql.call_args.args[0]
    assert ddl.startswith("CREATE OR REPLACE FUNCTION `tgt`.`sch`.`fn`(`x` int)")
    # source catalog "src" is renamed to target catalog "tgt" in the body
    assert ddl.endswith("RETURN x + (SELECT count(*) FROM tgt.sch.t)")


def test_replicate_functions_extra_catalog_reference_mapping(provider, schema_config):
    schema_config.replication_config = ReplicationConfig(
        source_catalog="src", catalog_reference_mapping={"other": "other_tgt"}
    )
    provider.source_dbops.get_functions_in_schema.return_value = ["fn"]
    provider.source_dbops.get_function_info.return_value = _fn(
        routine_definition="(SELECT 1 FROM other.s.t JOIN src.s.u)"
    )
    provider._uc_replicate_functions(schema_config)
    assert provider.target_spark.sql.call_args.args[0].endswith(
        "RETURN (SELECT 1 FROM other_tgt.s.t JOIN tgt.s.u)"
    )


def test_replicate_functions_if_not_exists(provider, schema_config):
    schema_config.replication_config = ReplicationConfig(
        source_catalog="src", create_or_replace_function=False
    )
    provider.source_dbops.get_functions_in_schema.return_value = ["fn"]
    provider.source_dbops.get_function_info.return_value = SCALAR_FN
    provider._uc_replicate_functions(schema_config)
    assert (
        "CREATE FUNCTION IF NOT EXISTS" in provider.target_spark.sql.call_args.args[0]
    )


def test_replicate_functions_failure_skip_and_not_found(provider, schema_config):
    provider.source_dbops.get_functions_in_schema.return_value = ["bad", "odd", "gone"]
    odd = _fn(
        routine_body=FunctionInfoRoutineBody.EXTERNAL,
        external_language="SCALA",
        routine_definition="x",
    )
    provider.source_dbops.get_function_info.side_effect = [SCALAR_FN, odd, None]
    provider.target_spark.sql.side_effect = Exception("boom")
    results = provider._uc_replicate_functions(schema_config)
    assert [r.status for r in results] == ["failed", "skipped", "failed"]
    assert "boom" in results[0].error_message
    assert "not found" in results[2].error_message


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
        "catalog",
        "catalog_tag",
        "catalog_grant",
        "schema",
        "schema_tag",
        "schema_grant",
        "function",
        "function_grant",
        "tables",
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
