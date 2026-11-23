"""Tests for ABAC policy replication (issue #39, part 3)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    ColumnMaskOptions,
    FunctionArgument,
    MatchColumn,
    PolicyInfo,
    PolicyType,
    RowFilterOptions,
    SecurableType,
)

from data_replication.abac_policy_ddl import (
    UnsupportedPolicyError,
    build_create_policy_ddl,
    map_function_name,
    policy_fingerprint,
)
from data_replication.cli import args as cli_args
from data_replication.config.models import (
    ConcurrencyConfig,
    ReplicationConfig,
    RetryConfig,
    SchemaConfig,
    TableConfig,
    TargetCatalogConfig,
    UCObjectType,
)
from data_replication.databricks_operations import DatabricksOperations
from data_replication.providers.replication_provider import ReplicationProvider

TARGET_ON = "`tgt`.`sch`"
CATALOG_MAP = {"src": "tgt"}


def _policy(**kw) -> PolicyInfo:
    """A column-mask policy on a schema (defaults mirror a live probe)."""
    base = dict(
        name="p",
        policy_type=PolicyType.POLICY_TYPE_COLUMN_MASK,
        on_securable_type=SecurableType.SCHEMA,
        on_securable_fullname="src.sch",
        for_securable_type=SecurableType.TABLE,
        to_principals=["account users"],
        except_principals=None,
        match_columns=[
            MatchColumn(alias="pii", condition="hasTag('class.email_address')")
        ],
        column_mask=ColumnMaskOptions(function_name="src.sch.mask_fn", on_column="pii"),
        comment=None,
    )
    base.update(kw)
    return PolicyInfo(**base)


def _row_filter(**kw) -> PolicyInfo:
    base = dict(
        name="rf",
        policy_type=PolicyType.POLICY_TYPE_ROW_FILTER,
        column_mask=None,
        row_filter=RowFilterOptions(
            function_name="src.sch.rf_fn", using=[FunctionArgument(alias="loc")]
        ),
        match_columns=[MatchColumn(alias="loc", condition="hasTag('class.location')")],
    )
    base.update(kw)
    return _policy(**base)


# ---------- DDL generation ----------


def test_column_mask_with_except_match_and_comment():
    ddl = build_create_policy_ddl(
        _policy(comment="it's masked", except_principals=["grp"]),
        target_on_securable=TARGET_ON,
        to_principals=["account users"],
        except_principals=["grp"],
        catalog_map=CATALOG_MAP,
    )
    assert ddl == "\n".join(
        [
            "CREATE OR REPLACE POLICY `p` ON SCHEMA `tgt`.`sch`",
            "COMMENT 'it\\'s masked'",
            "COLUMN MASK `tgt`.`sch`.`mask_fn`",
            "TO `account users`",
            "EXCEPT `grp`",
            "FOR TABLES",
            "MATCH COLUMNS hasTag('class.email_address') AS `pii`",
            "ON COLUMN `pii`",
        ]
    )


def test_row_filter_with_using_and_no_except():
    ddl = build_create_policy_ddl(
        _row_filter(),
        target_on_securable=TARGET_ON,
        to_principals=["account users"],
        catalog_map=CATALOG_MAP,
    )
    assert ddl == "\n".join(
        [
            "CREATE OR REPLACE POLICY `rf` ON SCHEMA `tgt`.`sch`",
            "ROW FILTER `tgt`.`sch`.`rf_fn`",
            "TO `account users`",
            "FOR TABLES",
            "MATCH COLUMNS hasTag('class.location') AS `loc`",
            "USING COLUMNS (`loc`)",
        ]
    )


def test_table_scope_when_condition_and_constant_argument():
    policy = _row_filter(
        on_securable_type=SecurableType.TABLE,
        when_condition="region <> 'x'",
        row_filter=RowFilterOptions(
            function_name="src.sch.rf_fn",
            using=[FunctionArgument(alias="loc"), FunctionArgument(constant="'US'")],
        ),
    )
    ddl = build_create_policy_ddl(
        policy,
        target_on_securable="`tgt`.`sch`.`t`",
        to_principals=["account users"],
        catalog_map=CATALOG_MAP,
    )
    assert ddl.splitlines()[0] == (
        "CREATE OR REPLACE POLICY `rf` ON TABLE `tgt`.`sch`.`t`"
    )
    assert "FOR TABLES\nWHEN region <> 'x'\nMATCH COLUMNS" in ddl
    assert ddl.endswith("USING COLUMNS (`loc`, 'US')")


def test_multiple_match_columns_and_unaliased_condition():
    policy = _policy(
        match_columns=[
            MatchColumn(alias="a", condition="hasTag('k1')"),
            MatchColumn(alias=None, condition="has_tag_value('k2', 'v')"),
        ],
    )
    ddl = build_create_policy_ddl(
        policy, TARGET_ON, ["account users"], catalog_map=CATALOG_MAP
    )
    assert "MATCH COLUMNS hasTag('k1') AS `a`, has_tag_value('k2', 'v')" in ddl


def test_quoting_of_policy_name_principals_and_mask_alias():
    policy = _policy(
        name="my`policy",
        column_mask=ColumnMaskOptions(function_name="src.sch.mask_fn", on_column="a`b"),
        match_columns=[MatchColumn(alias="a`b", condition="hasTag('k')")],
    )
    ddl = build_create_policy_ddl(
        policy,
        TARGET_ON,
        to_principals=["name with space", "o'hara@x.com"],
        except_principals=["back`tick"],
        catalog_map=CATALOG_MAP,
    )
    assert "CREATE OR REPLACE POLICY `my``policy` ON SCHEMA" in ddl
    assert "TO `name with space`, `o'hara@x.com`" in ddl
    assert "EXCEPT `back``tick`" in ddl
    assert "AS `a``b`" in ddl
    assert "ON COLUMN `a``b`" in ddl


def test_catalog_references_rewritten_only_outside_string_literals():
    policy = _row_filter(
        when_condition="src.sch.flags.enabled AND tag = 'src.sch.lit'",
    )
    ddl = build_create_policy_ddl(
        policy, TARGET_ON, ["account users"], catalog_map=CATALOG_MAP
    )
    assert "WHEN tgt.sch.flags.enabled AND tag = 'src.sch.lit'" in ddl


def test_function_in_other_catalog_is_not_renamed():
    policy = _policy(
        column_mask=ColumnMaskOptions(function_name="gov.sec.mask_fn", on_column="pii")
    )
    ddl = build_create_policy_ddl(
        policy, TARGET_ON, ["account users"], catalog_map=CATALOG_MAP
    )
    assert "COLUMN MASK `gov`.`sec`.`mask_fn`" in ddl


def test_map_function_name():
    assert map_function_name("src.s.f", {"src": "tgt"}) == "tgt.s.f"
    assert map_function_name("src.s.f", None) == "src.s.f"
    with pytest.raises(UnsupportedPolicyError):
        map_function_name("f", {})


@pytest.mark.parametrize(
    "policy, kwargs",
    [
        (_policy(), dict(to_principals=[])),
        (_policy(match_columns=None), {}),
        (_policy(for_securable_type=SecurableType.SCHEMA), {}),
        (_policy(on_securable_type=None), {}),
        (_policy(name=None), {}),
        (_policy(policy_type=None), {}),
        (_policy(column_mask=None), {}),
        (
            _policy(
                column_mask=ColumnMaskOptions(function_name="src.sch.m", on_column=None)
            ),
            {},
        ),
        (
            _policy(column_mask=ColumnMaskOptions(function_name="fn", on_column="pii")),
            {},
        ),
        (
            _policy(
                column_mask=ColumnMaskOptions(
                    function_name="src.sch.m",
                    on_column="pii",
                    using=[FunctionArgument()],
                )
            ),
            {},
        ),
        (_policy(match_columns=[MatchColumn(alias="x", condition=None)]), {}),
    ],
)
def test_unsupported_policies_raise(policy, kwargs):
    args = dict(to_principals=["account users"], catalog_map=CATALOG_MAP)
    args.update(kwargs)
    with pytest.raises(UnsupportedPolicyError):
        build_create_policy_ddl(policy, TARGET_ON, **args)


# ---------- fingerprints ----------


def test_fingerprint_ignores_ids_owners_and_principal_order():
    a = _policy(id="1", created_by="x", to_principals=["b", "a"])
    b = _policy(id="2", created_by="y", to_principals=["a", "b"])
    assert policy_fingerprint(a, ["b", "a"], [], "tgt.sch.mask_fn") == (
        policy_fingerprint(b, ["a", "b"], [], "tgt.sch.mask_fn")
    )


@pytest.mark.parametrize(
    "changed",
    [
        _policy(comment="changed"),
        _policy(when_condition="x = 1"),
        _policy(match_columns=[MatchColumn(alias="pii", condition="hasTag('other')")]),
        _policy(
            column_mask=ColumnMaskOptions(
                function_name="src.sch.mask_fn",
                on_column="pii",
                using=[FunctionArgument(alias="pii")],
            )
        ),
    ],
)
def test_fingerprint_detects_definition_changes(changed):
    base = _policy()
    assert policy_fingerprint(base, ["account users"], [], "f") != policy_fingerprint(
        changed, ["account users"], [], "f"
    )


def test_fingerprint_detects_function_and_except_changes():
    base = _policy()
    fp = policy_fingerprint(base, ["account users"], [], "tgt.sch.mask_fn")
    assert fp != policy_fingerprint(base, ["account users"], [], "tgt.sch.other")
    assert fp != policy_fingerprint(base, ["account users"], ["grp"], "tgt.sch.mask_fn")


# ---------- DatabricksOperations helpers ----------


def test_list_abac_policy_definitions_queries_catalog_information_schema():
    spark = MagicMock()
    row = MagicMock()
    row.asDict.return_value = {"policy_name": "p", "policy_type": "COLUMN_MASK"}
    spark.sql.return_value.collect.return_value = [row]
    ops = DatabricksOperations(spark=spark)
    assert ops.list_abac_policy_definitions("my`cat") == [
        {"policy_name": "p", "policy_type": "COLUMN_MASK"}
    ]
    sql = spark.sql.call_args.args[0]
    assert "FROM `my``cat`.information_schema.abac_policy_definitions" in sql


def test_get_abac_policy_uses_sdk_and_returns_none_when_missing():
    wc = MagicMock()
    policy = _policy()
    wc.policies.get_policy.return_value = policy
    ops = DatabricksOperations(spark=MagicMock(), workspace_client=wc)
    assert ops.get_abac_policy("SCHEMA", "src.sch", "p") is policy
    wc.policies.get_policy.assert_called_once_with("SCHEMA", "src.sch", "p")

    wc.policies.get_policy.side_effect = NotFound("missing")
    assert ops.get_abac_policy("SCHEMA", "src.sch", "p") is None


def test_abac_reads_require_workspace_client():
    ops = DatabricksOperations(spark=MagicMock(), workspace_client=None)
    with pytest.raises(ValueError):
        ops.get_abac_policy("SCHEMA", "src.sch", "p")


# ---------- enum and CLI ----------


def test_abac_policy_enum_and_cli():
    assert UCObjectType("abac_policy") is UCObjectType.ABAC_POLICY
    assert cli_args.parse_comma_delimited_enums(
        "abac_policy", UCObjectType, "--uc-object-types"
    ) == [UCObjectType.ABAC_POLICY]


# ---------- ReplicationProvider ----------


@pytest.fixture
def provider():
    p = ReplicationProvider.__new__(ReplicationProvider)
    p.logger = MagicMock()
    p.run_id = "run"
    p.catalog_config = MagicMock()
    p.catalog_config.catalog_name = "tgt"
    p.catalog_config.replication_config = ReplicationConfig(
        enabled=True, source_catalog="src"
    )
    p.catalog_config.retry = RetryConfig(max_attempts=1, retry_delay_seconds=1)
    p.source_dbops = MagicMock()
    p.target_dbops = MagicMock()
    p.target_spark = MagicMock()
    p.target_dbops.get_abac_policy.return_value = None
    p.catalog_config.schema_table_filter_expression = None
    p.source_dbops.get_tables_in_schema.return_value = ["t"]
    mapping = {"alice@src": "alice@tgt"}
    p._map_principal = lambda s: mapping.get(s, s)
    return p


SCHEMAS = [SchemaConfig(schema_name="sch", tables=[])]
SCHEMA_ROW = dict(
    policy_name="p",
    policy_type="COLUMN_MASK",
    on_securable_type="SCHEMA",
    schema_name="sch",
    securable_name=None,
)


def _rows(provider, *rows):
    provider.source_dbops.list_abac_policy_definitions.return_value = list(rows)


def test_schema_policy_created_when_missing(provider):
    _rows(provider, SCHEMA_ROW)
    provider.source_dbops.get_abac_policy.return_value = _policy()
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["success"]
    assert results[0].object_type == "abac_policy"
    assert results[0].object_name == "p"
    provider.source_dbops.get_abac_policy.assert_called_once_with(
        "SCHEMA", "src.sch", "p"
    )
    provider.target_dbops.get_abac_policy.assert_called_once_with(
        "SCHEMA", "tgt.sch", "p"
    )
    ddl = provider.target_spark.sql.call_args.args[0]
    assert ddl.startswith("CREATE OR REPLACE POLICY `p` ON SCHEMA `tgt`.`sch`")
    assert "COLUMN MASK `tgt`.`sch`.`mask_fn`" in ddl


def test_principals_are_mapped_in_ddl(provider):
    _rows(provider, SCHEMA_ROW)
    provider.source_dbops.get_abac_policy.return_value = _policy(
        to_principals=["alice@src"], except_principals=["bob"]
    )
    provider._uc_replicate_abac_policies(SCHEMAS)
    ddl = provider.target_spark.sql.call_args.args[0]
    assert "TO `alice@tgt`" in ddl
    assert "EXCEPT `bob`" in ddl


def test_unchanged_target_policy_is_skipped(provider):
    _rows(provider, SCHEMA_ROW)
    provider.source_dbops.get_abac_policy.return_value = _policy()
    # The target stores its own (renamed) function name.
    provider.target_dbops.get_abac_policy.return_value = _policy(
        id="target-id",
        column_mask=ColumnMaskOptions(function_name="tgt.sch.mask_fn", on_column="pii"),
    )
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["skipped"]
    provider.target_spark.sql.assert_not_called()


def test_changed_target_policy_is_replaced(provider):
    _rows(provider, SCHEMA_ROW)
    provider.source_dbops.get_abac_policy.return_value = _policy(comment="new")
    provider.target_dbops.get_abac_policy.return_value = _policy(comment="old")
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["success"]
    ddl = provider.target_spark.sql.call_args.args[0]
    assert ddl.startswith("CREATE OR REPLACE POLICY")
    assert "COMMENT 'new'" in ddl


def test_target_failure_is_per_object_and_run_continues(provider):
    _rows(
        provider,
        dict(SCHEMA_ROW, policy_name="first"),
        dict(SCHEMA_ROW, policy_name="second"),
    )
    provider.source_dbops.get_abac_policy.side_effect = [
        _policy(name="first"),
        _policy(name="second"),
    ]
    provider.target_spark.sql.side_effect = [
        Exception("Routine or Model 'tgt.sch.mask_fn' does not exist."),
        None,
    ]
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [(r.object_name, r.status) for r in results] == [
        ("first", "failed"),
        ("second", "success"),
    ]
    assert "does not exist" in results[0].error_message


def test_source_policy_missing_fails(provider):
    _rows(provider, SCHEMA_ROW)
    provider.source_dbops.get_abac_policy.return_value = None
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert results[0].status == "failed"
    assert "not found" in results[0].error_message
    provider.target_spark.sql.assert_not_called()


def test_unsupported_policy_is_skipped(provider):
    _rows(provider, SCHEMA_ROW)
    provider.source_dbops.get_abac_policy.return_value = _policy(match_columns=None)
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["skipped"]
    provider.target_spark.sql.assert_not_called()


def test_grant_and_deny_policy_types_are_skipped_without_lookup(provider):
    _rows(provider, dict(SCHEMA_ROW, policy_type="GRANT"))
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["skipped"]
    provider.source_dbops.get_abac_policy.assert_not_called()


def test_out_of_scope_schema_is_ignored(provider):
    _rows(provider, dict(SCHEMA_ROW, schema_name="other"))
    assert provider._uc_replicate_abac_policies(SCHEMAS) == []
    provider.source_dbops.get_abac_policy.assert_not_called()


def test_table_policy_only_for_tables_in_scope(provider):
    in_scope = dict(
        SCHEMA_ROW,
        policy_name="tp",
        on_securable_type="TABLE",
        securable_name="t",
    )
    out_of_scope = dict(in_scope, securable_name="not_replicated")
    _rows(provider, in_scope, out_of_scope)
    provider.source_dbops.get_abac_policy.return_value = _policy(
        name="tp",
        on_securable_type=SecurableType.TABLE,
        on_securable_fullname="src.sch.t",
    )
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["success"]
    provider.source_dbops.get_abac_policy.assert_called_once_with(
        "TABLE", "src.sch.t", "tp"
    )
    assert "ON TABLE `tgt`.`sch`.`t`" in provider.target_spark.sql.call_args.args[0]


def test_table_scope_uses_explicit_tables_minus_exclusions(provider):
    # Explicit configured tables are used as-is (no source listing), and
    # exclusions remove a table from scope.
    schemas = [
        SchemaConfig(
            schema_name="sch",
            tables=[TableConfig(table_name="t"), TableConfig(table_name="t2")],
            exclude_tables=[TableConfig(table_name="t2")],
        )
    ]
    _rows(
        provider,
        dict(
            SCHEMA_ROW,
            policy_name="on_t",
            on_securable_type="TABLE",
            securable_name="t",
        ),
        dict(
            SCHEMA_ROW,
            policy_name="on_t2",
            on_securable_type="TABLE",
            securable_name="t2",
        ),
    )
    provider.source_dbops.get_abac_policy.return_value = _policy(
        on_securable_type=SecurableType.TABLE, on_securable_fullname="src.sch.t"
    )
    results = provider._uc_replicate_abac_policies(schemas)
    assert [r.object_name for r in results] == ["on_t"]
    provider.source_dbops.get_tables_in_schema.assert_not_called()


def test_table_listing_failure_fails_table_policies(provider):
    _rows(
        provider,
        dict(
            SCHEMA_ROW, policy_name="tp", on_securable_type="TABLE", securable_name="t"
        ),
    )
    provider.source_dbops.get_tables_in_schema.side_effect = Exception("no access")
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert results[0].status == "failed"
    assert "Could not list tables" in results[0].error_message


def test_table_scope_ignores_processed_state(provider):
    # A table that failed earlier in the run is still in scope for its policy.
    provider._get_tables = MagicMock(side_effect=AssertionError("must not be used"))
    _rows(
        provider,
        dict(
            SCHEMA_ROW, policy_name="tp", on_securable_type="TABLE", securable_name="t"
        ),
    )
    provider.source_dbops.get_abac_policy.return_value = _policy(
        name="tp",
        on_securable_type=SecurableType.TABLE,
        on_securable_fullname="src.sch.t",
    )
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["success"]
    provider._get_tables.assert_not_called()


def test_catalog_scoped_policy(provider):
    _rows(
        provider,
        dict(
            SCHEMA_ROW,
            policy_name="cp",
            on_securable_type="CATALOG",
            schema_name=None,
        ),
    )
    provider.source_dbops.get_abac_policy.return_value = _policy(
        name="cp", on_securable_type=SecurableType.CATALOG, on_securable_fullname="src"
    )
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert [r.status for r in results] == ["success"]
    provider.source_dbops.get_abac_policy.assert_called_once_with(
        "CATALOG", "src", "cp"
    )
    ddl = provider.target_spark.sql.call_args.args[0]
    assert ddl.startswith("CREATE OR REPLACE POLICY `cp` ON CATALOG `tgt`\n")


def test_discovery_failure_returns_single_failed_result(provider):
    provider.source_dbops.list_abac_policy_definitions.side_effect = Exception(
        "no information_schema access"
    )
    results = provider._uc_replicate_abac_policies(SCHEMAS)
    assert len(results) == 1 and results[0].status == "failed"
    assert "no information_schema access" in results[0].error_message


# ---------- orchestration order ----------


def _orchestration_provider(uc_types, calls):
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
    p._uc_replicate_abac_policies = rec("abac", [])
    p.process_schema = rec("tables", ([], [], []))
    return p


def test_abac_runs_last_after_tables():
    calls = []
    _orchestration_provider([UCObjectType.ALL], calls).process_catalog()
    assert calls[-1] == "abac"
    assert calls.index("tables") < calls.index("abac")
    assert calls.index("function_grant") < calls.index("tables")


def test_abac_only_runs_once_without_schema_processing():
    calls = []
    _orchestration_provider([UCObjectType.ABAC_POLICY], calls).process_catalog()
    assert calls == ["abac"]


def test_abac_not_run_without_its_type():
    calls = []
    _orchestration_provider([UCObjectType.TABLE], calls).process_catalog()
    assert "abac" not in calls
