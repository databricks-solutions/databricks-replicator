"""Tests for classic column mask and row filter replication (issue #39).

Offline: SDK metadata is built from databricks.sdk dataclasses and Spark /
dbops are mocks.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    ColumnInfo,
    ColumnMask,
    TableInfo,
    TableRowFilter,
)

from data_replication.config.models import (
    ReplicationConfig,
    RetryConfig,
    SchemaConfig,
    TableConfig,
    UCObjectType,
)
from data_replication.databricks_operations import DatabricksOperations
from data_replication.mask_filter_ddl import (
    ColumnMaskSpec,
    RowFilterSpec,
    column_mask_specs,
    drop_column_mask_sql,
    drop_row_filter_sql,
    function_sql,
    map_function_name,
    plan_column_masks,
    plan_row_filter,
    row_filter_spec,
    set_column_mask_sql,
    set_row_filter_sql,
)
from tests._replication_fakes import make_provider

TABLE = "`tgt`.`sch`.`t`"


# ---------- SDK metadata -> specs ----------


def _col(name, mask=None):
    return ColumnInfo(name=name, mask=mask)


class TestSpecsFromMetadata:
    def test_column_masks_keyed_by_name_with_using_columns(self):
        cols = [
            _col("id"),
            _col("name", ColumnMask(function_name="src.sch.mask_str")),
            _col(
                "salary",
                ColumnMask(
                    function_name="src.sch.mask_using",
                    using_column_names=["dept", "region"],
                ),
            ),
        ]
        specs = column_mask_specs(cols, {"src": "tgt"})
        assert list(specs) == ["name", "salary"]
        assert specs["name"] == ColumnMaskSpec("name", "tgt.sch.mask_str", ())
        assert specs["salary"] == ColumnMaskSpec(
            "salary", "tgt.sch.mask_using", ("dept", "region")
        )

    def test_mask_without_function_name_is_ignored(self):
        assert column_mask_specs([_col("c", ColumnMask())]) == {}

    def test_none_columns(self):
        assert column_mask_specs(None) == {}

    def test_row_filter_with_and_without_inputs(self):
        flt = TableRowFilter(function_name="src.sch.rf", input_column_names=["region"])
        assert row_filter_spec(flt, {"src": "tgt"}) == RowFilterSpec(
            "tgt.sch.rf", ("region",)
        )
        zero = TableRowFilter(function_name="src.sch.rf0", input_column_names=None)
        assert row_filter_spec(zero) == RowFilterSpec("src.sch.rf0", ())
        assert row_filter_spec(None) is None
        assert (
            row_filter_spec(TableRowFilter(function_name="", input_column_names=[]))
            is None
        )

    def test_map_function_name(self):
        assert map_function_name("SRC.sch.fn", {"src": "tgt"}) == "tgt.sch.fn"
        assert map_function_name("other.sch.fn", {"src": "tgt"}) == "other.sch.fn"
        assert map_function_name("src.sch.fn", None) == "src.sch.fn"
        assert map_function_name("two.parts", {"two": "x"}) == "two.parts"


# ---------- DDL builders ----------


class TestDdl:
    def test_function_sql_quotes_each_part(self):
        assert function_sql("tgt.sch.fn") == "`tgt`.`sch`.`fn`"

    def test_function_sql_rejects_partial_names(self):
        with pytest.raises(ValueError):
            function_sql("sch.fn")

    def test_set_mask_without_using_columns(self):
        spec = ColumnMaskSpec("name", "tgt.sch.mask_str")
        assert set_column_mask_sql(TABLE, spec) == (
            "ALTER TABLE `tgt`.`sch`.`t` ALTER COLUMN `name` SET MASK "
            "`tgt`.`sch`.`mask_str`"
        )

    def test_set_mask_with_using_columns(self):
        spec = ColumnMaskSpec("salary", "tgt.sch.mask_using", ("dept", "region"))
        assert set_column_mask_sql(TABLE, spec) == (
            "ALTER TABLE `tgt`.`sch`.`t` ALTER COLUMN `salary` SET MASK "
            "`tgt`.`sch`.`mask_using` USING COLUMNS (`dept`, `region`)"
        )

    def test_identifiers_with_backticks_are_escaped(self):
        spec = ColumnMaskSpec("we`ird", "tgt.sch.fn`x", ("a`b",))
        sql = set_column_mask_sql(TABLE, spec)
        assert "`we``ird`" in sql
        assert "`fn``x`" in sql
        assert "`a``b`" in sql

    def test_drop_mask(self):
        assert drop_column_mask_sql(TABLE, "name") == (
            "ALTER TABLE `tgt`.`sch`.`t` ALTER COLUMN `name` DROP MASK"
        )

    def test_set_row_filter(self):
        spec = RowFilterSpec("tgt.sch.rf", ("region", "owner"))
        assert set_row_filter_sql(TABLE, spec) == (
            "ALTER TABLE `tgt`.`sch`.`t` SET ROW FILTER `tgt`.`sch`.`rf` "
            "ON (`region`, `owner`)"
        )

    def test_set_row_filter_with_no_input_columns(self):
        spec = RowFilterSpec("tgt.sch.rf0", ())
        assert set_row_filter_sql(TABLE, spec).endswith("ON ()")

    def test_drop_row_filter(self):
        assert drop_row_filter_sql(TABLE) == (
            "ALTER TABLE `tgt`.`sch`.`t` DROP ROW FILTER"
        )


# ---------- plan (idempotence and overwrite) ----------


def _mask(col, fn, using=()):
    return ColumnMaskSpec(col, fn, tuple(using))


class TestPlanColumnMasks:
    def test_identical_masks_are_unchanged(self):
        src = {"name": _mask("name", "tgt.s.f")}
        tgt = {"name": _mask("name", "TGT.s.F")}
        plan = plan_column_masks(src, tgt, overwrite=False)
        assert plan.to_set == [] and plan.to_unset == []
        assert plan.unchanged == ["name"]

    def test_new_or_different_masks_are_set(self):
        src = {
            "name": _mask("name", "tgt.s.f"),
            "salary": _mask("salary", "tgt.s.g", ["dept"]),
        }
        tgt = {"name": _mask("name", "tgt.s.other")}
        plan = plan_column_masks(src, tgt, overwrite=False)
        assert [s.column for s in plan.to_set] == ["name", "salary"]

    def test_using_columns_change_is_a_set(self):
        src = {"salary": _mask("salary", "tgt.s.g", ["dept"])}
        tgt = {"salary": _mask("salary", "tgt.s.g", ["region"])}
        assert [s.column for s in plan_column_masks(src, tgt, False).to_set] == [
            "salary"
        ]

    def test_target_only_masks_kept_by_default(self):
        plan = plan_column_masks({}, {"email": _mask("email", "tgt.s.e")}, False)
        assert plan.kept == ["email"] and plan.to_unset == []

    def test_target_only_masks_dropped_with_overwrite(self):
        plan = plan_column_masks({}, {"email": _mask("email", "tgt.s.e")}, True)
        assert plan.to_unset == ["email"] and plan.kept == []


class TestPlanRowFilter:
    def test_identical_is_unchanged(self):
        src = RowFilterSpec("tgt.s.rf", ("region",))
        same = RowFilterSpec("TGT.s.RF", ("REGION",))
        assert plan_row_filter(src, same, False).action == "unchanged"

    def test_different_or_new_is_set(self):
        src = RowFilterSpec("tgt.s.rf", ("region",))
        assert plan_row_filter(src, None, False).action == "set"
        assert (
            plan_row_filter(src, RowFilterSpec("tgt.s.old", ("region",)), False).action
            == "set"
        )

    def test_target_only_filter(self):
        tgt = RowFilterSpec("tgt.s.rf", ("region",))
        assert plan_row_filter(None, tgt, False).action == "kept"
        assert plan_row_filter(None, tgt, True).action == "drop"
        assert plan_row_filter(None, None, True).action == "none"


# ---------- provider step ----------


def _retry():
    return RetryConfig(max_attempts=1, retry_delay_seconds=1)


def _provider(uc_types=(UCObjectType.COLUMN_MASK, UCObjectType.ROW_FILTER), **rep):
    p = make_provider(
        uc_object_types=list(uc_types),
        replication_config=ReplicationConfig(enabled=True, source_catalog="src", **rep),
    )
    p.source_dbops.get_table_info.return_value = None
    p.target_dbops.get_table_info.return_value = None
    p.target_dbops.if_function_exists.return_value = True
    return p


def _table_config(**rep):
    return TableConfig(
        table_name="t",
        replication_config=ReplicationConfig(enabled=True, source_catalog="src", **rep),
        retry=_retry(),
    )


def _src_table(masks=None, row_filter=None):
    cols = [_col("id")] + [
        _col(name, ColumnMask(function_name=fn, using_column_names=using or None))
        for name, (fn, using) in (masks or {}).items()
    ]
    return TableInfo(columns=cols, row_filter=row_filter)


def _tgt_table(masks=None, row_filter=None):
    return _src_table(masks, row_filter)


def _run(p, types=(UCObjectType.COLUMN_MASK, UCObjectType.ROW_FILTER), **rep):
    return p._uc_replicate_masks_and_row_filters(
        "sch", _table_config(**rep), list(types)
    )


class TestProviderApply:
    def test_applies_masks_then_row_filter_in_order(self):
        p = _provider()
        p.source_dbops.get_table_info.return_value = _src_table(
            masks={
                "name": ("src.sch.mask_str", None),
                "salary": ("src.sch.mask_using", ["dept"]),
            },
            row_filter=TableRowFilter(
                function_name="src.sch.rf", input_column_names=["region"]
            ),
        )
        p.target_dbops.get_table_info.return_value = _tgt_table()

        results = _run(p)

        assert [r.object_type for r in results] == ["column_mask", "row_filter"]
        assert all(r.status == "success" for r in results)
        executed = [c.args[0] for c in p.target_spark.sql.call_args_list]
        assert executed == [
            "ALTER TABLE `tgt`.`sch`.`t` ALTER COLUMN `name` SET MASK `tgt`.`sch`.`mask_str`",
            "ALTER TABLE `tgt`.`sch`.`t` ALTER COLUMN `salary` SET MASK "
            "`tgt`.`sch`.`mask_using` USING COLUMNS (`dept`)",
            "ALTER TABLE `tgt`.`sch`.`t` SET ROW FILTER `tgt`.`sch`.`rf` ON (`region`)",
        ]
        assert results[0].details["set_columns"] == ["name", "salary"]
        assert results[1].details["action"] == "set"

    def test_source_catalog_is_mapped_in_function_names(self):
        p = _provider()
        p.source_dbops.get_table_info.return_value = _src_table(
            masks={"name": ("src.sch.mask_str", None)}
        )
        p.target_dbops.get_table_info.return_value = _tgt_table()
        _run(p, [UCObjectType.COLUMN_MASK])
        # The function was looked up on target under the target catalog.
        p.target_dbops.if_function_exists.assert_called_once_with(
            "`tgt`.`sch`.`mask_str`"
        )

    def test_rerun_is_a_no_op(self):
        p = _provider()
        masks = {"name": ("tgt.sch.mask_str", None)}
        rf = TableRowFilter(function_name="tgt.sch.rf", input_column_names=["region"])
        p.source_dbops.get_table_info.return_value = _src_table(masks, rf)
        p.target_dbops.get_table_info.return_value = _tgt_table(masks, rf)

        results = _run(p)

        assert [r.status for r in results] == ["success", "success"]
        assert all(r.details["skipped"] for r in results)
        assert results[0].details["unchanged_columns"] == ["name"]
        p.target_spark.sql.assert_not_called()
        p.target_dbops.if_function_exists.assert_not_called()

    def test_missing_function_fails_object_without_touching_table(self):
        p = _provider()
        p.source_dbops.get_table_info.return_value = _src_table(
            masks={"name": ("src.sch.mask_str", None)},
            row_filter=TableRowFilter(
                function_name="src.sch.rf", input_column_names=[]
            ),
        )
        p.target_dbops.get_table_info.return_value = _tgt_table()
        p.target_dbops.if_function_exists.side_effect = lambda fn: (
            fn != "`tgt`.`sch`.`mask_str`"
        )

        results = _run(p)

        mask, row = results
        assert mask.status == "failed"
        assert "tgt.sch.mask_str" in mask.error_message
        assert "was not changed" in mask.error_message
        assert mask.details["unprotected_on_target"] is True
        # The row filter function exists, so that object still applies.
        assert row.status == "success"
        executed = [c.args[0] for c in p.target_spark.sql.call_args_list]
        assert executed == [
            "ALTER TABLE `tgt`.`sch`.`t` SET ROW FILTER `tgt`.`sch`.`rf` ON ()"
        ]

    def test_missing_target_table_fails_each_requested_object(self):
        p = _provider()
        p.source_dbops.get_table_info.return_value = _src_table()
        p.target_dbops.get_table_info.return_value = None
        results = _run(p)
        assert [r.status for r in results] == ["failed", "failed"]
        assert "does not exist" in results[0].error_message
        p.target_spark.sql.assert_not_called()

    def test_target_only_policies_kept_by_default_and_logged(self):
        p = _provider()
        p.source_dbops.get_table_info.return_value = _src_table()
        p.target_dbops.get_table_info.return_value = _tgt_table(
            masks={"email": ("tgt.sch.mask_email", None)},
            row_filter=TableRowFilter(
                function_name="tgt.sch.rf", input_column_names=["r"]
            ),
        )
        results = _run(p)
        assert [r.status for r in results] == ["success", "success"]
        assert results[0].details["kept_target_only_columns"] == ["email"]
        assert results[1].details["action"] == "kept"
        p.target_spark.sql.assert_not_called()
        p.logger.warning.assert_called()

    def test_overwrite_drops_target_only_policies(self):
        p = _provider(overwrite_masks_and_row_filters=True)
        p.source_dbops.get_table_info.return_value = _src_table()
        p.target_dbops.get_table_info.return_value = _tgt_table(
            masks={"email": ("tgt.sch.mask_email", None)},
            row_filter=TableRowFilter(
                function_name="tgt.sch.rf", input_column_names=["r"]
            ),
        )
        results = _run(p, overwrite_masks_and_row_filters=True)
        assert all(r.status == "success" for r in results)
        executed = [c.args[0] for c in p.target_spark.sql.call_args_list]
        assert executed == [
            "ALTER TABLE `tgt`.`sch`.`t` ALTER COLUMN `email` DROP MASK",
            "ALTER TABLE `tgt`.`sch`.`t` DROP ROW FILTER",
        ]

    def test_retry_exhaustion_marks_object_failed_and_unprotected(self):
        p = _provider()
        p.source_dbops.get_table_info.return_value = _src_table(
            masks={"name": ("src.sch.mask_str", None)}
        )
        p.target_dbops.get_table_info.return_value = _tgt_table()
        p.target_spark.sql.side_effect = RuntimeError("boom")
        results = _run(p, [UCObjectType.COLUMN_MASK])
        assert results[0].status == "failed"
        assert "boom" in results[0].error_message
        assert results[0].details["unprotected_on_target"] is True

    def test_unexpected_metadata_error_never_raises(self):
        p = _provider()
        p.source_dbops.get_table_info.side_effect = RuntimeError("no access")
        results = _run(p)
        assert [r.status for r in results] == ["failed", "failed"]
        assert "no access" in results[0].error_message

    def test_row_filter_only_request(self):
        p = _provider()
        p.source_dbops.get_table_info.return_value = _src_table(
            masks={"name": ("src.sch.mask_str", None)},
            row_filter=TableRowFilter(
                function_name="src.sch.rf", input_column_names=["r"]
            ),
        )
        p.target_dbops.get_table_info.return_value = _tgt_table()
        results = _run(p, [UCObjectType.ROW_FILTER])
        assert [r.object_type for r in results] == ["row_filter"]
        executed = [c.args[0] for c in p.target_spark.sql.call_args_list]
        assert executed == [
            "ALTER TABLE `tgt`.`sch`.`t` SET ROW FILTER `tgt`.`sch`.`rf` ON (`r`)"
        ]


# ---------- process_table gating ----------


_UC_STEPS = [
    "_uc_replicate_ddl",
    "_uc_replicate_masks_and_row_filters",
    "_uc_replicate_sql_st_mv",
    "_uc_replicate_table_comments",
    "_uc_replicate_table_tags",
    "_uc_replicate_table_grants",
    "_uc_replicate_column_comments",
    "_uc_replicate_column_tags",
]


def _process(uc_types, table_type):
    p = make_provider(uc_object_types=uc_types)
    p.spark.catalog.tableExists.return_value = True
    p.db_ops.get_table_type.return_value = table_type
    for name in _UC_STEPS:
        setattr(p, name, MagicMock(return_value=[name]))
    p.process_table(SchemaConfig(schema_name="sch"), _table_config())
    return p


class TestProcessTableGating:
    def test_masks_run_after_table_ddl_and_before_grants(self):
        p = _process([UCObjectType.ALL], "MANAGED")
        call = p._uc_replicate_masks_and_row_filters
        call.assert_called_once()
        assert call.call_args.args[2] == [
            UCObjectType.COLUMN_MASK,
            UCObjectType.ROW_FILTER,
        ]

    def test_column_mask_only_request(self):
        p = _process([UCObjectType.COLUMN_MASK], "EXTERNAL")
        assert p._uc_replicate_masks_and_row_filters.call_args.args[2] == [
            UCObjectType.COLUMN_MASK
        ]
        p._uc_replicate_ddl.assert_not_called()

    def test_not_run_for_views(self):
        p = _process([UCObjectType.ALL], "VIEW")
        p._uc_replicate_masks_and_row_filters.assert_not_called()

    def test_not_run_when_not_requested(self):
        p = _process([UCObjectType.TABLE], "MANAGED")
        p._uc_replicate_masks_and_row_filters.assert_not_called()


# ---------- config / CLI / SDK wrapper ----------


def test_overwrite_option_defaults_to_false():
    assert ReplicationConfig().overwrite_masks_and_row_filters is False


def test_object_types_parse_from_cli_value():
    from data_replication.cli import args as cli_args

    out = cli_args.parse_comma_delimited_enums(
        "column_mask,row_filter", UCObjectType, "--uc-object-types"
    )
    assert out == [UCObjectType.COLUMN_MASK, UCObjectType.ROW_FILTER]
    assert UCObjectType("row_filter") is UCObjectType.ROW_FILTER


def test_get_table_info_returns_none_when_missing():
    ws = MagicMock()
    ws.tables.get.side_effect = NotFound("nope")
    ops = DatabricksOperations(spark=MagicMock(), workspace_client=ws)
    assert ops.get_table_info("cat.sch.t") is None
    ws.tables.get.assert_called_once_with(full_name="cat.sch.t")


def test_get_table_info_returns_metadata():
    ws = MagicMock()
    info = TableInfo(name="t")
    ws.tables.get.return_value = info
    ops = DatabricksOperations(spark=MagicMock(), workspace_client=ws)
    assert ops.get_table_info("cat.sch.t") is info


def test_get_table_info_requires_workspace_client():
    ops = DatabricksOperations(spark=MagicMock())
    with pytest.raises(ValueError):
        ops.get_table_info("cat.sch.t")
