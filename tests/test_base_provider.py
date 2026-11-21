"""Unit tests for BaseProvider orchestration, discovery, filters, tags and
catalog/schema metadata replication.

All tests are offline: a minimal concrete subclass is built with MagicMock
spark / DatabricksOperations / WorkspaceClient.
"""

from __future__ import annotations

import logging
import time
from concurrent.futures import TimeoutError as FuturesTimeoutError
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import (
    CatalogInfo,
    EnablePredictiveOptimization,
    PermissionsChange,
    Privilege,
    PrivilegeAssignment,
    SchemaInfo,
)

from data_replication.config.models import (
    ConcurrencyConfig,
    ReconciliationConfig,
    ReplicationConfig,
    RetryConfig,
    RunResult,
    SchemaConfig,
    TableConfig,
    TableType,
    TargetCatalogConfig,
    UCObjectType,
    VolumeConfig,
    VolumeType,
)
from data_replication.exceptions import ReplicationError
from data_replication.providers.base_provider import BaseProvider

# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _rr(name="obj", status="success", schema="s", object_type="table", catalog="c"):
    return RunResult(
        operation_type="test",
        catalog_name=catalog,
        schema_name=schema,
        object_name=name,
        object_type=object_type,
        status=status,
        start_time="2026-01-01T00:00:00",
        end_time="2026-01-01T00:00:01",
    )


class _Provider(BaseProvider):
    """Minimal concrete provider: records processed tables/volumes."""

    def __init__(self, *args, operation="replication", enabled=True, **kwargs):
        super().__init__(*args, **kwargs)
        self._operation = operation
        self._enabled = enabled
        self.table_calls: list = []
        self.volume_calls: list = []
        self.table_status: dict = {}
        self.table_error: dict = {}

    def setup_operation_catalogs(self):
        return self.catalog_config.catalog_name

    def process_table(self, schema_config, table_config):
        key = f"{schema_config.schema_name}.{table_config.table_name}"
        self.table_calls.append(key)
        if key in self.table_error:
            raise self.table_error[key]
        return [
            _rr(
                table_config.table_name,
                self.table_status.get(key, "success"),
                schema_config.schema_name,
            )
        ]

    def process_volume(self, schema_config, volume_config):
        self.volume_calls.append(
            f"{schema_config.schema_name}.{volume_config.volume_name}"
        )
        return [_rr(volume_config.volume_name, "success", schema_config.schema_name)]

    def get_operation_name(self):
        return self._operation

    def is_operation_enabled(self):
        return self._enabled


def _catalog_config(**kwargs):
    kwargs.setdefault("concurrency", ConcurrencyConfig(max_workers=2))
    kwargs.setdefault("retry", RetryConfig(max_attempts=1, retry_delay_seconds=1))
    kwargs.setdefault("replication_config", ReplicationConfig(source_catalog="src"))
    return TargetCatalogConfig(catalog_name=kwargs.pop("catalog_name", "c"), **kwargs)


def _provider(operation="replication", enabled=True, completed=None, **cfg_kwargs):
    db_ops = MagicMock()
    db_ops.refresh_schema_metadata.return_value = True
    # Table/volume type filters pass everything through by default.
    db_ops.filter_tables_by_type.side_effect = lambda c, s, names, types, p: list(names)
    db_ops.filter_volumes_by_type.side_effect = lambda c, s, names, types: list(names)
    spark = MagicMock()
    spark.catalog.databaseExists.return_value = True
    p = _Provider(
        spark,
        logging.getLogger("tests.base_provider"),
        db_ops,
        MagicMock(),
        "run-1",
        _catalog_config(**cfg_kwargs),
        None,
        None,
        cloud_url_mapping=None,
        audit_logger=MagicMock(),
        completed_run_results=completed,
        principal_mapping=None,
        operation=operation,
        enabled=enabled,
    )
    p.catalog_name = p.catalog_config.catalog_name
    p.source_dbops = MagicMock()
    p.target_dbops = MagicMock()
    p.target_spark = MagicMock()
    return p


def _stub_uc_steps(p, calls):
    """Replace catalog/schema-level UC steps with recorders."""

    def rec(name, result_name):
        def _f(*args):
            label = name if not args else f"{name}:{args[0].schema_name}"
            calls.append(label)
            return [_rr(result_name, object_type=name)]

        return _f

    p._uc_replicate_catalog = rec("catalog", "c")
    p._uc_replicate_catalog_tags = rec("catalog_tag", "c")
    p._uc_replicate_catalog_grants = rec("catalog_grant", "c")
    p._uc_replicate_schema = rec("schema", "s")
    p._uc_replicate_schema_tags = rec("schema_tag", "s")
    p._uc_replicate_schema_grants = rec("schema_grant", "s")


# --------------------------------------------------------------------------- #
# __init__
# --------------------------------------------------------------------------- #


def test_init_copies_concurrency_and_completed_objects():
    p = _provider(
        completed=[_rr("t1", schema="s1", catalog="c")],
        concurrency=ConcurrencyConfig(max_workers=3, timeout_seconds=120),
    )
    assert p.max_workers == 3
    assert p.timeout_seconds == 120
    assert p.completed_objects == ["c.s1.t1"]
    assert p.processed_objects == []


def test_default_process_volume_returns_none():
    class _NoVolumes(_Provider):
        process_volume = BaseProvider.process_volume

    p = _provider()
    p.__class__ = _NoVolumes
    assert (
        p.process_volume(SchemaConfig(schema_name="s"), VolumeConfig(volume_name="v"))
        is None
    )


# --------------------------------------------------------------------------- #
# process_catalog orchestration
# --------------------------------------------------------------------------- #


class TestProcessCatalogOrchestration:
    def test_disabled_operation_returns_nothing(self):
        p = _provider(enabled=False)
        p.setup_operation_catalogs = MagicMock()
        assert p.process_catalog() == []
        p.setup_operation_catalogs.assert_not_called()

    def test_catalog_only_stops_after_catalog(self):
        calls: list = []
        p = _provider(uc_object_types=[UCObjectType.CATALOG])
        _stub_uc_steps(p, calls)
        p._get_schemas = MagicMock()

        results = p.process_catalog()

        assert calls == ["catalog"]
        assert [r.object_type for r in results] == ["catalog"]
        p._get_schemas.assert_not_called()
        p.audit_logger.log_results.assert_called_once()

    def test_catalog_tag_and_grant_only(self):
        calls: list = []
        p = _provider(
            uc_object_types=[UCObjectType.CATALOG_TAG, UCObjectType.CATALOG_GRANT]
        )
        _stub_uc_steps(p, calls)
        p._get_schemas = MagicMock()

        p.process_catalog()

        assert calls == ["catalog_tag", "catalog_grant"]
        p._get_schemas.assert_not_called()

    def test_schema_types_run_per_schema_without_tables(self):
        calls: list = []
        p = _provider(
            uc_object_types=[
                UCObjectType.CATALOG,
                UCObjectType.SCHEMA,
                UCObjectType.SCHEMA_TAG,
                UCObjectType.SCHEMA_GRANT,
            ]
        )
        p.db_ops.get_all_schemas.return_value = ["s1", "s2"]
        _stub_uc_steps(p, calls)
        p.process_schema = MagicMock()

        results = p.process_catalog()

        assert calls == [
            "catalog",
            "schema:s1",
            "schema_tag:s1",
            "schema_grant:s1",
            "schema:s2",
            "schema_tag:s2",
            "schema_grant:s2",
        ]
        assert len(results) == 7
        # No table-level types requested -> tables are never discovered.
        p.process_schema.assert_not_called()

    def test_all_runs_every_step_then_tables(self):
        calls: list = []
        p = _provider(uc_object_types=[UCObjectType.ALL])
        p.db_ops.get_all_schemas.return_value = ["s1"]
        p.db_ops.get_tables_in_schema.return_value = ["t1", "t2"]
        p.db_ops.get_volumes_in_schema.return_value = ["v1"]
        _stub_uc_steps(p, calls)

        results = p.process_catalog()

        assert calls == [
            "catalog",
            "catalog_tag",
            "catalog_grant",
            "schema:s1",
            "schema_tag:s1",
            "schema_grant:s1",
        ]
        assert sorted(p.table_calls) == ["s1.t1", "s1.t2"]
        assert p.volume_calls == ["s1.v1"]
        names = sorted(r.object_name for r in results)
        assert names == ["c", "c", "c", "s", "s", "s", "t1", "t2", "v1"]
        assert sorted(p.processed_objects) == ["s1.t1", "s1.t2", "s1.v1"]

    def test_tables_processed_in_serial_per_schema(self):
        p = _provider(
            table_types=[TableType.MANAGED],
            concurrency=ConcurrencyConfig(
                max_workers=2, process_schemas_in_serial=True
            ),
        )
        p.db_ops.get_all_schemas.return_value = ["s1", "s2"]
        p.db_ops.get_tables_in_schema.side_effect = lambda c, s: [f"{s}_t"]
        p._process_schema_tables = MagicMock()
        p.table_status["s2.s2_t"] = "failed"

        results = p.process_catalog()

        assert sorted(p.table_calls) == ["s1.s1_t", "s2.s2_t"]
        assert sorted((r.object_name, r.status) for r in results) == [
            ("s1_t", "success"),
            ("s2_t", "failed"),
        ]
        # Serial mode never uses the catalog-wide pool.
        p._process_schema_tables.assert_not_called()

    def test_filter_expression_drives_schema_and_table_selection(self):
        p = _provider(
            schema_table_filter_expression="table_name like 'fact_%'",
            table_types=[TableType.MANAGED],
        )
        p.db_ops.get_schema_tables_by_filter.return_value = [
            {"schema_name": "s1", "table_name": "fact_a"},
            {"schema_name": "s1", "table_name": "fact_b"},
            {"schema_name": "s2", "table_name": "fact_c"},
        ]

        results = p.process_catalog()

        expr = p.db_ops.get_schema_tables_by_filter.call_args.args[1]
        assert "table_name like 'fact_%'" in expr
        assert "table_type = 'MANAGED'" in expr
        assert sorted(p.table_calls) == ["s1.fact_a", "s1.fact_b", "s2.fact_c"]
        assert len(results) == 3
        # Full-schema listing is never used when a filter expression is set.
        p.db_ops.get_tables_in_schema.assert_not_called()
        assert [s.schema_name for s in p.catalog_config.target_schemas] == ["s1", "s2"]

    def test_filter_expression_keeps_existing_table_config(self):
        custom = TableConfig(
            table_name="fact_a",
            retry=RetryConfig(max_attempts=7, retry_delay_seconds=1),
        )
        p = _provider(
            schema_table_filter_expression="1=1",
            target_schemas=[SchemaConfig(schema_name="s1", tables=[custom])],
            table_types=[TableType.MANAGED],
        )
        p.db_ops.get_schema_tables_by_filter.return_value = [
            {"schema_name": "s1", "table_name": "fact_a"},
        ]

        p.process_catalog()

        kept = p.catalog_config.target_schemas[0].tables[0]
        assert kept.retry.max_attempts == 7

    def test_reconciliation_sampling_limits_tables(self, monkeypatch):
        p = _provider(
            operation="reconciliation",
            schema_table_filter_expression="1=1",
            table_types=[TableType.MANAGED],
            reconciliation_config=ReconciliationConfig(
                enable_sampling=True, no_of_sampling_tables=2
            ),
        )
        p.db_ops.get_schema_tables_by_filter.return_value = [
            {"schema_name": "s1", "table_name": f"t{i}"} for i in range(5)
        ]
        monkeypatch.setattr(
            "data_replication.providers.base_provider.random.sample",
            lambda population, k: population[:k],
        )

        p.process_catalog()

        assert sorted(p.table_calls) == ["s1.t0", "s1.t1"]

    def test_catalog_level_exception_returns_flat_failed_result(self):
        p = _provider()
        p.setup_operation_catalogs = MagicMock(side_effect=RuntimeError("boom"))

        results = p.process_catalog()

        assert len(results) == 1
        assert isinstance(results[0], RunResult)
        assert results[0].status == "failed"

    @pytest.mark.xfail(
        reason=(
            "bug: a schema_table_filter_expression that matches no tables leaves "
            "target_schemas empty, and _get_schemas then reads an unassigned "
            "schema_list (UnboundLocalError)"
        ),
        strict=True,
    )
    def test_filter_expression_with_no_matches_is_a_noop(self):
        p = _provider(schema_table_filter_expression="table_name = 'missing'")
        p.db_ops.get_schema_tables_by_filter.return_value = []

        assert p.process_catalog() == []


# --------------------------------------------------------------------------- #
# process_schema
# --------------------------------------------------------------------------- #


class TestProcessSchema:
    def _schema(self, p, **kwargs):
        from copy import deepcopy

        from data_replication.utils import merge_models_recursive

        return merge_models_recursive(
            deepcopy(p.catalog_config), SchemaConfig(schema_name="s1", **kwargs)
        )

    def test_no_objects_returns_empty(self):
        p = _provider(uc_object_types=[UCObjectType.TABLE])
        p.db_ops.get_tables_in_schema.return_value = []

        assert p.process_schema(self._schema(p)) == ([], [], [])

    def test_volume_types_only_skip_table_discovery(self):
        p = _provider(volume_types=[VolumeType.MANAGED])
        p.db_ops.get_volumes_in_schema.return_value = ["v1"]

        results, tables, volumes = p.process_schema(self._schema(p))

        p.db_ops.get_tables_in_schema.assert_not_called()
        assert results == []  # parallel mode: processed later at catalog level
        assert tables == []
        assert [v["volume_config"].volume_name for v in volumes] == ["v1"]

    def test_table_config_inherits_schema_config(self):
        p = _provider(uc_object_types=[UCObjectType.TABLE])
        p.db_ops.get_tables_in_schema.return_value = ["t1"]
        schema = self._schema(
            p, retry=RetryConfig(max_attempts=4, retry_delay_seconds=1)
        )

        _, tables, _ = p.process_schema(schema)

        assert len(tables) == 1
        assert tables[0]["schema_config"] is schema
        assert tables[0]["table_config"].retry.max_attempts == 4

    def test_serial_mode_processes_tables_then_volumes(self):
        p = _provider(
            uc_object_types=[UCObjectType.TABLE, UCObjectType.VOLUME],
            concurrency=ConcurrencyConfig(
                max_workers=1, process_schemas_in_serial=True
            ),
        )
        p.db_ops.get_tables_in_schema.return_value = ["t1"]
        p.db_ops.get_volumes_in_schema.return_value = ["v1"]

        results, _, _ = p.process_schema(self._schema(p))

        assert [r.object_name for r in results] == ["t1", "v1"]

    def test_discovery_error_becomes_failed_schema_result(self):
        p = _provider(uc_object_types=[UCObjectType.TABLE])
        p.db_ops.get_tables_in_schema.side_effect = ReplicationError("no access")

        results, tables, volumes = p.process_schema(self._schema(p))

        assert tables == [] and volumes == []
        assert len(results) == 1
        assert results[0].status == "failed"
        assert results[0].object_type == "schema"
        assert "Failed to processing schema c.s1." in results[0].error_message


# --------------------------------------------------------------------------- #
# Concurrent processing helpers
# --------------------------------------------------------------------------- #


class TestConcurrentProcessing:
    def _items(self, p, names):
        schema = SchemaConfig(schema_name="s1", concurrency=ConcurrencyConfig())
        return schema, [
            {"schema_config": schema, "table_config": TableConfig(table_name=n)}
            for n in names
        ]

    def test_schema_tables_collects_results_and_failures(self, caplog):
        p = _provider()
        schema, items = self._items(p, ["ok", "bad", "skip", "boom"])
        p.table_status = {"s1.bad": "failed", "s1.skip": "skipped"}
        p.table_error = {"s1.boom": RuntimeError("kaput")}

        with caplog.at_level(logging.WARNING, logger="tests.base_provider"):
            results = p._process_schema_tables(items, "c", None)

        by_name = {r.object_name: r for r in results}
        assert by_name["ok"].status == "success"
        assert by_name["bad"].status == "failed"
        assert by_name["skip"].status == "skipped"
        assert by_name["boom"].status == "failed"
        assert "Unexpected error processing table c.s1.boom: kaput" in (
            by_name["boom"].error_message
        )
        assert sorted(p.processed_objects) == ["s1.bad", "s1.boom", "s1.ok", "s1.skip"]
        assert any("skipped" in m for m in caplog.messages)

    def test_process_tables_marks_exception_failed(self):
        p = _provider()
        schema, _ = self._items(p, [])
        p.table_error = {"s1.t2": ReplicationError("denied")}

        results = p._process_tables(
            schema,
            [TableConfig(table_name="t1"), TableConfig(table_name="t2")],
            "c",
            None,
        )

        statuses = {r.object_name: r.status for r in results}
        assert statuses == {"t1": "success", "t2": "failed"}
        failed = next(r for r in results if r.object_name == "t2")
        assert failed.error_message.startswith("Failed to processing table c.s1.t2")

    def test_volume_helpers_collect_results_and_errors(self):
        p = _provider()
        schema = SchemaConfig(schema_name="s1", concurrency=ConcurrencyConfig())

        def vol(schema_config, volume_config):
            if volume_config.volume_name == "bad":
                raise RuntimeError("io")
            status = "failed" if volume_config.volume_name == "half" else "success"
            return [_rr(volume_config.volume_name, status, schema_config.schema_name)]

        p.process_volume = vol
        vols = [VolumeConfig(volume_name=n) for n in ["good", "half", "bad"]]

        per_schema = p._process_volumes(schema, vols, "c", None)
        catalog_wide = p._process_schema_volumes(
            [{"schema_config": schema, "volume_config": v} for v in vols], "c", None
        )

        for results in (per_schema, catalog_wide):
            statuses = {r.object_name: r.status for r in results}
            assert statuses == {"good": "success", "half": "failed", "bad": "failed"}

    @pytest.mark.xfail(
        reason=(
            "bug: concurrency.timeout_seconds is never enforced; as_completed() "
            "only yields finished futures, so future.result(timeout=...) cannot "
            "time out and a slow object runs to completion"
        ),
        strict=True,
    )
    def test_slow_table_exceeding_timeout_is_failed(self):
        p = _provider()
        p.timeout_seconds = 0.05

        def slow(schema_config, table_config):
            time.sleep(0.3)
            return [_rr(table_config.table_name)]

        p.process_table = slow
        schema = SchemaConfig(schema_name="s1", concurrency=ConcurrencyConfig())

        results = p._process_tables(schema, [TableConfig(table_name="t")], "c", None)

        assert results[0].status == "failed"
        assert "Timeout" in results[0].error_message


# --------------------------------------------------------------------------- #
# _handle_exception / _create_failed_result
# --------------------------------------------------------------------------- #


class TestHandleException:
    @pytest.mark.parametrize(
        "exc,prefix",
        [
            (FuturesTimeoutError(), "Timeout replicating c.s.t after 1800s"),
            (TimeoutError(), "Timeout replicating c.s.t after 1800s"),
            (ReplicationError("nope"), "Failed to replicating c.s.t: nope"),
            (KeyError("k"), "Unexpected error replicating c.s.t: 'k'"),
        ],
    )
    def test_message_by_exception_kind(self, exc, prefix):
        p = _provider()
        [result] = p._handle_exception(exc, "replicating", "c", "s", "t", "table")
        assert result.status == "failed"
        assert result.object_type == "table"
        assert result.operation_type == "replication"
        assert result.error_message.startswith(prefix)

    def test_failed_result_defaults_start_time(self):
        p = _provider()
        [result] = p._create_failed_result("c", "s", "t", "table", "err")
        assert result.start_time and result.end_time
        assert result.error_message == "err"


# --------------------------------------------------------------------------- #
# Filter expressions
# --------------------------------------------------------------------------- #


class TestFilterExpressions:
    def test_external_adds_managed_for_replication_and_reconciliation(self):
        for op in ("replication", "reconciliation"):
            p = _provider(operation=op)
            assert p._adjust_table_types([TableType.EXTERNAL]) == [
                TableType.EXTERNAL,
                TableType.MANAGED,
            ]

    def test_external_unchanged_for_backup(self):
        p = _provider(operation="backup")
        assert p._adjust_table_types([TableType.EXTERNAL]) == [TableType.EXTERNAL]

    def test_table_type_filter_expression(self):
        p = _provider()
        assert p._table_type_filter_expression([]) is None
        assert (
            p._table_type_filter_expression(
                [TableType.MANAGED, TableType.STREAMING_TABLE]
            )
            == "table_type = 'MANAGED' or table_type = 'STREAMING_TABLE'"
        )

    def test_target_schemas_variants(self):
        p = _provider(
            target_schemas=[
                SchemaConfig(
                    schema_name="a",
                    tables=[TableConfig(table_name="t1"), TableConfig(table_name="t2")],
                ),
                SchemaConfig(
                    schema_name="b", exclude_tables=[TableConfig(table_name="x")]
                ),
                SchemaConfig(
                    schema_name="c", table_filter_expression="table_name like 'f%'"
                ),
                SchemaConfig(schema_name="d", table_types=[TableType.STREAMING_TABLE]),
            ]
        )

        expr = p._build_schema_table_filter_expression()

        assert expr == (
            "(table_schema = 'a' and (table_name = 't1' or table_name = 't2'))"
            " or (table_schema = 'b' and (table_name != 'x'))"
            " or (table_schema = 'c' and (table_name like 'f%'))"
            " or ((table_schema = 'd') and (table_type = 'STREAMING_TABLE'))"
        )

    def test_catalog_table_types_apply_to_target_schemas(self):
        p = _provider(
            target_schemas=[SchemaConfig(schema_name="a")],
            table_types=[TableType.EXTERNAL],
        )
        assert p._build_schema_table_filter_expression() == (
            "((table_schema = 'a') and "
            "(table_type = 'EXTERNAL' or table_type = 'MANAGED'))"
        )

    def test_schema_name_placeholder_is_substituted(self):
        p = _provider(
            target_schemas=[
                SchemaConfig(
                    schema_name="sales",
                    table_filter_expression="comment = '{{schema_name}}'",
                )
            ]
        )
        assert p._build_schema_table_filter_expression() == (
            "(table_schema = 'sales' and (comment = 'sales'))"
        )

    def test_exclude_schemas(self):
        p = _provider(
            exclude_schemas=[
                SchemaConfig(schema_name="x"),
                SchemaConfig(schema_name="y"),
            ]
        )
        assert (
            p._build_schema_table_filter_expression()
            == "table_schema != 'x' and table_schema != 'y'"
        )

    def test_catalog_filter_expression_with_table_types(self):
        p = _provider(
            schema_table_filter_expression="table_schema = 's'",
            table_types=[TableType.MANAGED],
        )
        assert p._build_schema_table_filter_expression() == (
            "(table_schema = 's' and (table_type = 'MANAGED'))"
        )

    def test_catalog_filter_expression_without_table_types(self):
        p = _provider(schema_table_filter_expression="table_schema = 's'")
        assert p._build_schema_table_filter_expression() == "table_schema = 's'"

    @pytest.mark.xfail(
        reason=(
            "bug: with no target_schemas/exclude_schemas/filter expression "
            "(e.g. reconciliation sampling alone), the method returns an "
            "unassigned local (UnboundLocalError)"
        ),
        strict=True,
    )
    def test_no_filters_configured_does_not_raise(self):
        p = _provider()
        assert not p._build_schema_table_filter_expression()


# --------------------------------------------------------------------------- #
# Discovery: schemas, tables, volumes
# --------------------------------------------------------------------------- #


class TestGetSchemas:
    def test_target_schemas_filtered_by_existence_and_exclusions(self):
        p = _provider(
            target_schemas=[
                SchemaConfig(schema_name="keep"),
                SchemaConfig(schema_name="missing"),
                SchemaConfig(schema_name="excluded"),
            ],
            exclude_schemas=[SchemaConfig(schema_name="excluded")],
        )
        p.spark.catalog.databaseExists.side_effect = lambda n: (
            not n.endswith(".missing")
        )

        assert [s.schema_name for s in p._get_schemas()] == ["keep"]
        p.db_ops.refresh_schema_metadata.assert_any_call("c.keep")

    def test_all_schemas_minus_exclusions(self):
        p = _provider(exclude_schemas=[SchemaConfig(schema_name="tmp")])
        p.db_ops.get_all_schemas.return_value = ["a", "tmp", "b"]

        assert [s.schema_name for s in p._get_schemas()] == ["a", "b"]
        p.db_ops.get_all_schemas.assert_called_once_with("c")


class TestGetSchemaTables:
    def test_skips_processed_and_completed_tables(self):
        p = _provider(completed=[_rr("done", schema="s", catalog="c")])
        p.processed_objects = ["s.seen"]
        p.db_ops.get_schema_tables_by_filter.return_value = [
            {"schema_name": "s", "table_name": n} for n in ["seen", "done", "new"]
        ]

        result = p._get_schema_tables("expr")

        assert result == [{"schema_name": "s", "table_name": "new"}]
        p.db_ops.get_schema_tables_by_filter.assert_called_once_with("c", "expr")


class TestGetTables:
    def _schema(self, **kwargs):
        kwargs.setdefault("concurrency", ConcurrencyConfig(parallel_table_filter=4))
        return SchemaConfig(schema_name="s", **kwargs)

    def test_explicit_tables_with_exclusions_processed_and_shared(self):
        p = _provider(completed=[_rr("done", schema="s", catalog="c")])
        p.processed_objects = ["s.seen"]
        p.shared_tables = ["c.s.shared"]
        tables = [
            TableConfig(table_name=n) for n in ["a", "excl", "seen", "done", "shared"]
        ]
        schema = self._schema(exclude_tables=[TableConfig(table_name="excl")])

        result = p._get_tables(schema, tables)

        assert [t.table_name for t in result] == ["a"]
        p.db_ops.get_tables_in_schema.assert_not_called()

    def test_table_filter_expression_used_for_discovery(self):
        p = _provider()
        p.db_ops.get_tables_by_filter.return_value = ["f1", "f2"]

        result = p._get_tables(self._schema(table_filter_expression="x = 1"), None)

        assert [t.table_name for t in result] == ["f1", "f2"]
        p.db_ops.get_tables_by_filter.assert_called_once_with("c", "s", "x = 1")

    def test_all_tables_listed_when_nothing_configured(self):
        p = _provider()
        p.db_ops.get_tables_in_schema.return_value = ["t1"]

        assert [t.table_name for t in p._get_tables(self._schema(), None)] == ["t1"]

    def test_no_listing_when_catalog_filter_expression_set(self):
        p = _provider(schema_table_filter_expression="1=1")

        assert p._get_tables(self._schema(), None) == []
        p.db_ops.get_tables_in_schema.assert_not_called()

    def test_type_filter_result_is_respected(self):
        p = _provider()
        p.db_ops.get_tables_in_schema.return_value = ["t1", "v1"]
        p.db_ops.filter_tables_by_type.side_effect = None
        p.db_ops.filter_tables_by_type.return_value = ["t1"]

        result = p._get_tables(self._schema(table_types=[TableType.MANAGED]), None)

        assert [t.table_name for t in result] == ["t1"]
        args = p.db_ops.filter_tables_by_type.call_args.args
        assert args[3] == ["managed"]
        assert args[4] == 4

    @pytest.mark.parametrize(
        "uc_types,expected",
        [
            ([UCObjectType.TABLE], {"managed", "external"}),
            ([UCObjectType.VIEW], {"view"}),
            ([UCObjectType.TABLE_COMMENT], {"managed", "external", "view"}),
            ([UCObjectType.MATERIALIZED_VIEW], {"materialized_view"}),
            ([UCObjectType.STREAMING_TABLE], {"streaming_table"}),
            (
                [UCObjectType.TABLE_GRANT],
                {"managed", "external", "view", "materialized_view", "streaming_table"},
            ),
            (
                [UCObjectType.VIEW, UCObjectType.COLUMN_TAG],
                {"managed", "external", "view", "materialized_view", "streaming_table"},
            ),
        ],
    )
    def test_uc_object_types_map_to_table_types(self, uc_types, expected):
        p = _provider()
        p.db_ops.get_tables_in_schema.return_value = ["t1"]

        p._get_tables(self._schema(uc_object_types=uc_types), None)

        assert set(p.db_ops.filter_tables_by_type.call_args.args[3]) == expected

    def test_materialized_view_type_dropped_for_replication(self):
        p = _provider()
        p.db_ops.get_tables_in_schema.return_value = ["t1"]
        schema = self._schema(
            table_types=[TableType.MANAGED, TableType.MATERIALIZED_VIEW]
        )

        p._get_tables(schema, None)

        assert p.db_ops.filter_tables_by_type.call_args.args[3] == ["managed"]

    def test_backup_skips_type_filtering(self):
        p = _provider(operation="backup")
        p.db_ops.get_tables_in_schema.return_value = ["t1"]

        result = p._get_tables(self._schema(table_types=[TableType.MANAGED]), None)

        assert [t.table_name for t in result] == ["t1"]
        p.db_ops.filter_tables_by_type.assert_not_called()


class TestGetVolumes:
    def test_explicit_volumes_with_exclusions(self):
        p = _provider()
        schema = SchemaConfig(
            schema_name="s", exclude_volumes=[VolumeConfig(volume_name="x")]
        )
        vols = [VolumeConfig(volume_name="a"), VolumeConfig(volume_name="x")]

        result = p._get_volumes(schema, vols)

        assert [v.volume_name for v in result] == ["a"]
        p.db_ops.get_volumes_in_schema.assert_not_called()

    def test_discovered_volumes_filtered_by_type(self):
        p = _provider()
        p.db_ops.get_volumes_in_schema.return_value = ["m", "e"]
        p.db_ops.filter_volumes_by_type.side_effect = None
        p.db_ops.filter_volumes_by_type.return_value = ["m"]
        schema = SchemaConfig(schema_name="s", volume_types=[VolumeType.MANAGED])

        result = p._get_volumes(schema, None)

        assert [v.volume_name for v in result] == ["m"]
        assert p.db_ops.filter_volumes_by_type.call_args.args[3] == ["managed"]

    def test_uc_volume_types_include_managed_and_external(self):
        p = _provider()
        p.db_ops.get_volumes_in_schema.return_value = ["v"]
        schema = SchemaConfig(
            schema_name="s", uc_object_types=[UCObjectType.VOLUME_TAG]
        )

        p._get_volumes(schema, None)

        assert p.db_ops.filter_volumes_by_type.call_args.args[3] == [
            "managed",
            "external",
        ]


# --------------------------------------------------------------------------- #
# Tags
# --------------------------------------------------------------------------- #


class TestBuildTagSql:
    @pytest.mark.parametrize(
        "kwargs,unset,set_",
        [
            (
                {},
                "ALTER CATALOG `c` UNSET TAGS ( 'a' )",
                "ALTER CATALOG `c` SET TAGS ( 'k' = 'v' )",
            ),
            (
                {"schema_name": "s"},
                "ALTER SCHEMA `c`.`s` UNSET TAGS ( 'a' )",
                "ALTER SCHEMA `c`.`s` SET TAGS ( 'k' = 'v' )",
            ),
            (
                {"schema_name": "s", "table_name": "t"},
                "ALTER TABLE `c`.`s`.`t` UNSET TAGS ( 'a' )",
                "ALTER TABLE `c`.`s`.`t` SET TAGS ( 'k' = 'v' )",
            ),
            (
                {"schema_name": "s", "table_name": "t", "column_name": "col"},
                "ALTER TABLE `c`.`s`.`t` ALTER COLUMN `col` UNSET TAGS ( 'a' )",
                "ALTER TABLE `c`.`s`.`t` ALTER COLUMN `col` SET TAGS ( 'k' = 'v' )",
            ),
            (
                {"schema_name": "s", "volume_name": "v"},
                "ALTER VOLUME `c`.`s`.`v` UNSET TAGS ( 'a' )",
                "ALTER VOLUME `c`.`s`.`v` SET TAGS ( 'k' = 'v' )",
            ),
        ],
    )
    def test_sql_per_object_level(self, kwargs, unset, set_):
        p = _provider()
        assert p._build_tag_sql(["a"], {"k": "v"}, "c", **kwargs) == (unset, set_)

    def test_nothing_to_do(self):
        assert _provider()._build_tag_sql([], {}, "c") == (None, None)


class TestReplicateTags:
    RETRY = RetryConfig(max_attempts=1, retry_delay_seconds=1)

    def test_no_difference_is_skipped_success(self):
        p = _provider()
        r = p._replicate_tags(
            "table_tag",
            [{"k": "v"}],
            ["k"],
            [{"k": "v"}],
            True,
            "src",
            "c",
            "s",
            "t",
            retry=self.RETRY,
        )
        assert r.status == "success"
        assert r.details["skipped"] is True
        p.target_spark.sql.assert_not_called()

    def test_overwrite_unsets_then_sets(self):
        p = _provider()
        r = p._replicate_tags(
            "table_tag",
            [{"k": "new"}],
            ["old"],
            [{"old": "x"}],
            True,
            "src",
            "c",
            "s",
            "t",
            retry=self.RETRY,
        )
        assert r.status == "success"
        assert r.object_name == "t"
        sqls = [c.args[0] for c in p.target_spark.sql.call_args_list]
        assert sqls == [
            "ALTER TABLE `c`.`s`.`t` UNSET TAGS ( 'old' )",
            "ALTER TABLE `c`.`s`.`t` SET TAGS ( 'k' = 'new' )",
        ]
        assert r.details["source_object"] == "`src`.`s`.`t`"

    def test_no_overwrite_keeps_target_tags(self):
        p = _provider()
        r = p._replicate_tags(
            "schema_tag",
            [{"k": "v"}],
            ["old"],
            [{"old": "x"}],
            False,
            "src",
            "c",
            "s",
            retry=self.RETRY,
        )
        assert r.status == "success"
        sqls = [c.args[0] for c in p.target_spark.sql.call_args_list]
        assert len(sqls) == 1 and "UNSET" not in sqls[0]

    def test_sql_failure_reports_failed(self):
        p = _provider()
        p.target_spark.sql.side_effect = RuntimeError("PERMISSION_DENIED")
        r = p._replicate_tags(
            "volume_tag",
            [{"k": "v"}],
            [],
            [],
            True,
            "src",
            "c",
            "s",
            volume_name="vol",
            retry=self.RETRY,
        )
        assert r.status == "failed"
        assert r.object_name == "vol"
        assert "PERMISSION_DENIED" in r.error_message

    def test_catalog_tags_missing_catalog_fails(self):
        p = _provider()
        p.target_dbops.if_catalog_exists.return_value = False
        [r] = p._uc_replicate_catalog_tags()
        assert r.status == "failed"
        assert "does not exist" in r.error_message

    def test_catalog_tags_reads_both_sides(self):
        p = _provider()
        p.source_dbops.get_catalog_tags.return_value = (["k"], [{"k": "v"}])
        p.target_dbops.get_catalog_tags.return_value = ([], [])
        [r] = p._uc_replicate_catalog_tags()
        assert r.status == "success"
        p.source_dbops.get_catalog_tags.assert_called_once_with("src")
        p.target_dbops.get_catalog_tags.assert_called_once_with("c")
        assert p.target_spark.sql.call_args.args[0] == (
            "ALTER CATALOG `c` SET TAGS ( 'k' = 'v' )"
        )

    def test_schema_tags(self):
        p = _provider()
        schema = SchemaConfig(
            schema_name="s",
            replication_config=ReplicationConfig(source_catalog="src"),
            retry=self.RETRY,
        )
        p.source_dbops.get_schema_tags.return_value = (["k"], [{"k": "v"}])
        p.target_dbops.get_schema_tags.return_value = ([], [])
        [r] = p._uc_replicate_schema_tags(schema)
        assert r.status == "success"
        p.source_dbops.get_schema_tags.assert_called_once_with("src", "s")

        p.source_dbops.if_schema_exists.return_value = False
        [r] = p._uc_replicate_schema_tags(schema)
        assert r.status == "failed"


# --------------------------------------------------------------------------- #
# Grants (wrapper paths not covered by test_grant_replication.py)
# --------------------------------------------------------------------------- #


def _pa(principal, *privs):
    return PrivilegeAssignment(
        principal=principal, privileges=[Privilege(p) for p in privs]
    )


class TestReplicateGrants:
    RETRY = RetryConfig(max_attempts=1, retry_delay_seconds=1)

    def _run(self, p, **kwargs):
        return p._replicate_grants(
            object_type="table_grant",
            securable_type="TABLE",
            source_full_name="src.s.t",
            target_full_name="c.s.t",
            target_catalog="c",
            schema_name="s",
            object_name="t",
            retry=self.RETRY,
            **kwargs,
        )

    def test_applies_changes_in_one_batch(self):
        p = _provider()
        p.source_dbops.get_grants.return_value = [_pa("alice", "SELECT")]
        p.target_dbops.get_grants.return_value = []

        r = self._run(p)

        assert r.status == "success"
        assert r.details["principals_synced"] == 1
        p.target_dbops.update_grants.assert_called_once()
        changes = p.target_dbops.update_grants.call_args.kwargs["changes"]
        assert changes == [
            PermissionsChange(principal="alice", add=[Privilege.SELECT], remove=None)
        ]

    def test_no_changes_is_skipped_success(self):
        p = _provider()
        p.source_dbops.get_grants.return_value = [_pa("alice", "SELECT")]
        p.target_dbops.get_grants.return_value = [_pa("alice", "SELECT")]

        r = self._run(p)

        assert r.status == "success"
        assert r.details["skipped"] is True
        p.target_dbops.update_grants.assert_not_called()

    def test_only_unresolvable_principals_is_failed(self):
        p = _provider()
        sp = "11111111-2222-3333-4444-555555555555"
        p.source_dbops.get_grants.return_value = [_pa(sp, "SELECT")]
        p.source_dbops.get_sp_display_name_by_app_id.return_value = None
        p.target_dbops.get_grants.return_value = []

        r = self._run(p)

        assert r.status == "failed"
        assert r.details["principals_skipped"] == 1
        p.target_dbops.update_grants.assert_not_called()

    def test_batch_failure_falls_back_per_principal_partial(self):
        p = _provider()
        p.source_dbops.get_grants.return_value = [
            _pa("alice", "SELECT"),
            _pa("ghost", "SELECT"),
        ]
        p.target_dbops.get_grants.return_value = []

        def update(securable_type, full_name, changes):
            if len(changes) > 1 or changes[0].principal == "ghost":
                raise RuntimeError("PRINCIPAL_DOES_NOT_EXIST")

        p.target_dbops.update_grants.side_effect = update

        r = self._run(p)

        assert r.status == "failed"
        assert r.details["principals_synced"] == 1
        assert r.details["principals_failed"] == 1
        assert r.details["failed_principals"][0]["principal"] == "ghost"
        assert r.error_message == "1 principal(s) failed"

    def test_all_principals_fail(self):
        p = _provider()
        p.source_dbops.get_grants.return_value = [_pa("ghost", "SELECT")]
        p.target_dbops.get_grants.return_value = []
        p.target_dbops.update_grants.side_effect = RuntimeError("nope")

        r = self._run(p)

        assert r.status == "failed"
        assert "nope" in r.error_message

    def test_fetch_error_is_failed(self):
        p = _provider()
        p.source_dbops.get_grants.side_effect = RuntimeError("forbidden")

        r = self._run(p)

        assert r.status == "failed"
        assert r.error_message == "forbidden"

    def test_catalog_and_schema_grant_wrappers(self):
        p = _provider(
            replication_config=ReplicationConfig(
                source_catalog="src", overwrite_grants=True
            )
        )
        p._replicate_grants = MagicMock(
            return_value=_rr("c", object_type="catalog_grant")
        )

        p._uc_replicate_catalog_grants()
        kwargs = p._replicate_grants.call_args.kwargs
        assert (
            kwargs["securable_type"],
            kwargs["source_full_name"],
            kwargs["target_full_name"],
        ) == (
            "CATALOG",
            "src",
            "c",
        )
        assert kwargs["overwrite_grants"] is True

        schema = SchemaConfig(
            schema_name="s",
            replication_config=ReplicationConfig(source_catalog="src"),
            retry=self.RETRY,
        )
        p._uc_replicate_schema_grants(schema)
        kwargs = p._replicate_grants.call_args.kwargs
        assert (
            kwargs["securable_type"],
            kwargs["source_full_name"],
            kwargs["target_full_name"],
        ) == (
            "SCHEMA",
            "src.s",
            "c.s",
        )
        assert kwargs["overwrite_grants"] is False

    def test_grant_wrappers_fail_when_missing(self):
        p = _provider()
        p.source_dbops.if_catalog_exists.return_value = False
        p.source_dbops.if_schema_exists.return_value = False
        schema = SchemaConfig(
            schema_name="s",
            replication_config=ReplicationConfig(source_catalog="src"),
            retry=self.RETRY,
        )

        [cat] = p._uc_replicate_catalog_grants()
        [sch] = p._uc_replicate_schema_grants(schema)

        assert (cat.status, cat.object_type) == ("failed", "catalog_grant")
        assert (sch.status, sch.object_type) == ("failed", "schema_grant")


# --------------------------------------------------------------------------- #
# Catalog / schema metadata replication
# --------------------------------------------------------------------------- #


class TestUcReplicateCatalog:
    def _p(self, mapping=None, **repl):
        repl.setdefault("source_catalog", "src")
        p = _provider(replication_config=ReplicationConfig(**repl))
        p.cloud_url_mapping = mapping
        p.target_dbops.get_catalog.side_effect = Exception("not found")
        return p

    def test_creates_managed_catalog_and_disables_po(self):
        p = self._p()
        p.source_dbops.get_catalog.return_value = CatalogInfo(
            name="src", comment="hello"
        )

        [r] = p._uc_replicate_catalog()

        assert r.status == "success"
        assert r.details["catalog_existed"] is False
        created = p.target_dbops.create_catalog.call_args.args[0]
        assert created["name"] == "c"
        assert created["comment"] == "hello"
        assert created["storage_root"] is None
        p.target_dbops.update_catalog.assert_called_once_with(
            {
                "name": "c",
                "enable_predictive_optimization": EnablePredictiveOptimization.DISABLE,
            }
        )

    def test_keeps_po_when_configured(self):
        p = self._p(replicate_enable_predictive_optimization=True)
        p.source_dbops.get_catalog.return_value = CatalogInfo(name="src")

        p._uc_replicate_catalog()

        p.target_dbops.update_catalog.assert_not_called()

    def test_external_storage_root_is_mapped(self):
        p = self._p(mapping={"abfss://a@b/": "s3://bucket/"})
        p.source_dbops.get_catalog.return_value = CatalogInfo(
            name="src", storage_root="abfss://a@b/cat"
        )

        [r] = p._uc_replicate_catalog()

        assert r.status == "success"
        assert p.target_dbops.create_catalog.call_args.args[0]["storage_root"] == (
            "s3://bucket/cat"
        )

    @pytest.mark.parametrize("mapping", [None, {"gs://other/": "s3://x/"}])
    def test_unmapped_storage_root_fails(self, mapping):
        p = self._p(mapping=mapping)
        p.source_dbops.get_catalog.return_value = CatalogInfo(
            name="src", storage_root="abfss://a@b/cat"
        )

        [r] = p._uc_replicate_catalog()

        assert r.status == "failed"
        assert "replicate_as_managed=true" in r.error_message
        p.target_dbops.create_catalog.assert_not_called()

    def test_replicate_as_managed_drops_storage_root(self):
        p = self._p(replicate_as_managed=True)
        p.source_dbops.get_catalog.return_value = CatalogInfo(
            name="src", storage_root="abfss://a@b/cat"
        )

        p._uc_replicate_catalog()

        assert p.target_dbops.create_catalog.call_args.args[0]["storage_root"] is None

    def test_existing_catalog_updated_only_when_different(self):
        p = self._p(replicate_enable_predictive_optimization=True)
        p.source_dbops.get_catalog.return_value = CatalogInfo(name="src", comment="x")
        p.target_dbops.get_catalog.side_effect = None
        p.target_dbops.get_catalog.return_value = CatalogInfo(name="c", comment="x")

        [r] = p._uc_replicate_catalog()
        assert r.details["catalog_existed"] is True
        p.target_dbops.update_catalog.assert_not_called()
        p.target_dbops.create_catalog.assert_not_called()

        p.source_dbops.get_catalog.return_value = CatalogInfo(name="src", comment="new")
        p._uc_replicate_catalog()
        p.target_dbops.update_catalog.assert_called_once_with(
            {"name": "c", "comment": "new"}
        )

    def test_existing_catalog_po_disabled(self):
        p = self._p()
        p.source_dbops.get_catalog.return_value = CatalogInfo(name="src")
        p.target_dbops.get_catalog.side_effect = None
        p.target_dbops.get_catalog.return_value = CatalogInfo(name="c")

        p._uc_replicate_catalog()

        update = p.target_dbops.update_catalog.call_args.args[0]
        assert (
            update["enable_predictive_optimization"]
            == EnablePredictiveOptimization.DISABLE
        )

    def test_source_error_is_failed(self):
        p = self._p()
        p.source_dbops.get_catalog.side_effect = RuntimeError("CATALOG_DOES_NOT_EXIST")

        [r] = p._uc_replicate_catalog()

        assert r.status == "failed"
        assert "Catalog metadata replication operation failed" in r.error_message
        assert "CATALOG_DOES_NOT_EXIST" in r.error_message


class TestUcReplicateSchema:
    def _schema(self, **repl):
        repl.setdefault("source_catalog", "src")
        return SchemaConfig(
            schema_name="s",
            replication_config=ReplicationConfig(**repl),
            retry=RetryConfig(max_attempts=1, retry_delay_seconds=1),
        )

    def _p(self, mapping=None):
        p = _provider()
        p.cloud_url_mapping = mapping
        p.target_dbops.get_schema.side_effect = Exception("not found")
        return p

    def test_creates_schema_in_target_catalog(self):
        p = self._p()
        p.source_dbops.get_schema.return_value = SchemaInfo(
            name="s", catalog_name="src", comment="hi"
        )

        [r] = p._uc_replicate_schema(self._schema())

        assert r.status == "success"
        p.source_dbops.get_schema.assert_called_once_with("src.s")
        created = p.target_dbops.create_schema.call_args.args[0]
        assert (created["catalog_name"], created["name"], created["comment"]) == (
            "c",
            "s",
            "hi",
        )
        p.target_dbops.update_schema.assert_called_once_with(
            {
                "full_name": "c.s",
                "enable_predictive_optimization": EnablePredictiveOptimization.DISABLE,
            }
        )

    def test_external_schema_mapping_and_missing_mapping(self):
        p = self._p(mapping={"abfss://a@b/": "s3://bucket/"})
        p.source_dbops.get_schema.return_value = SchemaInfo(
            name="s", catalog_name="src", storage_root="abfss://a@b/s"
        )
        [r] = p._uc_replicate_schema(self._schema())
        assert r.status == "success"
        assert p.target_dbops.create_schema.call_args.args[0]["storage_root"] == (
            "s3://bucket/s"
        )

        p.cloud_url_mapping = None
        [r] = p._uc_replicate_schema(self._schema())
        assert r.status == "failed"
        assert "cloud_url_mapping is not configured" in r.error_message

        p.cloud_url_mapping = {"gs://x/": "s3://y/"}
        [r] = p._uc_replicate_schema(self._schema())
        assert r.status == "failed"
        assert "No external location mapping found" in r.error_message

    def test_replicate_as_managed(self):
        p = self._p()
        p.source_dbops.get_schema.return_value = SchemaInfo(
            name="s", catalog_name="src", storage_root="abfss://a@b/s"
        )

        [r] = p._uc_replicate_schema(self._schema(replicate_as_managed=True))

        assert r.status == "success"
        assert p.target_dbops.create_schema.call_args.args[0]["storage_root"] is None

    def test_existing_schema_updates_when_different(self):
        p = self._p()
        p.target_dbops.get_schema.side_effect = None
        p.source_dbops.get_schema.return_value = SchemaInfo(
            name="s", catalog_name="src", full_name="src.s", comment="x"
        )
        p.target_dbops.get_schema.return_value = SchemaInfo(
            name="s", catalog_name="c", full_name="c.s", comment="x"
        )

        [r] = p._uc_replicate_schema(
            self._schema(replicate_enable_predictive_optimization=True)
        )
        assert r.details["schema_existed"] is True
        p.target_dbops.update_schema.assert_not_called()

        [r] = p._uc_replicate_schema(self._schema())
        update = p.target_dbops.update_schema.call_args.args[0]
        assert update["full_name"] == "c.s"
        assert update["enable_predictive_optimization"] == (
            EnablePredictiveOptimization.DISABLE
        )
        p.target_dbops.create_schema.assert_not_called()

    def test_create_error_is_failed(self):
        p = self._p()
        p.source_dbops.get_schema.return_value = SchemaInfo(
            name="s", catalog_name="src"
        )
        p.target_dbops.create_schema.side_effect = RuntimeError("quota")

        [r] = p._uc_replicate_schema(self._schema())

        assert r.status == "failed"
        assert "quota" in r.error_message
        assert r.details == {"source_schema": "src.s", "target_schema": "c.s"}
