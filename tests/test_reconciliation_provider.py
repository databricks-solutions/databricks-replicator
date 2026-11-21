"""Tests for providers/reconciliation_provider.py — schema, row count and data checks."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from data_replication.config.models import (
    DatabricksConnectConfig,
    ReconciliationConfig,
    RetryConfig,
    SchemaConfig,
    TableConfig,
)
from data_replication.providers import reconciliation_provider as rp
from data_replication.providers.base_provider import BaseProvider
from data_replication.providers.reconciliation_provider import ReconciliationProvider


@pytest.fixture(autouse=True)
def no_retry_sleep():
    with patch("data_replication.utils.time.sleep"):
        yield


def _recon(**kw):
    base = dict(
        enabled=True,
        source_catalog="src_cat",
        recon_outputs_catalog="recon_cat",
        recon_outputs_schema="recon_sch",
        share_name="the_share",
    )
    base.update(kw)
    return ReconciliationConfig(**base)


class FakeSpark:
    """Routes spark.sql() by query shape and records every query."""

    def __init__(
        self,
        schema_mismatches=0,
        source_count=10,
        target_count=10,
        missing=None,
        fail_on=None,
        missing_tables=(),
    ):
        self.queries = []
        self.schema_mismatches = schema_mismatches
        self.source_count = source_count
        self.target_count = target_count
        self.missing = missing or {}
        self.fail_on = fail_on
        self.catalog = MagicMock()
        self.catalog.tableExists.side_effect = lambda t: t not in missing_tables

    def sql(self, query):
        self.queries.append(query)
        if self.fail_on and self.fail_on in query:
            raise RuntimeError(f"query failed: {self.fail_on}")
        df = MagicMock()
        if "INSERT INTO" in query and "information_schema.columns" in query:
            df.select.return_value.collect.return_value = [(self.schema_mismatches,)]
        elif "source_row_count" in query:
            diff = self.target_count - self.source_count
            df.collect.return_value = [
                {
                    "source_row_count": self.source_count,
                    "target_row_count": self.target_count,
                    "row_count_diff": diff,
                    "comparison_result": "match" if diff == 0 else "mismatch",
                }
            ]
        elif "GROUP BY issue_type" in query:
            df.collect.return_value = [
                {"issue_type": k, "row_count": v} for k, v in self.missing.items()
            ]
        return df

    def queries_with(self, text):
        return [q for q in self.queries if text in q]


def _provider(spark=None, recon=None, sharing_identifier="azure:r:src"):
    p = ReconciliationProvider.__new__(ReconciliationProvider)
    p.spark = spark or FakeSpark()
    p.logger = MagicMock()
    p.db_ops = MagicMock()
    p.db_ops.get_table_type.return_value = "MANAGED"
    p.db_ops.get_common_fields.return_value = ["id", "name"]
    p.run_id = "run-1"
    p.audit_logger = MagicMock()
    p.source_dbops = MagicMock()
    p.catalog_config = SimpleNamespace(
        catalog_name="tgt_cat", reconciliation_config=recon or _recon()
    )
    p.source_databricks_config = DatabricksConnectConfig(
        name="src",
        host="https://src.example.com",
        sharing_identifier=sharing_identifier,
    )
    p.target_databricks_config = DatabricksConnectConfig(
        name="tgt", host="https://tgt.example.com"
    )
    return p


def _schema():
    return SchemaConfig(schema_name="sales")


def _table(name="orders", max_attempts=1, **recon_kw):
    return TableConfig(
        table_name=name,
        reconciliation_config=_recon(**recon_kw),
        retry=RetryConfig(max_attempts=max_attempts, retry_delay_seconds=1),
    )


# ---------- __init__ / simple helpers ----------


class TestInit:
    def _init(self, recon, sharing_identifier=None):
        def fake_base_init(self, *a, **kw):
            self.spark = MagicMock()
            self.logger = MagicMock()
            self.workspace_client = MagicMock()
            self.catalog_config = SimpleNamespace(
                catalog_name="c", reconciliation_config=recon
            )
            self.source_databricks_config = DatabricksConnectConfig(
                name="s",
                host="https://src.example.com",
                sharing_identifier=sharing_identifier,
            )
            self.target_databricks_config = DatabricksConnectConfig(
                name="t", host="https://tgt.example.com"
            )

        with (
            patch.object(BaseProvider, "__init__", fake_base_init),
            patch.object(rp, "create_workspace_client") as ws,
            patch.object(rp, "create_spark_session") as spark,
            patch.object(rp, "validate_spark_session") as validate,
            patch.object(rp, "DatabricksOperations") as dbops,
        ):
            provider = ReconciliationProvider()
        return provider, ws, spark, validate, dbops

    def test_target_dbops_built_on_current_spark(self):
        provider, ws, spark, _, dbops = self._init(_recon())
        assert ws.call_args.kwargs["host"] == "https://tgt.example.com"
        assert provider.target_spark is provider.spark
        assert provider.db_ops is provider.target_dbops is dbops.return_value
        spark.assert_not_called()
        assert provider.source_dbops is None

    def test_source_session_created_for_shared_catalog_without_identifier(self):
        provider, _, spark, validate, _ = self._init(_recon(create_shared_catalog=True))
        assert spark.call_args.kwargs["host"] == "https://src.example.com"
        validate.assert_called_once_with(spark.return_value, "src.example.com")
        assert provider.source_spark is spark.return_value

    def test_source_session_skipped_when_identifier_known(self):
        _, _, spark, _, _ = self._init(
            _recon(create_shared_catalog=True), sharing_identifier="azure:x:y"
        )
        spark.assert_not_called()


def test_operation_name_and_enabled():
    p = _provider()
    assert p.get_operation_name() == "reconciliation"
    assert p.is_operation_enabled()
    p.catalog_config.reconciliation_config = _recon(enabled=False)
    assert not p.is_operation_enabled()


def test_filtered_table_reference():
    p = _provider(recon=_recon(source_filter_expression="dt > '2026'"))
    assert p._get_filtered_table_reference("a.b.c", True) == (
        "(SELECT * FROM a.b.c WHERE dt > '2026')"
    )
    assert p._get_filtered_table_reference("a.b.c", False) == "a.b.c"
    p.catalog_config.reconciliation_config = _recon(target_filter_expression="x = 1")
    assert (
        p._get_filtered_table_reference("t", False) == "(SELECT * FROM t WHERE x = 1)"
    )


# ---------- setup_operation_catalogs ----------


class TestSetup:
    def test_creates_catalog_schema_and_output_tables(self):
        p = _provider(
            recon=_recon(create_recon_catalog=True, recon_catalog_location="s3://r")
        )
        assert p.setup_operation_catalogs() == "src_cat"
        p.db_ops.create_catalog_if_not_exists.assert_called_once_with(
            "recon_cat", "s3://r"
        )
        p.db_ops.create_schema_if_not_exists.assert_called_once_with(
            "recon_cat", "recon_sch"
        )
        ddl = [c.args[0] for c in p.db_ops.execute_query.call_args_list]
        assert "recon_cat.recon_sch.recon_schema_check" in ddl[0]
        assert "comparison_result STRING" in ddl[0]
        assert "recon_cat.recon_sch.recon_missing_data" in ddl[1]
        assert "data MAP<STRING, STRING>" in ddl[1]
        p.db_ops.create_catalog_using_share_if_not_exists.assert_not_called()

    def test_no_output_tables_when_disabled(self):
        p = _provider(
            recon=_recon(recon_schema_check_table=None, recon_missing_data_table=None)
        )
        p.setup_operation_catalogs()
        p.db_ops.execute_query.assert_not_called()
        p.db_ops.create_catalog_if_not_exists.assert_not_called()

    def test_shared_catalog_from_configured_identifier(self):
        p = _provider(recon=_recon(create_shared_catalog=True))
        p.db_ops.get_provider_name.return_value = "prov"
        p.setup_operation_catalogs()
        p.db_ops.get_provider_name.assert_called_once_with("azure:r:src")
        p.db_ops.create_catalog_using_share_if_not_exists.assert_called_once_with(
            "src_cat", "prov", "the_share"
        )

    def test_shared_catalog_looks_up_source_metastore(self):
        p = _provider(recon=_recon(create_shared_catalog=True), sharing_identifier=None)
        p.source_dbops.get_metastore_id.return_value = "azure:r:looked-up"
        p.setup_operation_catalogs()
        p.db_ops.get_provider_name.assert_called_once_with("azure:r:looked-up")


# ---------- _reconcile_table ----------


class TestReconcileTable:
    def test_all_checks_pass(self):
        spark = FakeSpark(schema_mismatches=0, source_count=5, target_count=5)
        p = _provider(spark)
        [res] = p._reconcile_table(_schema(), _table())
        assert res.status == "success"
        d = res.details
        assert d["source_table"] == "src_cat.sales.orders"
        assert d["target_table"] == "tgt_cat.sales.orders"
        assert d["table_type"] == "managed"
        assert d["failed_checks"] == [] and d["skipped_checks"] == []
        r = d["reconciliation_results"]
        assert r["schema_check"]["passed"] is True
        assert r["row_count_check"]["source_count"] == 5
        assert r["missing_data_check"]["total_missing_rows"] == 0
        assert set(d["check_durations"]) == {
            "schema_check_duration_seconds",
            "row_count_check_duration_seconds",
            "missing_data_check_duration_seconds",
        }

    def test_schema_mismatch_skips_remaining_checks(self):
        spark = FakeSpark(schema_mismatches=3)
        p = _provider(spark)
        [res] = p._reconcile_table(_schema(), _table())
        assert res.status == "failed"
        assert res.error_message == "Reconciliation failed checks: ['schema_check']"
        d = res.details
        assert d["skipped_checks"] == ["row_count_check", "missing_data_check"]
        assert d["reconciliation_results"]["schema_check"]["mismatch_count"] == 3
        assert d["reconciliation_results"]["row_count_check"]["status"] == "skipped"
        assert not spark.queries_with("source_row_count")
        assert not spark.queries_with(
            "INSERT INTO recon_cat.recon_sch.recon_missing_data"
        )

    def test_row_count_mismatch_skips_missing_data_check(self):
        spark = FakeSpark(source_count=10, target_count=8)
        p = _provider(spark)
        [res] = p._reconcile_table(_schema(), _table())
        assert res.status == "failed"
        rc = res.details["reconciliation_results"]["row_count_check"]
        assert rc == {
            "passed": False,
            "source_count": 10,
            "target_count": 8,
            "difference": -2,
            "source_filter": None,
            "target_filter": None,
            "details": "Row count check completed successfully",
            "attempt": 1,
            "max_attempts": 1,
        }
        assert res.details["skipped_checks"] == ["missing_data_check"]

    def test_missing_data_reported_with_breakdown(self):
        spark = FakeSpark(missing={"missing_in_target": 2, "missing_in_source": 1})
        p = _provider(spark)
        [res] = p._reconcile_table(_schema(), _table())
        assert res.status == "failed"
        assert res.details["failed_checks"] == ["missing_data_check"]
        md = res.details["reconciliation_results"]["missing_data_check"]
        assert md["total_missing_rows"] == 3
        assert md["missing_breakdown"] == {
            "missing_in_target": 2,
            "missing_in_source": 1,
        }
        assert md["output_table"] == "recon_cat.recon_sch.recon_missing_data"

    def test_only_enabled_checks_run(self):
        spark = FakeSpark()
        p = _provider(spark)
        [res] = p._reconcile_table(
            _schema(), _table(schema_check=False, missing_data_check=False)
        )
        assert res.status == "success"
        assert list(res.details["reconciliation_results"]) == ["row_count_check"]
        assert len(spark.queries) == 1

    def test_missing_source_table(self):
        p = _provider(FakeSpark(missing_tables={"src_cat.sales.orders"}))
        [res] = p._reconcile_table(_schema(), _table())
        assert res.status == "failed"
        assert "Source table does not exist: src_cat.sales.orders" in res.error_message
        assert res.details["table_type"] is None

    def test_missing_target_table(self):
        p = _provider(FakeSpark(missing_tables={"tgt_cat.sales.orders"}))
        [res] = p._reconcile_table(_schema(), _table())
        assert res.status == "failed"
        assert "Target table does not exist" in res.error_message
        assert res.details["table_type"] == "managed"

    def test_unexpected_error_wrapped(self):
        p = _provider()
        p.db_ops.get_table_type.side_effect = RuntimeError("no access")
        [res] = p._reconcile_table(_schema(), _table())
        assert "Reconciliation operation failed: no access" in res.error_message

    def test_process_table_logs_results(self):
        p = _provider(FakeSpark())
        results = p.process_table(_schema(), _table())
        assert results[0].status == "success"
        p.audit_logger.log_results.assert_called_once_with(results)

    def test_process_table_skips_logging_when_no_results(self):
        p = _provider()
        with patch.object(p, "_reconcile_table", return_value=[]):
            assert p.process_table(_schema(), _table()) == []
        p.audit_logger.log_results.assert_not_called()

    def test_table_level_filter_with_substitution_is_applied(self):
        spark = FakeSpark()
        p = _provider(spark)
        table = _table(source_filter_expression="name = '{{table_name}}'")
        p.process_table(_schema(), table)
        assert spark.queries_with("WHERE name = 'orders'")


# ---------- individual checks ----------


def _op(spark, max_attempts=2):
    from data_replication.utils import retry_with_logging

    @retry_with_logging(RetryConfig(max_attempts=max_attempts, retry_delay_seconds=1))
    def op(query):
        return spark.sql(query)

    return op


class TestSchemaCheck:
    def test_query_compares_information_schema_and_excludes_columns(self):
        spark = FakeSpark(schema_mismatches=0)
        p = _provider(spark)
        out = p._perform_schema_check(
            "src_cat.sales.orders",
            "tgt_cat.sales.orders",
            _op(spark),
            _recon(exclude_columns=["_etl_ts", "_rescued"]),
        )
        assert out["passed"] is True
        assert out["output_table"] == "recon_cat.recon_sch.recon_schema_check"
        q = spark.queries[0]
        assert "INSERT INTO recon_cat.recon_sch.recon_schema_check" in q
        assert "FROM src_cat.information_schema.columns" in q
        assert "FROM tgt_cat.information_schema.columns" in q
        assert "column_name NOT IN ('_etl_ts', '_rescued')" in q
        assert "'run-1' as run_id" in q
        assert "WHERE comparison_result != 'match'" in q

    def test_query_failure_after_retries(self):
        spark = FakeSpark(fail_on="information_schema")
        p = _provider(spark)
        out = p._perform_schema_check(
            "a.b.c", "d.e.f", _op(spark, max_attempts=2), _recon()
        )
        assert out["passed"] is False
        assert "Schema check insert failed" in out["error"]
        assert out["attempt"] == 2
        assert len(spark.queries) == 2

    def test_result_parse_error(self):
        spark = MagicMock()
        spark.sql.return_value.select.side_effect = RuntimeError("bad df")
        p = _provider()
        out = p._perform_schema_check("a.b.c", "d.e.f", _op(spark), _recon())
        assert out["passed"] is False
        assert out["error"] == "Schema check failed: bad df"


class TestRowCountCheck:
    def test_filters_applied_to_count_queries(self):
        spark = FakeSpark(source_count=3, target_count=3)
        recon = _recon(source_filter_expression="dt = 1", target_filter_expression="x")
        p = _provider(spark, recon=recon)
        out = p._perform_row_count_check("s.s.t", "t.s.t", _op(spark), recon)
        assert out["passed"] is True
        assert out["source_filter"] == "dt = 1"
        q = spark.queries[0]
        assert "FROM (SELECT * FROM s.s.t WHERE dt = 1)" in q
        assert "FROM (SELECT * FROM t.s.t WHERE x)" in q

    def test_more_rows_in_target_fails(self):
        spark = FakeSpark(source_count=1, target_count=4)
        p = _provider(spark)
        out = p._perform_row_count_check("s.s.t", "t.s.t", _op(spark), _recon())
        assert out["passed"] is False
        assert out["difference"] == 3

    def test_query_failure(self):
        spark = FakeSpark(fail_on="source_row_count")
        p = _provider(spark)
        out = p._perform_row_count_check(
            "s.s.t", "t.s.t", _op(spark, max_attempts=1), _recon()
        )
        assert out["passed"] is False
        assert "Row count check query failed" in out["error"]

    def test_result_parse_error(self):
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []
        p = _provider()
        out = p._perform_row_count_check("s.s.t", "t.s.t", _op(spark), _recon())
        assert out["passed"] is False
        assert out["error"].startswith("Row count check failed")


class TestMissingDataCheck:
    def test_hash_query_over_common_fields_minus_exclusions(self):
        spark = FakeSpark()
        p = _provider(spark)
        p.db_ops.get_common_fields.return_value = ["id", "name", "_etl_ts"]
        out = p._perform_missing_data_check(
            "src_cat.sales.orders",
            "tgt_cat.sales.orders",
            _op(spark),
            _recon(exclude_columns=["_etl_ts"]),
        )
        assert out["passed"] is True
        assert out["common_fields_count"] == 2
        insert = spark.queries_with(
            "INSERT INTO recon_cat.recon_sch.recon_missing_data"
        )
        assert len(insert) == 1
        q = insert[0]
        assert "hash(`id`,`name`)" in q
        assert "map('id', CAST(`id` AS STRING), 'name', CAST(`name` AS STRING))" in q
        assert "_etl_ts" not in q
        assert "'missing_in_target' as issue_type" in q
        assert "'missing_in_source' as issue_type" in q
        summary = spark.queries_with("GROUP BY issue_type")[0]
        assert "run_id = 'run-1'" in summary
        assert "target_table = 'orders'" in summary

    def test_no_common_fields_after_exclusions(self):
        spark = FakeSpark()
        p = _provider(spark)
        p.db_ops.get_common_fields.return_value = ["_etl_ts"]
        out = p._perform_missing_data_check(
            "a.b.c", "d.e.f", _op(spark), _recon(exclude_columns=["_etl_ts"])
        )
        assert out["passed"] is False
        assert "No common fields" in out["error"]
        assert spark.queries == []

    def test_insert_failure(self):
        spark = FakeSpark(fail_on="missing_in_target")
        p = _provider(spark)
        out = p._perform_missing_data_check(
            "a.b.c", "d.e.f", _op(spark, max_attempts=1), _recon()
        )
        assert out["passed"] is False
        assert "Missing data check insert failed" in out["error"]

    def test_summary_query_failure(self):
        spark = FakeSpark(fail_on="GROUP BY issue_type")
        p = _provider(spark)
        out = p._perform_missing_data_check("a.b.c", "d.e.f", _op(spark), _recon())
        assert out["passed"] is False
        assert "Missing data check failed" in out["error"]
        assert out["attempt"] == 1

    @pytest.mark.xfail(
        reason="bug: if get_common_fields raises, the except block reads `attempt` "
        "before assignment, so an UnboundLocalError hides the real error",
        strict=True,
    )
    def test_common_fields_error_is_reported(self):
        spark = FakeSpark()
        p = _provider(spark)
        p.db_ops.get_common_fields.side_effect = RuntimeError("describe denied")
        out = p._perform_missing_data_check("a.b.c", "d.e.f", _op(spark), _recon())
        assert out["passed"] is False
        assert "describe denied" in out["error"]
