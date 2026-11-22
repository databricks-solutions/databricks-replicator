"""Version-pinned reconciliation (reconcile_at_replicated_version).

Offline: providers are built by tests/test_reconciliation_provider.py helpers and
all Spark / DatabricksOperations calls are stubbed. These tests cover which
versions the checks read, the fallbacks to live tables, and the flag default.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from data_replication.config.models import ReconciliationConfig
from tests.test_reconciliation_provider import (
    FakeSpark,
    _provider,
    _recon,
    _schema,
    _table,
)

SRC = "src_cat.sales.orders"
TGT = "tgt_cat.sales.orders"
CLONE = {"target_version": 4, "source_table": SRC, "source_version": 7}


def _pinned_table(**recon_kw):
    """Table config with the version-pinned flag on (plus any other recon options)."""
    return _table(reconcile_at_replicated_version=True, **recon_kw)


class SourceWritesSpark(FakeSpark):
    """The source got rows after the clone: a read pinned to the clone sees the clone's count."""

    def __init__(self, clone_count=3, live_count=5, target_count=3):
        super().__init__(source_count=live_count, target_count=target_count)
        self.clone_count = clone_count

    def sql(self, query):
        if "source_row_count" not in query:
            return super().sql(query)
        self.queries.append(query)
        pinned = f"FROM {SRC} VERSION AS OF" in query
        source_count = self.clone_count if pinned else self.source_count
        diff = self.target_count - source_count
        df = MagicMock()
        df.collect.return_value = [
            {
                "source_row_count": source_count,
                "target_row_count": self.target_count,
                "row_count_diff": diff,
                "comparison_result": "match" if diff == 0 else "mismatch",
            }
        ]
        return df


def _configure_clone(p, clone=CLONE, writes_after=False, time_travel_error=None):
    """Stub the target dbops as if the table was produced by a deep clone."""
    p.db_ops.get_latest_clone_commit.return_value = clone
    p.db_ops.has_data_writes_after.return_value = writes_after
    p.db_ops.get_time_travel_error.side_effect = time_travel_error or (
        lambda table, version: None
    )
    p.db_ops.get_common_fields.return_value = ["id", "name"]


class TestFilteredReferenceWithVersion:
    def test_version_applied_before_filter(self):
        p = _provider()
        recon = _recon(source_filter_expression="x = 1")
        assert p._get_filtered_table_reference("a.b.c", True, recon, version=3) == (
            "(SELECT * FROM a.b.c VERSION AS OF 3 WHERE x = 1)"
        )

    def test_version_without_filter(self):
        p = _provider()
        assert (
            p._get_filtered_table_reference("a.b.c", False, _recon(), version=0)
            == "a.b.c VERSION AS OF 0"
        )

    def test_no_version_keeps_live_reference(self):
        p = _provider()
        assert p._get_filtered_table_reference("a.b.c", True, _recon()) == "a.b.c"


class TestFlagDefault:
    def test_flag_defaults_to_off(self):
        assert (
            ReconciliationConfig(enabled=True).reconcile_at_replicated_version is False
        )

    def test_flag_can_be_enabled(self):
        cfg = ReconciliationConfig(enabled=True, reconcile_at_replicated_version=True)
        assert cfg.reconcile_at_replicated_version is True


class TestVersionPinnedReconciliation:
    def test_flag_off_reads_live_tables_and_never_consults_history(self):
        spark = FakeSpark(source_count=5, target_count=5)
        p = _provider(spark)
        _configure_clone(p)
        [res] = p._reconcile_table(_schema(), _table())
        assert res.status == "success"
        assert res.details["reconciliation_mode"] == "live"
        assert res.details["pinned_versions"] is None
        assert res.details["version_fallback_reason"] is None
        p.db_ops.get_latest_clone_commit.assert_not_called()
        assert spark.queries_with("VERSION AS OF") == []

    def test_flag_on_pins_both_sides_to_replicated_versions(self):
        spark = FakeSpark(source_count=3, target_count=3)
        p = _provider(spark)
        _configure_clone(p)
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.status == "success"
        d = res.details
        assert d["reconciliation_mode"] == "version_pinned"
        assert d["pinned_versions"] == {"source_version": 7, "target_version": 4}
        assert d["version_fallback_reason"] is None
        assert spark.queries_with(f"FROM {SRC} VERSION AS OF 7")
        assert spark.queries_with(f"FROM {TGT} VERSION AS OF 4")
        # Missing data check reads the same pinned versions.
        missing = spark.queries_with(
            "INSERT INTO recon_cat.recon_sch.recon_missing_data"
        )
        assert len(missing) == 1
        assert f"FROM {SRC} VERSION AS OF 7" in missing[0]
        assert f"FROM {TGT} VERSION AS OF 4" in missing[0]
        p.db_ops.get_common_fields.assert_called_once_with(
            SRC, TGT, source_version=7, target_version=4
        )

    def test_schema_check_stays_on_live_metadata(self):
        spark = FakeSpark()
        p = _provider(spark)
        _configure_clone(p)
        p._reconcile_table(_schema(), _pinned_table())
        schema_queries = spark.queries_with("information_schema.columns")
        assert schema_queries
        assert all("VERSION AS OF" not in q for q in schema_queries)

    def test_concurrent_source_writes_after_clone_do_not_cause_false_mismatch(self):
        # Same data for both runs: the live source has 2 rows written after the clone.
        live = _provider(SourceWritesSpark())
        _configure_clone(live)
        [live_res] = live._reconcile_table(_schema(), _table())
        assert live_res.status == "failed"
        assert live_res.details["failed_checks"] == ["row_count_check"]

        pinned = _provider(SourceWritesSpark())
        _configure_clone(pinned)
        [pinned_res] = pinned._reconcile_table(_schema(), _pinned_table())
        assert pinned_res.status == "success"
        row_count = pinned_res.details["reconciliation_results"]["row_count_check"]
        assert row_count["source_count"] == 3
        assert row_count["target_count"] == 3

    def test_source_and_target_filters_combined_with_version(self):
        spark = FakeSpark(source_count=2, target_count=2)
        p = _provider(spark)
        _configure_clone(p)
        table = _pinned_table(
            source_filter_expression="id <= 2", target_filter_expression="id <= 1"
        )
        p._reconcile_table(_schema(), table)
        assert spark.queries_with(
            f"(SELECT * FROM {SRC} VERSION AS OF 7 WHERE id <= 2)"
        )
        assert spark.queries_with(
            f"(SELECT * FROM {TGT} VERSION AS OF 4 WHERE id <= 1)"
        )

    def test_version_zero_is_pinned_not_treated_as_missing(self):
        spark = FakeSpark(source_count=3, target_count=3)
        p = _provider(spark)
        _configure_clone(p, clone={**CLONE, "target_version": 0, "source_version": 0})
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.details["pinned_versions"] == {
            "source_version": 0,
            "target_version": 0,
        }
        assert spark.queries_with(f"FROM {SRC} VERSION AS OF 0")

    def test_clone_source_match_ignores_backticks_and_case(self):
        spark = FakeSpark(source_count=3, target_count=3)
        p = _provider(spark)
        _configure_clone(p, clone={**CLONE, "source_table": "`SRC_CAT`.sales.`orders`"})
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.details["reconciliation_mode"] == "version_pinned"

    def test_pinned_mode_recorded_on_failed_checks_too(self):
        spark = FakeSpark(source_count=3, target_count=9)
        p = _provider(spark)
        _configure_clone(p)
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.status == "failed"
        assert res.details["reconciliation_mode"] == "version_pinned"
        assert res.details["pinned_versions"] == {
            "source_version": 7,
            "target_version": 4,
        }


class TestFallbacksToLive:
    def _assert_live(self, res, reason, spark):
        assert res.details["reconciliation_mode"] == "live"
        assert res.details["pinned_versions"] is None
        assert res.details["version_fallback_reason"] == reason
        assert spark.queries_with("VERSION AS OF") == []

    def test_no_clone_commit(self):
        spark = FakeSpark(source_count=3, target_count=3)
        p = _provider(spark)
        _configure_clone(p, clone=None)
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.status == "success"
        self._assert_live(res, "no CLONE commit in target history", spark)
        p.logger.info.assert_any_call(
            f"Reconciling live tables for {SRC} vs {TGT}: no CLONE commit in target history",
            extra={"run_id": "run-1", "operation": "reconciliation"},
        )

    def test_clone_without_recorded_source_version(self):
        spark = FakeSpark()
        p = _provider(spark)
        _configure_clone(p, clone={**CLONE, "source_version": None})
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.details["reconciliation_mode"] == "live"
        assert (
            "does not record its source version"
            in (res.details["version_fallback_reason"])
        )
        assert spark.queries_with("VERSION AS OF") == []

    def test_intermediate_clone_is_not_pinned(self):
        # Step 2 of an intermediate replication clones the intermediate copy; its
        # version is not the source's version.
        inter = "inter_cat.sales.orders"
        spark = FakeSpark()
        p = _provider(spark)
        _configure_clone(p, clone={**CLONE, "source_table": inter})
        [res] = p._reconcile_table(_schema(), _pinned_table())
        reason = res.details["version_fallback_reason"]
        assert f"latest CLONE read {inter}, not {SRC}" in reason
        assert "intermediate" in reason
        self._assert_live(res, reason, spark)

    def test_target_data_writes_after_clone(self):
        spark = FakeSpark(source_count=3, target_count=4)
        p = _provider(spark)
        _configure_clone(p, writes_after=True)
        [res] = p._reconcile_table(_schema(), _pinned_table())
        self._assert_live(res, "target has data writes after the CLONE commit", spark)
        p.db_ops.has_data_writes_after.assert_called_once_with(TGT, 4)

    @pytest.mark.parametrize("side", ["source", "target"])
    def test_time_travel_failure(self, side):
        bad_table = SRC if side == "source" else TGT
        version = 7 if side == "source" else 4
        spark = FakeSpark(source_count=3, target_count=3)
        p = _provider(spark)
        _configure_clone(
            p,
            time_travel_error=lambda table, v: (
                "DELTA_UNSUPPORTED_TIME_TRAVEL" if table == bad_table else None
            ),
        )
        [res] = p._reconcile_table(_schema(), _pinned_table())
        reason = f"{side} time travel to version {version} failed: DELTA_UNSUPPORTED_TIME_TRAVEL"
        self._assert_live(res, reason, spark)

    @pytest.mark.parametrize(
        "table_type", ["EXTERNAL", "STREAMING_TABLE", "MATERIALIZED_VIEW"]
    )
    def test_non_pinnable_table_types_skip_history(self, table_type):
        spark = FakeSpark()
        p = _provider(spark)
        p.db_ops.get_table_type.return_value = table_type
        _configure_clone(p)
        [res] = p._reconcile_table(_schema(), _pinned_table())
        self._assert_live(
            res, f"table type {table_type.lower()} is not version-pinned", spark
        )
        p.db_ops.get_latest_clone_commit.assert_not_called()

    def test_history_lookup_error_falls_back_and_checks_still_run(self):
        spark = FakeSpark(source_count=3, target_count=3)
        p = _provider(spark)
        _configure_clone(p)
        p.db_ops.get_latest_clone_commit.side_effect = RuntimeError("no history access")
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.status == "success"
        self._assert_live(
            res, "could not read replicated versions: no history access", spark
        )
        assert spark.queries_with("source_row_count")

    def test_unexpected_error_path_records_live_mode(self):
        spark = FakeSpark()
        p = _provider(spark)
        _configure_clone(p)
        p.db_ops.get_table_type.side_effect = RuntimeError("boom")
        [res] = p._reconcile_table(_schema(), _pinned_table())
        assert res.status == "failed"
        assert res.details["reconciliation_mode"] == "live"
