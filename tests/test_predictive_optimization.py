"""Predictive optimization (PO) check on existing target tables before deep clone.

Offline: the target workspace is mocked. The SDK call is covered by building
real ``TableInfo`` objects and handing them to a mocked WorkspaceClient.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import (
    EffectivePredictiveOptimizationFlag,
    EffectivePredictiveOptimizationFlagInheritedFromType,
    EnablePredictiveOptimization,
    TableInfo,
)
from databricks.sdk.service.catalog import TableType as SdkTableType
from pydantic import ValidationError

from data_replication.config.models import (
    PredictiveOptimizationCheck,
    ReplicationConfig,
)
from data_replication.constants import PO_ENABLED_ON_TARGET
from data_replication.databricks_operations import DatabricksOperations
from data_replication.exceptions import TableNotFoundError
from data_replication.utils import merge_models_recursive
from tests._replication_fakes import make_provider, sql_calls, table_config


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("data_replication.utils.time.sleep", lambda *_: None)


def po(
    table_type="MANAGED",
    flag="ENABLE",
    inherited_from_name=None,
    inherited_from_type=None,
):
    """Shape returned by DatabricksOperations.get_table_predictive_optimization."""
    return {
        "table_type": table_type,
        "effective_flag": flag,
        "inherited_from_name": inherited_from_name,
        "inherited_from_type": inherited_from_type,
    }


def existing_target(po_value=None, po_side_effect=None):
    """Provider whose target table exists, is a plain deep-clone candidate, and
    whose target workspace returns ``po_value`` (or raises ``po_side_effect``)."""
    p = make_provider()
    p.spark.catalog.tableExists.return_value = True
    p.db_ops.get_table_type.return_value = "MANAGED"
    p.db_ops.get_table_details.return_value = {
        "table_name": "`tgt`.`s`.`t`",
        "is_dlt": False,
        "pipeline_id": None,
        "dlt_type": None,
    }
    p.db_ops.get_table_fields.return_value = ["a", "b"]
    if po_side_effect is not None:
        p.target_dbops.get_table_predictive_optimization.side_effect = po_side_effect
    else:
        p.target_dbops.get_table_predictive_optimization.return_value = po_value
    return p


def replicate(p, **replication_kwargs):
    [r] = p._replicate_table("s", table_config(**replication_kwargs))
    return r


# ---------- config model ----------


class TestConfig:
    def test_default_is_warn(self):
        assert (
            ReplicationConfig().predictive_optimization_check
            == PredictiveOptimizationCheck.WARN
        )

    @pytest.mark.parametrize("value", ["off", "warn", "fail"])
    def test_accepts_known_values(self, value):
        cfg = ReplicationConfig(predictive_optimization_check=value)
        assert cfg.predictive_optimization_check == PredictiveOptimizationCheck(value)

    def test_rejects_unknown_value(self):
        with pytest.raises(ValidationError):
            ReplicationConfig(predictive_optimization_check="block")

    def test_parent_fail_survives_unset_child(self):
        parent = ReplicationConfig(predictive_optimization_check="fail")
        child = ReplicationConfig(enabled=True)
        merged = merge_models_recursive(parent, child)
        assert merged.predictive_optimization_check == PredictiveOptimizationCheck.FAIL

    def test_explicit_child_overrides_parent(self):
        parent = ReplicationConfig(predictive_optimization_check="fail")
        child = ReplicationConfig(predictive_optimization_check="off")
        merged = merge_models_recursive(parent, child)
        assert merged.predictive_optimization_check == PredictiveOptimizationCheck.OFF


# ---------- DatabricksOperations.get_table_predictive_optimization ----------


def _ops_for(table_info):
    wc = MagicMock(name="workspace_client")
    wc.tables.get.return_value = table_info
    return DatabricksOperations(spark=None, workspace_client=wc), wc


class TestDbOpsRead:
    def test_inherited_enable_from_schema(self):
        info = TableInfo(
            table_type=SdkTableType.MANAGED,
            effective_predictive_optimization_flag=EffectivePredictiveOptimizationFlag(
                value=EnablePredictiveOptimization.ENABLE,
                inherited_from_name="s",
                inherited_from_type=EffectivePredictiveOptimizationFlagInheritedFromType.SCHEMA,
            ),
        )
        ops, wc = _ops_for(info)
        assert ops.get_table_predictive_optimization("`cat`.`s`.`t`") == {
            "table_type": "MANAGED",
            "effective_flag": "ENABLE",
            "inherited_from_name": "s",
            "inherited_from_type": "SCHEMA",
        }
        # backticks stripped before the SDK call
        wc.tables.get.assert_called_once_with(full_name="cat.s.t")

    def test_disable_set_directly_has_no_inheritance(self):
        info = TableInfo(
            table_type=SdkTableType.MANAGED,
            effective_predictive_optimization_flag=EffectivePredictiveOptimizationFlag(
                value=EnablePredictiveOptimization.DISABLE
            ),
        )
        ops, _ = _ops_for(info)
        result = ops.get_table_predictive_optimization("cat.s.t")
        assert result["effective_flag"] == "DISABLE"
        assert result["inherited_from_name"] is None
        assert result["inherited_from_type"] is None

    def test_missing_flag_and_type_read_as_none(self):
        ops, _ = _ops_for(TableInfo())
        result = ops.get_table_predictive_optimization("cat.s.t")
        assert result == {
            "table_type": None,
            "effective_flag": None,
            "inherited_from_name": None,
            "inherited_from_type": None,
        }

    def test_requires_workspace_client(self):
        with pytest.raises(ValueError):
            DatabricksOperations(
                spark=None, workspace_client=None
            ).get_table_predictive_optimization("cat.s.t")


# ---------- provider decision + deep clone integration ----------


class TestEnableOnManagedTarget:
    def test_warn_clones_and_records_inheritance(self):
        p = existing_target(
            po(
                flag="ENABLE",
                inherited_from_name="s",
                inherited_from_type="SCHEMA",
            )
        )
        r = replicate(p)  # default mode is warn
        assert r.status == "success"
        assert sql_calls(p.spark) == [r.details["step1_query"]]
        assert r.details["predictive_optimization"] == {
            "check": "warn",
            "table_type": "MANAGED",
            "effective_flag": "ENABLE",
            "inherited_from_name": "s",
            "inherited_from_type": "SCHEMA",
            "action": "warned",
        }
        warning_text = " ".join(c.args[0] for c in p.logger.warning.call_args_list)
        assert "Predictive optimization is ENABLED" in warning_text
        assert "inherited from SCHEMA s" in warning_text

    def test_fail_skips_table_with_error_code(self):
        p = existing_target(po(flag="ENABLE"))
        r = replicate(p, predictive_optimization_check="fail")
        assert r.status == "failed"
        assert r.object_type == "table" and r.object_name == "t"
        assert r.error_message.startswith(f"{PO_ENABLED_ON_TARGET}:")
        assert "`tgt`.`s`.`t`" in r.error_message
        assert r.details["predictive_optimization"]["action"] == "blocked"
        assert r.attempt_number is None  # no clone attempted
        p.spark.sql.assert_not_called()

    def test_fail_without_inheritance_has_no_inherited_clause(self):
        p = existing_target(po(flag="ENABLE"))
        r = replicate(p, predictive_optimization_check="fail")
        assert "inherited from" not in r.error_message

    def test_off_does_not_call_target_or_clone(self):
        p = existing_target(po(flag="ENABLE"))
        r = replicate(p, predictive_optimization_check="off")
        assert r.status == "success"
        assert r.details["predictive_optimization"] is None
        p.target_dbops.get_table_predictive_optimization.assert_not_called()
        assert len(sql_calls(p.spark)) == 1

    def test_warn_never_blocks_the_clone(self):
        p = existing_target(po(flag="ENABLE"))
        r = replicate(p, predictive_optimization_check="warn")
        assert r.status == "success"
        assert len(sql_calls(p.spark)) == 1


class TestNoAction:
    @pytest.mark.parametrize("flag", ["DISABLE", "INHERIT", None])
    @pytest.mark.parametrize("mode", ["warn", "fail"])
    def test_disable_inherit_or_unknown_flag_clones(self, flag, mode):
        p = existing_target(po(flag=flag))
        r = replicate(p, predictive_optimization_check=mode)
        assert r.status == "success"
        assert r.details["predictive_optimization"]["action"] == "none"
        assert r.details["predictive_optimization"]["effective_flag"] == flag
        p.logger.warning.assert_not_called()
        assert len(sql_calls(p.spark)) == 1

    def test_debug_log_when_flag_unreadable(self):
        p = existing_target(po(flag=None))
        replicate(p, predictive_optimization_check="fail")
        assert any("no action" in c.args[0] for c in p.logger.debug.call_args_list)

    def test_target_missing_makes_no_po_call(self):
        p = make_provider()
        p.spark.catalog.tableExists.side_effect = lambda t: t == "`src`.`s`.`t`"
        p.db_ops.get_table_type.return_value = "MANAGED"
        p.db_ops.get_table_details.side_effect = [
            TableNotFoundError("missing"),
            {"is_dlt": False},
        ]
        r = replicate(p, predictive_optimization_check="fail")
        assert r.status == "success"
        assert r.details["predictive_optimization"] is None
        p.target_dbops.get_table_predictive_optimization.assert_not_called()

    @pytest.mark.parametrize(
        "table_type", ["EXTERNAL", "STREAMING_TABLE", "VIEW", None]
    )
    def test_non_managed_target_is_not_blocked(self, table_type):
        # External / shared / DLT / unknown targets: flag is ignored.
        p = existing_target(po(table_type=table_type, flag="ENABLE"))
        r = replicate(p, predictive_optimization_check="fail")
        assert r.status == "success"
        assert r.details["predictive_optimization"]["action"] == "none"
        assert len(sql_calls(p.spark)) == 1

    def test_api_error_fails_open_with_debug_log(self):
        p = existing_target(po_side_effect=RuntimeError("permission denied"))
        r = replicate(p, predictive_optimization_check="fail")
        assert r.status == "success"
        assert r.details["predictive_optimization"] == {
            "check": "fail",
            "action": "none",
            "error": "permission denied",
        }
        assert any(
            "Could not read predictive optimization" in c.args[0]
            for c in p.logger.debug.call_args_list
        )
        assert len(sql_calls(p.spark)) == 1

    def test_api_error_in_warn_mode_also_clones(self):
        p = existing_target(po_side_effect=TimeoutError("slow"))
        r = replicate(p)
        assert r.status == "success"
        assert r.details["predictive_optimization"]["error"] == "slow"


class TestPrecedence:
    def test_schema_mismatch_still_fails_before_po_check(self):
        p = existing_target(po(flag="ENABLE"))
        p.db_ops.get_table_fields.side_effect = [["a"], ["a", "b"]]
        r = replicate(p, predictive_optimization_check="fail")
        assert r.status == "failed" and "Schema mismatch" in r.error_message
        assert not r.error_message.startswith(PO_ENABLED_ON_TARGET)
        p.target_dbops.get_table_predictive_optimization.assert_not_called()

    def test_check_runs_once_per_table_with_fail_blocking(self):
        p = existing_target(po(flag="ENABLE"))
        replicate(p, predictive_optimization_check="fail")
        p.target_dbops.get_table_predictive_optimization.assert_called_once_with(
            "`tgt`.`s`.`t`"
        )
