"""Tests for tag policy replication, following the external-location pattern."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.tags import TagPolicy, Value

from data_replication.config.models import (
    TagPolicyConfig,
    UCObjectType,
)
from data_replication.constants import (
    DICT_FOR_CREATION_TAG_POLICY,
    DICT_FOR_UPDATE_TAG_POLICY,
)
from data_replication.databricks_operations import DatabricksOperations
from data_replication.providers.provider_factory import ProviderFactory

# ---------- Constants contract ----------


def test_creation_and_update_dict_keys():
    assert set(DICT_FOR_CREATION_TAG_POLICY) == {"tag_key", "description", "values"}
    assert set(DICT_FOR_UPDATE_TAG_POLICY) == {"tag_key", "description", "values"}


# ---------- TagPolicyConfig ----------


class TestTagPolicyConfig:
    def test_defaults(self):
        cfg = TagPolicyConfig()
        assert cfg.tag_policies is None

    def test_list_of_keys(self):
        cfg = TagPolicyConfig(tag_policies=["cost_center", "environment"])
        assert cfg.tag_policies == ["cost_center", "environment"]

    def test_extra_forbidden(self):
        with pytest.raises(Exception):
            TagPolicyConfig(bogus="no")


# ---------- UCObjectType enum ----------


def test_uc_object_type_includes_tag_policy():
    assert UCObjectType.TAG_POLICY.value == "tag_policy"


# ---------- DatabricksOperations tag policy wrappers ----------


@pytest.fixture
def wc_ops():
    wc = MagicMock()
    return DatabricksOperations(spark=None, workspace_client=wc), wc


class TestTagPolicyWrappers:
    @pytest.mark.parametrize(
        "method,args",
        [
            ("list_tag_policies", ()),
            ("get_tag_policy", ("k",)),
            ("create_tag_policy", ({"tag_key": "k"},)),
            ("update_tag_policy", ({"tag_key": "k"},)),
        ],
    )
    def test_requires_workspace_client(self, method, args):
        ops = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            getattr(ops, method)(*args)

    def test_list_delegates_and_returns_list(self, wc_ops):
        ops, wc = wc_ops
        wc.tag_policies.list_tag_policies.return_value = iter(["a", "b"])
        assert ops.list_tag_policies() == ["a", "b"]

    def test_get_delegates(self, wc_ops):
        ops, wc = wc_ops
        tp = TagPolicy(tag_key="k")
        wc.tag_policies.get_tag_policy.return_value = tp
        assert ops.get_tag_policy("k") is tp
        wc.tag_policies.get_tag_policy.assert_called_once_with("k")

    def test_create_builds_tag_policy_object(self, wc_ops):
        ops, wc = wc_ops
        wc.tag_policies.create_tag_policy.return_value = TagPolicy(tag_key="k")
        ops.create_tag_policy(
            {
                "tag_key": "k",
                "description": "d",
                "values": [{"name": "low"}, Value(name="high")],
            }
        )
        call_kwargs = wc.tag_policies.create_tag_policy.call_args.kwargs
        tp = call_kwargs["tag_policy"]
        assert isinstance(tp, TagPolicy)
        assert tp.tag_key == "k"
        assert tp.description == "d"
        assert all(isinstance(v, Value) for v in tp.values)
        assert [v.name for v in tp.values] == ["low", "high"]

    def test_create_handles_none_values(self, wc_ops):
        ops, wc = wc_ops
        wc.tag_policies.create_tag_policy.return_value = TagPolicy(tag_key="k")
        ops.create_tag_policy({"tag_key": "k", "description": None, "values": None})
        tp = wc.tag_policies.create_tag_policy.call_args.kwargs["tag_policy"]
        assert tp.values is None

    def test_update_passes_mask(self, wc_ops):
        ops, wc = wc_ops
        wc.tag_policies.update_tag_policy.return_value = TagPolicy(tag_key="k")
        ops.update_tag_policy({"tag_key": "k", "description": "new"})
        call_kwargs = wc.tag_policies.update_tag_policy.call_args.kwargs
        assert call_kwargs["tag_key"] == "k"
        assert call_kwargs["update_mask"] == "description,values"

    def test_update_allows_custom_mask(self, wc_ops):
        ops, wc = wc_ops
        wc.tag_policies.update_tag_policy.return_value = TagPolicy(tag_key="k")
        ops.update_tag_policy(
            {"tag_key": "k", "description": "new"}, update_mask="description"
        )
        assert (
            wc.tag_policies.update_tag_policy.call_args.kwargs["update_mask"]
            == "description"
        )

    def test_create_wraps_errors(self, wc_ops):
        ops, wc = wc_ops
        wc.tag_policies.create_tag_policy.side_effect = RuntimeError("x")
        with pytest.raises(Exception, match="Failed to create tag policy"):
            ops.create_tag_policy({"tag_key": "k"})


# ---------- provider_factory._replicate_tag_policies ----------


def _make_source_policy(tag_key="cost_center", description="Cost", values=None):
    tp = TagPolicy(
        tag_key=tag_key,
        description=description,
        values=values or [Value(name="engineering"), Value(name="ops")],
    )
    return tp


@pytest.fixture
def factory_instance():
    def _build(*, tag_policies_cfg=None, source_list=None):
        source_ops = MagicMock()
        target_ops = MagicMock()

        cfg = MagicMock()
        cfg.tag_policy_config = tag_policies_cfg
        if source_list is not None:
            source_ops.list_tag_policies.return_value = source_list

        instance = ProviderFactory.__new__(ProviderFactory)
        instance.config = cfg
        instance.source_db_ops = source_ops
        instance.target_db_ops = target_ops
        instance.logger = logging.getLogger("test.tag_policy")
        instance.audit_logger = MagicMock()
        instance.run_id = "test-run"
        instance.completed_run_results = []
        return instance, source_ops, target_ops

    return _build


class TestReplicateTagPolicies:
    def test_creates_when_missing_on_target(self, factory_instance):
        inst, src, tgt = factory_instance(
            tag_policies_cfg=TagPolicyConfig(tag_policies=["cost_center"]),
        )
        src.get_tag_policy.return_value = _make_source_policy()
        tgt.get_tag_policy.side_effect = Exception("not found")
        tgt.create_tag_policy.return_value = _make_source_policy()

        results = inst._replicate_tag_policies()

        assert len(results) == 1
        assert results[0].status == "success"
        assert results[0].object_type == "tag_policy"
        assert results[0].object_name == "cost_center"
        assert results[0].details["policy_existed"] is False

        tgt.create_tag_policy.assert_called_once()
        create_dict = tgt.create_tag_policy.call_args.args[0]
        assert create_dict["tag_key"] == "cost_center"
        assert create_dict["description"] == "Cost"
        assert create_dict["values"] is not None
        tgt.update_tag_policy.assert_not_called()

    def test_updates_when_differs_on_target(self, factory_instance):
        source_tp = _make_source_policy(description="updated")
        target_tp = _make_source_policy(description="old")

        inst, src, tgt = factory_instance(
            tag_policies_cfg=TagPolicyConfig(tag_policies=["cost_center"]),
        )
        src.get_tag_policy.return_value = source_tp
        tgt.get_tag_policy.return_value = target_tp

        results = inst._replicate_tag_policies()

        assert results[0].status == "success"
        assert results[0].details["policy_existed"] is True
        tgt.update_tag_policy.assert_called_once()
        tgt.create_tag_policy.assert_not_called()

    def test_skips_update_when_already_matches(self, factory_instance):
        source_tp = _make_source_policy()
        # Build a target with identical fields
        target_tp = _make_source_policy()

        inst, src, tgt = factory_instance(
            tag_policies_cfg=TagPolicyConfig(tag_policies=["cost_center"]),
        )
        src.get_tag_policy.return_value = source_tp
        tgt.get_tag_policy.return_value = target_tp

        results = inst._replicate_tag_policies()
        assert results[0].status == "success"
        tgt.update_tag_policy.assert_not_called()
        tgt.create_tag_policy.assert_not_called()

    def test_discovers_from_source_when_config_omits_list(self, factory_instance):
        source_tp1 = _make_source_policy(tag_key="a")
        source_tp2 = _make_source_policy(tag_key="b")
        inst, src, tgt = factory_instance(
            tag_policies_cfg=None,
            source_list=[source_tp1, source_tp2],
        )
        # Return each policy when asked
        src.get_tag_policy.side_effect = lambda k: (
            source_tp1 if k == "a" else source_tp2
        )
        tgt.get_tag_policy.side_effect = Exception("not found")
        tgt.create_tag_policy.return_value = source_tp1

        results = inst._replicate_tag_policies()
        assert [r.object_name for r in results] == ["a", "b"]
        assert all(r.status == "success" for r in results)

    def test_records_failure_per_policy(self, factory_instance):
        inst, src, tgt = factory_instance(
            tag_policies_cfg=TagPolicyConfig(tag_policies=["cost_center"]),
        )
        src.get_tag_policy.side_effect = RuntimeError("source boom")

        results = inst._replicate_tag_policies()
        assert results[0].status == "failed"
        assert "source boom" in results[0].error_message
