"""Tests for grant replication helpers and wrappers."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import (
    GetPermissionsResponse,
    PermissionsChange,
    Privilege,
    PrivilegeAssignment,
)

from data_replication.config.models import RetryConfig, UCObjectType
from data_replication.databricks_operations import DatabricksOperations
from data_replication.providers.base_provider import BaseProvider

# ---------- Enum ----------


def test_grant_enum_values():
    values = {v.value for v in UCObjectType}
    assert {"catalog_grant", "schema_grant", "table_grant", "volume_grant"} <= values


# ---------- DBOps: get_grants / update_grants ----------


@pytest.fixture
def wc_ops():
    wc = MagicMock()
    return DatabricksOperations(spark=None, workspace_client=wc), wc


class TestGrantWrappers:
    def test_requires_workspace_client_get(self):
        ops = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            ops.get_grants("CATALOG", "c")

    def test_requires_workspace_client_update(self):
        ops = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            ops.update_grants("CATALOG", "c", [])

    def test_get_returns_assignments(self, wc_ops):
        ops, wc = wc_ops
        pa = PrivilegeAssignment(principal="alice", privileges=[Privilege.SELECT])
        wc.grants.get.return_value = GetPermissionsResponse(privilege_assignments=[pa])
        out = ops.get_grants("TABLE", "c.s.t")
        assert out == [pa]
        wc.grants.get.assert_called_once_with(
            securable_type="TABLE", full_name="c.s.t", page_token=None
        )

    def test_get_walks_pagination(self, wc_ops):
        ops, wc = wc_ops
        pa1 = PrivilegeAssignment(principal="alice", privileges=[Privilege.SELECT])
        pa2 = PrivilegeAssignment(principal="bob", privileges=[Privilege.MODIFY])
        pa3 = PrivilegeAssignment(principal="carol", privileges=[Privilege.APPLY_TAG])
        wc.grants.get.side_effect = [
            GetPermissionsResponse(
                privilege_assignments=[pa1], next_page_token="tok-1"
            ),
            GetPermissionsResponse(
                privilege_assignments=[pa2], next_page_token="tok-2"
            ),
            GetPermissionsResponse(privilege_assignments=[pa3], next_page_token=None),
        ]
        out = ops.get_grants("CATALOG", "c")
        assert out == [pa1, pa2, pa3]
        assert wc.grants.get.call_count == 3
        tokens = [call.kwargs["page_token"] for call in wc.grants.get.call_args_list]
        assert tokens == [None, "tok-1", "tok-2"]

    def test_get_empty_when_no_assignments(self, wc_ops):
        ops, wc = wc_ops
        wc.grants.get.return_value = GetPermissionsResponse(privilege_assignments=None)
        assert ops.get_grants("CATALOG", "c") == []

    def test_get_wraps_errors(self, wc_ops):
        ops, wc = wc_ops
        wc.grants.get.side_effect = RuntimeError("boom")
        with pytest.raises(Exception, match="Failed to get grants"):
            ops.get_grants("CATALOG", "c")

    def test_update_delegates(self, wc_ops):
        ops, wc = wc_ops
        changes = [PermissionsChange(principal="alice", add=[Privilege.SELECT])]
        ops.update_grants("TABLE", "c.s.t", changes)
        wc.grants.update.assert_called_once_with(
            securable_type="TABLE", full_name="c.s.t", changes=changes
        )

    def test_update_wraps_errors(self, wc_ops):
        ops, wc = wc_ops
        wc.grants.update.side_effect = RuntimeError("boom")
        with pytest.raises(Exception, match="Failed to update grants"):
            ops.update_grants("CATALOG", "c", [])


# ---------- _diff_privileges_per_principal (pure logic) ----------


class _Instance:
    """Bare BaseProvider-like instance exposing just the helpers under test."""

    def __init__(
        self,
        principal_mapping=None,
        source_sp_uuid_to_name=None,
        target_sp_uuid_to_name=None,
    ):
        self.principal_mapping = principal_mapping
        src_map = source_sp_uuid_to_name or {}
        tgt_uuid_to_name = target_sp_uuid_to_name or {}
        # reverse target map: display_name -> first-seen application_id
        tgt_name_to_uuid: dict = {}
        for uuid, name in tgt_uuid_to_name.items():
            tgt_name_to_uuid.setdefault(name, uuid)

        self.source_dbops = MagicMock()
        self.source_dbops.get_sp_display_name_by_app_id.side_effect = lambda app_id: (
            src_map.get(app_id)
        )
        self.target_dbops = MagicMock()
        self.target_dbops.get_sp_app_id_by_display_name.side_effect = lambda name: (
            tgt_name_to_uuid.get(name)
        )
        self.logger = logging.getLogger("test.grants._Instance")
        self._sp_src_app_id_to_name = {}
        self._sp_tgt_name_to_app_id = {}

    _map_principal = BaseProvider._map_principal
    _resolve_source_sp_name = BaseProvider._resolve_source_sp_name
    _resolve_target_sp_app_id = BaseProvider._resolve_target_sp_app_id
    _diff_privileges_per_principal = BaseProvider._diff_privileges_per_principal
    _looks_like_sp_uuid = staticmethod(BaseProvider._looks_like_sp_uuid)


def _diff(instance, source, target):
    changes, _skipped = instance._diff_privileges_per_principal(source, target)
    return changes


def _pa(principal, *privs):
    return PrivilegeAssignment(principal=principal, privileges=list(privs))


class TestDiffPrivileges:
    def test_empty_inputs_yields_no_changes(self):
        assert _diff(_Instance(), [], []) == []

    def test_add_only_when_principal_missing_on_target(self):
        src = [_pa("alice", Privilege.SELECT, Privilege.MODIFY)]
        out = _diff(_Instance(), src, [])
        assert len(out) == 1
        change = out[0]
        assert change.principal == "alice"
        assert set(change.add) == {Privilege.MODIFY, Privilege.SELECT}
        assert change.remove is None

    def test_add_and_remove_diff_within_principal(self):
        src = [_pa("alice", Privilege.SELECT, Privilege.MODIFY)]
        tgt = [_pa("alice", Privilege.SELECT, Privilege.APPLY_TAG)]
        out = _diff(_Instance(), src, tgt)
        assert len(out) == 1
        c = out[0]
        assert c.principal == "alice"
        assert c.add == [Privilege.MODIFY]
        assert c.remove == [Privilege.APPLY_TAG]

    def test_noop_when_identical(self):
        src = [_pa("alice", Privilege.SELECT)]
        tgt = [_pa("alice", Privilege.SELECT)]
        assert _diff(_Instance(), src, tgt) == []

    def test_target_only_principals_left_alone(self):
        src = [_pa("alice", Privilege.SELECT)]
        tgt = [
            _pa("alice", Privilege.SELECT),
            _pa("bob", Privilege.MODIFY),  # target-only
        ]
        # alice matches, bob is ignored → no changes
        assert _diff(_Instance(), src, tgt) == []

    def test_principal_mapping_applied(self):
        src = [_pa("src-alice", Privilege.SELECT)]
        tgt = [_pa("tgt-alice", Privilege.MODIFY)]
        inst = _Instance(principal_mapping={"src-alice": "tgt-alice"})
        out = _diff(inst, src, tgt)
        assert len(out) == 1
        assert out[0].principal == "tgt-alice"
        assert out[0].add == [Privilege.SELECT]
        assert out[0].remove == [Privilege.MODIFY]

    def test_unmapped_principal_passes_through(self):
        src = [_pa("carol@x", Privilege.SELECT)]
        inst = _Instance(principal_mapping={"other": "other-tgt"})
        out = _diff(inst, src, [])
        assert out[0].principal == "carol@x"

    def test_overwrite_grants_removes_target_only_principals(self):
        src = [_pa("alice", Privilege.SELECT)]
        tgt = [
            _pa("alice", Privilege.SELECT),
            _pa("bob", Privilege.MODIFY, Privilege.APPLY_TAG),
        ]
        out, _skipped = _Instance()._diff_privileges_per_principal(
            src, tgt, overwrite_grants=True
        )
        # Alice is in sync, so only bob appears as a remove-all change
        by_principal = {c.principal: c for c in out}
        assert set(by_principal) == {"bob"}
        bob = by_principal["bob"]
        assert bob.add is None
        assert set(bob.remove) == {Privilege.MODIFY, Privilege.APPLY_TAG}

    def test_overwrite_grants_default_false_preserves_target_only(self):
        src = [_pa("alice", Privilege.SELECT)]
        tgt = [_pa("alice", Privilege.SELECT), _pa("bob", Privilege.MODIFY)]
        out, _skipped = _Instance()._diff_privileges_per_principal(src, tgt)
        assert out == []  # bob untouched


class TestSpNameMapping:
    def test_sp_name_mapping_translates_uuid_via_display_name(self):
        inst = _Instance(
            principal_mapping={"etl-prod-sp": "etl-stg-sp"},
            source_sp_uuid_to_name={
                "11111111-1111-1111-1111-111111111111": "etl-prod-sp"
            },
            target_sp_uuid_to_name={
                "22222222-2222-2222-2222-222222222222": "etl-stg-sp"
            },
        )
        assert (
            inst._map_principal("11111111-1111-1111-1111-111111111111")
            == "22222222-2222-2222-2222-222222222222"
        )

    def test_explicit_uuid_to_uuid_still_works(self):
        inst = _Instance(
            principal_mapping={
                "11111111-1111-1111-1111-111111111111": "22222222-2222-2222-2222-222222222222"
            },
        )
        assert (
            inst._map_principal("11111111-1111-1111-1111-111111111111")
            == "22222222-2222-2222-2222-222222222222"
        )

    def test_name_mapping_resolves_display_name_in_value_to_target_uuid(self):
        # User typed UUID as key, but display-name as value; we should still
        # resolve the value to the target UUID via the target cache.
        inst = _Instance(
            principal_mapping={"11111111-1111-1111-1111-111111111111": "etl-stg-sp"},
            target_sp_uuid_to_name={
                "22222222-2222-2222-2222-222222222222": "etl-stg-sp"
            },
        )
        assert (
            inst._map_principal("11111111-1111-1111-1111-111111111111")
            == "22222222-2222-2222-2222-222222222222"
        )

    def test_unknown_sp_uuid_falls_through_verbatim(self):
        inst = _Instance(
            principal_mapping={"other": "other-tgt"},
            source_sp_uuid_to_name={
                "11111111-1111-1111-1111-111111111111": "etl-prod-sp"
            },
        )
        # Not in mapping by UUID, source display name not in mapping either
        assert (
            inst._map_principal("11111111-1111-1111-1111-111111111111")
            == "11111111-1111-1111-1111-111111111111"
        )

    def test_duplicate_target_display_names_first_wins(self):
        # The targeted SCIM lookup returns whichever matching SP the server
        # yields first; our test harness mocks that as the first dict entry.
        inst = _Instance(
            principal_mapping={"src-sp": "duplicate-name"},
            target_sp_uuid_to_name={
                "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa": "duplicate-name",
                "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb": "duplicate-name",
            },
        )
        out = inst._map_principal("src-sp")
        assert out == "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"


def test_replicate_grants_batch_failure_falls_back_per_principal():
    # Two source principals; target batch apply fails with a generic error,
    # per-principal retries: one succeeds, the other fails. Expect a failed
    # result with principals_failed=1 and principals_synced=1 — any per-
    # principal failure surfaces as failed in the overall summary.
    src = [
        _pa("alice", Privilege.SELECT),
        _pa("ghost-sp", Privilege.MODIFY),
    ]
    fp = _FakeProvider(src, [])

    # Batch call fails; per-principal calls: alice OK, ghost-sp fails
    def update_side_effect(**kwargs):
        changes = kwargs["changes"]
        if len(changes) > 1:
            raise RuntimeError("batch principal not found: ghost-sp")
        change = changes[0]
        if change.principal == "ghost-sp":
            raise RuntimeError("ghost-sp does not exist on target")
        return None

    fp.target_dbops.update_grants.side_effect = update_side_effect
    result = fp._replicate_grants(
        object_type="table_grant",
        securable_type="TABLE",
        source_full_name="c.s.t",
        target_full_name="c.s.t",
        target_catalog="c",
        schema_name="s",
        object_name="t",
        retry=RetryConfig(max_attempts=1, retry_delay_seconds=1),
    )
    assert result.status == "failed"
    assert result.details["principals_synced"] == 1
    assert result.details["principals_failed"] == 1
    failed = result.details["failed_principals"]
    assert len(failed) == 1
    assert failed[0]["principal"] == "ghost-sp"


def test_replicate_grants_total_failure_when_all_per_principal_fail():
    src = [_pa("alice", Privilege.SELECT)]
    fp = _FakeProvider(src, [])
    fp.target_dbops.update_grants.side_effect = RuntimeError("apply boom")
    result = fp._replicate_grants(
        object_type="catalog_grant",
        securable_type="CATALOG",
        source_full_name="c",
        target_full_name="c",
        target_catalog="c",
        retry=RetryConfig(max_attempts=1, retry_delay_seconds=1),
    )
    assert result.status == "failed"
    assert "apply boom" in result.error_message


# ---------- _replicate_grants integration ----------


class _FakeProvider:
    def __init__(self, source_assignments, target_assignments):
        self.run_id = "r"
        self.logger = logging.getLogger("test.grants")
        self.principal_mapping = None
        self.source_dbops = MagicMock()
        self.source_dbops.get_grants.return_value = source_assignments
        self.source_dbops.get_sp_display_name_by_app_id.return_value = None
        self.target_dbops = MagicMock()
        self.target_dbops.get_grants.return_value = target_assignments
        self.target_dbops.get_sp_app_id_by_display_name.return_value = None
        self._sp_src_app_id_to_name = {}
        self._sp_tgt_name_to_app_id = {}

    # Bind helper methods from BaseProvider
    _map_principal = BaseProvider._map_principal
    _resolve_source_sp_name = BaseProvider._resolve_source_sp_name
    _resolve_target_sp_app_id = BaseProvider._resolve_target_sp_app_id
    _diff_privileges_per_principal = BaseProvider._diff_privileges_per_principal
    _looks_like_sp_uuid = staticmethod(BaseProvider._looks_like_sp_uuid)
    _replicate_grants = BaseProvider._replicate_grants


def test_replicate_grants_no_diff_is_success_skipped():
    same = [_pa("alice", Privilege.SELECT)]
    fp = _FakeProvider(same, same)
    result = fp._replicate_grants(
        object_type="catalog_grant",
        securable_type="CATALOG",
        source_full_name="c",
        target_full_name="c",
        target_catalog="c",
        retry=RetryConfig(max_attempts=1, retry_delay_seconds=1),
    )
    assert result.status == "success"
    assert result.details["skipped"] is True
    fp.target_dbops.update_grants.assert_not_called()


def test_replicate_grants_issues_changes_and_succeeds():
    src = [_pa("alice", Privilege.SELECT, Privilege.MODIFY)]
    tgt = [_pa("alice", Privilege.SELECT)]
    fp = _FakeProvider(src, tgt)
    result = fp._replicate_grants(
        object_type="table_grant",
        securable_type="TABLE",
        source_full_name="c.s.t",
        target_full_name="c.s.t",
        target_catalog="c",
        schema_name="s",
        object_name="t",
        retry=RetryConfig(max_attempts=1, retry_delay_seconds=1),
    )
    assert result.status == "success"
    assert result.details["principals_synced"] == 1
    fp.target_dbops.update_grants.assert_called_once()
    kwargs = fp.target_dbops.update_grants.call_args.kwargs
    assert kwargs["securable_type"] == "TABLE"
    assert kwargs["full_name"] == "c.s.t"
    assert [c.principal for c in kwargs["changes"]] == ["alice"]
    assert kwargs["changes"][0].add == [Privilege.MODIFY]


def test_replicate_grants_failure_returns_failed_result():
    src = [_pa("alice", Privilege.SELECT)]
    fp = _FakeProvider(src, [])
    fp.target_dbops.update_grants.side_effect = RuntimeError("apply failed")
    result = fp._replicate_grants(
        object_type="catalog_grant",
        securable_type="CATALOG",
        source_full_name="c",
        target_full_name="c",
        target_catalog="c",
        retry=RetryConfig(max_attempts=1, retry_delay_seconds=1),
    )
    assert result.status == "failed"
    assert "apply failed" in result.error_message
