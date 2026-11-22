"""Tests for object ownership replication (UCObjectType.OWNERSHIP).

Covers the SDK owner helpers, owner/principal mapping and per-object
outcomes, the child-before-parent ordering inside the ownership pass, and
the factory placing ownership after all catalog work. Offline only: every
Databricks client is a mock.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import (
    CatalogInfo,
)
from databricks.sdk.service.iam import Group, ServicePrincipal, User

from data_replication.config.models import (
    ReplicationSystemConfig,
    RunResult,
    SchemaConfig,
    TableConfig,
    TargetCatalogConfig,
    UCObjectType,
    VolumeConfig,
)
from data_replication.databricks_operations import DatabricksOperations
from data_replication.providers import provider_factory as pf_module
from data_replication.providers.replication_provider import ReplicationProvider
from tests._replication_fakes import make_provider

LOGGER = logging.getLogger("tests.ownership")
APP_ID = "11111111-2222-3333-4444-555555555555"
TGT_APP_ID = "66666666-7777-8888-9999-000000000000"


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("data_replication.utils.time.sleep", lambda *_: None)


# --------------------------------------------------------------------------- #
# Enum
# --------------------------------------------------------------------------- #


def test_ownership_enum_value():
    assert UCObjectType.OWNERSHIP.value == "ownership"


# --------------------------------------------------------------------------- #
# DatabricksOperations: owner read/write and principal lookup
# --------------------------------------------------------------------------- #


@pytest.fixture
def wc_ops():
    wc = MagicMock()
    return DatabricksOperations(spark=None, workspace_client=wc), wc


class TestGetSecurableOwner:
    @pytest.mark.parametrize(
        "securable,attr,method,full_name",
        [
            ("CATALOG", "catalogs", "get", "c"),
            ("SCHEMA", "schemas", "get", "c.s"),
            ("TABLE", "tables", "get", "c.s.t"),
            ("VOLUME", "volumes", "read", "c.s.v"),
            ("FUNCTION", "functions", "get", "c.s.f"),
        ],
    )
    def test_reads_owner_from_matching_sdk_api(
        self, wc_ops, securable, attr, method, full_name
    ):
        ops, wc = wc_ops
        getattr(getattr(wc, attr), method).return_value = MagicMock(owner="alice")
        assert ops.get_securable_owner(securable, full_name) == "alice"
        getattr(getattr(wc, attr), method).assert_called_once_with(full_name)

    def test_none_owner_is_returned_as_none(self, wc_ops):
        ops, wc = wc_ops
        wc.catalogs.get.return_value = CatalogInfo(name="c", owner=None)
        assert ops.get_securable_owner("CATALOG", "c") is None

    def test_not_found_propagates_unwrapped(self, wc_ops):
        ops, wc = wc_ops
        wc.tables.get.side_effect = NotFound("gone")
        with pytest.raises(NotFound):
            ops.get_securable_owner("TABLE", "c.s.t")

    def test_other_errors_are_wrapped(self, wc_ops):
        ops, wc = wc_ops
        wc.schemas.get.side_effect = RuntimeError("boom")
        with pytest.raises(Exception, match="Failed to get owner of SCHEMA c.s"):
            ops.get_securable_owner("SCHEMA", "c.s")

    def test_unsupported_type_raises(self, wc_ops):
        ops, _ = wc_ops
        with pytest.raises(ValueError, match="Unsupported securable type"):
            ops.get_securable_owner("METASTORE", "m")

    def test_requires_workspace_client(self):
        ops = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            ops.get_securable_owner("CATALOG", "c")


class TestSetSecurableOwner:
    @pytest.mark.parametrize(
        "securable,attr,kwargs",
        [
            ("CATALOG", "catalogs", {"name": "c"}),
            ("SCHEMA", "schemas", {"full_name": "c.s"}),
            ("TABLE", "tables", {"full_name": "c.s.t"}),
            ("VOLUME", "volumes", {"name": "c.s.v"}),
            ("FUNCTION", "functions", {"name": "c.s.f"}),
        ],
    )
    def test_updates_owner_via_matching_sdk_api(self, wc_ops, securable, attr, kwargs):
        ops, wc = wc_ops
        full_name = next(iter(kwargs.values()))
        ops.set_securable_owner(securable, full_name, "bob")
        getattr(getattr(wc, attr), "update").assert_called_once_with(
            **kwargs, owner="bob"
        )

    def test_sdk_error_is_wrapped(self, wc_ops):
        ops, wc = wc_ops
        wc.tables.update.side_effect = RuntimeError("denied")
        with pytest.raises(Exception, match="Failed to set owner of TABLE c.s.t"):
            ops.set_securable_owner("TABLE", "c.s.t", "bob")

    def test_unsupported_type_raises(self, wc_ops):
        ops, _ = wc_ops
        with pytest.raises(ValueError, match="Unsupported securable type"):
            ops.set_securable_owner("METASTORE", "m", "bob")


class TestPrincipalExists:
    def _lookups(self, wc, users=(), groups=(), sps=()):
        wc.users.list.return_value = iter(users)
        wc.groups.list.return_value = iter(groups)
        wc.service_principals.list.return_value = iter(sps)

    def test_user_found(self, wc_ops):
        ops, wc = wc_ops
        self._lookups(wc, users=[User(user_name="alice@x.com")])
        assert ops.principal_exists("alice@x.com") is True
        wc.users.list.assert_called_once_with(filter='userName eq "alice@x.com"')

    def test_group_found_after_user_miss(self, wc_ops):
        ops, wc = wc_ops
        self._lookups(wc, groups=[Group(display_name="analysts")])
        assert ops.principal_exists("analysts") is True
        wc.groups.list.assert_called_once_with(filter='displayName eq "analysts"')

    def test_service_principal_found_by_application_id(self, wc_ops):
        ops, wc = wc_ops
        self._lookups(wc, sps=[ServicePrincipal(application_id=APP_ID)])
        assert ops.principal_exists(APP_ID) is True
        wc.service_principals.list.assert_called_once_with(
            filter=f'applicationId eq "{APP_ID}"'
        )

    def test_missing_everywhere_is_false(self, wc_ops):
        ops, wc = wc_ops
        self._lookups(wc)
        assert ops.principal_exists("ghost@x.com") is False

    def test_quotes_are_escaped_in_filter(self, wc_ops):
        ops, wc = wc_ops
        self._lookups(wc)
        ops.principal_exists('we"ird')
        wc.users.list.assert_called_once_with(filter='userName eq "we\\"ird"')

    def test_requires_workspace_client(self):
        ops = DatabricksOperations(spark=None, workspace_client=None)
        with pytest.raises(Exception, match="WorkspaceClient is required"):
            ops.principal_exists("x")


# --------------------------------------------------------------------------- #
# Ownership pass (ReplicationProvider.process_ownership)
# --------------------------------------------------------------------------- #


def _owner_provider(principal_mapping=None, owners=None, targets=None):
    """Replication provider with owner reads driven by dicts keyed by full name.

    ``owners`` maps source full names to owners; ``targets`` maps target full
    names to owners. A name absent from the dict raises NotFound.
    """
    p = make_provider(
        uc_object_types=[UCObjectType.OWNERSHIP],
        retry={"max_attempts": 1, "retry_delay_seconds": 1},
    )
    assert isinstance(p, ReplicationProvider)
    p.principal_mapping = principal_mapping or {}
    p._sp_src_app_id_to_name = {}
    p._sp_tgt_name_to_app_id = {}
    p.processed_objects = []
    p.completed_objects = []
    p.shared_tables = []
    p.target_dbops.principal_exists = MagicMock(return_value=True)
    # No SP display-name match unless a test sets one (mock would otherwise
    # turn every mapped value into a MagicMock).
    p.source_dbops.get_sp_display_name_by_app_id = MagicMock(return_value=None)
    p.target_dbops.get_sp_app_id_by_display_name = MagicMock(return_value=None)

    def read(table):
        def get(securable, full_name):
            if full_name not in table:
                raise NotFound(f"{full_name} not found")
            return table[full_name]

        return get

    p.source_dbops.get_securable_owner = MagicMock(side_effect=read(owners or {}))
    p.target_dbops.get_securable_owner = MagicMock(side_effect=read(targets or {}))
    p.source_dbops.get_functions_in_schema = MagicMock(return_value=[])
    p._get_schemas = MagicMock(return_value=[SchemaConfig(schema_name="s")])
    p._get_tables = MagicMock(return_value=[])
    p._get_volumes = MagicMock(return_value=[])
    return p


ALL_SOURCE_OWNERS = {
    "src": "alice@src.com",
    "src.s": "alice@src.com",
    "src.s.t": "alice@src.com",
    "src.s.v": "alice@src.com",
    "src.s.f": "alice@src.com",
}
ALL_TARGET_OWNERS = {
    "tgt": "old@tgt.com",
    "tgt.s": "old@tgt.com",
    "tgt.s.t": "old@tgt.com",
    "tgt.s.v": "old@tgt.com",
    "tgt.s.f": "old@tgt.com",
}


def _set_calls(p):
    return [c.args for c in p.target_dbops.set_securable_owner.call_args_list]


class TestOwnerMapping:
    def test_applies_mapped_user_owner_to_every_object_type(self):
        p = _owner_provider(
            principal_mapping={"alice@src.com": "alice@tgt.com"},
            owners=ALL_SOURCE_OWNERS,
            targets=ALL_TARGET_OWNERS,
        )
        p._get_tables.return_value = [TableConfig(table_name="t")]
        p._get_volumes.return_value = [VolumeConfig(volume_name="v")]
        p.source_dbops.get_functions_in_schema.return_value = ["f"]

        results = p.process_ownership()

        assert {r.status for r in results} == {"success"}
        assert _set_calls(p) == [
            ("TABLE", "tgt.s.t", "alice@tgt.com"),
            ("VOLUME", "tgt.s.v", "alice@tgt.com"),
            ("FUNCTION", "tgt.s.f", "alice@tgt.com"),
            ("SCHEMA", "tgt.s", "alice@tgt.com"),
            ("CATALOG", "tgt", "alice@tgt.com"),
        ]
        assert {r.object_type for r in results} == {
            "table_ownership",
            "volume_ownership",
            "function_ownership",
            "schema_ownership",
            "catalog_ownership",
        }

    def test_unmapped_user_owner_is_used_verbatim(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        p.process_ownership()
        assert ("CATALOG", "tgt", "alice@src.com") in _set_calls(p)

    def test_sp_uuid_is_mapped_through_principal_mapping(self):
        p = _owner_provider(
            principal_mapping={APP_ID: TGT_APP_ID},
            owners={"src": APP_ID},
            targets={"tgt": "old@tgt.com"},
        )
        p._get_schemas.return_value = []
        results = p.process_ownership()
        assert results[0].status == "success"
        assert _set_calls(p) == [("CATALOG", "tgt", TGT_APP_ID)]

    def test_sp_uuid_resolved_by_display_name_when_unmapped(self):
        p = _owner_provider(owners={"src": APP_ID}, targets={"tgt": "old@tgt.com"})
        p._get_schemas.return_value = []
        p.source_dbops.get_sp_display_name_by_app_id = MagicMock(return_value="etl-sp")
        p.target_dbops.get_sp_app_id_by_display_name = MagicMock(
            return_value=TGT_APP_ID
        )
        p.process_ownership()
        assert _set_calls(p) == [("CATALOG", "tgt", TGT_APP_ID)]

    def test_principal_lookup_is_cached_per_owner(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        p._get_tables.return_value = [TableConfig(table_name="t")]
        p.process_ownership()
        # One distinct owner across five objects -> one principal lookup.
        assert p.target_dbops.principal_exists.call_count == 1


class TestOutcomes:
    def test_already_matching_owner_is_a_noop_skip(self):
        owners = dict(ALL_SOURCE_OWNERS)
        targets = dict(ALL_TARGET_OWNERS, **{"tgt": "ALICE@src.com"})
        p = _owner_provider(owners=owners, targets=targets)
        p._get_schemas.return_value = []
        (result,) = p.process_ownership()
        assert result.status == "skipped"
        assert result.details["reason"] == "owner already matches"
        assert _set_calls(p) == []

    def test_target_object_missing_is_skipped(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets={})
        p._get_schemas.return_value = []
        (result,) = p.process_ownership()
        assert result.status == "skipped"
        assert result.details["reason"] == "target object does not exist"
        assert _set_calls(p) == []

    def test_source_owner_unset_is_skipped(self):
        p = _owner_provider(owners={"src": None}, targets={"tgt": "old@tgt.com"})
        p._get_schemas.return_value = []
        (result,) = p.process_ownership()
        assert result.status == "skipped"
        assert result.details["reason"] == "source owner is not set"
        assert _set_calls(p) == []

    def test_missing_target_principal_is_skipped_with_warning(self):
        p = _owner_provider(
            principal_mapping={"alice@src.com": "ghost@tgt.com"},
            owners=ALL_SOURCE_OWNERS,
            targets=ALL_TARGET_OWNERS,
        )
        p.target_dbops.principal_exists = MagicMock(return_value=False)
        p._get_schemas.return_value = []

        (result,) = p.process_ownership()

        assert result.status == "skipped"
        assert result.details["reason"] == "owner principal not found on target"
        p.target_dbops.principal_exists.assert_called_once_with("ghost@tgt.com")
        assert _set_calls(p) == []
        assert any(
            "ghost@tgt.com" in str(c.args[0]) for c in p.logger.warning.call_args_list
        )

    def test_failed_update_is_recorded_and_run_continues(self):
        p = _owner_provider(
            owners=ALL_SOURCE_OWNERS,
            targets=ALL_TARGET_OWNERS,
        )
        p._get_tables.return_value = [TableConfig(table_name="t")]

        def flaky(securable, full_name, owner):
            if securable == "TABLE":
                raise Exception("permission denied")

        p.target_dbops.set_securable_owner = MagicMock(side_effect=flaky)

        results = p.process_ownership()

        by_type = {r.object_type: r for r in results}
        assert by_type["table_ownership"].status == "failed"
        assert "permission denied" in by_type["table_ownership"].error_message
        assert by_type["schema_ownership"].status == "success"
        assert by_type["catalog_ownership"].status == "success"

    def test_unexpected_read_error_is_failed_not_raised(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        p._get_schemas.return_value = []
        p.source_dbops.get_securable_owner = MagicMock(
            side_effect=RuntimeError("source down")
        )
        (result,) = p.process_ownership()
        assert result.status == "failed"
        assert "source down" in result.error_message

    def test_source_object_missing_is_failed(self):
        p = _owner_provider(owners={}, targets=ALL_TARGET_OWNERS)
        p._get_schemas.return_value = []
        (result,) = p.process_ownership()
        assert result.status == "failed"
        assert "Source object does not exist" in result.error_message


class TestOrderingAndScope:
    def test_children_before_schema_before_catalog(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        p._get_tables.return_value = [TableConfig(table_name="t")]
        p._get_volumes.return_value = [VolumeConfig(volume_name="v")]
        p.source_dbops.get_functions_in_schema.return_value = ["f"]

        p.process_ownership()

        order = [c[0] for c in _set_calls(p)]
        assert order == ["TABLE", "VOLUME", "FUNCTION", "SCHEMA", "CATALOG"]

    def test_multiple_schemas_all_children_before_any_schema(self):
        owners = {
            "src": "alice@src.com",
            "src.a": "alice@src.com",
            "src.a.t": "alice@src.com",
            "src.b": "alice@src.com",
            "src.b.t": "alice@src.com",
        }
        targets = {k.replace("src", "tgt", 1): "old@tgt.com" for k in owners}
        p = _owner_provider(owners=owners, targets=targets)
        p._get_schemas.return_value = [
            SchemaConfig(schema_name="a"),
            SchemaConfig(schema_name="b"),
        ]
        p._get_tables.side_effect = lambda scope, _: [TableConfig(table_name="t")]

        p.process_ownership()

        order = [c[0] for c in _set_calls(p)]
        assert order == ["TABLE", "TABLE", "SCHEMA", "SCHEMA", "CATALOG"]

    def test_scope_clears_uc_object_types_and_defaults_volume_types(self):
        from data_replication.config.models import VolumeType

        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        scope = p._ownership_scope(SchemaConfig(schema_name="s"))
        assert scope.uc_object_types is None
        assert scope.volume_types == [VolumeType.MANAGED, VolumeType.EXTERNAL]

    def test_scope_keeps_explicit_table_types(self):
        from data_replication.config.models import TableType

        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        scope = p._ownership_scope(
            SchemaConfig(schema_name="s", table_types=[TableType.MANAGED])
        )
        assert scope.table_types == [TableType.MANAGED]

    def test_processed_object_filters_are_reset(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        p.completed_objects = ["tgt.s.t"]
        p._get_schemas.return_value = []
        p.process_ownership()
        assert p.completed_objects == []

    def test_schema_listing_error_returns_single_failed_result(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        p._get_schemas.side_effect = RuntimeError("cannot list")
        results = p.process_ownership()
        assert [r.status for r in results] == ["failed"]
        assert _set_calls(p) == []

    def test_one_schema_listing_error_does_not_stop_others(self):
        p = _owner_provider(owners=ALL_SOURCE_OWNERS, targets=ALL_TARGET_OWNERS)
        p._get_schemas.return_value = [
            SchemaConfig(schema_name="bad"),
            SchemaConfig(schema_name="s"),
        ]

        def tables(scope, _):
            if scope.schema_name == "bad":
                raise RuntimeError("no tables")
            return []

        p._get_tables.side_effect = tables

        results = p.process_ownership()

        statuses = {(r.schema_name, r.object_type, r.status) for r in results}
        # The failing schema's object listing is recorded as failed...
        assert ("bad", "schema", "failed") in statuses
        # ...while the other schema and the catalog still get their owners.
        assert ("s", "schema_ownership", "success") in statuses
        assert (None, "catalog_ownership", "success") in statuses


# --------------------------------------------------------------------------- #
# process_catalog: ownership must not drive catalog/schema processing
# --------------------------------------------------------------------------- #


class TestProcessCatalogIgnoresOwnership:
    def test_ownership_only_skips_catalog_and_schema_stages(self):
        p = make_provider(
            uc_object_types=[UCObjectType.OWNERSHIP],
            concurrency={"max_workers": 1, "timeout_seconds": 60},
        )
        p.catalog_name = None
        p._get_schemas = MagicMock(return_value=[SchemaConfig(schema_name="s")])
        p._uc_replicate_catalog = MagicMock()
        p.process_schema = MagicMock()

        assert p.process_catalog() == []

        p._uc_replicate_catalog.assert_not_called()
        p.process_schema.assert_not_called()

    def test_catalog_plus_ownership_returns_after_catalog_stage(self):
        p = make_provider(
            uc_object_types=[UCObjectType.CATALOG, UCObjectType.OWNERSHIP],
            concurrency={"max_workers": 1, "timeout_seconds": 60},
        )
        p._uc_replicate_catalog = MagicMock(return_value=[])
        p._get_schemas = MagicMock()
        p.process_schema = MagicMock()

        p.process_catalog()

        p._uc_replicate_catalog.assert_called_once()
        p._get_schemas.assert_not_called()
        p.process_schema.assert_not_called()


# --------------------------------------------------------------------------- #
# ProviderFactory: ownership runs last
# --------------------------------------------------------------------------- #


@pytest.fixture
def patched(monkeypatch):
    dbops_cls = MagicMock(name="DatabricksOperations")
    audit_cls = MagicMock(name="AuditLogger")
    create_wc = MagicMock(name="create_workspace_client")
    monkeypatch.setattr(pf_module, "DatabricksOperations", dbops_cls)
    monkeypatch.setattr(pf_module, "AuditLogger", audit_cls)
    monkeypatch.setattr(pf_module, "create_workspace_client", create_wc)
    return dbops_cls, audit_cls, create_wc


@pytest.fixture
def config(valid_system_config_dict):
    def _build(**overrides):
        d = dict(valid_system_config_dict)
        d["target_catalogs"] = [
            {
                "catalog_name": name,
                "table_types": ["managed"],
                "replication_config": {"enabled": True},
            }
            for name in ("c1", "c2")
        ]
        d.update(overrides)
        if d.get("uc_object_types"):
            # System-level uc_object_types propagate to every catalog, and a
            # catalog may set only one of table/volume/uc object types.
            d["target_catalogs"] = [
                {k: v for k, v in c.items() if k != "table_types"}
                for c in d["target_catalogs"]
            ]
        return ReplicationSystemConfig(**d)

    return _build


def _rr(status="success", catalog="c1", object_type="table"):
    return RunResult(
        operation_type="replication",
        catalog_name=catalog,
        schema_name="s",
        object_name="t",
        object_type=object_type,
        status=status,
        start_time="2026-01-01T00:00:00",
        end_time="2026-01-01T00:00:01",
    )


class TestFactoryOrdering:
    def _factory(self, config, **cfg):
        f = pf_module.ProviderFactory(
            "replication", config(**cfg), MagicMock(), MagicMock(), MagicMock(), LOGGER
        )
        f._replicate_storage_credentials = MagicMock(return_value=[])
        f._replicate_external_locations = MagicMock(return_value=[])
        f._replicate_tag_policies = MagicMock(return_value=[])
        return f

    def test_ownership_runs_after_all_catalog_operations(self, patched, config):
        f = self._factory(config, uc_object_types=[UCObjectType.OWNERSHIP])
        calls = MagicMock()
        f._run_catalog_operations = calls.catalogs
        f._run_ownership_operations = calls.ownership
        calls.attach_mock(f._run_catalog_operations, "catalogs")
        calls.attach_mock(f._run_ownership_operations, "ownership")

        f.run_operations()

        assert [c[0] for c in calls.mock_calls] == ["catalogs", "ownership"]

    def test_ownership_runs_after_real_catalog_loop(self, patched, config, monkeypatch):
        """The ownership pass sees every catalog's results already recorded."""
        f = self._factory(config, uc_object_types=[UCObjectType.OWNERSHIP])
        seen_before_ownership = []

        def catalog_run(catalogs):
            f.completed_run_results.append(_rr(catalog="c1"))
            f.completed_run_results.append(_rr(catalog="c2"))

        def ownership_run(catalogs):
            seen_before_ownership.append(len(f.completed_run_results))
            return []

        f._run_catalog_operations = MagicMock(side_effect=catalog_run)
        f._run_ownership_operations = MagicMock(side_effect=ownership_run)
        f.run_operations()
        assert seen_before_ownership == [2]

    def test_all_alone_does_not_run_ownership(self, patched, config):
        """`all` is not an opt-in for ownership: no provider gets an ownership pass."""
        f = self._factory(config, uc_object_types=[UCObjectType.ALL])
        provider = MagicMock()
        provider.process_catalog.return_value = []
        f.create_provider = MagicMock(return_value=provider)

        f.run_operations()

        # Catalog work runs for each enabled catalog; the ownership pass never does.
        assert provider.process_catalog.call_count == 2
        provider.process_ownership.assert_not_called()

    def test_all_plus_explicit_ownership_runs_ownership_last(self, patched, config):
        f = self._factory(
            config, uc_object_types=[UCObjectType.ALL, UCObjectType.OWNERSHIP]
        )
        calls = MagicMock()
        f._run_catalog_operations = MagicMock()
        f._run_ownership_operations = MagicMock(return_value=[])
        calls.attach_mock(f._run_catalog_operations, "catalogs")
        calls.attach_mock(f._run_ownership_operations, "ownership")

        f.run_operations()

        assert [c[0] for c in calls.mock_calls] == ["catalogs", "ownership"]

    def test_ownership_not_run_for_storage_credential_only(self, patched, config):
        f = self._factory(config, uc_object_types=[UCObjectType.STORAGE_CREDENTIAL])
        f._run_catalog_operations = MagicMock()
        f._run_ownership_operations = MagicMock()
        f.run_operations()
        f._run_ownership_operations.assert_not_called()

    def test_only_catalogs_listing_ownership_get_a_provider(self, patched, config):
        f = self._factory(config)
        cats = [
            TargetCatalogConfig(
                catalog_name="yes",
                uc_object_types=[UCObjectType.OWNERSHIP],
                replication_config={"enabled": True},
            ),
            TargetCatalogConfig(
                catalog_name="both",
                uc_object_types=[UCObjectType.ALL, UCObjectType.OWNERSHIP],
                replication_config={"enabled": True},
            ),
            TargetCatalogConfig(
                catalog_name="all",
                uc_object_types=[UCObjectType.ALL],
                replication_config={"enabled": True},
            ),
            TargetCatalogConfig(
                catalog_name="no",
                uc_object_types=[UCObjectType.TABLE],
                replication_config={"enabled": True},
            ),
        ]
        provider = MagicMock()
        provider.process_ownership.return_value = [_rr(catalog="yes")]
        f.create_provider = MagicMock(return_value=provider)

        out = f._run_ownership_operations(cats)

        assert [c.args[0].catalog_name for c in f.create_provider.call_args_list] == [
            "yes",
            "both",
        ]
        assert len(out) == 2
        assert f.audit_logger.log_results.call_count == 2

    def test_backup_never_runs_ownership(self, patched, config):
        f = self._factory(config)
        f.create_provider = MagicMock()
        cat = TargetCatalogConfig(
            catalog_name="x",
            uc_object_types=[UCObjectType.OWNERSHIP],
            replication_config={"enabled": True},
        )
        f._operation_type = "backup"
        assert f._run_ownership_operations([cat]) == []
        f.create_provider.assert_not_called()

    def test_catalog_failure_is_recorded_and_others_continue(self, patched, config):
        f = self._factory(config)
        good = TargetCatalogConfig(
            catalog_name="good",
            uc_object_types=[UCObjectType.OWNERSHIP],
            replication_config={"enabled": True},
        )
        bad = TargetCatalogConfig(
            catalog_name="bad",
            uc_object_types=[UCObjectType.OWNERSHIP],
            replication_config={"enabled": True},
        )
        good_provider = MagicMock()
        good_provider.process_ownership.return_value = [_rr(catalog="good")]
        bad_provider = MagicMock()
        bad_provider.process_ownership.side_effect = RuntimeError("kaput")
        f.create_provider = MagicMock(side_effect=[bad_provider, good_provider])

        out = f._run_ownership_operations([bad, good])

        statuses = {(r.catalog_name, r.status) for r in out}
        assert ("bad", "failed") in statuses
        assert ("good", "success") in statuses
        assert any(
            "Ownership failed for catalog bad" in (r.error_message or "") for r in out
        )
        assert f.completed_run_results == out
