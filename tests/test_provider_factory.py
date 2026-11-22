"""Unit tests for ProviderFactory: construction, provider selection, run
orchestration, summaries, and storage credential / external location
replication. Offline only (all Databricks clients are mocked)."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import (
    AwsIamRoleRequest,
    AzureManagedIdentityRequest,
    ExternalLocationInfo,
    StorageCredentialInfo,
)

from data_replication.config.models import (
    CredentialType,
    ExternalLocationConfig,
    ReplicationSystemConfig,
    RunResult,
    StorageCredentialConfig,
    TargetCatalogConfig,
    UCObjectType,
)
from data_replication.providers import provider_factory as pf_module
from data_replication.providers.provider_factory import ProviderFactory

LOGGER = logging.getLogger("tests.provider_factory")


def _rr(status="success", catalog="c", schema="s", name="t"):
    return RunResult(
        operation_type="replication",
        catalog_name=catalog,
        schema_name=schema,
        object_name=name,
        status=status,
        start_time="2026-01-01T00:00:00",
        end_time="2026-01-01T00:00:01",
    )


@pytest.fixture
def patched(monkeypatch):
    """Patch out every Databricks client the factory constructs."""
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
                "catalog_name": "cat1",
                "table_types": ["managed"],
                "replication_config": {"enabled": True},
            }
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


def _factory(operation, cfg):
    return ProviderFactory(
        operation, cfg, MagicMock(), MagicMock(), MagicMock(), LOGGER
    )


# --------------------------------------------------------------------------- #
# Construction
# --------------------------------------------------------------------------- #


class TestInit:
    def test_rejects_unknown_operation(self, patched, config):
        with pytest.raises(ValueError, match="Invalid operation type"):
            _factory("restore", config())

    def test_builds_ops_and_audit_logger(self, patched, config):
        dbops_cls, audit_cls, create_wc = patched
        f = _factory("backup", config())

        assert f.get_operation_name() == "backup"
        assert f.run_id  # generated uuid
        assert dbops_cls.call_count == 2  # main + logging spark
        audit_kwargs = audit_cls.call_args.kwargs
        assert audit_kwargs["audit_table"] == "cat.sch.tbl"
        assert audit_kwargs["run_id"] == f.run_id
        assert audit_kwargs["config_details"]["replication_group"] == "test_group"
        create_wc.assert_not_called()
        assert f.target_db_ops is None

    def test_explicit_run_id_is_kept(self, patched, config):
        f = ProviderFactory(
            "replication",
            config(),
            MagicMock(),
            MagicMock(),
            MagicMock(),
            LOGGER,
            "r-9",
        )
        assert f.run_id == "r-9"

    @pytest.mark.parametrize(
        "uc_type",
        [
            UCObjectType.STORAGE_CREDENTIAL,
            UCObjectType.EXTERNAL_LOCATION,
            UCObjectType.TAG_POLICY,
            UCObjectType.ALL,
        ],
    )
    def test_metastore_types_create_source_and_target_clients(
        self, patched, config, uc_type
    ):
        dbops_cls, _, create_wc = patched
        _factory("replication", config(uc_object_types=[uc_type]))

        hosts = [c.kwargs["host"] for c in create_wc.call_args_list]
        assert hosts == ["https://src.example.com", "https://tgt.example.com"]
        # main, logging, source, target
        assert dbops_cls.call_count == 4

    def test_metastore_types_ignored_for_other_operations(self, patched, config):
        _, _, create_wc = patched
        _factory("reconciliation", config(uc_object_types=[UCObjectType.ALL]))
        create_wc.assert_not_called()

    def test_creates_warehouse_for_sql_mv_st(
        self, patched, config, valid_system_config_dict
    ):
        dbops_cls, _, create_wc = patched
        target_ops = MagicMock()
        target_ops.create_or_get_sql_warehouse.return_value = ("wh-1", "repl-wh")
        dbops_cls.side_effect = [MagicMock(), MagicMock(), target_ops]
        tgt = dict(valid_system_config_dict["target_databricks_connect_config"])
        tgt["warehouse_config"] = {"name": "repl-wh", "cluster_size": "Small"}

        f = _factory(
            "replication",
            config(
                uc_object_types=[UCObjectType.MATERIALIZED_VIEW],
                target_databricks_connect_config=tgt,
            ),
        )

        assert create_wc.call_count == 1  # target only
        assert (
            target_ops.create_or_get_sql_warehouse.call_args.args[0]["name"]
            == "repl-wh"
        )
        assert f.config.target_databricks_connect_config.warehouse_id == "wh-1"

    def test_existing_warehouse_id_is_reused(
        self, patched, config, valid_system_config_dict
    ):
        _, _, create_wc = patched
        tgt = dict(valid_system_config_dict["target_databricks_connect_config"])
        tgt["warehouse_id"] = "wh-0"
        tgt["warehouse_config"] = {"name": "x"}

        f = _factory(
            "replication",
            config(
                uc_object_types=[UCObjectType.STREAMING_TABLE],
                target_databricks_connect_config=tgt,
            ),
        )

        create_wc.assert_not_called()
        assert f.config.target_databricks_connect_config.warehouse_id == "wh-0"


# --------------------------------------------------------------------------- #
# Catalog gating and provider selection
# --------------------------------------------------------------------------- #


class TestProviderSelection:
    @pytest.mark.parametrize(
        "operation,field",
        [
            ("backup", "backup_config"),
            ("replication", "replication_config"),
            ("reconciliation", "reconciliation_config"),
        ],
    )
    def test_is_catalog_enabled(self, patched, config, operation, field):
        f = _factory(operation, config())
        assert f.is_catalog_enabled(TargetCatalogConfig(catalog_name="c")) is False
        disabled = TargetCatalogConfig(catalog_name="c", **{field: {"enabled": False}})
        enabled = TargetCatalogConfig(catalog_name="c", **{field: {"enabled": True}})
        assert f.is_catalog_enabled(disabled) is False
        assert f.is_catalog_enabled(enabled) is True

    def test_is_catalog_enabled_unknown_operation(self, patched, config):
        f = _factory("backup", config())
        f._operation_type = "other"
        assert f.is_catalog_enabled(TargetCatalogConfig(catalog_name="c")) is False

    @pytest.mark.parametrize(
        "operation,module,cls",
        [
            ("backup", "backup_provider", "BackupProvider"),
            ("replication", "replication_provider", "ReplicationProvider"),
            ("reconciliation", "reconciliation_provider", "ReconciliationProvider"),
        ],
    )
    def test_create_provider_passes_factory_state(
        self, patched, config, monkeypatch, operation, module, cls
    ):
        fake = MagicMock(name=cls)
        monkeypatch.setattr(f"data_replication.providers.{module}.{cls}", fake)
        cfg = config(
            cloud_url_mapping={"s3://a/": "s3://b/"}, principal_mapping={"u": "v"}
        )
        f = _factory(operation, cfg)
        catalog = TargetCatalogConfig(catalog_name="c")

        provider = f.create_provider(catalog)

        assert provider is fake.return_value
        args = fake.call_args.args
        assert args[4] == f.run_id
        assert args[5] is catalog
        assert args[8] == {"s3://a/": "s3://b/"}
        assert args[9] is f.audit_logger
        assert args[10] is f.completed_run_results
        assert args[11] == {"u": "v"}

    def test_create_provider_unknown_operation(self, patched, config):
        f = _factory("backup", config())
        f._operation_type = "other"
        with pytest.raises(ValueError, match="Unknown operation type"):
            f.create_provider(TargetCatalogConfig(catalog_name="c"))

    @pytest.mark.parametrize(
        "maker,operation",
        [
            (ProviderFactory.create_backup_factory, "backup"),
            (ProviderFactory.create_replication_factory, "replication"),
            (ProviderFactory.create_reconciliation_factory, "reconciliation"),
        ],
    )
    def test_classmethod_constructors(self, patched, config, maker, operation):
        f = maker(config(), MagicMock(), MagicMock(), MagicMock(), LOGGER)
        assert f.get_operation_name() == operation

    def test_run_helpers_reject_wrong_operation(self, patched, config):
        f = _factory("backup", config())
        f.run_operations = MagicMock(return_value="summary")
        assert f.run_backup_operations() == "summary"
        with pytest.raises(ValueError, match="replication"):
            f.run_replication_operations()
        with pytest.raises(ValueError, match="reconciliation"):
            f.run_reconciliation_operations()

        g = _factory("reconciliation", config())
        g.run_operations = MagicMock(return_value="ok")
        assert g.run_reconciliation_operations() == "ok"
        with pytest.raises(ValueError, match="backup"):
            g.run_backup_operations()

        h = _factory("replication", config())
        h.run_operations = MagicMock(return_value="ok")
        assert h.run_replication_operations() == "ok"


# --------------------------------------------------------------------------- #
# Summaries
# --------------------------------------------------------------------------- #


class TestSummary:
    def test_counts_and_unique_objects(self, patched, config):
        f = _factory("replication", config())
        start = datetime.now(timezone.utc) - timedelta(seconds=5)
        results = [
            _rr("success", "c1", "s1", "t1"),
            _rr("success", "c1", "s1", "t1"),  # duplicate object
            _rr("failed", "c1", "s2", "t2"),
            _rr("skipped", "c2", "s1", "t3"),
        ]

        s = f.create_summary(start, results, "replication")

        assert s.status == "completed_with_failures"
        assert (s.total_catalogs, s.total_schemas, s.total_tables) == (2, 3, 3)
        assert (s.successful_operations, s.failed_operations) == (2, 1)
        assert s.duration >= 5
        assert "2 successful, 1 failed,1 skipped" in s.summary
        assert "Success rate: 2/4 (50.0%)" in s.summary

    def test_empty_results(self, patched, config):
        f = _factory("backup", config())
        s = f.create_summary(datetime.now(timezone.utc), [], "backup")
        assert s.status == "completed"
        assert "Success rate: 0/0 (0.0%)" in s.summary

    def test_log_run_summary_writes_audit_row(self, patched, config):
        f = _factory("replication", config())
        summary = f.create_summary(datetime.now(timezone.utc), [_rr()], "replication")

        f.log_run_summary(summary, "replication")

        kwargs = f.audit_logger.log_operation.call_args.kwargs
        assert kwargs["operation_type"] == "replication_summary"
        assert kwargs["status"] == "completed"
        assert json.loads(kwargs["details"])["successful_operations"] == 1

    def test_log_run_summary_survives_audit_failure(self, patched, config, caplog):
        f = _factory("replication", config())
        f.audit_logger.log_operation.side_effect = RuntimeError("table locked")
        summary = f.create_summary(datetime.now(timezone.utc), [], "replication")

        with caplog.at_level(logging.WARNING, logger=LOGGER.name):
            f.log_run_summary(summary, "replication")

        assert any("table locked" in m for m in caplog.messages)

    def test_log_run_summary_without_audit_logger(self, patched, config):
        f = _factory("replication", config())
        f.audit_logger = None
        summary = f.create_summary(datetime.now(timezone.utc), [], "replication")
        f.log_run_summary(summary)  # must not raise


# --------------------------------------------------------------------------- #
# run_operations / _run_catalog_operations
# --------------------------------------------------------------------------- #


class TestRunOperations:
    def _factory(self, config, **cfg):
        cfg.setdefault(
            "target_catalogs",
            [
                {
                    "catalog_name": "c1",
                    "table_types": ["managed"],
                    "replication_config": {"enabled": True},
                },
                {
                    "catalog_name": "c2",
                    "table_types": ["managed"],
                    "replication_config": {"enabled": False},
                },
            ],
        )
        f = _factory("replication", config(**cfg))
        f._replicate_storage_credentials = MagicMock(return_value=[_rr(name="cred")])
        f._replicate_external_locations = MagicMock(return_value=[_rr(name="loc")])
        f._replicate_tag_policies = MagicMock(return_value=[_rr(name="tp")])
        return f

    def test_storage_credential_only_returns_early(self, patched, config):
        f = self._factory(config, uc_object_types=[UCObjectType.STORAGE_CREDENTIAL])
        f._run_catalog_operations = MagicMock()

        summary = f.run_operations()

        f._replicate_storage_credentials.assert_called_once()
        f._replicate_external_locations.assert_not_called()
        f._run_catalog_operations.assert_not_called()
        assert summary.successful_operations == 1
        f.audit_logger.log_results.assert_called_once()

    @pytest.mark.parametrize(
        "uc_type,method",
        [
            (UCObjectType.EXTERNAL_LOCATION, "_replicate_external_locations"),
            (UCObjectType.TAG_POLICY, "_replicate_tag_policies"),
        ],
    )
    def test_other_metastore_types_return_early(self, patched, config, uc_type, method):
        f = self._factory(config, uc_object_types=[uc_type])
        f._run_catalog_operations = MagicMock()

        f.run_operations()

        getattr(f, method).assert_called_once()
        f._run_catalog_operations.assert_not_called()

    def test_all_runs_metastore_steps_then_enabled_catalogs(
        self, patched, config, monkeypatch
    ):
        f = self._factory(config, uc_object_types=[UCObjectType.ALL])
        provider = MagicMock()
        provider.process_catalog.return_value = [_rr(catalog="c1"), _rr("failed", "c1")]
        provider.process_ownership.return_value = []
        f.create_provider = MagicMock(return_value=provider)

        summary = f.run_operations()

        f._replicate_storage_credentials.assert_called_once()
        f._replicate_external_locations.assert_called_once()
        f._replicate_tag_policies.assert_called_once()
        # Only the enabled catalog gets a provider: once for catalog work and
        # once for the final ownership pass (ALL includes ownership).
        provider.process_ownership.assert_called_once()
        assert [c.args[0].catalog_name for c in f.create_provider.call_args_list] == [
            "c1",
            "c1",
        ]
        assert summary.successful_operations == 4  # cred, loc, tp, 1 table
        assert summary.failed_operations == 1
        assert f.config.uc_object_types == [UCObjectType.ALL]

    def test_no_enabled_catalogs(self, patched, config):
        f = self._factory(
            config,
            target_catalogs=[
                {
                    "catalog_name": "c1",
                    "table_types": ["managed"],
                    "replication_config": {"enabled": False},
                }
            ],
            uc_object_types=None,
        )
        f._run_catalog_operations = MagicMock()

        summary = f.run_operations()

        f._run_catalog_operations.assert_not_called()
        assert summary.successful_operations == 0

    def test_unexpected_error_is_logged_not_raised(self, patched, config, caplog):
        f = self._factory(config, uc_object_types=[UCObjectType.STORAGE_CREDENTIAL])
        f._replicate_storage_credentials.side_effect = RuntimeError("boom")

        with caplog.at_level(logging.ERROR, logger=LOGGER.name):
            summary = f.run_operations()

        assert summary.successful_operations == 0
        assert any("Replication operation failed: boom" in m for m in caplog.messages)
        f.audit_logger.log_operation.assert_called_once()

    def test_unexpected_error_is_not_summarised_as_completed(self, patched, config):
        f = self._factory(config, uc_object_types=[UCObjectType.ALL])
        f._replicate_external_locations.side_effect = RuntimeError("boom")

        summary = f.run_operations()

        assert summary.status != "completed"
        assert summary.successful_operations == 1  # the storage credential

    def test_catalog_failure_is_recorded_and_others_continue(self, patched, config):
        f = _factory("replication", config())
        ok = MagicMock()
        ok.process_catalog.return_value = [_rr(catalog="good"), _rr("skipped", "good")]
        f.create_provider = MagicMock(side_effect=[RuntimeError("no spark"), ok])

        results = f._run_catalog_operations(
            [
                TargetCatalogConfig(catalog_name="bad"),
                TargetCatalogConfig(catalog_name="good"),
            ]
        )

        assert [(r.catalog_name, r.status) for r in results] == [
            ("bad", "failed"),
            ("good", "success"),
            ("good", "skipped"),
        ]
        assert (
            "Replication failed for catalog bad: no spark" in results[0].error_message
        )
        # Failures from create_provider are not added to completed results.
        assert [r.catalog_name for r in f.completed_run_results] == ["good", "good"]


# --------------------------------------------------------------------------- #
# Storage credentials
# --------------------------------------------------------------------------- #


def _bare_factory(cfg):
    f = ProviderFactory.__new__(ProviderFactory)
    f.config = cfg
    f.source_db_ops = MagicMock()
    f.target_db_ops = MagicMock()
    f.logger = LOGGER
    f.audit_logger = MagicMock()
    f.run_id = "r"
    f.completed_run_results = []
    return f


class TestStorageCredentials:
    def _f(self, cred_type=CredentialType.AWS, mapping=None):
        cfg = MagicMock()
        cfg.storage_credential_config = StorageCredentialConfig(
            target_credential_type=cred_type,
            mapping=mapping
            if mapping is not None
            else {"cred": "arn:aws:iam::1:role/r"},
        )
        f = _bare_factory(cfg)
        f.source_db_ops.get_storage_credential.return_value = StorageCredentialInfo(
            name="cred", comment="c", read_only=False
        )
        f.target_db_ops.get_storage_credential.side_effect = Exception("not found")
        return f

    def test_no_config_skips(self):
        cfg = MagicMock()
        cfg.storage_credential_config = None
        assert _bare_factory(cfg)._replicate_storage_credentials() == []
        assert self._f(mapping={})._replicate_storage_credentials() == []

    def test_creates_aws_credential(self):
        f = self._f()

        [r] = f._replicate_storage_credentials()

        assert (r.status, r.object_type, r.object_name) == (
            "success",
            "storage_credential",
            "cred",
        )
        created = f.target_db_ops.create_storage_credential.call_args.args[0]
        assert created["name"] == "cred"
        assert created["skip_validation"] is False
        assert created["aws_iam_role"] == AwsIamRoleRequest(
            role_arn="arn:aws:iam::1:role/r"
        )
        assert r.details["credential_existed"] is False

    @pytest.mark.parametrize(
        "cred_type",
        [CredentialType.AZURE_ACCESS_CONNECTOR, CredentialType.AZURE_MANAGED_IDENTITY],
    )
    def test_creates_azure_credential(self, cred_type):
        f = self._f(cred_type=cred_type, mapping={"cred": "/subscriptions/x/connector"})

        f._replicate_storage_credentials()

        created = f.target_db_ops.create_storage_credential.call_args.args[0]
        assert created["azure_managed_identity"] == AzureManagedIdentityRequest(
            access_connector_id="/subscriptions/x/connector"
        )

    def test_existing_credential_updated_only_when_different(self):
        f = self._f()
        f.target_db_ops.get_storage_credential.side_effect = None
        f.target_db_ops.get_storage_credential.return_value = StorageCredentialInfo(
            name="cred", comment="c", read_only=False
        )

        [r] = f._replicate_storage_credentials()

        assert r.details["credential_existed"] is True
        f.target_db_ops.create_storage_credential.assert_not_called()
        # The update dict always carries the target role, so it differs.
        update = f.target_db_ops.update_storage_credential.call_args.args[0]
        assert update["name"] == "cred"
        assert update["aws_iam_role"].role_arn == "arn:aws:iam::1:role/r"

    def test_per_credential_failure_does_not_stop_others(self):
        f = self._f(mapping={"bad": "arn:1", "good": "arn:2"})
        good = StorageCredentialInfo(name="good")

        def get(name):
            if name == "bad":
                raise RuntimeError("missing on source")
            return good

        f.source_db_ops.get_storage_credential.side_effect = get

        results = f._replicate_storage_credentials()

        assert [(r.object_name, r.status) for r in results] == [
            ("bad", "failed"),
            ("good", "success"),
        ]
        assert "missing on source" in results[0].error_message


# --------------------------------------------------------------------------- #
# External locations
# --------------------------------------------------------------------------- #


class TestExternalLocations:
    MAPPING = {"abfss://c@acct.dfs.core.windows.net/": "s3://bucket/"}

    def _f(self, locations=None, mapping=MAPPING):
        cfg = MagicMock()
        cfg.cloud_url_mapping = mapping
        cfg.storage_credential_config = None
        cfg.external_location_config = (
            ExternalLocationConfig(external_locations=locations) if locations else None
        )
        f = _bare_factory(cfg)
        f.target_db_ops.get_external_location.side_effect = Exception("not found")
        return f

    def _loc(self, name, url, **kw):
        return ExternalLocationInfo(name=name, url=url, credential_name="cred", **kw)

    def test_no_mapping_skips(self):
        assert self._f(mapping=None)._replicate_external_locations() == []

    def test_discovers_only_mapped_locations_and_creates_them(self):
        f = self._f()
        src = {
            "in": self._loc("in", "abfss://c@acct.dfs.core.windows.net/data"),
            "out": self._loc("out", "abfss://other@acct.dfs.core.windows.net/x"),
        }
        f.source_db_ops.list_external_locations.return_value = list(src.values())
        f.source_db_ops.get_external_location.side_effect = lambda n: src[n]

        [r] = f._replicate_external_locations()

        assert (r.object_name, r.status) == ("in", "success")
        created = f.target_db_ops.create_external_location.call_args.args[0]
        assert created["url"] == "s3://bucket/data"
        assert created["credential_name"] == "cred"
        assert created["skip_validation"] is False

    def test_configured_list_and_up_to_date_location(self):
        f = self._f(locations=["loc"])
        info = self._loc("loc", "abfss://c@acct.dfs.core.windows.net/d", comment="x")
        f.source_db_ops.get_external_location.return_value = info
        f.target_db_ops.get_external_location.side_effect = None
        f.target_db_ops.get_external_location.return_value = self._loc(
            "loc", "s3://bucket/d", comment="x"
        )

        [r] = f._replicate_external_locations()

        assert r.status == "success"
        assert r.details["location_existed"] is True
        f.source_db_ops.list_external_locations.assert_not_called()
        f.target_db_ops.create_external_location.assert_not_called()
        f.target_db_ops.update_external_location.assert_not_called()

    def test_existing_location_updated_when_different(self):
        f = self._f(locations=["loc"])
        f.source_db_ops.get_external_location.return_value = self._loc(
            "loc", "abfss://c@acct.dfs.core.windows.net/d", comment="new"
        )
        f.target_db_ops.get_external_location.side_effect = None
        f.target_db_ops.get_external_location.return_value = self._loc(
            "loc", "s3://bucket/d", comment="old"
        )

        f._replicate_external_locations()

        update = f.target_db_ops.update_external_location.call_args.args[0]
        assert (update["comment"], update["url"]) == ("new", "s3://bucket/d")

    @pytest.mark.parametrize(
        "url,message",
        [
            ("gs://unmapped/x", "No URL mapping found"),
            (None, "has no URL"),
        ],
    )
    def test_bad_url_is_failed(self, url, message):
        f = self._f(locations=["loc"])
        f.source_db_ops.get_external_location.return_value = ExternalLocationInfo(
            name="loc", url=url
        )

        [r] = f._replicate_external_locations()

        assert r.status == "failed"
        assert message in r.error_message
        f.target_db_ops.create_external_location.assert_not_called()

    def test_listing_failure_is_reported(self):
        f = self._f()
        f.source_db_ops.list_external_locations.side_effect = RuntimeError("403")

        [r] = f._replicate_external_locations()

        assert (r.status, r.object_type, r.object_name) == (
            "failed",
            "external_location",
            None,
        )
        assert "403" in r.error_message
