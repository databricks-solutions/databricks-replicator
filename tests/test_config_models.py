"""Tests for config/models.py — validators, defaults, and system-level merging."""

from __future__ import annotations

import copy

import pytest
from pydantic import ValidationError

from data_replication.config.models import (
    AuditConfig,
    BackupConfig,
    CredentialType,
    EnvironmentConfig,
    EnvironmentsConfig,
    ExternalLocationConfig,
    LoggingConfig,
    ReconciliationConfig,
    ReplicationConfig,
    ReplicationSystemConfig,
    SecretConfig,
    StorageCredentialConfig,
    TableConfig,
    TableType,
    TargetCatalogConfig,
    UCObjectType,
    VolumeConfig,
    VolumeFilesReplicationConfig,
    VolumeType,
)


# ---------- Enums ----------

def test_credential_type_values():
    values = {c.value for c in CredentialType if isinstance(c.value, str)}
    assert {"aws", "azure_access_connector", "azure_managed_identity"} <= values


# ---------- AuditConfig ----------

class TestAuditConfig:
    def test_normalizes_to_lowercase(self):
        cfg = AuditConfig(audit_table="CAT.SCH.TBL")
        assert cfg.audit_table == "cat.sch.tbl"

    def test_strips_whitespace(self):
        cfg = AuditConfig(audit_table="  cat.sch.tbl  ")
        assert cfg.audit_table == "cat.sch.tbl"

    @pytest.mark.parametrize("bad", ["", "   ", "onlyone", "cat.sch", "a.b.c.d"])
    def test_rejects_malformed(self, bad):
        with pytest.raises(ValidationError):
            AuditConfig(audit_table=bad)

    def test_extra_forbidden(self):
        with pytest.raises(ValidationError):
            AuditConfig(audit_table="a.b.c", extra_field="no")


# ---------- SecretConfig ----------

class TestSecretConfig:
    def test_pat_only(self):
        cfg = SecretConfig(secret_scope="scope", secret_pat="pat")
        assert cfg.secret_pat == "pat"

    def test_oauth_pair(self):
        cfg = SecretConfig(
            secret_scope="scope",
            secret_client_id="cid",
            secret_client_secret="csec",
        )
        assert cfg.secret_client_id == "cid"

    def test_requires_at_least_one_secret(self):
        with pytest.raises(ValidationError, match="At least one secret field"):
            SecretConfig(secret_scope="scope")

    def test_client_id_without_secret(self):
        with pytest.raises(ValidationError, match="secret_client_secret is required"):
            SecretConfig(secret_scope="scope", secret_client_id="cid")

    def test_client_secret_without_id(self):
        with pytest.raises(ValidationError, match="secret_client_id is required"):
            SecretConfig(secret_scope="scope", secret_client_secret="csec")


# ---------- LoggingConfig ----------

class TestLoggingConfig:
    def test_defaults(self):
        cfg = LoggingConfig()
        assert cfg.level == "INFO" or cfg.level == "info"  # stored lowercased
        assert cfg.format in ("json",)

    def test_normalizes_level(self):
        cfg = LoggingConfig(level="debug")
        assert cfg.level == "debug"

    def test_invalid_level(self):
        with pytest.raises(ValidationError):
            LoggingConfig(level="TRACE")

    def test_invalid_format(self):
        with pytest.raises(ValidationError):
            LoggingConfig(format="yaml")

    def test_format_case_insensitive(self):
        cfg = LoggingConfig(format="JSON")
        assert cfg.format == "json"


# ---------- VolumeFilesReplicationConfig bounds ----------

class TestVolumeFilesReplicationConfig:
    def test_defaults(self):
        cfg = VolumeFilesReplicationConfig()
        assert cfg.max_concurrent_copies == 10
        assert cfg.streaming_timeout_seconds >= 60

    def test_max_concurrent_copies_bounds(self):
        with pytest.raises(ValidationError):
            VolumeFilesReplicationConfig(max_concurrent_copies=0)
        with pytest.raises(ValidationError):
            VolumeFilesReplicationConfig(max_concurrent_copies=101)

    def test_streaming_timeout_minimum(self):
        with pytest.raises(ValidationError):
            VolumeFilesReplicationConfig(streaming_timeout_seconds=30)


# ---------- Backup / Replication / Reconciliation field-level lowercase ----------

class TestBackupConfig:
    def test_lowercases_catalogs(self):
        cfg = BackupConfig(
            enabled=True, source_catalog="CAT", backup_catalog="BKUP"
        )
        assert cfg.source_catalog == "cat"
        assert cfg.backup_catalog == "bkup"


def test_replication_config_lowercases_catalogs():
    cfg = ReplicationConfig(source_catalog="SRC", intermediate_catalog="IC")
    assert cfg.source_catalog == "src"
    assert cfg.intermediate_catalog == "ic"


def test_reconciliation_config_lowercases_catalogs_and_schemas():
    cfg = ReconciliationConfig(
        source_catalog="SRC",
        recon_outputs_catalog="R_CAT",
        recon_outputs_schema="R_SCH",
    )
    assert cfg.source_catalog == "src"
    assert cfg.recon_outputs_catalog == "r_cat"
    assert cfg.recon_outputs_schema == "r_sch"


def test_table_volume_schema_catalog_lowercase():
    assert TableConfig(table_name="T").table_name == "t"
    assert VolumeConfig(volume_name="V").volume_name == "v"
    assert TargetCatalogConfig(catalog_name="C").catalog_name == "c"


# ---------- StorageCredentialConfig ----------

class TestStorageCredentialConfig:
    def test_accepts_known_types(self):
        cfg = StorageCredentialConfig(
            target_credential_type=CredentialType.AWS,
            mapping={"src": "arn"},
        )
        assert cfg.target_credential_type == CredentialType.AWS

    def test_rejects_unknown_type(self):
        with pytest.raises(ValidationError):
            StorageCredentialConfig(target_credential_type="bogus", mapping={})


# ---------- ReplicationSystemConfig ----------

def _valid_dict():
    return {
        "version": "1.0",
        "replication_group": "grp",
        "source_databricks_connect_config": {
            "name": "src",
            "host": "https://src/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        },
        "target_databricks_connect_config": {
            "name": "tgt",
            "host": "https://tgt/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        },
        "audit_config": {"audit_table": "audit.sch.tbl"},
        "uc_object_types": ["catalog"],
        "replication_config": {"enabled": True},
        "target_catalogs": [{"catalog_name": "c1"}],
    }


class TestReplicationSystemConfig:
    def test_builds_with_minimum(self):
        cfg = ReplicationSystemConfig(**_valid_dict())
        assert cfg.replication_group == "grp"
        assert cfg.target_catalogs[0].catalog_name == "c1"

    def test_requires_target_catalogs_by_default(self):
        d = _valid_dict()
        d["target_catalogs"] = []
        with pytest.raises(ValidationError, match="At least one target catalog"):
            ReplicationSystemConfig(**d)

    def test_allows_empty_catalogs_for_metastore_only(self):
        d = _valid_dict()
        d["target_catalogs"] = []
        d["uc_object_types"] = ["storage_credential", "external_location"]
        cfg = ReplicationSystemConfig(**d)
        assert cfg.target_catalogs == []

    def test_cloud_url_mapping_keys_must_end_with_slash(self):
        d = _valid_dict()
        d["cloud_url_mapping"] = {"s3://src": "s3://dst/"}
        with pytest.raises(ValidationError, match="end with '/'"):
            ReplicationSystemConfig(**d)

    def test_cloud_url_mapping_values_must_end_with_slash(self):
        d = _valid_dict()
        d["cloud_url_mapping"] = {"s3://src/": "s3://dst"}
        with pytest.raises(ValidationError, match="end with '/'"):
            ReplicationSystemConfig(**d)

    def test_table_types_all_expanded(self):
        d = _valid_dict()
        d.pop("uc_object_types")
        d["table_types"] = ["all"]
        cfg = ReplicationSystemConfig(**d)
        assert TableType.ALL not in cfg.table_types
        assert TableType.MANAGED in cfg.table_types
        assert TableType.EXTERNAL in cfg.table_types

    def test_volume_types_all_expanded(self):
        d = _valid_dict()
        d.pop("uc_object_types")
        d["volume_types"] = ["all"]
        cfg = ReplicationSystemConfig(**d)
        assert VolumeType.ALL not in cfg.volume_types
        assert VolumeType.MANAGED in cfg.volume_types

    def test_reconciliation_defaults_from_audit(self):
        d = _valid_dict()
        d["reconciliation_config"] = {"enabled": True}
        cfg = ReplicationSystemConfig(**d)
        assert cfg.reconciliation_config.recon_outputs_catalog == "audit"
        assert cfg.reconciliation_config.recon_outputs_schema == "sch"

    def test_derive_default_backup_catalog_for_streaming(self):
        d = _valid_dict()
        d.pop("uc_object_types")
        d["table_types"] = ["streaming_table"]
        d["target_catalogs"] = [
            {
                "catalog_name": "cat",
                "table_types": ["streaming_table"],
                "backup_config": {"enabled": True},
            }
        ]
        cfg = ReplicationSystemConfig(**d)
        bc = cfg.target_catalogs[0].backup_config
        # Derived: __replication_internal_<catalog>_to_<target_name>
        assert bc.backup_catalog == "__replication_internal_cat_to_tgt"
        assert bc.recipient_name == "tgt_recipient"


# ---------- EnvironmentsConfig ----------

def _env_cfg_dict():
    return {
        "description": "d",
        "is_default": False,
        "source_databricks_connect_config": {
            "name": "src",
            "host": "https://src/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        },
        "target_databricks_connect_config": {
            "name": "tgt",
            "host": "https://tgt/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        },
        "audit_config": {"audit_table": "a.b.c"},
    }


class TestEnvironmentsConfig:
    def test_single_default_required(self):
        env1 = _env_cfg_dict()
        env2 = _env_cfg_dict()
        with pytest.raises(ValidationError, match="At least one environment"):
            EnvironmentsConfig(version="1.0", environments={"a": env1, "b": env2})

    def test_multiple_defaults_rejected(self):
        env1 = _env_cfg_dict()
        env1["is_default"] = True
        env2 = _env_cfg_dict()
        env2["is_default"] = True
        with pytest.raises(ValidationError, match="Only one environment"):
            EnvironmentsConfig(version="1.0", environments={"a": env1, "b": env2})

    def test_get_environment_lookup(self):
        env1 = _env_cfg_dict()
        env1["is_default"] = True
        cfg = EnvironmentsConfig(version="1.0", environments={"dev": env1})
        name, found = cfg.get_default_environment()
        assert name == "dev"
        assert isinstance(found, EnvironmentConfig)
        with pytest.raises(ValueError, match="not found"):
            cfg.get_environment("prod")


# ---------- extra='forbid' hygiene ----------

def test_replication_system_config_rejects_unknown_field():
    d = _valid_dict()
    d["unknown_extra"] = "nope"
    with pytest.raises(ValidationError):
        ReplicationSystemConfig(**d)


def test_external_location_config_rejects_extras():
    with pytest.raises(ValidationError):
        ExternalLocationConfig(bogus=True)
