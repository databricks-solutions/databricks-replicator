"""Tests for providers/backup_provider.py — shares, recipients and DLT backups."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from data_replication.config.models import (
    BackupConfig,
    DatabricksConnectConfig,
    RetryConfig,
    SchemaConfig,
    TableConfig,
    TableType,
)
from data_replication.providers import backup_provider as bp
from data_replication.providers.backup_provider import BackupProvider
from data_replication.providers.base_provider import BaseProvider


@pytest.fixture(autouse=True)
def no_retry_sleep():
    with patch("data_replication.utils.time.sleep"):
        yield


def _backup_config(**kw):
    base = dict(
        enabled=True,
        source_catalog="src_cat",
        backup_catalog="bk_cat",
        backup_schema_prefix="bk_",
        share_name="src_share",
        backup_share_name="bk_share",
        recipient_name="rcpt",
        dpm_backing_table_share_name="dpm_share",
    )
    base.update(kw)
    return BackupConfig(**base)


def _provider(backup_config=None, sharing_identifier="aws:us-west-2:abc"):
    p = BackupProvider.__new__(BackupProvider)
    p.spark = MagicMock()
    p.logger = MagicMock()
    p.db_ops = MagicMock()
    p.workspace_client = MagicMock()
    p.run_id = "run-1"
    p.audit_logger = MagicMock()
    p.target_dbops = MagicMock()
    p.catalog_config = SimpleNamespace(
        catalog_name="tgt_cat",
        backup_config=backup_config if backup_config is not None else _backup_config(),
    )
    p.source_databricks_config = DatabricksConnectConfig(
        name="src", host="https://src.example.com"
    )
    p.target_databricks_config = DatabricksConnectConfig(
        name="tgt",
        host="https://tgt.example.com",
        sharing_identifier=sharing_identifier,
    )
    return p


def _schema(**kw):
    base = dict(schema_name="sales", backup_config=_backup_config())
    base.update(kw)
    return SchemaConfig(**base)


def _table(name="orders", max_attempts=2, **bk):
    return TableConfig(
        table_name=name,
        backup_config=_backup_config(**bk),
        retry=RetryConfig(max_attempts=max_attempts, retry_delay_seconds=1),
    )


# ---------- __init__ ----------


class TestInit:
    def _init(self, backup_config, sharing_identifier=None):
        catalog_config = SimpleNamespace(
            catalog_name="c",
            backup_config=backup_config,
            concurrency=SimpleNamespace(max_workers=1, timeout_seconds=10),
        )

        def fake_base_init(self, *a, **kw):
            self.spark = MagicMock()
            self.logger = MagicMock()
            self.workspace_client = MagicMock()
            self.catalog_config = catalog_config
            self.source_databricks_config = DatabricksConnectConfig(
                name="s", host="https://src.example.com"
            )
            self.target_databricks_config = DatabricksConnectConfig(
                name="t",
                host="https://tgt.example.com",
                sharing_identifier=sharing_identifier,
            )

        with (
            patch.object(BaseProvider, "__init__", fake_base_init),
            patch.object(bp, "create_workspace_client") as ws,
            patch.object(bp, "create_spark_session") as spark,
            patch.object(bp, "validate_spark_session") as validate,
            patch.object(bp, "DatabricksOperations") as dbops,
        ):
            provider = BackupProvider()
        return provider, ws, spark, validate, dbops

    def test_target_session_created_when_shares_need_metastore_id(self):
        provider, ws, spark, validate, dbops = self._init(
            _backup_config(create_share=True, dpm_backing_table_share_name=None)
        )
        spark.assert_called_once()
        assert spark.call_args.kwargs["host"] == "https://tgt.example.com"
        validate.assert_called_once_with(spark.return_value, "tgt.example.com")
        assert ws.call_count == 2  # source + target workspace clients
        assert provider.target_dbops is dbops.return_value

    def test_no_target_session_when_sharing_identifier_known(self):
        provider, ws, spark, _, dbops = self._init(
            _backup_config(create_recipient=True), sharing_identifier="aws:x:y"
        )
        spark.assert_not_called()
        assert ws.call_count == 1
        # dpm share configured -> shared tables are loaded up front
        dbops.return_value.get_shared_tables.assert_called_once_with("dpm_share")
        assert (
            provider.shared_tables is dbops.return_value.get_shared_tables.return_value
        )

    def test_no_backup_config(self):
        provider, _, spark, _, dbops = self._init(None)
        spark.assert_not_called()
        dbops.return_value.get_shared_tables.assert_not_called()
        assert provider.get_operation_name() == "backup"


# ---------- simple helpers ----------


def test_backup_schema_name_uses_prefix():
    p = _provider()
    assert p.get_backup_schema_name("sales", _backup_config()) == "bk_sales"
    assert (
        p.get_backup_schema_name("sales", _backup_config(backup_schema_prefix=None))
        == "sales"
    )
    assert (
        p.get_backup_schema_name("sales", _backup_config(backup_catalog=None)) is None
    )


def test_is_operation_enabled():
    assert _provider(_backup_config(enabled=True)).is_operation_enabled()
    assert not _provider(_backup_config(enabled=False)).is_operation_enabled()
    p = _provider()
    p.catalog_config.backup_config = None
    assert not p.is_operation_enabled()


# ---------- setup_operation_catalogs ----------


class TestSetupOperationCatalogs:
    def test_creates_backup_catalog_and_returns_source_catalog(self):
        p = _provider(
            _backup_config(create_backup_catalog=True, backup_catalog_location="s3://x")
        )
        assert p.setup_operation_catalogs() == "src_cat"
        p.db_ops.create_catalog_if_not_exists.assert_called_once_with(
            "bk_cat", "s3://x"
        )
        p.db_ops.create_delta_share.assert_not_called()

    def test_backup_catalog_failure_is_raised(self):
        p = _provider(_backup_config(create_backup_catalog=True))
        p.db_ops.create_catalog_if_not_exists.side_effect = RuntimeError("no perms")
        with pytest.raises(RuntimeError, match="no perms"):
            p.setup_operation_catalogs()
        p.logger.error.assert_called_once()

    def test_create_recipient_uses_configured_sharing_identifier(self):
        p = _provider(_backup_config(create_recipient=True))
        p.db_ops.create_recipient.return_value = "rcpt"
        p.setup_operation_catalogs()
        p.db_ops.create_recipient.assert_called_once_with("aws:us-west-2:abc", "rcpt")
        p.target_dbops.get_metastore_id.assert_not_called()

    def test_create_recipient_name_mismatch_updates_config(self):
        bc = _backup_config(create_recipient=True)
        p = _provider(bc, sharing_identifier=None)
        p.target_dbops.get_metastore_id.return_value = "aws:r:metastore"
        p.db_ops.create_recipient.return_value = "existing_rcpt"
        p.setup_operation_catalogs()
        p.db_ops.create_recipient.assert_called_once_with("aws:r:metastore", "rcpt")
        assert bc.recipient_name == "existing_rcpt"
        p.logger.warning.assert_called_once()

    def test_creates_all_shares_for_resolved_recipient(self):
        bc = _backup_config(
            create_share=True,
            create_backup_share=True,
            create_dpm_backing_table_share=True,
        )
        p = _provider(bc, sharing_identifier=None)
        p.target_dbops.get_metastore_id.return_value = "aws:r:m"
        p.db_ops.get_recipient_name.return_value = "resolved"
        p.setup_operation_catalogs()
        p.db_ops.get_recipient_name.assert_called_once_with("aws:r:m")
        assert [c.args for c in p.db_ops.create_delta_share.call_args_list] == [
            ("src_share", "resolved"),
            ("bk_share", "resolved"),
            ("dpm_share", "resolved"),
        ]

    def test_backup_share_skipped_without_name(self):
        bc = _backup_config(create_backup_share=True, backup_share_name=None)
        p = _provider(bc)
        p.setup_operation_catalogs()
        p.db_ops.create_delta_share.assert_not_called()


# ---------- process_schema / _add_schema_to_shares ----------


class TestProcessSchema:
    def test_skips_non_streaming_schemas_but_still_shares(self):
        p = _provider()
        schema = _schema(
            table_types=[TableType.MANAGED],
            backup_config=_backup_config(add_to_share=True),
        )
        with patch.object(BaseProvider, "process_schema") as base:
            results, tables, extra = p.process_schema(schema)
        base.assert_not_called()
        p.db_ops.create_schema_if_not_exists.assert_called_once_with(
            "bk_cat", "bk_sales"
        )
        assert tables == [] and extra == []
        assert len(results) == 1
        assert results[0].status == "success"
        assert results[0].object_type == "schema"
        assert [c.args for c in p.db_ops.add_schema_to_share.call_args_list] == [
            ("src_share", "src_cat", "sales"),
            ("bk_share", "bk_cat", "bk_sales"),
        ]

    def test_skips_when_no_table_types(self):
        p = _provider()
        with patch.object(BaseProvider, "process_schema") as base:
            assert p.process_schema(_schema()) == ([], [], [])
        base.assert_not_called()

    def test_streaming_schema_delegates_with_streaming_only(self):
        p = _provider()
        schema = _schema(table_types=[TableType.MANAGED, TableType.STREAMING_TABLE])
        table_result = MagicMock()
        with patch.object(
            BaseProvider, "process_schema", return_value=([table_result], ["t1"], [])
        ) as base:
            results, tables, extra = p.process_schema(schema)
        base.assert_called_once()
        assert base.call_args[0][0].table_types == [TableType.STREAMING_TABLE]
        assert results == [table_result]
        assert tables == ["t1"]

    def test_no_backup_catalog_skips_schema_creation(self):
        p = _provider()
        schema = _schema(backup_config=_backup_config(backup_catalog=None))
        p.process_schema(schema)
        p.db_ops.create_schema_if_not_exists.assert_not_called()

    def test_add_schema_to_shares_failure_returns_failed_result(self):
        p = _provider()
        p.db_ops.add_schema_to_share.side_effect = RuntimeError("share missing")
        result = p._add_schema_to_shares("sales", _backup_config(add_to_share=True))
        assert result.status == "failed"
        assert "share missing" in result.error_message
        assert result.details["backup_schema_name"] == "bk_sales"


# ---------- _backup_table / process_table ----------


def _table_details(p, table_type, is_dlt=True, dlt_type="legacy", name="real.t"):
    p.db_ops.get_table_details.return_value = {
        "table_name": name,
        "is_dlt": is_dlt,
        "dlt_type": dlt_type,
    }
    p.db_ops.get_table_type.return_value = table_type


class TestBackupTable:
    def test_legacy_streaming_table_deep_clone_and_unset_parent(self):
        p = _provider()
        _table_details(p, "STREAMING_TABLE", dlt_type="legacy", name="`src`.`s`.`bt`")
        [res] = p._backup_table(_schema(), _table(backup_legacy_backing_tables=True))
        assert res.status == "success"
        q1, q2 = [c.args[0] for c in p.spark.sql.call_args_list]
        assert "CREATE OR REPLACE TABLE bk_cat.bk_sales.orders" in q1
        assert "DEEP CLONE `src`.`s`.`bt`" in q1
        assert "ALTER TABLE bk_cat.bk_sales.orders" in q2
        assert "UNSET TBLPROPERTIES (spark.sql.internal.pipelines.parentTableId)" in q2
        assert res.details["source_table"] == "`src`.`s`.`bt`"
        assert res.catalog_name == "src_cat"

    def test_legacy_backing_table_skipped_when_disabled(self):
        p = _provider()
        _table_details(p, "STREAMING_TABLE", dlt_type="legacy")
        assert p._backup_table(_schema(), _table()) == []
        p.spark.sql.assert_not_called()

    def test_dpm_streaming_table_added_to_share(self):
        p = _provider()
        _table_details(p, "streaming_table", dlt_type="dpm", name="__dpm.bt")
        p.db_ops.is_table_in_share.return_value = False
        [res] = p._backup_table(_schema(), _table())
        assert res.status == "success"
        q = p.spark.sql.call_args[0][0]
        assert "ALTER SHARE dpm_share" in q
        assert "ADD TABLE __dpm.bt AS sales.orders" in q
        assert res.details["step2_query"] is None

    def test_dpm_table_already_in_share_is_noop_success(self):
        p = _provider()
        _table_details(p, "streaming_table", dlt_type="dpm", name="__dpm.bt")
        p.db_ops.is_table_in_share.return_value = True
        [res] = p._backup_table(_schema(), _table())
        assert res.status == "success"
        p.db_ops.is_table_in_share.assert_called_once_with("dpm_share", "__dpm.bt")
        p.spark.sql.assert_not_called()

    @pytest.mark.parametrize(
        "table_type,is_dlt,dlt_type",
        [
            ("managed", False, None),  # not DLT
            ("materialized_view", True, "legacy"),  # DLT but not a streaming table
        ],
    )
    def test_non_backup_tables_skipped(self, table_type, is_dlt, dlt_type):
        p = _provider()
        _table_details(p, table_type, is_dlt=is_dlt, dlt_type=dlt_type)
        assert p._backup_table(_schema(), _table()) == []
        p.spark.sql.assert_not_called()

    def test_clone_failure_after_retries_returns_failed(self):
        p = _provider()
        _table_details(p, "streaming_table", dlt_type="legacy")
        p.spark.sql.side_effect = RuntimeError("clone failed")
        [res] = p._backup_table(
            _schema(), _table(max_attempts=2, backup_legacy_backing_tables=True)
        )
        assert res.status == "failed"
        assert "after 2 attempts" in res.error_message
        assert "clone failed" in res.error_message
        assert res.attempt_number == 2
        assert p.spark.sql.call_count == 2

    def test_metadata_lookup_error_wrapped_as_backup_error(self):
        p = _provider()
        p.db_ops.get_table_details.side_effect = RuntimeError("table not found")
        [res] = p._backup_table(_schema(), _table())
        assert res.status == "failed"
        assert "Backup operation failed: table not found" in res.error_message
        assert res.details["source_table"] is None

    def test_process_table_substitutes_table_name_and_logs(self):
        p = _provider()
        _table_details(p, "streaming_table", dlt_type="legacy")
        table = TableConfig(
            table_name="orders",
            backup_config=_backup_config(
                backup_legacy_backing_tables=True,
                backup_schema_prefix="{{table_name}}_",
            ),
            retry=RetryConfig(max_attempts=1, retry_delay_seconds=1),
        )
        results = p.process_table(_schema(), table)
        assert results[0].details["backup_table"] == "bk_cat.orders_sales.orders"
        p.audit_logger.log_results.assert_called_once_with(results)

    def test_process_table_skipped_table_not_logged(self):
        p = _provider()
        _table_details(p, "managed", is_dlt=False)
        assert p.process_table(_schema(), _table()) == []
        p.audit_logger.log_results.assert_not_called()
