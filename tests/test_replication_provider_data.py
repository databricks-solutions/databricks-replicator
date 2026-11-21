"""ReplicationProvider: setup, dispatch, deep-clone table replication and volume files.

Offline: providers are built with ``__new__`` and all Spark/SDK objects are mocks.
"""

from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from data_replication.config.models import (
    ReplicationConfig,
    SchemaConfig,
    TableType,
    UCObjectType,
    VolumeFilesReplicationConfig,
)
from data_replication.exceptions import TableNotFoundError
from data_replication.providers import replication_provider as rp_module
from data_replication.providers.base_provider import BaseProvider
from data_replication.providers.replication_provider import ReplicationProvider
from tests._replication_fakes import (
    df,
    make_provider,
    sql_calls,
    table_config,
    volume_config,
)


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("data_replication.utils.time.sleep", lambda *_: None)


# ---------- __init__ ----------


class TestInit:
    @pytest.fixture
    def patched(self, monkeypatch):
        """Stub BaseProvider.__init__ and every factory ReplicationProvider calls."""
        calls = {"spark": [], "wc": []}

        def fake_base_init(self, catalog_config, source_cfg):
            self.catalog_config = catalog_config
            self.source_databricks_config = source_cfg
            self.target_databricks_config = SimpleNamespace(
                host="https://tgt", token=None, auth_type="pat"
            )
            self.workspace_client = MagicMock()
            self.logger = MagicMock()
            self.spark = MagicMock(name="target_spark")

        monkeypatch.setattr(BaseProvider, "__init__", fake_base_init)
        monkeypatch.setattr(
            rp_module,
            "create_workspace_client",
            lambda **kw: calls["wc"].append(kw["host"]) or MagicMock(),
        )
        monkeypatch.setattr(
            rp_module,
            "create_spark_session",
            lambda **kw: calls["spark"].append(kw["host"]) or MagicMock(name="src"),
        )
        monkeypatch.setattr(rp_module, "validate_spark_session", lambda *a: True)
        monkeypatch.setattr(
            rp_module,
            "DatabricksOperations",
            lambda spark, *a: SimpleNamespace(spark=spark),
        )
        return calls

    def _source_cfg(self, sharing_identifier=None):
        return SimpleNamespace(
            host="https://src",
            auth_type="pat",
            token=None,
            cluster_id=None,
            sharing_identifier=sharing_identifier,
        )

    def test_data_replication_uses_target_spark_only(self, patched):
        cat = make_provider().catalog_config
        p = ReplicationProvider(cat, self._source_cfg())
        assert p.source_spark is None
        assert p.db_ops is p.target_dbops
        assert p.target_dbops.spark is p.target_spark
        assert patched["spark"] == []
        assert patched["wc"] == ["https://tgt"]

    def test_uc_replication_drives_with_source_spark(self, patched):
        cat = make_provider(uc_object_types=[UCObjectType.TABLE]).catalog_config
        p = ReplicationProvider(cat, self._source_cfg())
        assert patched["spark"] == ["https://src"]
        assert p.spark is p.source_spark
        assert p.db_ops is p.source_dbops
        assert p.target_spark is not p.source_spark

    def test_shared_catalog_without_identifier_creates_source_session(self, patched):
        cat = make_provider(
            replication_config=ReplicationConfig(
                enabled=True, source_catalog="src", create_shared_catalog=True
            )
        ).catalog_config
        p = ReplicationProvider(cat, self._source_cfg())
        assert patched["spark"] == ["https://src"]
        # data replication keeps the target session as the driver
        assert p.db_ops is p.target_dbops

    def test_shared_catalog_with_identifier_skips_source_session(self, patched):
        cat = make_provider(
            replication_config=ReplicationConfig(
                enabled=True, source_catalog="src", create_shared_catalog=True
            )
        ).catalog_config
        ReplicationProvider(cat, self._source_cfg(sharing_identifier="azure:x:y"))
        assert patched["spark"] == []


# ---------- simple accessors ----------


def test_operation_name_and_enabled():
    p = make_provider()
    assert p.get_operation_name() == "replication"
    assert p.is_operation_enabled()
    p.catalog_config.replication_config.enabled = False
    assert not p.is_operation_enabled()


# ---------- setup_operation_catalogs ----------


class TestSetupOperationCatalogs:
    def test_uc_replication_creates_nothing(self):
        p = make_provider(uc_object_types=[UCObjectType.ALL])
        assert p.setup_operation_catalogs() == "src"
        p.db_ops.create_catalog_if_not_exists.assert_not_called()

    def test_creates_target_and_intermediate_catalogs(self):
        p = make_provider(
            replication_config=ReplicationConfig(
                source_catalog="src",
                create_target_catalog=True,
                target_catalog_location="s3://t",
                create_intermediate_catalog=True,
                intermediate_catalog="inter",
                intermediate_catalog_location="s3://i",
            )
        )
        assert p.setup_operation_catalogs() == "src"
        assert p.db_ops.create_catalog_if_not_exists.call_args_list[0].args == (
            "tgt",
            "s3://t",
        )
        assert p.db_ops.create_catalog_if_not_exists.call_args_list[1].args == (
            "inter",
            "s3://i",
        )

    def test_shared_catalogs_with_explicit_provider(self):
        p = make_provider(
            replication_config=ReplicationConfig(
                source_catalog="src",
                create_shared_catalog=True,
                share_name="sh",
                create_backup_shared_catalog=True,
                backup_catalog="bk",
                backup_share_name="bk_sh",
                create_dpm_backing_table_shared_catalog=True,
                dpm_backing_table_catalog="dpm",
                dpm_backing_table_share_name="dpm_sh",
                provider_name="prov",
            )
        )
        p.setup_operation_catalogs()
        calls = [
            c.args
            for c in p.db_ops.create_catalog_using_share_if_not_exists.call_args_list
        ]
        assert calls == [
            ("src", "prov", "sh"),
            ("bk", "prov", "bk_sh"),
            ("dpm", "prov", "dpm_sh"),
        ]
        p.db_ops.get_provider_name.assert_not_called()

    def test_provider_resolved_from_source_metastore(self):
        p = make_provider(
            replication_config=ReplicationConfig(
                source_catalog="src", create_shared_catalog=True, share_name="sh"
            )
        )
        p.source_dbops.get_metastore_id.return_value = "azure:r:id"
        p.db_ops.get_provider_name.return_value = "resolved"
        p.setup_operation_catalogs()
        p.db_ops.get_provider_name.assert_called_once_with("azure:r:id")
        p.db_ops.create_catalog_using_share_if_not_exists.assert_called_once_with(
            "src", "resolved", "sh"
        )

    def test_provider_resolved_from_sharing_identifier(self):
        p = make_provider(
            replication_config=ReplicationConfig(
                source_catalog="src", create_shared_catalog=True, share_name="sh"
            )
        )
        p.source_databricks_config.sharing_identifier = "azure:r:cfg"
        p.setup_operation_catalogs()
        p.db_ops.get_provider_name.assert_called_once_with("azure:r:cfg")
        p.source_dbops.get_metastore_id.assert_not_called()

    def test_volume_config_creates_logging_table(self):
        vc = VolumeFilesReplicationConfig(
            file_ingestion_logging_catalog="logc",
            file_ingestion_logging_schema="logs",
        )
        p = make_provider(
            replication_config=ReplicationConfig(source_catalog="src", volume_config=vc)
        )
        p.setup_operation_catalogs()
        p.db_ops.create_schema_if_not_exists.assert_called_once_with("logc", "logs")
        ddl = sql_calls(p.spark)[0]
        assert (
            "create table if not exists `logc`.`logs`.`detail_file_ingestion_logging`"
            in ddl
        )


def test_logging_table_creates_catalog_when_requested():
    p = make_provider()
    vc = VolumeFilesReplicationConfig(
        create_file_ingestion_logging_catalog=True,
        file_ingestion_logging_catalog="logc",
        file_ingestion_logging_catalog_location="s3://l",
        file_ingestion_logging_schema="logs",
        file_ingestion_logging_table="files",
    )
    p._create_file_ingestion_logging_table(vc)
    p.db_ops.create_catalog_if_not_exists.assert_called_once_with("logc", "s3://l")
    assert "`logc`.`logs`.`files`" in sql_calls(p.spark)[0]


# ---------- process_schema ----------


class TestProcessSchema:
    @pytest.fixture(autouse=True)
    def _base(self, monkeypatch):
        monkeypatch.setattr(BaseProvider, "process_schema", lambda self, sc: ["base"])

    def test_creates_intermediate_and_target_schema(self):
        p = make_provider()
        sc = SchemaConfig(
            schema_name="s",
            replication_config=ReplicationConfig(intermediate_catalog="inter"),
        )
        assert p.process_schema(sc) == ["base"]
        assert [
            c.args for c in p.db_ops.create_schema_if_not_exists.call_args_list
        ] == [
            ("inter", "s"),
            ("tgt", "s"),
        ]

    def test_uc_replication_does_not_create_schemas(self):
        p = make_provider(uc_object_types=[UCObjectType.SCHEMA])
        sc = SchemaConfig(schema_name="s", replication_config=ReplicationConfig())
        assert p.process_schema(sc) == ["base"]
        p.db_ops.create_schema_if_not_exists.assert_not_called()


# ---------- process_table / process_volume dispatch ----------


UC_TABLE_METHODS = [
    "_uc_replicate_ddl",
    "_uc_replicate_sql_st_mv",
    "_uc_replicate_table_comments",
    "_uc_replicate_table_tags",
    "_uc_replicate_table_grants",
    "_uc_replicate_column_comments",
    "_uc_replicate_column_tags",
]


def _stub(p, names):
    for name in names:
        setattr(p, name, MagicMock(return_value=[name]))


class TestProcessTable:
    def test_data_replication_only(self):
        p = make_provider()
        p._replicate_table = MagicMock(return_value=["deep_clone"])
        sc = SchemaConfig(schema_name="s", table_types=[TableType.MANAGED])
        assert p.process_table(sc, table_config()) == ["deep_clone"]
        p.audit_logger.log_results.assert_called_once_with(["deep_clone"])

    def test_no_work_no_audit(self):
        p = make_provider()
        assert p.process_table(SchemaConfig(schema_name="s"), table_config()) == []
        p.audit_logger.log_results.assert_not_called()

    def test_all_on_managed_table(self):
        p = make_provider(uc_object_types=[UCObjectType.ALL])
        p.spark.catalog.tableExists.return_value = True
        p.db_ops.get_table_type.return_value = "MANAGED"
        _stub(p, UC_TABLE_METHODS)
        out = p.process_table(SchemaConfig(schema_name="s"), table_config())
        assert out == [m for m in UC_TABLE_METHODS if m != "_uc_replicate_sql_st_mv"]
        p.db_ops.get_table_type.assert_called_once_with("`src`.`s`.`t`")

    def test_all_on_streaming_table_uses_st_mv(self):
        p = make_provider(uc_object_types=[UCObjectType.ALL])
        p.spark.catalog.tableExists.return_value = True
        p.db_ops.get_table_type.return_value = "STREAMING_TABLE"
        _stub(p, UC_TABLE_METHODS)
        out = p.process_table(SchemaConfig(schema_name="s"), table_config())
        assert "_uc_replicate_sql_st_mv" in out and "_uc_replicate_ddl" not in out

    def test_only_selected_types(self):
        p = make_provider(uc_object_types=[UCObjectType.TABLE_TAG])
        p.spark.catalog.tableExists.return_value = True
        p.db_ops.get_table_type.return_value = "VIEW"
        _stub(p, UC_TABLE_METHODS)
        out = p.process_table(SchemaConfig(schema_name="s"), table_config())
        assert out == ["_uc_replicate_table_tags"]

    def test_missing_source_raises(self):
        p = make_provider(uc_object_types=[UCObjectType.TABLE])
        p.spark.catalog.tableExists.return_value = False
        with pytest.raises(TableNotFoundError):
            p.process_table(SchemaConfig(schema_name="s"), table_config())

    def test_table_name_substituted_in_config(self):
        p = make_provider()
        p._replicate_table = MagicMock(return_value=[])
        tc = table_config(name="orders", intermediate_catalog="inter_{{table_name}}")
        p.process_table(
            SchemaConfig(schema_name="s", table_types=[TableType.MANAGED]), tc
        )
        passed = p._replicate_table.call_args.args[1]
        assert passed.replication_config.intermediate_catalog == "inter_orders"


class TestProcessVolume:
    VOLUME_METHODS = [
        "_uc_replicate_volume",
        "_uc_replicate_volume_tags",
        "_uc_replicate_volume_grants",
        "_replicate_volume_files",
    ]

    def test_all_uc_types(self):
        p = make_provider(uc_object_types=[UCObjectType.ALL])
        _stub(p, self.VOLUME_METHODS)
        out = p.process_volume(SchemaConfig(schema_name="s"), volume_config())
        assert out == self.VOLUME_METHODS[:3]
        p.audit_logger.log_results.assert_called_once()

    def test_volume_files_only(self):
        p = make_provider(volume_types=["managed"])
        _stub(p, self.VOLUME_METHODS)
        out = p.process_volume(SchemaConfig(schema_name="s"), volume_config())
        assert out == ["_replicate_volume_files"]

    def test_nothing_selected(self):
        p = make_provider()
        _stub(p, self.VOLUME_METHODS)
        assert p.process_volume(SchemaConfig(schema_name="s"), volume_config()) == []
        p.audit_logger.log_results.assert_not_called()


# ---------- _build_deep_clone_query ----------


class TestBuildDeepCloneQuery:
    def test_plain(self):
        p = make_provider()
        assert (
            p._build_deep_clone_query("src_t", "tgt_t")
            == "CREATE OR REPLACE TABLE tgt_t DEEP CLONE src_t "
        )

    def test_pipeline_only(self):
        q = make_provider()._build_deep_clone_query("s", "t", "pid")
        assert q.endswith("TBLPROPERTIES ('pipelines.pipelineId'='pid')")

    def test_pipeline_and_parent(self):
        q = make_provider()._build_deep_clone_query("s", "t", "pid", "ptid")
        assert "'pipelines.pipelineId'='pid'" in q
        assert "'spark.sql.internal.pipelines.parentTableId'='ptid'" in q


# ---------- _replicate_table ----------


def _table_provider(source_type="MANAGED", target_exists=True, same_fields=True):
    p = make_provider()
    p.spark.catalog.tableExists.return_value = True
    p.db_ops.get_table_type.return_value = source_type
    if target_exists:
        p.db_ops.get_table_details.return_value = {
            "table_name": "`tgt`.`s`.`t`",
            "is_dlt": False,
            "pipeline_id": None,
            "dlt_type": None,
        }
    fields = iter([["a", "b"], ["a", "b"] if same_fields else ["a"]])
    p.db_ops.get_table_fields.side_effect = lambda t: next(fields)
    return p


class TestReplicateTable:
    def test_direct_deep_clone_success(self):
        p = _table_provider()
        [r] = p._replicate_table("s", table_config())
        assert r.status == "success" and r.object_type == "table"
        assert r.details["step1_query"] == (
            "CREATE OR REPLACE TABLE `tgt`.`s`.`t` DEEP CLONE `src`.`s`.`t` "
        )
        assert r.details["step2_query"] is None
        assert sql_calls(p.spark) == [r.details["step1_query"]]
        assert (r.attempt_number, r.max_attempts) == (1, 2)

    def test_source_missing_is_failed_result(self):
        p = make_provider()
        p.spark.catalog.tableExists.return_value = False
        [r] = p._replicate_table("s", table_config())
        assert r.status == "failed"
        assert "Source table does not exist" in r.error_message

    def test_new_non_dlt_target(self):
        p = _table_provider(target_exists=False)
        p.db_ops.get_table_details.side_effect = [
            TableNotFoundError("missing"),
            {"is_dlt": False},
        ]
        p.spark.catalog.tableExists.side_effect = lambda t: t == "`src`.`s`.`t`"
        [r] = p._replicate_table("s", table_config())
        assert r.status == "success"
        assert r.details["dlt_flag"] is False
        p.db_ops.get_table_fields.assert_not_called()

    def test_dlt_source_requires_existing_target(self):
        p = _table_provider(target_exists=False)
        p.db_ops.get_table_details.side_effect = [
            TableNotFoundError("missing"),
            {"is_dlt": True},
        ]
        [r] = p._replicate_table("s", table_config())
        assert r.status == "failed"
        assert "must exist before replicating" in r.error_message

    def test_schema_mismatch_enforced_fails(self):
        p = _table_provider(same_fields=False)
        [r] = p._replicate_table("s", table_config())
        assert r.status == "failed" and "Schema mismatch" in r.error_message
        p.spark.sql.assert_not_called()

    def test_schema_mismatch_not_enforced_warns_and_clones(self):
        p = _table_provider(same_fields=False)
        [r] = p._replicate_table("s", table_config(enforce_schema=False))
        assert r.status == "success"
        assert any(
            "Schema mismatch" in c.args[0] for c in p.logger.warning.call_args_list
        )

    def test_query_failure_retries_then_fails(self):
        p = _table_provider()
        p.spark.sql.side_effect = RuntimeError("clone failed")
        [r] = p._replicate_table("s", table_config(max_attempts=3))
        assert r.status == "failed"
        assert p.spark.sql.call_count == 3
        assert "Replication failed after 3 attempts" in r.error_message
        assert "clone failed" in r.error_message

    def test_dlt_target_uses_pipeline_properties(self):
        p = _table_provider()
        p.db_ops.get_table_details.return_value = {
            "table_name": "`tgt`.`s`.`__mat_t`",
            "is_dlt": True,
            "pipeline_id": "pid",
            "dlt_type": "dpm",
            "parent_table_id": "ptid",
        }
        [r] = p._replicate_table("s", table_config())
        assert r.status == "success"
        assert r.details["target_table"] == "`tgt`.`s`.`__mat_t`"
        assert r.details["dlt_type"] == "dpm"
        assert "'pipelines.pipelineId'='pid'" in r.details["step1_query"]

    def test_dlt_parent_table_error_is_ignored(self):
        p = _table_provider()
        p.db_ops.get_table_details.return_value = {
            "table_name": "`tgt`.`s`.`t`",
            "is_dlt": True,
            "pipeline_id": "pid",
            "dlt_type": "legacy",
        }
        p.spark.sql.side_effect = RuntimeError("Parent table with id xyz not found")
        [r] = p._replicate_table("s", table_config())
        assert r.status == "success"
        assert p.spark.sql.call_count == 1

    def test_streaming_table_reads_from_dpm_backing_catalog(self):
        p = _table_provider(source_type="STREAMING_TABLE")
        [r] = p._replicate_table("s", table_config(dpm_backing_table_catalog="dpm"))
        assert r.status == "success"
        assert r.details["source_table"] == "`dpm`.`s`.`t`"
        p.db_ops.refresh_table_metadata.assert_called_with("`dpm`.`s`.`t`")

    def test_streaming_table_falls_back_to_backup_catalog(self):
        p = _table_provider(source_type="STREAMING_TABLE")
        p.spark.catalog.tableExists.side_effect = lambda t: t != "`dpm`.`s`.`t`"
        [r] = p._replicate_table(
            "s", table_config(dpm_backing_table_catalog="dpm", backup_catalog="bk")
        )
        assert r.status == "success"
        assert r.details["source_table"] == "`bk`.`s`.`t`"

    def test_streaming_table_missing_from_backup_fails(self):
        p = _table_provider(source_type="STREAMING_TABLE")
        p.spark.catalog.tableExists.side_effect = lambda t: t == "`src`.`s`.`t`"
        [r] = p._replicate_table("s", table_config(backup_catalog="bk"))
        assert r.status == "failed"
        assert "not found in dpm or backup catalog" in r.error_message

    def test_streaming_table_without_backup_catalog_fails(self):
        p = _table_provider(source_type="STREAMING_TABLE")
        [r] = p._replicate_table("s", table_config())
        assert r.status == "failed"
        assert "Backup catalog not specified" in r.error_message

    def test_intermediate_catalog_two_steps(self):
        p = _table_provider()
        [r] = p._replicate_table("s", table_config(intermediate_catalog="inter"))
        assert r.status == "success"
        assert r.details["intermediate_catalog"] == "inter"
        assert r.details["step1_query"] == (
            "CREATE OR REPLACE TABLE inter.s.t DEEP CLONE `src`.`s`.`t` "
        )
        assert r.details["step2_query"].startswith(
            "CREATE OR REPLACE TABLE `tgt`.`s`.`t` DEEP CLONE"
        )
        assert len(sql_calls(p.spark)) == 2

    def test_intermediate_step1_failure_skips_step2(self):
        p = _table_provider()
        p.spark.sql.side_effect = RuntimeError("step1")
        [r] = p._replicate_table(
            "s", table_config(intermediate_catalog="inter", max_attempts=1)
        )
        assert r.status == "failed"
        assert r.details["step2_query"] is None
        assert p.spark.sql.call_count == 1

    def test_external_table_uses_location_mapping(self):
        p = _table_provider(source_type="EXTERNAL")
        p.cloud_url_mapping = {"abfss://c@acct/": "s3://bucket/"}
        p.db_ops.describe_table_detail.return_value = {"location": "abfss://c@acct/t1"}
        [r] = p._replicate_table("s", table_config(intermediate_catalog="ignored"))
        assert r.status == "success"
        assert sql_calls(p.spark) == [
            "CREATE OR REPLACE TABLE delta.`s3://bucket/t1` DEEP CLONE `src`.`s`.`t` ",
            "DROP TABLE IF EXISTS `tgt`.`s`.`t`",
            r.details["step2_query"],
        ]
        assert "LOCATION 's3://bucket/t1'" in r.details["step2_query"]

    def test_external_replicated_as_managed_uses_deep_clone(self):
        p = _table_provider(source_type="EXTERNAL")
        [r] = p._replicate_table("s", table_config(replicate_as_managed=True))
        assert r.status == "success"
        assert r.details["step1_query"].startswith(
            "CREATE OR REPLACE TABLE `tgt`.`s`.`t` DEEP CLONE"
        )
        p.db_ops.describe_table_detail.assert_not_called()


class TestReplicateExternalTable:
    def _op(self, outcomes=None):
        queries = []
        outcomes = iter(outcomes or [])

        def op(q):
            queries.append(q)
            return next(outcomes, (True, None, 1, 1))

        return op, queries

    def test_no_location(self):
        p = make_provider(cloud_url_mapping={"a": "b"})
        p.db_ops.describe_table_detail.return_value = {}
        op, _ = self._op()
        with pytest.raises(Exception, match="does not have a storage location"):
            p._replicate_external_table("s", "t", op, ReplicationConfig())

    def test_no_mapping_configured(self):
        p = make_provider()
        p.db_ops.describe_table_detail.return_value = {"location": "s3://x/t"}
        op, _ = self._op()
        with pytest.raises(Exception, match="cloud_url_mapping is required"):
            p._replicate_external_table("s", "t", op, ReplicationConfig())

    def test_no_matching_mapping(self):
        p = make_provider(cloud_url_mapping={"gs://other/": "s3://b/"})
        p.db_ops.describe_table_detail.return_value = {"location": "s3://x/t"}
        op, _ = self._op()
        with pytest.raises(Exception, match="No external location mapping found"):
            p._replicate_external_table("s", "t", op, ReplicationConfig())

    def test_copy_files_disabled_only_recreates_table(self):
        p = make_provider(cloud_url_mapping={"s3://x/": "s3://y/"})
        p.db_ops.describe_table_detail.return_value = {"location": "s3://x/t"}
        op, queries = self._op()
        out = p._replicate_external_table(
            "src_t", "tgt_t", op, ReplicationConfig(copy_files=False)
        )
        assert out[0] is True
        assert out[4] is None  # no deep clone step
        assert queries[0] == "DROP TABLE IF EXISTS tgt_t"
        assert "LOCATION 's3://y/t'" in queries[1]

    def test_clone_failure_stops(self):
        p = make_provider(cloud_url_mapping={"s3://x/": "s3://y/"})
        p.db_ops.describe_table_detail.return_value = {"location": "s3://x/t"}
        err = RuntimeError("clone")
        op, queries = self._op([(False, err, 1, 1)])
        out = p._replicate_external_table("src_t", "tgt_t", op, ReplicationConfig())
        assert out[:2] == (False, err)
        assert len(queries) == 1

    def test_drop_failure_stops(self):
        p = make_provider(cloud_url_mapping={"s3://x/": "s3://y/"})
        p.db_ops.describe_table_detail.return_value = {"location": "s3://x/t"}
        err = RuntimeError("drop")
        op, queries = self._op([(True, None, 1, 1), (False, err, 1, 1)])
        out = p._replicate_external_table("src_t", "tgt_t", op, ReplicationConfig())
        assert out[:2] == (False, err)
        assert out[5] is None
        assert len(queries) == 2


# ---------- _replicate_volume_files ----------


def _volume_files_provider(total=2, errors=0, terminated=True, **vc_kwargs):
    vc_kwargs.setdefault("file_ingestion_logging_catalog", "logc")
    vc_kwargs.setdefault("file_ingestion_logging_schema", "logs")
    p = make_provider()
    p.db_ops.if_volume_exists.return_value = True
    p.db_ops.get_volume_type.return_value = "MANAGED"

    stream_df = MagicMock(name="stream_df")
    p.spark.readStream.format.return_value.options.return_value.load.return_value.select.return_value = stream_df
    query = MagicMock(name="query")
    query.awaitTermination.return_value = terminated
    writer = stream_df.writeStream.foreachBatch.return_value
    writer.option.return_value.trigger.return_value.start.return_value = query

    def sql(q):
        if "status = 'error'" in q:
            return df([[errors]])
        return df([[total]])

    p.spark.sql.side_effect = sql
    vfc = VolumeFilesReplicationConfig(**vc_kwargs)
    return p, stream_df, query, volume_config(volume_config=vfc)


class TestReplicateVolumeFiles:
    def test_success_reports_counts_and_paths(self):
        p, stream_df, query, vc = _volume_files_provider(total=4, errors=1)
        [r] = p._replicate_volume_files("s", vc)
        assert r.status == "success" and r.object_type == "volume"
        assert r.details["source_path"] == "/Volumes/src/s/v"
        assert r.details["target_path"] == "/Volumes/tgt/s/v"
        assert r.details["checkpoint_path"] == "/Volumes/tgt/s/v/_checkpoints"
        assert (r.details["total_count"], r.details["error_count"]) == (4, 1)
        p.spark.readStream.format.assert_called_once_with("cloudFiles")
        options = p.spark.readStream.format.return_value.options.call_args.kwargs
        assert options == {"cloudFiles.format": "binaryFile"}
        stream_df.writeStream.foreachBatch.return_value.option.assert_called_once_with(
            "checkpointLocation", "/Volumes/tgt/s/v/_checkpoints"
        )
        p.spark.conf.set.assert_called_once_with(
            "spark.databricks.execution.timeout", 43200
        )

    def test_folder_path_and_autoloader_options(self):
        p, _, _, vc = _volume_files_provider(
            folder_path="/raw/2024/",
            autoloader_options={"cloudFiles.maxFilesPerTrigger": "10"},
        )
        [r] = p._replicate_volume_files("s", vc)
        assert r.details["source_path"] == "/Volumes/src/s/v/raw/2024/"
        assert r.details["target_path"] == "/Volumes/tgt/s/v/raw/2024/"
        assert r.details["checkpoint_path"] == "/Volumes/tgt/s/v/_checkpoints/raw/2024/"
        options = p.spark.readStream.format.return_value.options.call_args.kwargs
        assert options == {
            "cloudFiles.maxFilesPerTrigger": "10",
            "cloudFiles.format": "binaryFile",
        }

    def test_delete_and_reload_removes_checkpoint_and_target(self):
        p, _, _, vc = _volume_files_provider(delete_and_reload=True)
        p.target_workspace_client.dbutils.fs.rm.side_effect = [
            None,
            RuntimeError("gone"),
        ]
        [r] = p._replicate_volume_files("s", vc)
        assert r.status == "success"
        assert [
            c.args for c in p.target_workspace_client.dbutils.fs.rm.call_args_list
        ] == [
            ("/Volumes/tgt/s/v/_checkpoints", True),
            ("/Volumes/tgt/s/v", True),
        ]
        assert p.logger.warning.called  # second rm failure is logged, not raised

    def test_copy_files_batch_logs_each_file(self, monkeypatch):
        p, stream_df, _, vc = _volume_files_provider()
        p._replicate_volume_files("s", vc)
        copy_files = stream_df.writeStream.foreachBatch.call_args.args[0]

        commands = []
        monkeypatch.setattr(os, "makedirs", lambda *a, **k: None)
        monkeypatch.setattr(
            os, "system", lambda cmd: commands.append(cmd) or (1 if "bad" in cmd else 0)
        )
        batch = MagicMock()
        batch.collect.return_value = [
            {
                "path": "dbfs:/Volumes/src/s/v/a/good.csv",
                "length": 10,
                "modificationTime": "t1",
            },
            {"path": "/Volumes/src/s/v/bad.csv", "length": 5, "modificationTime": "t2"},
        ]
        copy_files(batch, 7)

        assert sorted(commands) == [
            'cp "/Volumes/src/s/v/a/good.csv" "/Volumes/tgt/s/v/a/good.csv"',
            'cp "/Volumes/src/s/v/bad.csv" "/Volumes/tgt/s/v/bad.csv"',
        ]
        inserts = sorted(c.args[0] for c in batch.sparkSession.sql.call_args_list)
        assert len(inserts) == 2
        assert all(
            "INSERT INTO `logc`.`logs`.`detail_file_ingestion_logging`" in q
            for q in inserts
        )
        bad = next(q for q in inserts if "bad.csv" in q)
        assert "'error'" in bad and "Copy failed with exit code 1" in bad
        good = next(q for q in inserts if "good.csv" in q)
        assert "'success'" in good and "'7'" in good

    def test_missing_source_volume_returns_failed_result(self):
        p, _, _, vc = _volume_files_provider()
        p.db_ops.if_volume_exists.return_value = False
        [r] = p._replicate_volume_files("s", vc)
        assert r.status == "failed"

    def test_stream_timeout_returns_failed_result(self):
        p, _, _, vc = _volume_files_provider(terminated=False)
        [r] = p._replicate_volume_files("s", vc)
        assert r.status == "failed"
