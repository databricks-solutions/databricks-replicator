"""ReplicationProvider records the source and target versions a deep clone produced.

Offline: the provider comes from tests/_replication_fakes.make_provider and the
target DatabricksOperations is a mock.
"""

from __future__ import annotations

from tests._replication_fakes import make_provider, table_config


def _deep_clone_provider(clone=None):
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
    p.db_ops.get_latest_clone_commit.return_value = clone
    return p


def test_success_records_clone_source_and_target_versions():
    clone = {
        "target_version": 1,
        "source_table": "src.s.t",
        "source_version": 9,
    }
    p = _deep_clone_provider(clone)
    [r] = p._replicate_table("s", table_config())
    assert r.status == "success"
    assert r.details["replicated_versions"] == {
        "target_table": "`tgt`.`s`.`t`",
        "target_version": 1,
        "source_table": "src.s.t",
        "source_version": 9,
    }
    p.db_ops.get_latest_clone_commit.assert_called_once_with("`tgt`.`s`.`t`")


def test_intermediate_records_the_table_actually_cloned_from():
    # Step 2 reads the intermediate copy, so the recorded pair describes that table.
    clone = {
        "target_version": 0,
        "source_table": "inter.s.t",
        "source_version": 0,
    }
    p = _deep_clone_provider(clone)
    [r] = p._replicate_table("s", table_config(intermediate_catalog="inter"))
    assert r.status == "success"
    assert r.details["replicated_versions"]["source_table"] == "inter.s.t"


def test_history_read_failure_does_not_fail_replication():
    p = _deep_clone_provider()
    p.db_ops.get_latest_clone_commit.side_effect = RuntimeError("history denied")
    [r] = p._replicate_table("s", table_config())
    assert r.status == "success"
    assert r.details["replicated_versions"] is None
    p.logger.warning.assert_called_once()


def test_no_clone_commit_records_none():
    p = _deep_clone_provider(None)
    [r] = p._replicate_table("s", table_config())
    assert r.status == "success"
    assert r.details["replicated_versions"] is None
