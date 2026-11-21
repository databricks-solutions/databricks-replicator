"""Shared offline fakes for ReplicationProvider tests (not a test module)."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock

from data_replication.config.models import (
    ReplicationConfig,
    RetryConfig,
    TableConfig,
    TargetCatalogConfig,
    VolumeConfig,
)
from data_replication.providers.replication_provider import ReplicationProvider


def df(rows=None, empty=None, count=None):
    """Minimal DataFrame stand-in with chainable transforms."""
    rows = rows if rows is not None else []
    d = MagicMock()
    d.collect.return_value = rows
    d.isEmpty.return_value = (not rows) if empty is None else empty
    d.count.return_value = len(rows) if count is None else count
    for m in ("filter", "select", "selectExpr", "join", "withColumnRenamed"):
        getattr(d, m).return_value = d
    return d


def make_provider(uc_object_types=None, cloud_url_mapping=None, **catalog_kwargs):
    """ReplicationProvider with __init__ bypassed and every dependency mocked."""
    p = ReplicationProvider.__new__(ReplicationProvider)
    p.logger = MagicMock()
    p.run_id = "run-1"
    p.catalog_config = TargetCatalogConfig(
        catalog_name="tgt",
        uc_object_types=uc_object_types,
        replication_config=catalog_kwargs.pop(
            "replication_config", ReplicationConfig(enabled=True, source_catalog="src")
        ),
        **catalog_kwargs,
    )
    p.spark = MagicMock(name="spark")
    p.source_spark = MagicMock(name="source_spark")
    p.target_spark = MagicMock(name="target_spark")
    p.db_ops = MagicMock(name="db_ops")
    p.source_dbops = MagicMock(name="source_dbops")
    p.target_dbops = MagicMock(name="target_dbops")
    p.workspace_client = MagicMock(name="workspace_client")
    p.target_workspace_client = MagicMock(name="target_workspace_client")
    p.source_databricks_config = SimpleNamespace(
        host="https://src", sharing_identifier=None
    )
    p.target_databricks_config = SimpleNamespace(
        host="https://tgt", warehouse_id="wh-1"
    )
    p.cloud_url_mapping = cloud_url_mapping if cloud_url_mapping is not None else {}
    p.audit_logger = MagicMock(name="audit_logger")
    return p


def table_config(name="t", max_attempts=2, **replication_kwargs):
    replication_kwargs.setdefault("source_catalog", "src")
    return TableConfig(
        table_name=name,
        replication_config=ReplicationConfig(**replication_kwargs),
        retry=RetryConfig(max_attempts=max_attempts, retry_delay_seconds=1),
    )


def volume_config(name="v", max_attempts=1, **replication_kwargs):
    replication_kwargs.setdefault("source_catalog", "src")
    return VolumeConfig(
        volume_name=name,
        replication_config=ReplicationConfig(**replication_kwargs),
        retry=RetryConfig(max_attempts=max_attempts, retry_delay_seconds=1),
    )


def sql_calls(spark):
    return [c.args[0] for c in spark.sql.call_args_list]
