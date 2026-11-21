"""Smoke test for the GCP branch of storage credential replication.

Covers: create is invoked with skip_validation=True, a
DatabricksGcpServiceAccountRequest is attached, and the post-create warning
log carries the auto-provisioned service account email and the gcloud
bucket-binding command the user needs to run.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.catalog import (
    DatabricksGcpServiceAccountRequest,
    DatabricksGcpServiceAccountResponse,
    ExternalLocationInfo,
    StorageCredentialInfo,
)

from data_replication.config.models import (
    CredentialType,
    ExternalLocationConfig,
    StorageCredentialConfig,
)
from data_replication.providers.provider_factory import ProviderFactory


@pytest.fixture
def factory(caplog):
    caplog.set_level(logging.DEBUG)

    source_info = StorageCredentialInfo(
        name="src_gcp_cred",
        comment="source comment",
        read_only=False,
    )
    created_info = StorageCredentialInfo(
        name="src_gcp_cred",
        databricks_gcp_service_account=DatabricksGcpServiceAccountResponse(
            email="db-auto-sa@databricks-proj.iam.gserviceaccount.com",
        ),
    )

    source_ops = MagicMock()
    source_ops.get_storage_credential.return_value = source_info

    target_ops = MagicMock()
    target_ops.get_storage_credential.side_effect = Exception("not found")
    target_ops.create_storage_credential.return_value = created_info

    cfg = MagicMock()
    cfg.storage_credential_config = StorageCredentialConfig(
        target_credential_type=CredentialType.GCP_SERVICE_ACCOUNT,
        mapping={"src_gcp_cred": "ignored"},
    )

    instance = ProviderFactory.__new__(ProviderFactory)
    instance.config = cfg
    instance.source_db_ops = source_ops
    instance.target_db_ops = target_ops
    instance.logger = logging.getLogger("test.gcp_storage_credential")
    instance.audit_logger = MagicMock()
    instance.run_id = "test-run"
    instance.completed_run_results = []
    return instance, source_ops, target_ops


def test_gcp_create_skips_validation_and_warns(factory, caplog):
    instance, _source_ops, target_ops = factory

    results = instance._replicate_storage_credentials()

    assert len(results) == 1
    assert results[0].status == "success"

    target_ops.create_storage_credential.assert_called_once()
    call_args = target_ops.create_storage_credential.call_args.args[0]

    assert call_args["skip_validation"] is True
    assert call_args["name"] == "src_gcp_cred"
    assert isinstance(
        call_args["databricks_gcp_service_account"],
        DatabricksGcpServiceAccountRequest,
    )

    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning_records, "expected a WARNING log after GCP create"
    warning_msg = warning_records[-1].getMessage()
    assert "db-auto-sa@databricks-proj.iam.gserviceaccount.com" in warning_msg
    assert "gcloud storage buckets add-iam-policy-binding" in warning_msg
    assert "roles/storage.objectAdmin" in warning_msg


def test_gcp_external_location_skips_validation_and_warns(caplog):
    caplog.set_level(logging.DEBUG)

    source_ext_loc = ExternalLocationInfo(
        name="src_ext_loc",
        url="gs://source-bucket/path/",
        credential_name="src_gcp_cred",
        read_only=False,
    )

    source_ops = MagicMock()
    source_ops.get_external_location.return_value = source_ext_loc

    target_ops = MagicMock()
    target_ops.get_external_location.side_effect = Exception("not found")
    target_ops.create_external_location.return_value = source_ext_loc

    cfg = MagicMock()
    cfg.cloud_url_mapping = {"gs://source-bucket/": "gs://target-bucket/"}
    cfg.external_location_config = ExternalLocationConfig(
        external_locations=["src_ext_loc"],
    )
    cfg.storage_credential_config = StorageCredentialConfig(
        target_credential_type=CredentialType.GCP_SERVICE_ACCOUNT,
        mapping={"src_gcp_cred": "ignored"},
    )

    instance = ProviderFactory.__new__(ProviderFactory)
    instance.config = cfg
    instance.source_db_ops = source_ops
    instance.target_db_ops = target_ops
    instance.logger = logging.getLogger("test.gcp_external_location")
    instance.audit_logger = MagicMock()
    instance.run_id = "test-run"
    instance.completed_run_results = []

    results = instance._replicate_external_locations()

    assert len(results) == 1
    assert results[0].status == "success", results[0]

    target_ops.create_external_location.assert_called_once()
    call_args = target_ops.create_external_location.call_args.args[0]
    assert call_args["skip_validation"] is True
    assert call_args["name"] == "src_ext_loc"
    assert call_args["url"] == "gs://target-bucket/path/"

    warning_records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert warning_records, "expected a WARNING log after GCP external location create"
    warning_msg = warning_records[-1].getMessage()
    assert "gs://target-bucket/path/" in warning_msg
    assert "gcloud storage buckets add-iam-policy-binding" in warning_msg


def test_non_gcp_external_location_validates(caplog):
    caplog.set_level(logging.DEBUG)

    source_ext_loc = ExternalLocationInfo(
        name="src_ext_loc",
        url="s3://source-bucket/path/",
        credential_name="src_aws_cred",
        read_only=False,
    )

    source_ops = MagicMock()
    source_ops.get_external_location.return_value = source_ext_loc

    target_ops = MagicMock()
    target_ops.get_external_location.side_effect = Exception("not found")
    target_ops.create_external_location.return_value = source_ext_loc

    cfg = MagicMock()
    cfg.cloud_url_mapping = {"s3://source-bucket/": "s3://target-bucket/"}
    cfg.external_location_config = ExternalLocationConfig(
        external_locations=["src_ext_loc"],
    )
    cfg.storage_credential_config = StorageCredentialConfig(
        target_credential_type=CredentialType.AWS,
        mapping={"src_aws_cred": "arn:aws:iam::123:role/r"},
    )

    instance = ProviderFactory.__new__(ProviderFactory)
    instance.config = cfg
    instance.source_db_ops = source_ops
    instance.target_db_ops = target_ops
    instance.logger = logging.getLogger("test.aws_external_location")
    instance.audit_logger = MagicMock()
    instance.run_id = "test-run"
    instance.completed_run_results = []

    results = instance._replicate_external_locations()

    assert results[0].status == "success", results[0]
    call_args = target_ops.create_external_location.call_args.args[0]
    assert call_args["skip_validation"] is False
