"""Shared fixtures for tests."""

from __future__ import annotations

import logging
from typing import Any, Dict

import pytest


@pytest.fixture
def valid_system_config_dict() -> Dict[str, Any]:
    """Minimal valid dict for ReplicationSystemConfig with a single target catalog."""
    return {
        "version": "1.0",
        "replication_group": "test_group",
        "source_databricks_connect_config": {
            "name": "src",
            "host": "https://src.example.com",
            "auth_type": "pat",
            "token": {"secret_scope": "scope", "secret_pat": "pat_key"},
        },
        "target_databricks_connect_config": {
            "name": "tgt",
            "host": "https://tgt.example.com",
            "auth_type": "pat",
            "token": {"secret_scope": "scope", "secret_pat": "pat_key"},
        },
        "audit_config": {"audit_table": "cat.sch.tbl"},
        "target_catalogs": [{"catalog_name": "cat1"}],
    }


@pytest.fixture
def test_logger() -> logging.Logger:
    return logging.getLogger("tests.shared")
