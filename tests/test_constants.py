"""Tests for constant shape contracts."""

from data_replication import constants


def test_creation_catalog_keys():
    expected = {
        "name",
        "comment",
        "connection_name",
        "options",
        "properties",
        "provider_name",
        "share_name",
        "storage_root",
    }
    assert set(constants.DICT_FOR_CREATION_CATALOG) == expected


def test_update_catalog_keys():
    expected = {
        "name",
        "comment",
        "enable_predictive_optimization",
        "isolation_mode",
        "options",
        "properties",
    }
    assert set(constants.DICT_FOR_UPDATE_CATALOG) == expected


def test_storage_credential_dicts():
    assert set(constants.DICT_FOR_CREATION_STORAGE_CREDENTIAL) == {
        "name",
        "comment",
        "read_only",
        "skip_validation",
    }
    assert set(constants.DICT_FOR_UPDATE_STORAGE_CREDENTIAL) == {
        "name",
        "comment",
        "force",
        "isolation_mode",
        "read_only",
        "skip_validation",
    }


def test_external_location_dicts():
    create_keys = set(constants.DICT_FOR_CREATION_EXTERNAL_LOCATION)
    update_keys = set(constants.DICT_FOR_UPDATE_EXTERNAL_LOCATION)
    assert "credential_name" in create_keys
    assert "url" in create_keys
    assert create_keys.issubset(update_keys)  # update is a superset


def test_volume_dicts():
    assert set(constants.DICT_FOR_CREATION_VOLUME) >= {
        "catalog_name",
        "schema_name",
        "name",
        "volume_type",
    }
    assert set(constants.DICT_FOR_UPDATE_VOLUME) == {"full_name", "comment"}


def test_skip_processed_tables_flag():
    assert constants.SKIP_PROCESSED_TABLES is True
