"""
global constants
"""

from typing import Tuple

DICT_FOR_CREATION_CATALOG = {
    "name": None,
    "comment": None,
    "connection_name": None,
    "options": None,
    "properties": None,
    "provider_name": None,
    "share_name": None,
    "storage_root": None,
}

DICT_FOR_UPDATE_CATALOG = {
    "name": None,
    "comment": None,
    "enable_predictive_optimization": None,
    "isolation_mode": None,
    "options": None,
    "properties": None,
}

DICT_FOR_CREATION_SCHEMA = {
    "catalog_name": None,
    "name": None,
    "comment": None,
    "properties": None,
    "storage_root": None,
}

DICT_FOR_UPDATE_SCHEMA = {
    "full_name": None,
    "comment": None,
    "enable_predictive_optimization": None,
    "properties": None,
}

DICT_FOR_CREATION_STORAGE_CREDENTIAL = {
    "name": None,
    "comment": None,
    "read_only": None,
    "skip_validation": None,
}

DICT_FOR_UPDATE_STORAGE_CREDENTIAL = {
    "name": None,
    "comment": None,
    "force": None,
    "isolation_mode": None,
    "read_only": None,
    "skip_validation": None,
}

DICT_FOR_CREATION_TAG_POLICY = {
    "tag_key": None,
    "description": None,
    "values": None,
}

DICT_FOR_UPDATE_TAG_POLICY = {
    "tag_key": None,
    "description": None,
    "values": None,
}

DICT_FOR_CREATION_EXTERNAL_LOCATION = {
    "name": None,
    "comment": None,
    "credential_name": None,
    "url": None,
    "fallback": None,
    "read_only": None,
    "skip_validation": None,
}

DICT_FOR_UPDATE_EXTERNAL_LOCATION = {
    "name": None,
    "comment": None,
    "credential_name": None,
    "url": None,
    "fallback": None,
    "force": None,
    "isolation_mode": None,
    "read_only": None,
    "skip_validation": None,
}

DICT_FOR_CREATION_VOLUME = {
    "catalog_name": None,
    "schema_name": None,
    "name": None,
    "volume_type": None,
    "storage_location": None,
    "comment": None,
}

DICT_FOR_UPDATE_VOLUME = {
    "full_name": None,
    "comment": None,
}

SKIP_PROCESSED_TABLES = True

# RunResult error code for a table skipped because predictive optimization is
# ENABLED on the existing managed target table (predictive_optimization_check=fail).
PO_ENABLED_ON_TARGET = "PO_ENABLED_ON_TARGET"

# Table properties that are derived by the Delta engine and cannot (or should
# not) be replayed in a CREATE statement. Matching is case-insensitive. Entries
# ending in "*" match any key with that prefix.
UNREPLAYABLE_TBLPROPERTIES: Tuple[str, ...] = (
    # Protocol versions are derived from the enabled table features.
    "delta.minReaderVersion",
    "delta.minWriterVersion",
    # Row tracking: the materialized column names are assigned by the engine.
    "delta.rowTracking.materializedRowIdColumnName",
    "delta.rowTracking.materializedRowCommitVersionColumnName",
    # Column mapping: the max column id is maintained by the engine.
    "delta.columnMapping.maxColumnId",
    # Liquid clustering columns are expressed via CLUSTER BY instead.
    "clusteringColumns",
    # Commit metadata surfaced on some (e.g. shared) tables.
    "delta.lastCommitTimestamp",
    "delta.lastUpdateVersion",
)
