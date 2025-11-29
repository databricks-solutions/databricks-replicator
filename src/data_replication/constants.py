"""
global constants
"""

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
