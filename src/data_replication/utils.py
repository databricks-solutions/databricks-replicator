"""
Retry utilities for data replication operations.

This module provides retry functionality with exponential backoff
and configurable retry strategies.
"""

from copy import deepcopy
import os
import time
from functools import wraps
from typing import Dict, Any, TypeVar, Optional
from enum import Enum
from tenacity import RetryError, retry, wait_exponential, stop_after_attempt
from pydantic import BaseModel
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient


T = TypeVar("T", bound=BaseModel)


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=10, max=60))
def get_token_from_secret(secret_config, workspace_client, auth_type):
    """Retrieve the authentication token from Databricks secrets."""
    if auth_type.lower() == "oauth":
        client_id = workspace_client.dbutils.secrets.get(
            secret_config.secret_scope,
            secret_config.secret_client_id,
        )
        client_secret = workspace_client.dbutils.secrets.get(
            secret_config.secret_scope,
            secret_config.secret_client_secret,
        )
        return client_id, client_secret
    # Default to PAT
    return workspace_client.dbutils.secrets.get(
        secret_config.secret_scope,
        secret_config.secret_pat,
    )


def set_envs(
    host: str,
    secret_config=None,
    cluster_id=None,
    workspace_client=None,
    auth_type=None,
):
    """Set environment variables for Databricks connection."""
    if not host:
        raise ValueError("Host URL must be provided to set environment variables.")
    # Clear any existing environment variables to avoid conflicts
    os.environ.pop("DATABRICKS_HOST", None)
    os.environ.pop("DATABRICKS_TOKEN", None)
    os.environ.pop("DATABRICKS_CLIENT_ID", None)
    os.environ.pop("DATABRICKS_CLIENT_SECRET", None)
    os.environ.pop("DATABRICKS_CLUSTER_ID", None)

    client_id = None
    client_secret = None
    pat = None
    if secret_config:
        if auth_type.lower() == "oauth":
            client_id, client_secret = get_token_from_secret(
                secret_config, workspace_client, auth_type
            )
            os.environ["DATABRICKS_CLIENT_ID"] = client_id
            os.environ["DATABRICKS_CLIENT_SECRET"] = client_secret
        else:
            pat = get_token_from_secret(secret_config, workspace_client, auth_type)
            os.environ["DATABRICKS_TOKEN"] = pat
    if cluster_id:
        os.environ["DATABRICKS_CLUSTER_ID"] = cluster_id
    os.environ["DATABRICKS_HOST"] = host


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=10, max=60))
def create_spark_session(
    host: str,
    secret_config=None,
    cluster_id=None,
    workspace_client=None,
    auth_type=None,
) -> DatabricksSession:
    """Create a Databricks Spark session using the provided host and token."""
    set_envs(
        host=host,
        secret_config=secret_config,
        cluster_id=cluster_id,
        workspace_client=workspace_client,
        auth_type=auth_type,
    )
    try:
        # Create Databricks session with default compute
        spark = DatabricksSession.builder.remote(host=host).getOrCreate()
    except Exception:
        # Fallback on Creating Databricks session with serverless compute
        spark = (
            DatabricksSession.builder.remote(host=host).serverless(True).getOrCreate()
        )
    return spark


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=10, max=60))
def create_workspace_client(
    host: str,
    secret_config=None,
    workspace_client=None,
    auth_type=None,
) -> WorkspaceClient:
    """Create a Databricks workspace client using the provided host and token."""

    set_envs(
        host=host,
        secret_config=secret_config,
        workspace_client=workspace_client,
        auth_type=auth_type,
    )

    return WorkspaceClient(host=host)


def get_spark_workspace_url(spark: DatabricksSession) -> str:
    """Get the workspace URL from the Spark session configuration."""
    return spark.conf.get("spark.databricks.workspaceUrl")


def validate_spark_session(spark: DatabricksSession, workspace_url: str) -> bool:
    """Validate if the Spark session is connected to the right Databricks workspace."""
    spark_workspace_url = get_spark_workspace_url(spark)
    return spark_workspace_url == workspace_url


def get_spark_current_user(spark: DatabricksSession) -> str:
    """Get the current user from the Spark session configuration."""
    # Get current execution user using Spark SQL
    execution_user = spark.sql("SELECT current_user() as user").collect()[0]["user"]

    return execution_user


def get_workspace_url_from_host(host: str) -> str:
    """Extract the workspace URL from the host."""
    return host.replace("https://", "").replace("http://", "").split("/")[0]


def retry_with_logging(retry_config, logger=None):
    """
    Decorator for retrying operations with logging.

    Args:
        retry_config: RetryConfig object with retry settings
        logger: Optional logger for logging attempts

    Returns:
        Decorated function with retry logic and logging
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = retry_config.retry_delay_seconds

            for attempt in range(1, retry_config.max_attempts + 1):
                try:
                    if logger and hasattr(logger, "debug"):
                        logger.debug(
                            f"Attempting {func.__name__} (attempt {attempt}/{retry_config.max_attempts})"
                        )

                    result = func(*args, **kwargs)

                    # logger.info(
                    #     f"{func.__name__} succeeded on attempt {attempt}/{retry_config.max_attempts}"
                    # )

                    return result, None, attempt, retry_config.max_attempts

                except Exception as e:
                    last_exception = e

                    if logger and hasattr(logger, "warning"):
                        logger.warning(
                            f"{func.__name__} failed on attempt {attempt}/{retry_config.max_attempts}: {str(e)}"
                        )

                    if attempt < retry_config.max_attempts:
                        if logger and hasattr(logger, "debug"):
                            logger.debug(
                                f"Waiting {current_delay:.1f}s before retry..."
                            )
                        time.sleep(current_delay)
                        current_delay *= 2.0  # Exponential backoff
                        if logger and hasattr(logger, "info"):
                            logger.info(
                                f"{func.__name__} failed on attempt {attempt}/{retry_config.max_attempts}"
                            )
                    else:
                        if logger and hasattr(logger, "error"):
                            logger.error(
                                f"{func.__name__} failed after {retry_config.max_attempts} attempts"
                            )

            return False, last_exception, attempt, retry_config.max_attempts

        return wrapper

    return decorator


def merge_maps(source_maps: list, target_maps: list, overwrite: bool) -> dict:
    """Merge source and target maps based on overwrite setting."""
    merged_maps = (
        {k: v for d in source_maps for k, v in d.items() if v is not None}
        if source_maps
        else {}
    )
    if not overwrite and target_maps:
        existing_maps = {
            k: v for d in target_maps for k, v in d.items() if v is not None
        }
        merged_maps = {**merged_maps, **existing_maps}
    return merged_maps


def filter_common_maps(source_maps: list, target_maps: list) -> tuple:
    """Get uncommon maps from source compared to target maps."""
    if source_maps is None or target_maps is None:
        return source_maps, target_maps
    source_set = set(frozenset(d.items()) for d in source_maps)
    target_set = set(frozenset(d.items()) for d in target_maps)

    common = source_set & target_set

    source_maps = [d for d in source_maps if frozenset(d.items()) not in common]
    target_maps = [d for d in target_maps if frozenset(d.items()) not in common]
    return source_maps, target_maps


def merge_dicts_recursive(
    base_dict: Dict[str, Any], update_dict: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Recursively merge two dictionaries, considering update fields set.

    Args:
        base_dict: The base dictionary to merge into
        update_dict: The dictionary containing updates to merge

    Returns:
        A new merged dictionary

    Notes:
        - Values from update_dict take precedence over base_dict
        - Values in update_dict are ignored if not in update_fields_set
        - Nested dictionaries are recursively merged
        - Lists are replaced entirely (not merged element-wise)
    """
    if base_dict is None and update_dict is None:
        return None
    if base_dict is None:
        return {k: v for k, v in update_dict.items() if v is not None}
    if update_dict is None:
        return base_dict.copy()
    for key in update_dict:
        if (
            key in base_dict
            and isinstance(base_dict.get(key, None), dict)
            and isinstance(update_dict.get(key, None), dict)
        ):
            merge_dicts_recursive(base_dict[key], update_dict[key])
        elif (
            key in base_dict
            and issubclass(type(base_dict.get(key, None)), BaseModel)
            and issubclass(type(update_dict.get(key, None)), BaseModel)
        ):
            merge_models_recursive(base_dict.get(key, None), update_dict.get(key, None))
        else:
            if base_dict.get(key, None) is not None:
                base_dict[key] = (
                    update_dict[key]
                    if update_dict.get(key, None) is not None
                    else base_dict[key]
                )
            elif update_dict.get(key, None) is not None:
                base_dict[key] = update_dict[key]
    return base_dict


def merge_models_recursive(
    base_model: Optional[BaseModel],
    update_model: Optional[BaseModel],
) -> Optional[T]:
    """
    Merge two Pydantic models recursively, ignoring None values.

    Args:
        base_model: The base model to merge into (can be None)
        update_model: The model containing updates to merge (can be None)
        model_type: The Pydantic model class type

    Returns:
        A new merged model instance, or None if both inputs are None
    """
    if base_model is None and update_model is None:
        return None

    model_type = type(update_model)
    if base_model is None:
        return model_type(**update_model.model_dump())
    if update_model is None:
        return model_type(**base_model.model_dump())
    base_model_copy = deepcopy(base_model)
    base_model = model_type.model_construct(**base_model.model_dump())
    for field_name, _ in update_model:
        if (
            field_name in base_model_copy.model_fields_set
            and issubclass(type(getattr(base_model_copy, field_name, None)), BaseModel)
            and issubclass(type(getattr(update_model, field_name, None)), BaseModel)
        ):
            setattr(
                base_model,
                field_name,
                merge_models_recursive(
                    getattr(base_model_copy, field_name, None),
                    getattr(update_model, field_name, None),
                ),
            )
        elif (
            field_name in base_model_copy.model_fields_set
            and isinstance(getattr(base_model_copy, field_name, None), dict)
            and isinstance(getattr(update_model, field_name, None), dict)
        ):
            setattr(
                base_model,
                field_name,
                merge_dicts_recursive(
                    getattr(base_model_copy, field_name, None),
                    getattr(update_model, field_name, None),
                ),
            )
        else:
            if getattr(base_model_copy, field_name, None) is not None:
                setattr(
                    base_model,
                    field_name,
                    (
                        getattr(update_model, field_name, None)
                        if field_name in update_model.model_fields_set
                        else getattr(base_model_copy, field_name, None)
                    ),
                )
            elif getattr(update_model, field_name, None) is not None:
                setattr(base_model, field_name, getattr(update_model, field_name, None))

    # Create and return new model instance
    return base_model


def map_cloud_url(source_storage_root: str, cloud_url_mapping: dict) -> Optional[str]:
    """
    Map a source storage root to a target storage root using external location mapping.

    Args:
        source_storage_root: Source storage root path
        cloud_url_mapping: Dictionary mapping source to target cloud URLs

    Returns:
        Mapped target storage root path, or None if no mapping found
    """
    if not source_storage_root or not cloud_url_mapping:
        return None

    # Find matching source external location
    for src_location, tgt_location in cloud_url_mapping.items():
        if source_storage_root.startswith(src_location):
            # Calculate relative path and construct target location
            relative_path = source_storage_root[len(src_location) :].lstrip("/")
            target_storage_root = (
                f"{tgt_location.rstrip('/')}/{relative_path}"
                if relative_path
                else tgt_location
            )
            return target_storage_root

    return None


def map_user(source_user: str, user_mapping: dict) -> Optional[str]:
    """
    Map a source user to a target user using user mapping.

    Args:
        source_user: Source user name
        user_mapping: Dictionary mapping source to target users

    Returns:
        Mapped target user name, or None if no mapping found
    """
    if not source_user or not user_mapping:
        return source_user

    # Find matching source external location
    for src_user, tgt_user in user_mapping.items():
        if src_user == source_user:
            return tgt_user
    return source_user


def recursive_substitute(obj, substitute_value, target_pattern=""):
    """
    Recursively substitute all occurrences of target_pattern in strings with substitute_value.

    Args:
        obj: The object to process (dict, BaseModel, list, or primitive)
        substitute_value: The value to substitute target_pattern with
        target_pattern: The pattern to search for and replace (default: "value_1")

    Returns:
        The processed object with substitutions made
    """
    if obj is None:
        return obj

    if isinstance(obj, Enum):
        # Don't modify enum values - return as-is
        return obj

    if isinstance(obj, str):
        return obj.replace(target_pattern, str(substitute_value))

    if isinstance(obj, BaseModel):
        # Create a new instance with substituted values
        # model_dict_all = obj.model_dump()
        # model_dict_field_set = {
        #     k: v for k, v in model_dict_all.items() if k in obj.model_fields_set
        # }
        for field_name, field_value in obj:
            if field_name in obj.model_fields_set:
                setattr(
                    obj,
                    field_name,
                    recursive_substitute(field_value, substitute_value, target_pattern),
                )
            continue
        return obj

    if isinstance(obj, dict):
        return {
            key: recursive_substitute(value, substitute_value, target_pattern)
            for key, value in obj.items()
        }

    if isinstance(obj, list):
        return [
            recursive_substitute(item, substitute_value, target_pattern) for item in obj
        ]

    if isinstance(obj, tuple):
        return tuple(
            recursive_substitute(item, substitute_value, target_pattern) for item in obj
        )

    # Return primitive types as-is (int, float, bool, etc.)
    return obj

def unwrap_retry_error(error: Exception) -> str:
    """
    Unwrap RetryError to get the actual underlying exception message.
    
    Args:
        error: The exception to unwrap
        
    Returns:
        String representation of the underlying error
    """
    if isinstance(error, RetryError):
        # Get the last attempt's exception
        if hasattr(error, 'last_attempt') and error.last_attempt:
            if hasattr(error.last_attempt, 'exception'):
                return str(error.last_attempt.exception())
            elif hasattr(error.last_attempt, 'result'):
                return str(error.last_attempt.result())
    return str(error)