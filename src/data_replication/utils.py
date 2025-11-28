"""
Retry utilities for data replication operations.

This module provides retry functionality with exponential backoff
and configurable retry strategies.
"""

import os
import time
from functools import wraps
from typing import Optional
from tenacity import retry, wait_exponential, stop_after_attempt

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from .audit.logger import DataReplicationLogger
from .config.models import AuthType, RetryConfig, SecretConfig


def get_token_from_secret(
    secret_config: SecretConfig, workspace_client: WorkspaceClient, auth_type: AuthType
):
    """Retrieve the authentication token from Databricks secrets."""
    if auth_type == AuthType.OAUTH:
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


@retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=10, max=60))
def create_spark_session(
    host: str,
    secret_config: SecretConfig = None,
    cluster_id: str = None,
    workspace_client: WorkspaceClient = None,
    auth_type: AuthType = None,
) -> DatabricksSession:
    """Create a Databricks Spark session using the provided host and token."""
    if not host:
        raise ValueError("Host URL must be provided to create Spark session.")
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
        if auth_type == AuthType.OAUTH:
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
    try:
        # Create Databricks session with default compute
        spark = DatabricksSession.builder.remote(host=host).getOrCreate()
    except Exception:
        # Fallback on Creating Databricks session with serverless compute
        spark = (
            DatabricksSession.builder.remote(host=host).serverless(True).getOrCreate()
        )
    return spark


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


def retry_with_logging(
    retry_config: RetryConfig, logger: Optional[DataReplicationLogger] = None
):
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
