"""
Retry utilities for data replication operations.

This module provides retry functionality with exponential backoff
and configurable retry strategies.
"""

import os
import time
from functools import wraps
from typing import Optional

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from .audit.logger import DataReplicationLogger
from .config.models import RetryConfig, SecretConfig


def create_spark_session(
    host: str,
    secret_config: SecretConfig,
    cluster_id: str,
    workspace_client: WorkspaceClient,
) -> DatabricksSession:
    """Create a Databricks Spark session using the provided host and token."""
    token = None
    if secret_config:
        token = workspace_client.dbutils.secrets.get(
            secret_config.secret_scope,
            secret_config.secret_key,
        )
    if host and token:
        os.environ["DATABRICKS_HOST"] = host
        os.environ["DATABRICKS_TOKEN"] = token
    if cluster_id:
        os.environ["DATABRICKS_CLUSTER_ID"] = cluster_id
        spark = DatabricksSession.builder.getOrCreate()
        return spark
    try:
        # Create Databricks session with default compute
        spark = DatabricksSession.builder.getOrCreate()
        return spark
    except Exception:
        # Fallback on Creating Databricks session with serverless compute
        spark = DatabricksSession.builder.serverless(True).getOrCreate()
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
        {k: v for d in source_maps for k, v in d.items() if v is not None} if source_maps else {}
    )
    if not overwrite and target_maps:
        existing_maps = {k: v for d in target_maps for k, v in d.items() if v is not None}
        merged_maps = {**merged_maps, **existing_maps}
    return merged_maps
