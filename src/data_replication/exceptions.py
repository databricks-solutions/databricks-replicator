"""
Core exceptions for the data replication system.

This module defines custom exception classes used throughout
the data replication system.
"""


class DataReplicationError(Exception):
    """Base exception for data replication errors."""


class ConfigurationError(DataReplicationError):
    """Raised when there are configuration-related errors."""


class BackupError(DataReplicationError):
    """Raised when backup operations fail."""


class DeltaShareError(DataReplicationError):
    """Raised when Delta Share operations fail."""


class ReplicationError(DataReplicationError):
    """Raised when replication operations fail."""


class ReconciliationError(DataReplicationError):
    """Raised when reconciliation operations fail."""


class ValidationError(DataReplicationError):
    """Raised when validation operations fail."""


class RetryExhaustedError(DataReplicationError):
    """Raised when retry attempts are exhausted."""


class SparkSessionError(DataReplicationError):
    """Raised when Spark session operations fail."""


class TableNotFoundError(Exception):
    """Base exception for table not found errors."""
