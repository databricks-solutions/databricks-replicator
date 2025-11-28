"""
Audit module for data replication system.

This module provides audit logging and tracking functionality for all
data replication operations.
"""

from data_replication.audit.logger import DataReplicationLogger

__all__ = [
    "DataReplicationLogger",
]
