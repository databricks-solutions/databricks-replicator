"""
Data Replication System for Databricks.

A comprehensive data replication system with support for backup,
delta sharing, replication, and reconciliation of DLT tables.
"""

from data_replication.providers import BaseProvider, ProviderFactory

__version__ = "1.0.0"

__all__ = [
    "ProviderFactory",
    "BaseProvider",
]
