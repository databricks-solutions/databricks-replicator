"""Tests for exception class hierarchy."""

import pytest

from data_replication import exceptions as exc


@pytest.mark.parametrize(
    "cls",
    [
        exc.ConfigurationError,
        exc.BackupError,
        exc.DeltaShareError,
        exc.ReplicationError,
        exc.ReconciliationError,
        exc.ValidationError,
        exc.RetryExhaustedError,
        exc.SparkSessionError,
    ],
)
def test_data_replication_subclasses(cls):
    """All domain errors inherit from DataReplicationError."""
    assert issubclass(cls, exc.DataReplicationError)
    assert issubclass(cls, Exception)


def test_table_not_found_independent():
    """TableNotFoundError intentionally sits outside the DataReplicationError tree."""
    assert issubclass(exc.TableNotFoundError, Exception)
    assert not issubclass(exc.TableNotFoundError, exc.DataReplicationError)


def test_raise_with_message():
    """Exceptions carry their message like built-in Exception."""
    with pytest.raises(exc.ConfigurationError, match="bad config"):
        raise exc.ConfigurationError("bad config")
