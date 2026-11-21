"""Tests for audit/logger.py — JSON/Text formatters + DataReplicationLogger wrapper."""

from __future__ import annotations

import json
import logging

import pytest

from data_replication.audit.logger import (
    DataReplicationLogger,
    JSONFormatter,
    TextFormatter,
    get_logger,
)
from data_replication.config.models import LoggingConfig

# ---------- Formatters ----------


class TestJSONFormatter:
    def test_produces_valid_json_with_core_fields(self):
        fmt = JSONFormatter()
        record = logging.LogRecord(
            name="x",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="hello",
            args=None,
            exc_info=None,
        )
        out = fmt.format(record)
        parsed = json.loads(out)
        assert parsed["level"] == "INFO"
        assert parsed["message"] == "hello"
        assert parsed["logger"] == "x"
        assert "timestamp" in parsed

    def test_includes_extra_fields(self):
        fmt = JSONFormatter()
        record = logging.LogRecord(
            name="x",
            level=logging.INFO,
            pathname=__file__,
            lineno=1,
            msg="m",
            args=None,
            exc_info=None,
        )
        record.run_id = "run-1"
        record.catalog_name = "c"
        record.table_name = "t"
        parsed = json.loads(fmt.format(record))
        assert parsed["run_id"] == "run-1"
        assert parsed["catalog_name"] == "c"
        assert parsed["table_name"] == "t"

    def test_serializes_exception(self):
        fmt = JSONFormatter()
        try:
            raise RuntimeError("kaboom")
        except RuntimeError:
            import sys

            record = logging.LogRecord(
                name="x",
                level=logging.ERROR,
                pathname=__file__,
                lineno=1,
                msg="failed",
                args=None,
                exc_info=sys.exc_info(),
            )
            parsed = json.loads(fmt.format(record))
            assert "exception" in parsed
            assert "kaboom" in parsed["exception"]


def test_text_formatter_contains_standard_fields():
    fmt = TextFormatter()
    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="hello",
        args=None,
        exc_info=None,
    )
    out = fmt.format(record)
    assert "hello" in out
    assert "INFO" in out


# ---------- DataReplicationLogger ----------


@pytest.fixture
def logger():
    return DataReplicationLogger(name="test_data_replication_logger_unique")


class _CaptureHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record):
        self.records.append(record)


@pytest.fixture
def capture(logger):
    """Attach a capturing handler to the logger-under-test."""
    h = _CaptureHandler()
    logger.logger.addHandler(h)
    yield h
    logger.logger.removeHandler(h)


class TestDataReplicationLogger:
    def test_default_has_one_handler(self, logger):
        assert len(logger.logger.handlers) == 1
        assert logger.logger.level == logging.INFO

    def test_does_not_propagate(self, logger):
        assert logger.logger.propagate is False

    def test_setup_logging_json(self, logger):
        logger.setup_logging(LoggingConfig(level="DEBUG", format="json"))
        assert logger.logger.level == logging.DEBUG
        assert isinstance(logger.logger.handlers[0].formatter, JSONFormatter)

    def test_setup_logging_text(self, logger):
        logger.setup_logging(LoggingConfig(level="INFO", format="text"))
        assert isinstance(logger.logger.handlers[0].formatter, TextFormatter)

    def test_setup_logging_file(self, logger, tmp_path):
        log_path = tmp_path / "sub" / "app.log"
        logger.setup_logging(
            LoggingConfig(
                level="INFO",
                format="json",
                log_to_file=True,
                log_file_path=str(log_path),
            )
        )
        # parent should be created
        assert log_path.parent.exists()
        file_handlers = [
            h for h in logger.logger.handlers if isinstance(h, logging.FileHandler)
        ]
        assert len(file_handlers) == 1

    def test_log_methods_do_not_raise(self, logger):
        # Each should accept kwargs
        logger.debug("d", run_id="x")
        logger.info("i", run_id="x")
        logger.warning("w", run_id="x")
        logger.error("e", run_id="x")
        logger.critical("c", run_id="x")

    def test_log_operation_start_table(self, logger, capture):
        logger.log_operation_start("replication", "cat", "sch", "tbl")
        assert any("cat.sch.tbl" in r.getMessage() for r in capture.records)

    def test_log_operation_start_schema_only(self, logger, capture):
        logger.log_operation_start("replication", "cat", "sch")
        assert any("cat.sch" in r.getMessage() for r in capture.records)

    def test_log_operation_start_catalog_only(self, logger, capture):
        logger.log_operation_start("replication", "cat")
        assert any(
            "Starting replication: cat" in r.getMessage() for r in capture.records
        )

    def test_log_operation_success_includes_duration(self, logger, capture):
        logger.log_operation_success("replication", "cat", 1.25)
        assert any("1.25s" in r.getMessage() for r in capture.records)

    def test_log_operation_failure_routes_to_error(self, logger, capture):
        logger.log_operation_failure("replication", "cat", "boom", 2.0)
        errors = [r for r in capture.records if r.levelno == logging.ERROR]
        assert errors
        assert "boom" in errors[-1].getMessage()
        assert "2.00s" in errors[-1].getMessage()

    def test_get_logger_returns_underlying(self, logger):
        assert logger.get_logger() is logger.logger


def test_get_logger_returns_instance():
    inst = get_logger("another_unique_name")
    assert isinstance(inst, DataReplicationLogger)
