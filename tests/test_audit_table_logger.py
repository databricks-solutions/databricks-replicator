"""Tests for audit/audit_logger.py — AuditLogger table creation and writes."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from data_replication.audit.audit_logger import AuditLogger
from data_replication.config.models import RunResult


def _make(audit_table="cat.sch.audit", create_catalog=False, config=None):
    spark = MagicMock()
    db_ops = MagicMock()
    logger = MagicMock()
    with patch(
        "data_replication.audit.audit_logger.get_spark_current_user",
        return_value="me@example.com",
    ):
        audit = AuditLogger(
            spark=spark,
            db_ops=db_ops,
            logger=logger,
            run_id="run-1",
            create_audit_catalog=create_catalog,
            audit_table=audit_table,
            audit_catalog_location="s3://loc",
            config_details=config,
        )
    return audit, spark, db_ops, logger


def _result(**kw):
    base = dict(
        operation_type="replication",
        catalog_name="Cat",
        schema_name="sch",
        object_name="tbl",
        object_type="table",
        status="success",
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-01T00:00:02.5Z",
    )
    base.update(kw)
    return RunResult(**base)


def _written_rows(spark):
    """Rows passed to the last spark.createDataFrame call."""
    return spark.createDataFrame.call_args[0][0]


class TestInit:
    def test_creates_schema_and_table(self):
        audit, spark, db_ops, _ = _make()
        db_ops.create_catalog_if_not_exists.assert_not_called()
        db_ops.create_schema_if_not_exists.assert_called_once_with("cat", "sch")
        sql = spark.sql.call_args[0][0]
        assert "CREATE TABLE IF NOT EXISTS cat.sch.audit" in sql
        assert "execution_user STRING" in sql
        assert audit.execution_user == "me@example.com"

    def test_creates_catalog_when_configured(self):
        _, _, db_ops, _ = _make(create_catalog=True)
        db_ops.create_catalog_if_not_exists.assert_called_once_with("cat", "s3://loc")

    def test_catalog_creation_failure_is_raised_and_logged(self):
        spark, db_ops, logger = MagicMock(), MagicMock(), MagicMock()
        db_ops.create_catalog_if_not_exists.side_effect = RuntimeError("denied")
        with (
            patch(
                "data_replication.audit.audit_logger.get_spark_current_user",
                return_value="u",
            ),
            pytest.raises(RuntimeError, match="denied"),
        ):
            AuditLogger(spark, db_ops, logger, "r", True, "cat.sch.audit")
        logger.error.assert_called_once()
        spark.sql.assert_not_called()

    def test_invalid_table_name_raises(self):
        with (
            patch(
                "data_replication.audit.audit_logger.get_spark_current_user",
                return_value="u",
            ),
            pytest.raises(ValueError, match="catalog.schema.table"),
        ):
            AuditLogger(MagicMock(), MagicMock(), MagicMock(), "r", False, "audit")

    def test_config_details_serialized_to_json(self):
        audit, *_ = _make(config={"a": 1, "when": datetime(2026, 1, 1)})
        parsed = json.loads(audit.config_details_json)
        assert parsed["a"] == 1
        assert parsed["when"].startswith("2026-01-01")

    def test_no_config_details(self):
        audit, *_ = _make()
        assert audit.config_details_json is None


class TestLogResults:
    def test_empty_results_writes_nothing(self):
        audit, spark, *_ = _make()
        audit.log_results([])
        spark.createDataFrame.assert_not_called()

    def test_writes_one_row_per_result(self):
        audit, spark, _, _ = _make(config={"k": "v"})
        audit.log_results(
            [
                _result(details={"rows": 3}, attempt_number=2, max_attempts=3),
                _result(object_name="t2", status="failed", error_message="boom"),
            ]
        )
        rows = _written_rows(spark)
        assert len(rows) == 2
        first = rows[0]
        assert first[0] == "run-1"
        assert first[2:8] == (
            "replication",
            "cat",  # RunResult lowercases catalog_name
            "sch",
            "tbl",
            "table",
            "success",
        )
        assert first[8] == datetime(2026, 1, 1, tzinfo=timezone.utc)
        assert first[10] == pytest.approx(2.5)
        assert json.loads(first[12]) == {"rows": 3}
        assert first[13:15] == (2, 3)
        assert json.loads(first[15]) == {"k": "v"}
        assert first[16] == "me@example.com"
        assert rows[1][7] == "failed"
        assert rows[1][11] == "boom"
        assert rows[1][12] is None
        spark.createDataFrame.return_value.write.mode.assert_called_with("append")
        spark.createDataFrame.return_value.write.mode.return_value.saveAsTable.assert_called_with(
            "cat.sch.audit"
        )

    def test_missing_schema_object_names_become_empty_strings(self):
        audit, spark, *_ = _make()
        audit.log_results(
            [_result(schema_name=None, object_name=None, object_type=None)]
        )
        row = _written_rows(spark)[0]
        assert row[4:7] == ("", "", "")

    def test_bad_timestamp_skips_record_with_warning(self):
        audit, spark, _, logger = _make()
        audit.log_results([_result(start_time="not-a-date"), _result()])
        assert len(_written_rows(spark)) == 1
        logger.warning.assert_called_once()
        assert "cat.sch.tbl" in logger.warning.call_args[0][0]

    def test_all_records_invalid_logs_and_returns(self):
        audit, spark, _, logger = _make()
        audit.log_results([_result(end_time="bad")])
        spark.createDataFrame.assert_not_called()
        assert "No valid audit records" in logger.warning.call_args_list[-1][0][0]

    def test_write_failure_is_raised(self):
        audit, spark, _, logger = _make()
        spark.createDataFrame.return_value.write.mode.return_value.saveAsTable.side_effect = RuntimeError(
            "write failed"
        )
        with pytest.raises(RuntimeError, match="write failed"):
            audit.log_results([_result()])
        logger.error.assert_called_once()


class TestLogOperation:
    def test_writes_single_row(self):
        audit, spark, *_ = _make()
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        end = datetime(2026, 1, 1, 0, 0, 5, tzinfo=timezone.utc)
        audit.log_operation(
            "backup",
            "c",
            "s",
            "o",
            "table",
            "success",
            start,
            end,
            5.0,
            details="d",
            attempt_number=1,
            max_attempts=2,
        )
        rows = _written_rows(spark)
        assert len(rows) == 1
        row = rows[0]
        assert row[0] == "run-1"
        assert row[2:8] == ("backup", "c", "s", "o", "table", "success")
        assert row[8:11] == (start, end, 5.0)
        assert row[12:15] == ("d", 1, 2)

    def test_write_failure_only_warns(self):
        audit, spark, _, logger = _make()
        spark.createDataFrame.side_effect = RuntimeError("nope")
        now = datetime.now(timezone.utc)
        audit.log_operation("x", "c", "s", "o", "t", "failed", now, now, 0.0)
        logger.warning.assert_called_once()
        assert "nope" in logger.warning.call_args[0][0]
