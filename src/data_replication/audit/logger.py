"""
Data replication audit logger.

This module provides logging functionality specifically designed for
data replication operations with structured logging and audit trails.
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

from data_replication.config.models import LoggingConfig


class JSONFormatter(logging.Formatter):
    """JSON formatter for structured logging."""

    def format(self, record):
        """Format log record as JSON."""
        log_entry = {
            "timestamp": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Add extra fields from record
        for attr_name in [
            "run_id",
            "operation",
            "catalog_name",
            "schema_name",
            "table_name",
        ]:
            if hasattr(record, attr_name):
                log_entry[attr_name] = getattr(record, attr_name)

        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry, default=str)


class TextFormatter(logging.Formatter):
    """Text formatter for human-readable logging."""

    def __init__(self):
        """Initialize text formatter."""
        super().__init__(
            fmt="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )


class DataReplicationLogger:
    """Logger for data replication operations."""

    def __init__(self, name: str = "data_replication"):
        """
        Initialize the logger.

        Args:
            name: Logger name
        """
        self.logger = logging.getLogger(name)
        self.logger.handlers.clear()
        self.logger.propagate = False
        self.logger.setLevel(logging.INFO)

        # Set up default console handler
        self._setup_default_handler()

    def _setup_default_handler(self):
        """Set up default console handler."""
        console_handler = logging.StreamHandler(sys.stdout)
        formatter = TextFormatter()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def setup_logging(self, config: LoggingConfig) -> None:
        """
        Setup logging based on configuration.

        Args:
            config: Logging configuration
        """
        self.logger.handlers.clear()
        self.logger.setLevel(getattr(logging, config.level.upper()))

        # Choose formatter
        if config.format == "json":
            formatter = JSONFormatter()
        else:
            formatter = TextFormatter()

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        # File handler if configured
        if config.log_to_file and config.log_file_path:
            log_path = Path(config.log_file_path)
            log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = logging.FileHandler(log_path)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def debug(self, message: str, **kwargs) -> None:
        """Log debug message."""
        self.logger.debug(message, extra=kwargs)

    def info(self, message: str, **kwargs) -> None:
        """Log info message."""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs) -> None:
        """Log warning message."""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log error message."""
        self.logger.error(message, exc_info=exc_info, extra=kwargs)

    def critical(self, message: str, exc_info: bool = False, **kwargs) -> None:
        """Log critical message."""
        self.logger.critical(message, exc_info=exc_info, extra=kwargs)

    def log_operation_start(
        self,
        operation_type: str,
        catalog_name: str,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Log the start of an operation."""
        extra = {
            "operation": operation_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            **kwargs,
        }

        if table_name:
            message = (
                f"Starting {operation_type}: {catalog_name}.{schema_name}.{table_name}"
            )
        elif schema_name:
            message = f"Starting {operation_type}: {catalog_name}.{schema_name}"
        else:
            message = f"Starting {operation_type}: {catalog_name}"

        self.logger.info(message, extra=extra)

    def log_operation_success(
        self,
        operation_type: str,
        catalog_name: str,
        duration_seconds: float,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Log successful completion of an operation."""
        extra = {
            "operation": operation_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "duration_seconds": duration_seconds,
            "status": "success",
            **kwargs,
        }

        if table_name:
            message = f"Completed {operation_type}: {catalog_name}.{schema_name}.{table_name} ({duration_seconds:.2f}s)"
        elif schema_name:
            message = f"Completed {operation_type}: {catalog_name}.{schema_name} ({duration_seconds:.2f}s)"
        else:
            message = (
                f"Completed {operation_type}: {catalog_name} ({duration_seconds:.2f}s)"
            )

        self.logger.info(message, extra=extra)

    def log_operation_failure(
        self,
        operation_type: str,
        catalog_name: str,
        error_message: str,
        duration_seconds: float,
        schema_name: Optional[str] = None,
        table_name: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Log failed operation."""
        extra = {
            "operation": operation_type,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "duration_seconds": duration_seconds,
            "status": "failed",
            "error_message": error_message,
            **kwargs,
        }

        if table_name:
            message = f"Failed {operation_type}: {catalog_name}.{schema_name}.{table_name} - {error_message} ({duration_seconds:.2f}s)"
        elif schema_name:
            message = f"Failed {operation_type}: {catalog_name}.{schema_name} - {error_message} ({duration_seconds:.2f}s)"
        else:
            message = f"Failed {operation_type}: {catalog_name} - {error_message} ({duration_seconds:.2f}s)"

        self.logger.error(message, extra=extra)

    def get_logger(self) -> logging.Logger:
        """Get the underlying logger instance."""
        return self.logger


def get_logger(name: str = "data_replication") -> DataReplicationLogger:
    """Get a logger instance."""
    return DataReplicationLogger(name)
