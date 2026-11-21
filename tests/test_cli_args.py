"""Tests for cli/args.py — parsing, enum coercion, and validation rules."""

from __future__ import annotations

import argparse

import pytest

from data_replication.cli import args as cli_args
from data_replication.config.models import TableType, UCObjectType, VolumeType

# ---------- parse_comma_delimited_operations ----------


class TestParseOperations:
    def test_empty_defaults_to_all(self):
        assert cli_args.parse_comma_delimited_operations("") == ["all"]

    def test_single_operation(self):
        assert cli_args.parse_comma_delimited_operations("backup") == ["backup"]

    def test_multiple_operations_preserve_order(self):
        out = cli_args.parse_comma_delimited_operations("backup,replication")
        assert out == ["backup", "replication"]

    def test_deduplicates(self):
        out = cli_args.parse_comma_delimited_operations("backup,backup,replication")
        assert out == ["backup", "replication"]

    def test_case_and_whitespace_normalized(self):
        out = cli_args.parse_comma_delimited_operations(" Backup , REPLICATION ")
        assert out == ["backup", "replication"]

    def test_all_exclusive(self):
        with pytest.raises(ValueError, match="'all' operation cannot be combined"):
            cli_args.parse_comma_delimited_operations("all,backup")

    def test_rejects_unknown(self):
        with pytest.raises(ValueError, match="Invalid operation"):
            cli_args.parse_comma_delimited_operations("nonsense")


# ---------- parse_comma_delimited_enums ----------


class TestParseEnums:
    def test_empty_returns_none(self):
        assert cli_args.parse_comma_delimited_enums("", TableType, "--x") is None

    def test_parses_valid_enums(self):
        out = cli_args.parse_comma_delimited_enums(
            "managed,external", TableType, "--table-types"
        )
        assert out == [TableType.MANAGED, TableType.EXTERNAL]

    def test_rejects_invalid_with_helpful_message(self):
        with pytest.raises(ValueError, match="Valid values are"):
            cli_args.parse_comma_delimited_enums("bogus", VolumeType, "--volume-types")

    def test_uc_object_types_parsed(self):
        out = cli_args.parse_comma_delimited_enums(
            "catalog,schema", UCObjectType, "--uc-object-types"
        )
        assert out == [UCObjectType.CATALOG, UCObjectType.SCHEMA]


# ---------- validate_args ----------


def _ns(**kw):
    """Build a Namespace with validate_args-required attrs defaulted to None/False."""
    defaults = {
        "target_catalogs": None,
        "target_schemas": None,
        "target_tables": None,
        "target_volumes": None,
        "schema_table_filter_expression": None,
        "volume_max_concurrent_copies": None,
        "volume_streaming_timeout_secs": None,
        "volume_autoloader_options": None,
    }
    defaults.update(kw)
    return argparse.Namespace(**defaults)


class TestValidateArgs:
    def test_no_constraints_violated(self):
        cli_args.validate_args(_ns())

    def test_schemas_require_catalog(self):
        with pytest.raises(ValueError, match="target-schemas is provided"):
            cli_args.validate_args(_ns(target_schemas="s1"))

    def test_multi_catalog_with_schemas_rejected(self):
        with pytest.raises(ValueError, match="should only contain 1 catalog"):
            cli_args.validate_args(_ns(target_catalogs="c1,c2", target_schemas="s1"))

    def test_tables_require_schemas_and_catalogs(self):
        with pytest.raises(ValueError, match="target-schemas must also be provided"):
            cli_args.validate_args(_ns(target_tables="t"))
        with pytest.raises(ValueError, match="target-catalogs must also be provided"):
            cli_args.validate_args(_ns(target_tables="t", target_schemas="s"))

    def test_multi_schema_with_tables_rejected(self):
        with pytest.raises(ValueError, match="should only contain 1 schema"):
            cli_args.validate_args(
                _ns(target_tables="t", target_catalogs="c", target_schemas="s1,s2")
            )

    def test_volumes_require_schemas_and_catalogs(self):
        with pytest.raises(ValueError, match="target-schemas must also be provided"):
            cli_args.validate_args(_ns(target_volumes="v"))

    def test_filter_expr_conflicts_with_tables(self):
        with pytest.raises(ValueError, match="cannot be used together"):
            cli_args.validate_args(
                _ns(
                    schema_table_filter_expression="table_name like 'x%'",
                    target_tables="t",
                    target_catalogs="c",
                    target_schemas="s",
                )
            )

    def test_filter_expr_requires_catalogs(self):
        with pytest.raises(ValueError, match="target-catalogs must also be provided"):
            cli_args.validate_args(
                _ns(schema_table_filter_expression="table_name like 'x%'")
            )

    def test_volume_concurrent_copies_range(self):
        with pytest.raises(ValueError, match="between 1 and 100"):
            cli_args.validate_args(_ns(volume_max_concurrent_copies=0))
        with pytest.raises(ValueError, match="between 1 and 100"):
            cli_args.validate_args(_ns(volume_max_concurrent_copies=101))
        cli_args.validate_args(_ns(volume_max_concurrent_copies=50))  # OK

    def test_volume_streaming_timeout_minimum(self):
        with pytest.raises(ValueError, match="at least 60 seconds"):
            cli_args.validate_args(_ns(volume_streaming_timeout_secs=30))
        cli_args.validate_args(_ns(volume_streaming_timeout_secs=60))

    def test_volume_autoloader_options_json(self):
        cli_args.validate_args(_ns(volume_autoloader_options='{"k": "v"}'))
        with pytest.raises(ValueError, match="valid JSON"):
            cli_args.validate_args(_ns(volume_autoloader_options="not-json"))


# ---------- setup_argument_parser ----------


class TestArgumentParser:
    def test_config_file_required(self):
        parser = cli_args.setup_argument_parser()
        with pytest.raises(SystemExit):
            parser.parse_args([])

    def test_parses_minimal(self):
        parser = cli_args.setup_argument_parser()
        ns = parser.parse_args(["my.yaml"])
        assert ns.config_file == "my.yaml"
        assert ns.operation == "all"
        assert ns.validate_only is False

    def test_parses_operation_and_flags(self):
        parser = cli_args.setup_argument_parser()
        ns = parser.parse_args(
            [
                "cfg.yaml",
                "--operation",
                "backup,replication",
                "--validate-only",
                "--target-catalogs",
                "c1",
            ]
        )
        assert ns.operation == "backup,replication"
        assert ns.validate_only is True
        assert ns.target_catalogs == "c1"

    def test_invalid_logging_level_rejected(self):
        parser = cli_args.setup_argument_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["cfg.yaml", "--logging-level", "VERBOSE"])

    def test_invalid_volume_flag_choice_rejected(self):
        parser = cli_args.setup_argument_parser()
        with pytest.raises(SystemExit):
            parser.parse_args(["cfg.yaml", "--volume-delete-and-reload", "maybe"])
