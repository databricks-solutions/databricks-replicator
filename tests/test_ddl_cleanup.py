"""Tests for sql_utils.py — TBLPROPERTIES cleanup and inline mask/row filter stripping."""

from __future__ import annotations

from unittest.mock import MagicMock

from data_replication.config.models import ReplicationConfig
from data_replication.providers.replication_provider import ReplicationProvider
from data_replication.sql_utils import (
    _tokenize,
    strip_inline_masks_and_row_filters,
    strip_unreplayable_tblproperties,
)

ROW_TRACKING_DDL = """CREATE TABLE `src`.`s`.`t` (
  id BIGINT,
  name STRING COMMENT 'delta.minReaderVersion is not a key here')
USING delta
TBLPROPERTIES (
  'delta.enableRowTracking' = 'true',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.rowTracking.materializedRowCommitVersionColumnName' = '_row-commit-version-col-abc',
  'delta.rowTracking.materializedRowIdColumnName' = '_row-id-col-abc',
  'delta.feature.rowTracking' = 'supported',
  'custom.note' = 'it''s, (tricky)')"""


class TestTokenize:
    def test_roundtrip(self):
        sql = "SELECT 'a''b', `x``y`, \"q\" -- c\n /* d */ FROM t"
        assert "".join(t.text for t in _tokenize(sql)) == sql

    def test_nested_block_comment_is_one_token(self):
        sql = "SELECT /* a /* b */ c */ 1"
        comments = [t.text for t in _tokenize(sql) if t.kind == "comment"]
        assert comments == ["/* a /* b */ c */"]

    def test_property_inside_nested_comment_is_untouched(self):
        ddl = (
            "CREATE TABLE t (id INT)\nTBLPROPERTIES (\n"
            "  'delta.minWriterVersion' = '7',\n"
            "  /* old /* note */ 'delta.minReaderVersion' = '3' */\n"
            "  'custom.keep' = 'yes')"
        )
        out, removed = strip_unreplayable_tblproperties(ddl)
        assert removed == ["delta.minWriterVersion"]
        assert "/* old /* note */ 'delta.minReaderVersion' = '3' */" in out
        assert "'custom.keep' = 'yes'" in out


class TestStripUnreplayableTblproperties:
    def test_removes_row_tracking_and_protocol_keys(self):
        out, removed = strip_unreplayable_tblproperties(ROW_TRACKING_DDL)
        assert "minReaderVersion' =" not in out
        assert "delta.minWriterVersion" not in out
        assert "materializedRow" not in out
        assert set(removed) == {
            "delta.minReaderVersion",
            "delta.minWriterVersion",
            "delta.rowTracking.materializedRowCommitVersionColumnName",
            "delta.rowTracking.materializedRowIdColumnName",
        }

    def test_preserves_other_properties_and_literals(self):
        out, _ = strip_unreplayable_tblproperties(ROW_TRACKING_DDL)
        assert "'delta.enableRowTracking' = 'true'" in out
        assert "'delta.feature.rowTracking' = 'supported'" in out
        assert "'custom.note' = 'it''s, (tricky)')" in out
        # Comment string mentioning a reserved key is untouched.
        assert "COMMENT 'delta.minReaderVersion is not a key here'" in out

    def test_exact_output(self):
        ddl = (
            "CREATE TABLE t (id INT)\nTBLPROPERTIES (\n"
            "  'a' = '1',\n  'delta.minReaderVersion' = '1',\n  'b' = '2')"
        )
        out, _ = strip_unreplayable_tblproperties(ddl)
        assert (
            out
            == "CREATE TABLE t (id INT)\nTBLPROPERTIES (\n  'a' = '1',\n  'b' = '2')"
        )

    def test_last_entry_removed_keeps_valid_syntax(self):
        ddl = "CREATE TABLE t (id INT) TBLPROPERTIES ('a' = '1', 'delta.minWriterVersion' = '2')"
        out, _ = strip_unreplayable_tblproperties(ddl)
        assert out == "CREATE TABLE t (id INT) TBLPROPERTIES ('a' = '1')"

    def test_clause_dropped_when_empty(self):
        ddl = (
            "CREATE TABLE t (id INT)\nUSING delta\nTBLPROPERTIES (\n"
            "  'delta.minReaderVersion' = '1',\n  'delta.minWriterVersion' = '2')"
        )
        out, _ = strip_unreplayable_tblproperties(ddl)
        assert out == "CREATE TABLE t (id INT)\nUSING delta"

    def test_case_insensitive_and_unquoted_keys(self):
        ddl = "CREATE TABLE t (id INT) TBLPROPERTIES (DELTA.MINREADERVERSION = 1, keep = 2)"
        out, removed = strip_unreplayable_tblproperties(ddl)
        assert out == "CREATE TABLE t (id INT) TBLPROPERTIES ( keep = 2)"
        assert removed == ["DELTA.MINREADERVERSION"]

    def test_prefix_match(self):
        ddl = "CREATE TABLE t (id INT) TBLPROPERTIES ('x.y' = '1', 'x.z' = '2', 'k' = '3')"
        out, _ = strip_unreplayable_tblproperties(ddl, reserved=["x.*"])
        assert out == "CREATE TABLE t (id INT) TBLPROPERTIES ( 'k' = '3')"

    def test_no_tblproperties_is_noop(self):
        ddl = "CREATE TABLE t (tblproperties STRING) USING delta"
        assert strip_unreplayable_tblproperties(ddl) == (ddl, [])


MASKED_DDL = """CREATE TABLE `src`.`s`.`t` (
  id BIGINT,
  ssn STRING MASK `src`.`s`.`mask_ssn`,
  email STRING COMMENT 'uses MASK fn' MASK src.s.mask_email USING COLUMNS (id, 'x'),
  mask STRING,
  g INT GENERATED ALWAYS AS (mask(id)))
USING delta
WITH ROW FILTER `src`.`s`.`region_filter` ON (id, 'US')
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true')"""


class TestStripInlineMasksAndRowFilters:
    def test_strips_masks_and_row_filter(self):
        out, removed = strip_inline_masks_and_row_filters(MASKED_DDL)
        assert (
            out
            == """CREATE TABLE `src`.`s`.`t` (
  id BIGINT,
  ssn STRING,
  email STRING COMMENT 'uses MASK fn',
  mask STRING,
  g INT GENERATED ALWAYS AS (mask(id)))
USING delta
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true')"""
        )
        assert removed == [
            "MASK `src`.`s`.`mask_ssn`",
            "MASK src.s.mask_email USING COLUMNS (id, 'x')",
            "WITH ROW FILTER `src`.`s`.`region_filter` ON (id, 'US')",
        ]

    def test_row_filter_with_empty_column_list(self):
        ddl = "CREATE TABLE t (id INT) WITH ROW FILTER f ON () USING delta"
        out, _ = strip_inline_masks_and_row_filters(ddl)
        assert out == "CREATE TABLE t (id INT) USING delta"

    def test_no_masks_is_noop(self):
        ddl = "CREATE TABLE t (id INT, mask STRING COMMENT 'WITH ROW FILTER x ON (y)')"
        assert strip_inline_masks_and_row_filters(ddl) == (ddl, [])


def _provider_with_logger():
    provider = ReplicationProvider.__new__(ReplicationProvider)
    provider.logger = MagicMock()
    provider.run_id = "run"
    return provider


class TestCleanTableDdlConfig:
    def test_strips_by_default_and_warns(self):
        provider = _provider_with_logger()
        cfg = ReplicationConfig()
        assert cfg.strip_inline_masks_and_row_filters is True
        out = provider._clean_table_ddl(MASKED_DDL, "src", "tgt", cfg)
        assert "MASK `src`" not in out
        assert "ROW FILTER" not in out
        provider.logger.warning.assert_called_once()
        assert "WITHOUT" in provider.logger.warning.call_args[0][0]

    def test_keeps_masks_when_disabled(self):
        provider = _provider_with_logger()
        cfg = ReplicationConfig(strip_inline_masks_and_row_filters=False)
        out = provider._clean_table_ddl(MASKED_DDL, "src", "tgt", cfg)
        assert "MASK `src`.`s`.`mask_ssn`" in out
        assert "WITH ROW FILTER" in out
        provider.logger.warning.assert_not_called()

    def test_tblproperties_always_cleaned(self):
        provider = _provider_with_logger()
        cfg = ReplicationConfig(strip_inline_masks_and_row_filters=False)
        out = provider._clean_table_ddl(ROW_TRACKING_DDL, "src", "tgt", cfg)
        assert "delta.minWriterVersion" not in out
        assert "'delta.enableRowTracking' = 'true'" in out
