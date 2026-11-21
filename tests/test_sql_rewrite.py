"""Tests for catalog reference rewriting in replayed view / MV / ST DDL (#37)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from data_replication.providers.replication_provider import ReplicationProvider
from data_replication.sql_rewrite import rewrite_catalog_references

M = {"src": "tgt"}


@pytest.mark.parametrize(
    "sql, expected",
    [
        ("SELECT * FROM src.sch.t", "SELECT * FROM tgt.sch.t"),
        ("SELECT * FROM `src`.`sch`.`t`", "SELECT * FROM `tgt`.`sch`.`t`"),
        ("SELECT * FROM `src`.sch.`t`", "SELECT * FROM `tgt`.sch.`t`"),
        ("SELECT * FROM SRC.Sch.T", "SELECT * FROM tgt.Sch.T"),
        ("SELECT * FROM `Src`.`sch`.`t`", "SELECT * FROM `tgt`.`sch`.`t`"),
        ("SELECT * FROM src . sch . t", "SELECT * FROM tgt . sch . t"),
        ("SELECT src.sch.fn(x) FROM a", "SELECT tgt.sch.fn(x) FROM a"),
        (
            "SELECT a.id FROM src.s.a JOIN `src`.s.b ON a.id = b.id",
            "SELECT a.id FROM tgt.s.a JOIN `tgt`.s.b ON a.id = b.id",
        ),
    ],
)
def test_rewrites_qualified_references(sql, expected):
    assert rewrite_catalog_references(sql, M) == expected


@pytest.mark.parametrize(
    "sql",
    [
        # substrings / other identifiers containing the name
        "SELECT * FROM src_other.sch.t",
        "SELECT * FROM mysrc.sch.t",
        "SELECT * FROM `src_x`.sch.t",
        # not the first part of a qualified name
        "SELECT * FROM other.src.t",
        "SELECT x.src.y FROM other.sch.t",
        # two-part names are alias.column / schema.table
        "SELECT src.id FROM other.sch.t AS src",
        # unqualified
        "SELECT src FROM other.sch.t",
        # string literals and comments
        "SELECT 'src.sch.t' AS s FROM other.sch.t",
        'SELECT "src.sch.t" AS s FROM other.sch.t',
        "SELECT 'it''s src.sch.t' FROM other.sch.t",
        "SELECT 'a\\'src.sch.t' FROM other.sch.t",
        "SELECT 1 -- from src.sch.t\nFROM other.sch.t",
        "SELECT 1 /* src.sch.t /* nested */ src.sch.t */ FROM other.sch.t",
    ],
)
def test_no_false_positives(sql):
    assert rewrite_catalog_references(sql, M) == sql


def test_literal_then_reference():
    sql = "SELECT 'src.sch.t', x FROM src.sch.t -- src.sch.t"
    assert (
        rewrite_catalog_references(sql, M)
        == "SELECT 'src.sch.t', x FROM tgt.sch.t -- src.sch.t"
    )


def test_target_needing_quotes_is_backticked():
    assert (
        rewrite_catalog_references("SELECT * FROM src.s.t", {"src": "tgt-cat"})
        == "SELECT * FROM `tgt-cat`.s.t"
    )
    assert (
        rewrite_catalog_references("SELECT * FROM `src-cat`.s.t", {"SRC-CAT": "t`x"})
        == "SELECT * FROM `t``x`.s.t"
    )


def test_multi_catalog_mapping():
    sql = "SELECT * FROM a.s.t JOIN b.s.u JOIN c.s.v"
    assert (
        rewrite_catalog_references(sql, {"a": "a2", "B": "b2"})
        == "SELECT * FROM a2.s.t JOIN b2.s.u JOIN c.s.v"
    )


def test_no_chained_rewrites():
    assert (
        rewrite_catalog_references(
            "SELECT * FROM a.s.t JOIN b.s.u", {"a": "b", "b": "c"}
        )
        == "SELECT * FROM b.s.t JOIN c.s.u"
    )


@pytest.mark.parametrize("mapping", [None, {}, {"src": "src"}, {"src": "SRC"}])
def test_noop_mappings(mapping):
    sql = "SELECT * FROM src.sch.t"
    assert rewrite_catalog_references(sql, mapping) == sql


def test_unterminated_tokens_do_not_crash():
    assert rewrite_catalog_references("SELECT 'src.s.t", M) == "SELECT 'src.s.t"
    assert rewrite_catalog_references("SELECT /* src.s.t", M) == "SELECT /* src.s.t"


# --- provider wiring -------------------------------------------------------


def _provider(source_ddl, target_catalog="tgt"):
    provider = ReplicationProvider.__new__(ReplicationProvider)
    provider.source_dbops = SimpleNamespace(show_create_table_ddl=lambda _: source_ddl)
    provider.catalog_config = SimpleNamespace(catalog_name=target_catalog)
    provider.cloud_url_mapping = {}
    return provider


def _config(**kw):
    base = dict(
        source_catalog="src",
        catalog_reference_mapping=None,
        create_or_replace_view=True,
        create_or_replace_materialized_view=True,
        create_or_replace_streaming_table=False,
    )
    base.update(kw)
    return SimpleNamespace(**base)


def _capture():
    queries = []

    def op(query):
        queries.append(query)
        return True, None, 1, 3

    return queries, op


VIEW_DDL = (
    "CREATE VIEW `src`.`sch`.`v` (\n  id,\n  name)\n"
    "AS SELECT a.id, b.name FROM src.sch.a a "
    "JOIN `src`.`sch`.`b` b ON a.id = b.id WHERE b.note <> 'src.sch.a'"
)


def test_view_body_references_rewritten_to_target_catalog():
    queries, op = _capture()
    _provider(VIEW_DDL)._uc_replicate_view(
        "`src`.`sch`.`v`", "`tgt`.`sch`.`v`", 3, op, _config()
    )
    assert queries == [
        "CREATE OR REPLACE VIEW `tgt`.`sch`.`v` (\n  id,\n  name)\n"
        "AS SELECT a.id, b.name FROM tgt.sch.a a "
        "JOIN `tgt`.`sch`.`b` b ON a.id = b.id WHERE b.note <> 'src.sch.a'"
    ]


def test_view_with_same_catalog_is_unchanged():
    ddl = VIEW_DDL.replace("src", "tgt")
    queries, op = _capture()
    _provider(ddl)._uc_replicate_view(
        "`tgt`.`sch`.`v`",
        "`tgt`.`sch`.`v`",
        3,
        op,
        _config(source_catalog="tgt", create_or_replace_view=False),
    )
    assert queries == [
        ddl.replace(
            "CREATE VIEW `tgt`.`sch`.`v` ", "CREATE VIEW IF NOT EXISTS `tgt`.`sch`.`v` "
        )
    ]


def test_view_uses_extra_catalog_reference_mapping():
    ddl = "CREATE VIEW `src`.`sch`.`v` (id) AS SELECT id FROM other.s.t JOIN src.s.u USING (id)"
    queries, op = _capture()
    _provider(ddl)._uc_replicate_view(
        "`src`.`sch`.`v`",
        "`tgt`.`sch`.`v`",
        3,
        op,
        _config(catalog_reference_mapping={"other": "other_tgt"}),
    )
    assert queries[0].endswith("FROM other_tgt.s.t JOIN tgt.s.u USING (id)")


def test_materialized_view_body_rewritten():
    ddl = "CREATE MATERIALIZED VIEW `src`.`sch`.`mv` (id) AS SELECT id FROM src.sch.t"
    queries, op = _capture()
    _provider(ddl)._uc_replicate_sql_materialized_view(
        "`src`.`sch`.`mv`", "`tgt`.`sch`.`mv`", 3, op, _config()
    )
    assert queries == [
        "CREATE OR REPLACE MATERIALIZED VIEW `tgt`.`sch`.`mv` (id) AS SELECT id FROM tgt.sch.t"
    ]


def test_streaming_table_body_rewritten():
    ddl = "CREATE STREAMING TABLE `src`.`sch`.`st` (id) AS SELECT id FROM STREAM(src.sch.t)"
    queries, op = _capture()
    _provider(ddl)._uc_replicate_sql_streaming_table(
        "`src`.`sch`.`st`", "`tgt`.`sch`.`st`", 3, op, _config()
    )
    assert queries == [
        "CREATE STREAMING TABLE IF NOT EXISTS `tgt`.`sch`.`st` (id) "
        "AS SELECT id FROM STREAM(tgt.sch.t)"
    ]


def test_catalog_reference_mapping_config_field():
    from data_replication.config.models import ReplicationConfig

    cfg = ReplicationConfig(catalog_reference_mapping={"a": "b"})
    assert cfg.catalog_reference_mapping == {"a": "b"}
    assert ReplicationConfig().catalog_reference_mapping is None
