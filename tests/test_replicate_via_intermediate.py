"""Tests for ReplicationProvider._replicate_via_intermediate."""

from __future__ import annotations

from types import SimpleNamespace

from data_replication.providers.replication_provider import ReplicationProvider


def _provider():
    # Bypass __init__ (it builds Spark sessions / workspace clients).
    return ReplicationProvider.__new__(ReplicationProvider)


def _run(provider, results, pipeline_id=None, parent_table_id=None):
    queries = []
    outcomes = iter(results)

    def op(query):
        queries.append(query)
        return next(outcomes)

    out = provider._replicate_via_intermediate(
        "`share_cat`.`sch`.`tbl`",
        "`tgt_cat`.`sch`.`tbl`",
        "sch",
        "tbl",
        pipeline_id,
        parent_table_id,
        op,
        SimpleNamespace(intermediate_catalog="inter_cat"),
    )
    return out, queries


def test_step2_clones_from_intermediate_not_source():
    out, queries = _run(_provider(), [(True, None, 1, 3), (True, None, 1, 3)])

    assert queries == [
        "CREATE OR REPLACE TABLE `inter_cat`.`sch`.`tbl` "
        "DEEP CLONE `share_cat`.`sch`.`tbl` ",
        "CREATE OR REPLACE TABLE `tgt_cat`.`sch`.`tbl` "
        "DEEP CLONE `inter_cat`.`sch`.`tbl` ",
    ]
    result, _, _, _, step1, step2 = out
    assert result is True
    assert step1 == queries[0]
    assert step2 == queries[1]


def test_step2_keeps_pipeline_properties_for_dlt_targets():
    _, queries = _run(
        _provider(),
        [(True, None, 1, 3), (True, None, 1, 3)],
        pipeline_id="pid",
        parent_table_id="ptid",
    )

    assert "DEEP CLONE `inter_cat`.`sch`.`tbl`" in queries[1]
    assert "'pipelines.pipelineId'='pid'" in queries[1]
    assert "'spark.sql.internal.pipelines.parentTableId'='ptid'" in queries[1]
    # Intermediate copy is a plain Delta table: no pipeline properties.
    assert "TBLPROPERTIES" not in queries[0]


def test_step1_failure_skips_step2():
    err = RuntimeError("boom")
    out, queries = _run(_provider(), [(False, err, 3, 3)])

    assert len(queries) == 1
    assert out == (False, err, 3, 3, queries[0], None)
