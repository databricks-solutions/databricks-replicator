"""Tests for utils.py — URL mapping, user mapping, merge helpers, and recursive substitution."""

from __future__ import annotations

import os
from typing import Optional
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel
from tenacity import RetryError

from data_replication import utils


# ---------- map_cloud_url ----------

class TestMapCloudUrl:
    def test_returns_none_for_empty_inputs(self):
        assert utils.map_cloud_url("", {"s3://a/": "s3://b/"}) is None
        assert utils.map_cloud_url("s3://a/x", {}) is None
        assert utils.map_cloud_url(None, None) is None

    def test_exact_prefix_match(self):
        mapping = {"s3://src/": "s3://dst/"}
        assert utils.map_cloud_url("s3://src/foo/bar", mapping) == "s3://dst/foo/bar"

    def test_handles_trailing_slash_variance(self):
        # source URL missing trailing slash on the prefix still matches via rstrip
        mapping = {"s3://src/": "s3://dst/"}
        assert utils.map_cloud_url("s3://src", mapping) == "s3://dst/"

    def test_case_insensitive_match(self):
        mapping = {"s3://Src/": "s3://dst/"}
        assert utils.map_cloud_url("S3://src/x", mapping) == "s3://dst/x"

    def test_no_match_returns_none(self):
        mapping = {"s3://src/": "s3://dst/"}
        assert utils.map_cloud_url("gs://other/x", mapping) is None


# ---------- replace_cloud_url ----------

class TestReplaceCloudUrl:
    def test_passes_through_empty(self):
        assert utils.replace_cloud_url("", {"a/": "b/"}) == ""
        assert utils.replace_cloud_url("abc", None) == "abc"

    def test_first_only_true_returns_after_first_replace(self):
        mapping = {"s3://a/": "s3://x/", "s3://b/": "s3://y/"}
        out = utils.replace_cloud_url("s3://a/path and s3://b/path", mapping)
        # first_only=True: second mapping not applied
        assert "s3://x/path" in out

    def test_first_only_false_applies_all(self):
        mapping = {"s3://a/": "s3://x/", "s3://b/": "s3://y/"}
        out = utils.replace_cloud_url(
            "s3://a/path and s3://b/path", mapping, first_only=False
        )
        assert "s3://x/path" in out and "s3://y/path" in out

    def test_no_mapping_found_unchanged(self):
        assert utils.replace_cloud_url("gs://z/", {"s3://a/": "s3://b/"}) == "gs://z/"


# ---------- map_user ----------

class TestMapUser:
    def test_direct_mapping(self):
        assert utils.map_user("alice", {"alice": "bob"}) == "bob"

    def test_unmapped_returned_as_is(self):
        assert utils.map_user("alice", {"other": "x"}) == "alice"

    def test_empty_inputs(self):
        assert utils.map_user("", {"a": "b"}) == ""
        assert utils.map_user("alice", {}) == "alice"


# ---------- merge_maps ----------

class TestMergeMaps:
    def test_source_only(self):
        out = utils.merge_maps([{"a": 1}, {"b": 2}], None, overwrite=False)
        assert out == {"a": 1, "b": 2}

    def test_overwrite_true_target_ignored(self):
        out = utils.merge_maps([{"a": 1}], [{"a": 9, "b": 2}], overwrite=True)
        assert out == {"a": 1}  # target not merged in

    def test_overwrite_false_target_wins_on_conflict(self):
        out = utils.merge_maps([{"a": 1}], [{"a": 9, "b": 2}], overwrite=False)
        # merged_maps = source first, then ** existing_maps (target overrides)
        assert out == {"a": 9, "b": 2}

    def test_drops_none_values(self):
        out = utils.merge_maps([{"a": None, "b": 2}], None, overwrite=False)
        assert out == {"b": 2}

    def test_none_source_empty(self):
        assert utils.merge_maps(None, None, overwrite=False) == {}


# ---------- filter_common_maps ----------

class TestFilterCommonMaps:
    def test_returns_uncommon_pairs(self):
        src, tgt = utils.filter_common_maps(
            [{"a": 1}, {"b": 2}], [{"a": 1}, {"c": 3}]
        )
        assert src == [{"b": 2}]
        assert tgt == [{"c": 3}]

    def test_none_passthrough(self):
        src, tgt = utils.filter_common_maps(None, [{"a": 1}])
        assert src is None and tgt == [{"a": 1}]


# ---------- merge_dicts_recursive ----------

class TestMergeDictsRecursive:
    def test_both_none_returns_none(self):
        assert utils.merge_dicts_recursive(None, None) is None

    def test_base_none_filters_update(self):
        assert utils.merge_dicts_recursive(None, {"a": 1, "b": None}) == {"a": 1}

    def test_update_none_copies_base(self):
        base = {"a": 1}
        result = utils.merge_dicts_recursive(base, None)
        assert result == {"a": 1}
        assert result is not base  # must be a copy

    def test_nested_merge(self):
        base = {"outer": {"a": 1, "b": 2}}
        update = {"outer": {"b": 20, "c": 3}}
        utils.merge_dicts_recursive(base, update)
        # nested merge is in-place: 'a' retained, 'b' overridden, 'c' added
        assert base["outer"]["a"] == 1
        assert base["outer"]["b"] == 20
        assert base["outer"]["c"] == 3


# ---------- merge_models_recursive ----------

class _ChildModel(BaseModel):
    x: Optional[int] = None
    y: Optional[str] = None


class _ParentModel(BaseModel):
    name: Optional[str] = None
    child: Optional[_ChildModel] = None
    data: Optional[dict] = None


class TestMergeModelsRecursive:
    def test_both_none(self):
        assert utils.merge_models_recursive(None, None) is None

    def test_base_none_returns_copy_of_update(self):
        upd = _ChildModel(x=1)
        out = utils.merge_models_recursive(None, upd)
        assert isinstance(out, _ChildModel)
        assert out.x == 1

    def test_update_none_returns_copy_of_base(self):
        base = _ChildModel(x=2)
        out = utils.merge_models_recursive(base, None)
        assert isinstance(out, _ChildModel)
        assert out.x == 2

    def test_nested_merge_overrides_fields(self):
        base = _ParentModel(name="base", child=_ChildModel(x=1, y="a"))
        upd = _ParentModel(child=_ChildModel(y="b"))
        merged = utils.merge_models_recursive(base, upd)
        assert merged.name == "base"  # preserved from base
        assert merged.child.x == 1  # preserved
        assert merged.child.y == "b"  # overridden


# ---------- recursive_substitute ----------

class TestRecursiveSubstitute:
    def test_string_substitution(self):
        assert utils.recursive_substitute("hello {{name}}", "world", "{{name}}") == "hello world"

    def test_none_preserved(self):
        assert utils.recursive_substitute(None, "x") is None

    def test_primitive_preserved(self):
        assert utils.recursive_substitute(42, "x") == 42
        assert utils.recursive_substitute(True, "x") is True

    def test_dict_substitution(self):
        result = utils.recursive_substitute({"a": "{{x}}", "b": 1}, "Y", "{{x}}")
        assert result == {"a": "Y", "b": 1}

    def test_list_substitution(self):
        assert utils.recursive_substitute(["{{x}}", "y"], "Z", "{{x}}") == ["Z", "y"]

    def test_tuple_substitution(self):
        assert utils.recursive_substitute(("{{x}}",), "Z", "{{x}}") == ("Z",)

    def test_model_substitution(self):
        m = _ParentModel(name="hi {{who}}")
        out = utils.recursive_substitute(m, "world", "{{who}}")
        assert out.name == "hi world"


# ---------- unwrap_retry_error ----------

class TestUnwrapRetryError:
    def test_non_retry_error(self):
        assert utils.unwrap_retry_error(ValueError("boom")) == "boom"

    def test_retry_error_unwrap(self):
        inner = ValueError("inner")
        last_attempt = MagicMock()
        last_attempt.exception.return_value = inner
        re = RetryError(last_attempt=last_attempt)
        assert utils.unwrap_retry_error(re) == "inner"


# ---------- get_workspace_url_from_host ----------

class TestGetWorkspaceUrlFromHost:
    @pytest.mark.parametrize(
        "host,expected",
        [
            ("https://foo.cloud.databricks.com/", "foo.cloud.databricks.com"),
            ("http://foo.cloud.databricks.com", "foo.cloud.databricks.com"),
            ("https://foo.cloud.databricks.com/api/2.0", "foo.cloud.databricks.com"),
        ],
    )
    def test_strips_scheme_and_path(self, host, expected):
        assert utils.get_workspace_url_from_host(host) == expected


# ---------- set_envs ----------

class TestSetEnvs:
    @pytest.fixture(autouse=True)
    def _isolate_env(self, monkeypatch):
        # Ensure no leakage between tests
        for v in [
            "DATABRICKS_HOST",
            "DATABRICKS_TOKEN",
            "DATABRICKS_CLIENT_ID",
            "DATABRICKS_CLIENT_SECRET",
            "DATABRICKS_CLUSTER_ID",
        ]:
            monkeypatch.delenv(v, raising=False)

    def test_requires_host(self):
        with pytest.raises(ValueError, match="Host URL must be provided"):
            utils.set_envs(host="")

    def test_sets_host_only(self):
        utils.set_envs(host="https://x/")
        assert os.environ["DATABRICKS_HOST"] == "https://x/"

    def test_sets_cluster_id(self):
        utils.set_envs(host="https://x/", cluster_id="abc")
        assert os.environ["DATABRICKS_CLUSTER_ID"] == "abc"

    def test_pat_path(self):
        mock_wc = MagicMock()
        mock_wc.dbutils.secrets.get.return_value = "secret_pat"
        secret_cfg = MagicMock()
        secret_cfg.secret_scope = "scope"
        secret_cfg.secret_pat = "pat_key"

        utils.set_envs(
            host="https://x/",
            secret_config=secret_cfg,
            workspace_client=mock_wc,
            auth_type="pat",
        )
        assert os.environ["DATABRICKS_TOKEN"] == "secret_pat"
        assert "DATABRICKS_CLIENT_ID" not in os.environ

    def test_oauth_path(self):
        mock_wc = MagicMock()
        mock_wc.dbutils.secrets.get.side_effect = ["cid", "csecret"]
        secret_cfg = MagicMock()
        secret_cfg.secret_scope = "scope"
        secret_cfg.secret_client_id = "cid_key"
        secret_cfg.secret_client_secret = "csec_key"

        utils.set_envs(
            host="https://x/",
            secret_config=secret_cfg,
            workspace_client=mock_wc,
            auth_type="oauth",
        )
        assert os.environ["DATABRICKS_CLIENT_ID"] == "cid"
        assert os.environ["DATABRICKS_CLIENT_SECRET"] == "csecret"
        assert "DATABRICKS_TOKEN" not in os.environ


# ---------- retry_with_logging ----------

class TestRetryWithLogging:
    def _cfg(self, max_attempts=3, delay=0):
        cfg = MagicMock()
        cfg.max_attempts = max_attempts
        cfg.retry_delay_seconds = delay
        return cfg

    def test_succeeds_first_attempt(self):
        @utils.retry_with_logging(self._cfg())
        def op():
            return "ok"

        result, err, attempt, total = op()
        assert result == "ok"
        assert err is None
        assert attempt == 1
        assert total == 3

    def test_retries_then_succeeds(self):
        calls = {"n": 0}

        @utils.retry_with_logging(self._cfg())
        def op():
            calls["n"] += 1
            if calls["n"] < 2:
                raise RuntimeError("transient")
            return "ok"

        with patch.object(utils.time, "sleep"):
            result, err, attempt, _ = op()
        assert result == "ok"
        assert err is None
        assert attempt == 2

    def test_exhausts_attempts(self):
        @utils.retry_with_logging(self._cfg(max_attempts=2))
        def op():
            raise RuntimeError("always")

        with patch.object(utils.time, "sleep"):
            result, err, attempt, total = op()
        assert result is False
        assert isinstance(err, RuntimeError)
        assert attempt == 2
        assert total == 2
