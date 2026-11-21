"""Tests for config/loader.py — CLI overrides, environment merging and search."""

from __future__ import annotations

import pytest
import yaml

from data_replication.config.loader import (
    ConfigLoader,
    ConfigurationError,
    load_config,
    save_config,
)
from data_replication.config.models import TableType, UCObjectType, VolumeType


def _env(**extra_env):
    env = {
        "is_default": True,
        "source_databricks_connect_config": {
            "name": "src",
            "host": "https://src.example.com/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        },
        "target_databricks_connect_config": {
            "name": "tgt",
            "host": "https://tgt.example.com/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        },
    }
    env.update(extra_env)
    return env


def _write(tmp_path, cfg, envs=None, env_dir=None):
    env_dir = env_dir or tmp_path
    env_dir.mkdir(parents=True, exist_ok=True)
    (env_dir / "environments.yaml").write_text(
        yaml.safe_dump({"version": "1.0", "environments": envs or {"dev": _env()}})
    )
    path = tmp_path / "cfg.yaml"
    path.write_text(yaml.safe_dump(cfg))
    return path


def _cfg(**kw):
    cfg = {
        "version": "1.0",
        "replication_group": "grp",
        "audit_config": {"audit_table": "a.b.c"},
        "uc_object_types": ["catalog"],
        "replication_config": {"enabled": True},
        "target_catalogs": [{"catalog_name": "c1"}],
    }
    cfg.update(kw)
    return cfg


def _load(path, **kw):
    return ConfigLoader.load_from_file(path, **kw)


# ---------- environments.yaml merge and search ----------


class TestEnvironmentMerge:
    def test_environment_values_override_config_values(self, tmp_path):
        cfg = _cfg(audit_config={"audit_table": "cfg.sch.tbl"}, principal_mapping={})
        envs = {
            "dev": _env(
                audit_config={"audit_table": "env.sch.tbl"},
                principal_mapping={"a": "b"},
            )
        }
        config = _load(_write(tmp_path, cfg, envs))
        # environments.yaml wins on conflicts
        assert config.audit_config.audit_table == "env.sch.tbl"
        assert config.principal_mapping == {"a": "b"}

    def test_config_only_sections_survive_merge(self, tmp_path):
        cfg = _cfg(cloud_url_mapping={"abfss://a/": "s3://b/"})
        config = _load(_write(tmp_path, cfg))
        assert config.cloud_url_mapping == {"abfss://a/": "s3://b/"}

    def test_env_defaults_do_not_override_explicit_config_values(self, tmp_path):
        cfg = _cfg(
            audit_config={"audit_table": "cfg.sch.tbl", "logging_workspace": "source"}
        )
        envs = {"dev": _env(audit_config={"audit_table": "env.sch.tbl"})}
        config = _load(_write(tmp_path, cfg, envs))
        assert config.audit_config.logging_workspace == "source"

    def test_default_environment_used_and_name_recorded(self, tmp_path):
        envs = {
            "a": _env(is_default=False),
            "b": _env(
                source_databricks_connect_config={
                    "name": "b-src",
                    "host": "https://b.example.com/",
                }
            ),
        }
        config = _load(_write(tmp_path, _cfg(), envs))
        assert config.source_databricks_connect_config.name == "b-src"

    def test_named_environment_selected(self, tmp_path):
        envs = {
            "dev": _env(),
            "prod": _env(
                is_default=False,
                target_databricks_connect_config={
                    "name": "prod-tgt",
                    "host": "https://prod.example.com/",
                },
            ),
        }
        config = _load(_write(tmp_path, _cfg(), envs), environment_name="prod")
        assert config.target_databricks_connect_config.name == "prod-tgt"

    def test_environments_yaml_found_in_parent_directory(self, tmp_path):
        sub = tmp_path / "configs" / "group"
        sub.mkdir(parents=True)
        (tmp_path / "environments.yaml").write_text(
            yaml.safe_dump({"version": "1.0", "environments": {"dev": _env()}})
        )
        path = sub / "cfg.yaml"
        path.write_text(yaml.safe_dump(_cfg()))
        config = _load(path)
        assert config.target_databricks_connect_config.name == "tgt"

    def test_invalid_environments_yaml_raises_configuration_error(self, tmp_path):
        (tmp_path / "environments.yaml").write_text("version: '1.0'\nenvironments: [")
        path = tmp_path / "cfg.yaml"
        path.write_text(yaml.safe_dump(_cfg()))
        with pytest.raises(ConfigurationError, match="environments.yaml"):
            _load(path)

    def test_environments_schema_error_raises_configuration_error(self, tmp_path):
        (tmp_path / "environments.yaml").write_text(
            yaml.safe_dump({"version": "1.0", "environments": {"dev": {"bogus": 1}}})
        )
        path = tmp_path / "cfg.yaml"
        path.write_text(yaml.safe_dump(_cfg()))
        with pytest.raises(ConfigurationError):
            _load(path)


# ---------- enum list overrides ----------


@pytest.mark.parametrize(
    "field,override_kw,override,expected",
    [
        (
            "uc_object_types",
            "uc_object_types_override",
            [UCObjectType.TABLE, UCObjectType.VIEW],
            [UCObjectType.TABLE, UCObjectType.VIEW],
        ),
        (
            "table_types",
            "table_types_override",
            [TableType.MANAGED],
            [TableType.MANAGED],
        ),
        (
            "volume_types",
            "volume_types_override",
            [VolumeType.EXTERNAL],
            [VolumeType.EXTERNAL],
        ),
    ],
)
def test_type_overrides_replace_config_values(
    tmp_path, field, override_kw, override, expected
):
    cfg = _cfg()
    cfg.pop("uc_object_types")
    cfg[field] = (
        [e.value for e in expected][:1] if field != "uc_object_types" else ["catalog"]
    )
    config = _load(_write(tmp_path, cfg), **{override_kw: override})
    assert getattr(config, field) == expected


@pytest.mark.parametrize(
    "kw", ["uc_object_types_override", "table_types_override", "volume_types_override"]
)
def test_type_override_with_non_enum_raises(tmp_path, kw):
    with pytest.raises(ConfigurationError, match="Invalid"):
        _load(_write(tmp_path, _cfg()), **{kw: ["table"]})


# ---------- target catalog / schema / table / volume overrides ----------


class TestTargetOverrides:
    def test_existing_catalog_kept_and_others_built_from_group_config(self, tmp_path):
        cfg = _cfg(
            target_catalogs=[
                {"catalog_name": "c1", "target_schemas": [{"schema_name": "keep"}]},
                {"catalog_name": "dropped"},
            ],
            concurrency={"max_workers": 7},
        )
        config = _load(_write(tmp_path, cfg), target_catalog_override=" c1 , new_cat,")
        names = [c.catalog_name for c in config.target_catalogs]
        assert names == ["c1", "new_cat"]
        assert config.target_catalogs[0].target_schemas[0].schema_name == "keep"
        # new catalog inherits replication-group level settings
        new = config.target_catalogs[1]
        assert new.replication_config.enabled is True
        assert new.concurrency.max_workers == 7

    def test_schema_table_and_volume_overrides_apply_to_every_catalog(self, tmp_path):
        cfg = _cfg(target_catalogs=[{"catalog_name": "c1"}, {"catalog_name": "c2"}])
        config = _load(
            _write(tmp_path, cfg),
            target_schemas_override="Sales, hr",
            target_tables_override="Orders,items",
            target_volumes_override="Files",
        )
        for cat in config.target_catalogs:
            assert [s.schema_name for s in cat.target_schemas] == ["sales", "hr"]
            for schema in cat.target_schemas:
                assert [t.table_name for t in schema.tables] == ["orders", "items"]
                assert [v.volume_name for v in schema.volumes] == ["files"]

    def test_schema_override_without_tables_has_no_table_list(self, tmp_path):
        config = _load(_write(tmp_path, _cfg()), target_schemas_override="s1")
        schema = config.target_catalogs[0].target_schemas[0]
        assert schema.schema_name == "s1"
        assert not schema.tables

    def test_filter_expression_override_applies_to_all_catalogs(self, tmp_path):
        cfg = _cfg(target_catalogs=[{"catalog_name": "c1"}, {"catalog_name": "c2"}])
        config = _load(
            _write(tmp_path, cfg),
            schema_table_filter_expression_override="table_name like 'f_%'",
        )
        assert {c.schema_table_filter_expression for c in config.target_catalogs} == {
            "table_name like 'f_%'"
        }

    def test_invalid_catalog_name_raises_configuration_error(self, tmp_path):
        with pytest.raises(ConfigurationError):
            _load(_write(tmp_path, _cfg()), target_catalog_override="bad name!")

    def test_invalid_schema_name_raises_configuration_error(self, tmp_path):
        with pytest.raises(ConfigurationError):
            _load(_write(tmp_path, _cfg()), target_schemas_override="bad;schema")


# ---------- concurrency / logging / host overrides ----------


class TestScalarOverrides:
    def test_concurrency_override_applies_at_every_level(self, tmp_path):
        cfg = _cfg(
            concurrency={"max_workers": 2, "timeout_seconds": 99},
            target_catalogs=[
                {
                    "catalog_name": "c1",
                    "concurrency": {"max_workers": 3},
                    "target_schemas": [
                        {"schema_name": "s1", "concurrency": {"max_workers": 4}}
                    ],
                }
            ],
        )
        config = _load(_write(tmp_path, cfg), concurrency_override=9)
        assert config.concurrency.max_workers == 9
        assert config.concurrency.timeout_seconds == 99
        cat = config.target_catalogs[0]
        assert cat.concurrency.max_workers == 9
        assert cat.target_schemas[0].concurrency.max_workers == 9

    @pytest.mark.parametrize("bad", [0, -1, "4"])
    def test_concurrency_override_must_be_positive_int(self, tmp_path, bad):
        with pytest.raises(ConfigurationError, match="positive integer"):
            _load(_write(tmp_path, _cfg()), concurrency_override=bad)

    def test_logging_level_override_keeps_other_logging_settings(self, tmp_path):
        cfg = _cfg(logging={"level": "INFO", "format": "text"})
        config = _load(_write(tmp_path, cfg), logging_level_override="debug")
        assert config.logging.level == "debug"  # model normalizes case
        assert config.logging.format == "text"

    def test_logging_level_override_without_logging_section(self, tmp_path):
        config = _load(_write(tmp_path, _cfg()), logging_level_override="WARNING")
        assert config.logging.level == "warning"
        assert config.logging.format == "json"

    def test_invalid_logging_level_raises_configuration_error(self, tmp_path):
        with pytest.raises(ConfigurationError):
            _load(_write(tmp_path, _cfg()), logging_level_override="LOUD")

    def test_host_overrides_win_over_environment(self, tmp_path):
        config = _load(
            _write(tmp_path, _cfg()),
            source_host_override="https://src2.example.com/",
            target_host_override="https://tgt2.example.com/",
        )
        assert (
            config.source_databricks_connect_config.host == "https://src2.example.com/"
        )
        assert (
            config.target_databricks_connect_config.host == "https://tgt2.example.com/"
        )

    @pytest.mark.parametrize("kw", ["source_host_override", "target_host_override"])
    def test_host_override_without_scheme_raises_configuration_error(
        self, tmp_path, kw
    ):
        with pytest.raises(ConfigurationError):
            _load(_write(tmp_path, _cfg()), **{kw: "src.example.com"})


# ---------- volume overrides ----------


class TestVolumeOverrides:
    def test_volume_overrides_merge_into_catalog_and_group_config(self, tmp_path):
        cfg = _cfg(
            replication_config={
                "enabled": True,
                "volume_config": {"max_concurrent_copies": 2, "folder_path": "/keep"},
            },
            target_catalogs=[{"catalog_name": "c1"}],
        )
        config = _load(
            _write(tmp_path, cfg),
            volume_max_concurrent_copies_override=16,
            volume_delete_and_reload_override="TRUE",
            volume_delete_checkpoint_override="false",
            volume_streaming_timeout_secs_override=120,
            volume_autoloader_options_override='{"cloudFiles.maxFilesPerTrigger": "10"}',
        )
        for vc in (
            config.replication_config.volume_config,
            config.target_catalogs[0].replication_config.volume_config,
        ):
            assert vc.max_concurrent_copies == 16
            assert vc.delete_and_reload is True
            assert vc.delete_checkpoint is False
            assert vc.streaming_timeout_seconds == 120
            assert vc.autoloader_options == {"cloudFiles.maxFilesPerTrigger": "10"}
        # existing group-level value not overridden is preserved
        assert config.replication_config.volume_config.folder_path == "/keep"

    def test_folder_path_override(self, tmp_path):
        config = _load(_write(tmp_path, _cfg()), volume_folder_path_override="/sub")
        assert config.target_catalogs[
            0
        ].replication_config.volume_config.folder_path == ("/sub")

    def test_invalid_autoloader_json_raises(self, tmp_path):
        with pytest.raises(ConfigurationError, match="autoloader_options"):
            _load(
                _write(tmp_path, _cfg()), volume_autoloader_options_override="{not json"
            )


# ---------- validation / convenience wrappers ----------


def test_invalid_final_config_raises_configuration_error(tmp_path):
    cfg = _cfg(audit_config={"audit_table": "a.b.c", "unknown_key": 1})
    with pytest.raises(ConfigurationError, match="validation failed"):
        _load(_write(tmp_path, cfg))


def test_save_config_writes_yaml(tmp_path):
    config = load_config(_write(tmp_path, _cfg()))
    out = tmp_path / "out.yaml"
    save_config(config, out)
    text = out.read_text()
    assert "replication_group: grp" in text
    assert "catalog_name: c1" in text


def test_load_config_and_save_config_roundtrip(tmp_path):
    path = _write(tmp_path, _cfg())
    config = load_config(path)
    out = tmp_path / "out.yaml"
    save_config(config, out)
    saved = yaml.safe_load(out.read_text())
    assert saved["replication_group"] == "grp"
    assert saved["target_catalogs"][0]["catalog_name"] == "c1"


def test_save_to_file_error_raises_configuration_error(tmp_path):
    config = load_config(_write(tmp_path, _cfg()))
    with pytest.raises(ConfigurationError, match="Error saving"):
        save_config(config, tmp_path / "missing_dir" / "out.yaml")


def test_load_from_dict_invalid_raises():
    with pytest.raises(ConfigurationError):
        ConfigLoader.load_from_dict({"version": "1.0"})


def test_load_environments_config_invalid_yaml(tmp_path):
    p = tmp_path / "environments.yaml"
    p.write_text("environments: [")
    with pytest.raises(ConfigurationError, match="parsing"):
        ConfigLoader.load_environments_config(p)


def test_load_environments_config_invalid_schema(tmp_path):
    p = tmp_path / "environments.yaml"
    p.write_text(yaml.safe_dump({"version": "1.0", "environments": {"x": {"a": 1}}}))
    with pytest.raises(ConfigurationError, match="Invalid environments"):
        ConfigLoader.load_environments_config(p)
