"""Tests for config/loader.py — load_from_dict, load_from_file, environments merging."""

from __future__ import annotations

import pytest
import yaml

from data_replication.config.loader import (
    ConfigLoader,
    ConfigurationError,
    validate_config_dict,
)
from data_replication.config.models import ReplicationSystemConfig

# ---------- Helpers ----------


def _base_cfg():
    return {
        "version": "1.0",
        "replication_group": "grp",
        "audit_config": {"audit_table": "a.b.c"},
        "uc_object_types": ["catalog"],
        "replication_config": {"enabled": True},
        "target_catalogs": [{"catalog_name": "c1"}],
    }


def _env_cfg_single_default():
    return {
        "version": "1.0",
        "environments": {
            "dev": {
                "is_default": True,
                "source_databricks_connect_config": {
                    "name": "src",
                    "host": "https://src/",
                    "token": {"secret_scope": "s", "secret_pat": "p"},
                },
                "target_databricks_connect_config": {
                    "name": "tgt",
                    "host": "https://tgt/",
                    "token": {"secret_scope": "s", "secret_pat": "p"},
                },
                "audit_config": {"audit_table": "a.b.c"},
            },
        },
    }


# ---------- load_from_dict ----------


class TestLoadFromDict:
    def test_requires_connect_configs(self):
        with pytest.raises(ConfigurationError):
            ConfigLoader.load_from_dict(_base_cfg())

    def test_accepts_complete_dict(self):
        cfg = _base_cfg()
        cfg["source_databricks_connect_config"] = {
            "name": "src",
            "host": "https://src/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        }
        cfg["target_databricks_connect_config"] = {
            "name": "tgt",
            "host": "https://tgt/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        }
        out = ConfigLoader.load_from_dict(cfg)
        assert isinstance(out, ReplicationSystemConfig)
        assert out.replication_group == "grp"


# ---------- load_from_file + environments.yaml merging ----------


class TestLoadFromFile:
    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(ConfigurationError, match="Configuration file not found"):
            ConfigLoader.load_from_file(tmp_path / "missing.yaml")

    def test_invalid_yaml_raises(self, tmp_path):
        path = tmp_path / "cfg.yaml"
        path.write_text("foo: [\nunterminated")
        with pytest.raises(ConfigurationError, match="parsing YAML"):
            ConfigLoader.load_from_file(path)

    def test_empty_yaml_raises(self, tmp_path):
        path = tmp_path / "cfg.yaml"
        path.write_text("")
        with pytest.raises(ConfigurationError, match="Configuration file is empty"):
            ConfigLoader.load_from_file(path)

    def test_missing_environments_yaml_raises(self, tmp_path):
        # Write config without connect configs; environments lookup should fail
        path = tmp_path / "cfg.yaml"
        path.write_text(yaml.safe_dump(_base_cfg()))
        with pytest.raises(ConfigurationError, match="environments.yaml not found"):
            ConfigLoader.load_from_file(path)

    def test_merges_environment_connect_configs(self, tmp_path):
        cfg = _base_cfg()
        cfg_path = tmp_path / "cfg.yaml"
        cfg_path.write_text(yaml.safe_dump(cfg))

        env_path = tmp_path / "environments.yaml"
        env_path.write_text(yaml.safe_dump(_env_cfg_single_default()))

        result = ConfigLoader.load_from_file(cfg_path)
        assert result.source_databricks_connect_config.name == "src"
        assert result.env_name == "dev"

    def test_custom_env_path(self, tmp_path):
        cfg = _base_cfg()
        cfg_path = tmp_path / "cfg.yaml"
        cfg_path.write_text(yaml.safe_dump(cfg))

        env_path = tmp_path / "alt" / "envs.yaml"
        env_path.parent.mkdir()
        env_path.write_text(yaml.safe_dump(_env_cfg_single_default()))

        result = ConfigLoader.load_from_file(cfg_path, env_path=str(env_path))
        assert result.env_name == "dev"

    def test_custom_env_path_missing(self, tmp_path):
        cfg_path = tmp_path / "cfg.yaml"
        cfg_path.write_text(yaml.safe_dump(_base_cfg()))
        with pytest.raises(ConfigurationError, match="Custom environments file"):
            ConfigLoader.load_from_file(cfg_path, env_path=str(tmp_path / "nope.yaml"))

    def test_unknown_environment_name(self, tmp_path):
        cfg_path = tmp_path / "cfg.yaml"
        cfg_path.write_text(yaml.safe_dump(_base_cfg()))
        env_path = tmp_path / "environments.yaml"
        env_path.write_text(yaml.safe_dump(_env_cfg_single_default()))
        with pytest.raises(ConfigurationError):
            ConfigLoader.load_from_file(cfg_path, environment_name="nonexistent")


# ---------- save_to_file round-trip ----------


class TestSaveRoundtrip:
    def test_save_writes_file(self, tmp_path):
        cfg = _base_cfg()
        cfg["source_databricks_connect_config"] = {
            "name": "src",
            "host": "https://src/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        }
        cfg["target_databricks_connect_config"] = {
            "name": "tgt",
            "host": "https://tgt/",
            "token": {"secret_scope": "s", "secret_pat": "p"},
        }
        cfg_obj = ConfigLoader.load_from_dict(cfg)
        out_path = tmp_path / "out.yaml"
        ConfigLoader.save_to_file(cfg_obj, out_path)
        assert out_path.exists()
        content = out_path.read_text()
        assert "replication_group: grp" in content


# ---------- Convenience wrappers ----------


def test_validate_config_dict(tmp_path):
    cfg = _base_cfg()
    cfg["source_databricks_connect_config"] = {
        "name": "src",
        "host": "https://src/",
        "token": {"secret_scope": "s", "secret_pat": "p"},
    }
    cfg["target_databricks_connect_config"] = {
        "name": "tgt",
        "host": "https://tgt/",
        "token": {"secret_scope": "s", "secret_pat": "p"},
    }
    out = validate_config_dict(cfg)
    assert isinstance(out, ReplicationSystemConfig)


# ---------- load_environments_config ----------


class TestLoadEnvironmentsConfig:
    def test_missing_file(self, tmp_path):
        with pytest.raises(ConfigurationError, match="Environments file not found"):
            ConfigLoader.load_environments_config(tmp_path / "nope.yaml")

    def test_empty_file(self, tmp_path):
        p = tmp_path / "env.yaml"
        p.write_text("")
        with pytest.raises(ConfigurationError, match="Environments file is empty"):
            ConfigLoader.load_environments_config(p)

    def test_valid_file(self, tmp_path):
        p = tmp_path / "env.yaml"
        p.write_text(yaml.safe_dump(_env_cfg_single_default()))
        out = ConfigLoader.load_environments_config(p)
        assert "dev" in out.environments
