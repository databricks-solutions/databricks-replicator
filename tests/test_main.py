"""Tests for main.py — per-operation runners and main() dispatch."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from data_replication import main as m
from data_replication.exceptions import ConfigurationError

SRC_HOST = "https://src.example.com"
TGT_HOST = "https://tgt.example.com"


def _conn(host, name):
    return SimpleNamespace(
        name=name, host=host, auth_type="pat", token="tok-" + name, cluster_id=None
    )


def _cat(name, backup=False, replication=False, reconciliation=False):
    def cfg(on):
        return SimpleNamespace(enabled=on) if on else None

    return SimpleNamespace(
        catalog_name=name,
        backup_config=cfg(backup),
        replication_config=cfg(replication),
        reconciliation_config=cfg(reconciliation),
    )


def _config(logging_workspace="target", catalogs=None, uc_object_types=None):
    return SimpleNamespace(
        source_databricks_connect_config=_conn(SRC_HOST, "src"),
        target_databricks_connect_config=_conn(TGT_HOST, "tgt"),
        audit_config=SimpleNamespace(
            logging_workspace=logging_workspace, audit_table="c.s.t"
        ),
        target_catalogs=catalogs or [],
        uc_object_types=uc_object_types,
        logging=None,
    )


def _summary(failed=0):
    return SimpleNamespace(failed_operations=failed)


# ---------- create_logger ----------


def test_create_logger_applies_logging_config():
    cfg = SimpleNamespace(logging=MagicMock())
    with patch.object(m, "DataReplicationLogger") as cls:
        logger = m.create_logger(cfg)
    cls.assert_called_once_with("data_replication")
    logger.setup_logging.assert_called_once_with(cfg.logging)


def test_create_logger_without_logging_config():
    with patch.object(m, "DataReplicationLogger") as cls:
        logger = m.create_logger(SimpleNamespace(logging=None))
    logger.setup_logging.assert_not_called()
    assert logger is cls.return_value


# ---------- run_backup / run_replication / run_reconciliation ----------

RUNNERS = [
    # (runner, factory method, workspace that reuses logging spark, host of new session)
    (m.run_backup, "run_backup_operations", "source", SRC_HOST, "backup"),
    (
        m.run_replication,
        "run_replication_operations",
        "target",
        TGT_HOST,
        "replication",
    ),
    (
        m.run_reconciliation,
        "run_reconciliation_operations",
        "target",
        TGT_HOST,
        "reconciliation",
    ),
]


@pytest.mark.parametrize("runner,method,same_ws,host,op", RUNNERS)
def test_runner_reuses_logging_spark_when_same_workspace(
    runner, method, same_ws, host, op
):
    cfg = _config(logging_workspace=same_ws)
    logging_spark = MagicMock()
    with (
        patch.object(m, "ProviderFactory") as factory,
        patch.object(m, "create_spark_session") as create,
    ):
        getattr(factory.return_value, method).return_value = _summary(0)
        rc = runner(cfg, MagicMock(), logging_spark, "run-1", "ws")
    assert rc == 0
    create.assert_not_called()
    factory.assert_called_once_with(
        op, cfg, logging_spark, logging_spark, "ws", factory.call_args[0][5], "run-1"
    )


@pytest.mark.parametrize("runner,method,same_ws,host,op", RUNNERS)
def test_runner_creates_session_for_other_workspace(runner, method, same_ws, host, op):
    other_ws = "target" if same_ws == "source" else "source"
    cfg = _config(logging_workspace=other_ws)
    new_spark = MagicMock()
    with (
        patch.object(m, "ProviderFactory") as factory,
        patch.object(m, "create_spark_session", return_value=new_spark) as create,
        patch.object(m, "validate_spark_session", return_value=True),
    ):
        getattr(factory.return_value, method).return_value = _summary(2)
        rc = runner(cfg, MagicMock(), "logging_spark", "run-1", "ws")
    # failures in the summary are reported as a non-zero runner result
    assert rc == 1
    assert create.call_args[0][0] == host
    assert create.call_args[0][3] == "ws"
    assert factory.call_args[0][2] is new_spark
    assert factory.call_args[0][3] == "logging_spark"


@pytest.mark.parametrize("runner,method,same_ws,host,op", RUNNERS)
def test_runner_rejects_session_connected_to_wrong_workspace(
    runner, method, same_ws, host, op
):
    other_ws = "target" if same_ws == "source" else "source"
    cfg = _config(logging_workspace=other_ws)
    with (
        patch.object(m, "ProviderFactory") as factory,
        patch.object(m, "create_spark_session"),
        patch.object(m, "validate_spark_session", return_value=False),
        patch.object(m, "get_spark_workspace_url", return_value="https://other"),
    ):
        with pytest.raises(ConfigurationError, match="https://other"):
            runner(cfg, MagicMock(), "logging_spark", "run-1", "ws")
    factory.assert_not_called()


# ---------- main() ----------


def _args(**kw):
    base = dict(
        target_catalogs=None,
        target_schemas=None,
        target_tables=None,
        target_volumes=None,
        schema_table_filter_expression=None,
        concurrency=None,
        logging_level=None,
        source_host=None,
        target_host=None,
        validate_only=False,
        run_id=None,
        profile=None,
        replication_wait_secs=None,
        uc_object_types=None,
        environment=None,
        env_path=None,
    )
    base.update(kw)
    return SimpleNamespace(**base)


class _Main:
    """Patches every external dependency of main() and records calls."""

    def __init__(self, operations, config, args=None):
        self.args = args or _args()
        self.operations = operations
        self.config = config

    def __enter__(self):
        self.patches = {
            "parse": patch.object(
                m,
                "parse_and_validate_args",
                return_value=(self.args, self.operations, None, None, None, "c.yaml"),
            ),
            "load": patch.object(
                m.ConfigLoader, "load_from_file", return_value=self.config
            ),
            "logger": patch.object(m, "create_logger"),
            "ws": patch.object(m, "WorkspaceClient"),
            "spark": patch.object(m, "create_spark_session"),
            "validate": patch.object(m, "validate_spark_session", return_value=True),
            "backup": patch.object(m, "run_backup", return_value=0),
            "replication": patch.object(m, "run_replication", return_value=0),
            "reconciliation": patch.object(m, "run_reconciliation", return_value=0),
            "sleep": patch.object(m.time, "sleep"),
        }
        self.mocks = {k: p.start() for k, p in self.patches.items()}
        return self

    def __exit__(self, *exc):
        for p in self.patches.values():
            p.stop()

    def __getitem__(self, key):
        return self.mocks[key]


def test_main_argument_error_returns_1(capsys):
    with patch.object(m, "parse_and_validate_args", side_effect=ValueError("bad arg")):
        assert m.main() == 1
    assert "bad arg" in capsys.readouterr().err


def test_main_configuration_error_returns_1(capsys):
    with (
        patch.object(
            m,
            "parse_and_validate_args",
            return_value=(_args(), [], None, None, None, "c"),
        ),
        patch.object(
            m.ConfigLoader,
            "load_from_file",
            side_effect=ConfigurationError("missing host"),
        ),
    ):
        assert m.main() == 1
    assert "Configuration error: missing host" in capsys.readouterr().err


def test_main_unexpected_config_error_returns_1(capsys):
    with (
        patch.object(
            m,
            "parse_and_validate_args",
            return_value=(_args(), [], None, None, None, "c"),
        ),
        patch.object(m.ConfigLoader, "load_from_file", side_effect=KeyError("x")),
    ):
        assert m.main() == 1
    assert "Unexpected error" in capsys.readouterr().err


def test_main_passes_cli_overrides_to_loader():
    args = _args(target_catalogs="c1", concurrency=4, environment="dev", env_path="e")
    with _Main(["all"], _config(), args=args) as mm:
        mm["parse"].return_value = (args, ["all"], ["table"], ["managed"], None, "c.y")
        m.main()
    kw = mm["load"].call_args.kwargs
    assert kw["config_path"] == "c.y"
    assert kw["target_catalog_override"] == "c1"
    assert kw["concurrency_override"] == 4
    assert kw["uc_object_types_override"] == ["table"]
    assert kw["table_types_override"] == ["managed"]
    assert kw["environment_name"] == "dev"
    assert kw["env_path"] == "e"


def test_main_validate_only_returns_0_without_connecting():
    with _Main(["all"], _config(), args=_args(validate_only=True)) as mm:
        assert m.main() == 0
    mm["ws"].assert_not_called()
    mm["spark"].assert_not_called()


def test_main_uses_profile_and_target_logging_workspace():
    cfg = _config(logging_workspace="target")
    with _Main(["replication"], cfg, args=_args(profile="AZURE")) as mm:
        m.main()
    mm["ws"].assert_called_once_with(profile="AZURE")
    assert mm["spark"].call_args[0][0] == TGT_HOST
    assert mm["spark"].call_args[0][1] == "tok-tgt"


def test_main_source_logging_workspace():
    with _Main(["replication"], _config(logging_workspace="source")) as mm:
        m.main()
    mm["ws"].assert_called_once_with()
    assert mm["spark"].call_args[0][0] == SRC_HOST


def test_main_logging_session_on_wrong_workspace_returns_1():
    with _Main(["all"], _config()) as mm:
        mm["validate"].return_value = False
        assert m.main() == 1
    mm["replication"].assert_not_called()


def test_main_uses_explicit_run_id():
    cats = [_cat("c1", replication=True)]
    with _Main(["replication"], _config(catalogs=cats), _args(run_id="fixed")) as mm:
        m.main()
    assert mm["replication"].call_args[0][3] == "fixed"


def test_main_all_runs_every_enabled_operation_in_order():
    cats = [_cat("c1", backup=True, replication=True, reconciliation=True)]
    order = []
    with _Main(["all"], _config(catalogs=cats)) as mm:
        for op in ("backup", "replication", "reconciliation"):
            mm[op].side_effect = lambda *a, _op=op: order.append(_op) or 0
        m.main()
    assert order == ["backup", "replication", "reconciliation"]
    # "all" without UC object types waits for Delta Share schemas (default 60s)
    mm["sleep"].assert_called_once_with(60)


def test_main_all_uses_custom_wait_and_skips_wait_for_uc_metadata():
    cats = [_cat("c1", replication=True)]
    with _Main(["all"], _config(catalogs=cats), _args(replication_wait_secs=5)) as mm:
        m.main()
    mm["sleep"].assert_called_once_with(5)

    with _Main(["all"], _config(catalogs=cats, uc_object_types=["table"])) as mm:
        m.main()
    mm["sleep"].assert_not_called()


def test_main_single_operation_runs_only_that_operation():
    cats = [_cat("c1", backup=True, replication=True, reconciliation=True)]
    with _Main(["reconciliation"], _config(catalogs=cats)) as mm:
        m.main()
    mm["reconciliation"].assert_called_once()
    mm["backup"].assert_not_called()
    mm["replication"].assert_not_called()
    mm["sleep"].assert_not_called()


def test_main_skips_disabled_operations():
    cats = [_cat("c1")]  # nothing enabled
    with _Main(
        ["backup", "replication", "reconciliation"], _config(catalogs=cats)
    ) as mm:
        m.main()
    mm["backup"].assert_not_called()
    mm["replication"].assert_not_called()
    mm["reconciliation"].assert_not_called()


def test_main_replication_runs_for_uc_metadata_without_enabled_catalogs():
    cfg = _config(catalogs=[_cat("c1")], uc_object_types=["all"])
    with _Main(["replication"], cfg) as mm:
        m.main()
    mm["replication"].assert_called_once()


def test_main_runner_exception_returns_1():
    cats = [_cat("c1", backup=True)]
    with _Main(["backup"], _config(catalogs=cats)) as mm:
        mm["backup"].side_effect = RuntimeError("share failed")
        assert m.main() == 1
    assert any(
        "share failed" in str(c) for c in mm["logger"].return_value.error.call_args_list
    )


def test_main_runner_configuration_error_returns_1():
    cats = [_cat("c1", replication=True)]
    with _Main(["replication"], _config(catalogs=cats)) as mm:
        mm["replication"].side_effect = ConfigurationError("wrong ws")
        assert m.main() == 1
    mm["logger"].return_value.error.assert_called_with("Configuration error: wrong ws")


# ---------- cli/main.py wrapper ----------


def test_cli_main_delegates_to_main():
    from data_replication.cli import main as cli

    with patch.object(m, "main", return_value=0) as impl:
        assert cli.main() == 0
    impl.assert_called_once_with()


def test_cli_main_unexpected_error_returns_1(capsys):
    from data_replication.cli import main as cli

    with patch.object(m, "main", side_effect=RuntimeError("boom")):
        assert cli.main() == 1
    assert "boom" in capsys.readouterr().err


def test_cli_main_import_error_returns_1(capsys):
    import builtins

    from data_replication.cli import main as cli

    real_import = builtins.__import__

    def fake_import(name, *a, **kw):
        if name == "data_replication.main":
            raise ImportError("no module")
        return real_import(name, *a, **kw)

    with patch.object(builtins, "__import__", side_effect=fake_import):
        assert cli.main() == 1
    assert "no module" in capsys.readouterr().err
