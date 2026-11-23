# CLAUDE.md

Databricks replicator: replicates Unity Catalog metadata and data between workspaces/metastores (Delta Sharing, deep clone, Auto Loader). CLI `data-replicator`, also deployable as a DAB job (`databricks.yml`, `resources/`).

## Commands
- Setup: `make setup` (creates `.venv`; Python 3.12, required by databricks-connect 17.1).
- Tests: `pytest` (config in `pytest.ini`, which also sets the 90% coverage minimum CI enforces).
- Checks (same as CI): `make quality` = `ruff check .` + `ruff format --check .` + `mypy` (mypy is report-only, ~590 known errors). Auto-fix: `make fix`.
- Validate a config without connecting: `data-replicator <config.yaml> --validate-only`.

## Layout
- `src/data_replication/main.py` — entry point; runs backup → replication → reconciliation.
- `config/models.py` — Pydantic models and enums (`UCObjectType`, `TableType`, …); `config/loader.py` merges YAML + `environments.yaml` + CLI overrides.
- `providers/` — `provider_factory.py` builds providers and fans out over catalogs/schemas; `base_provider.py` holds catalog/schema orchestration; `replication_provider.py`, `backup_provider.py`, `reconciliation_provider.py` do the per-object work.
- `databricks_operations.py` — SQL/SDK helpers against a Spark session (one instance per source/target).
- `sql_utils.py` — token-aware SQL helpers (one shared tokenizer): TBLPROPERTIES/mask cleanup, catalog reference rewrite.
- `constants.py` — domain constants (creation/update field maps, unreplayable table properties).
- `configs/README.yaml` — the annotated reference for every config option.

## Conventions
- New config option: add it to `config/models.py` **and** document it in `configs/README.yaml`.
- New UC object type: add to `UCObjectType`, make sure `all` covers it, update `--uc-object-types` help in `cli/args.py` and the README object matrix. Exception: `ownership` is opt-in and not part of `all`, because owner transfers can remove the run principal's privileges.
- Per-object work returns `RunResult`s and uses `retry_with_logging` (`utils.py`); never let one object's failure abort the run.
- DDL replay (tables/views/MV/ST) rewrites only the statement header of `SHOW CREATE TABLE` output; body rewrites (catalog names, masks, properties) must be token-aware — never naive regex over SQL that may contain string literals. Add such helpers to `sql_utils.py` and reuse its tokenizer.
- Before adding a helper, search `src/` for existing code that does the same job and extend it. Domain constants go in `constants.py`; keep `utils.py` for generic helpers and give a self-contained concern its own module.
- Tests are offline: construct providers with `ReplicationProvider.__new__(ReplicationProvider)` and stub `spark.sql` / `DatabricksOperations`; no workspace access in unit tests.

## Gotchas
- `information_schema.routines`: `is_deterministic` is `'true'/'false'` (not YES/NO); TVF result columns live in `routine_columns` (0-based `ordinal_position`, unordered).
- By design, the CLI exits 0 when individual objects fail; failures are recorded per object in the audit table (`audit_config.audit_table`). Check the audit table or run summary, not the exit code.
- The editable install points at the main checkout; in a git worktree run with `PYTHONPATH=src`.

## Git / PRs
- Force-push and deletion are blocked on `main` only. Update PR branches by merging `main` (avoids rewriting reviewed history).
- Merged branches are auto-deleted, so a PR stacked on another PR's branch is retargeted to `main` when the base merges; after a squash merge, merge `main` into the stacked branch.
- Merge PRs that touch `.git-blame-ignore-revs` with a merge commit (squash/rebase changes the SHAs it lists).

Personal settings (workspace profiles, test catalogs) go in `CLAUDE.local.md` (gitignored), not here.
