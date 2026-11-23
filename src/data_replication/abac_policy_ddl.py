"""
Reconstruct CREATE POLICY DDL for ABAC column-mask and row-filter policies.

Policy definitions come from the Databricks SDK (``policies.get_policy`` ->
``PolicyInfo``), which returns the structured definition: function name, ON
COLUMN alias, USING arguments, MATCH COLUMNS conditions, WHEN condition, TO /
EXCEPT principals and comment. The statement is rebuilt from those fields
rather than parsed from DESCRIBE POLICY text, and replayed with
``CREATE OR REPLACE POLICY`` over the replicator's Spark session.

Everything here is pure (PolicyInfo in, string out) so it can be unit-tested
without Spark or a workspace. Principal mapping is done by the caller; this
module quotes the principals it is given.
"""

from enum import Enum
from typing import Any, Dict, Iterable, Optional, Sequence, Tuple

from databricks.sdk.service.catalog import FunctionArgument, MatchColumn, PolicyInfo

from .sql_utils import quote_identifier, quote_string, rewrite_catalog_references

ROW_FILTER = "POLICY_TYPE_ROW_FILTER"
COLUMN_MASK = "POLICY_TYPE_COLUMN_MASK"
_SECURABLE_TYPES = ("CATALOG", "SCHEMA", "TABLE")


class UnsupportedPolicyError(ValueError):
    """Raised when a policy cannot be reconstructed into replayable DDL."""


def _text(value: Any) -> str:
    """Upper-case text of an SDK enum (or plain string); "" for None."""
    if value is None:
        return ""
    if isinstance(value, Enum):
        value = value.value
    return str(value).strip().upper()


def policy_function_name(policy: PolicyInfo) -> str:
    """The raw (dotted) function name a row-filter or column-mask policy calls."""
    if _text(policy.policy_type) == ROW_FILTER and policy.row_filter:
        return policy.row_filter.function_name or ""
    if _text(policy.policy_type) == COLUMN_MASK and policy.column_mask:
        return policy.column_mask.function_name or ""
    return ""


def map_function_name(full_name: str, catalog_map: Optional[Dict[str, str]]) -> str:
    """
    Rename the catalog of ``catalog.schema.function`` through ``catalog_map``
    (source -> target). Returns the dotted, unquoted name.

    Raises:
        UnsupportedPolicyError: if the name is not three plain parts.
    """
    parts = (full_name or "").split(".")
    if len(parts) != 3 or not all(parts):
        raise UnsupportedPolicyError(
            f"Expected a catalog.schema.function name for the policy function, got {full_name!r}"
        )
    catalog, schema, function = parts
    catalog = (catalog_map or {}).get(catalog, catalog)
    return f"{catalog}.{schema}.{function}"


def _quote_dotted(dotted: str) -> str:
    return ".".join(quote_identifier(part) for part in dotted.split("."))


def _principal_list(principals: Sequence[str]) -> str:
    return ", ".join(quote_identifier(p) for p in principals)


def _match_columns(
    match_columns: Iterable[MatchColumn], catalog_map: Optional[Dict[str, str]]
) -> str:
    parts = []
    for match in match_columns:
        if not match.condition:
            raise UnsupportedPolicyError("MATCH COLUMNS entry has no condition")
        condition = rewrite_catalog_references(match.condition, catalog_map)
        if match.alias:
            parts.append(f"{condition} AS {quote_identifier(match.alias)}")
        else:
            parts.append(condition)
    return ", ".join(parts)


def _using_arguments(arguments: Iterable[FunctionArgument]) -> str:
    parts = []
    for argument in arguments:
        if argument.alias:
            parts.append(quote_identifier(argument.alias))
        elif argument.constant is not None:
            # Constants are SQL literals from the source definition; replayed as-is.
            parts.append(str(argument.constant))
        else:
            raise UnsupportedPolicyError(
                "USING argument has neither alias nor constant"
            )
    return ", ".join(parts)


def build_create_policy_ddl(
    policy: PolicyInfo,
    target_on_securable: str,
    to_principals: Sequence[str],
    except_principals: Sequence[str] = (),
    catalog_map: Optional[Dict[str, str]] = None,
) -> str:
    """
    Build a ``CREATE OR REPLACE POLICY`` statement from SDK policy metadata.

    Args:
        policy: ``PolicyInfo`` from ``WorkspaceClient.policies.get_policy``.
        target_on_securable: already-quoted target securable, e.g.
            ```cat`.`sch``` for a schema policy or ```cat`.`sch`.`t``` for a table.
        to_principals: target principal names (already mapped by the caller).
        except_principals: target EXCEPT principal names (already mapped).
        catalog_map: source -> target catalog names. Applied to the policy
            function name and to conditions (token-aware rewrite).

    Returns:
        The CREATE OR REPLACE POLICY DDL string.

    Raises:
        UnsupportedPolicyError: if the policy type, scope or clauses cannot be
            replayed.
    """
    policy_type = _text(policy.policy_type)
    on_type = _text(policy.on_securable_type)
    if not policy.name:
        raise UnsupportedPolicyError("Policy has no name")
    if on_type not in _SECURABLE_TYPES:
        raise UnsupportedPolicyError(
            f"Unsupported policy securable type: {on_type or None}"
        )
    if _text(policy.for_securable_type) != "TABLE":
        raise UnsupportedPolicyError(
            f"Unsupported FOR securable type: {_text(policy.for_securable_type) or None}"
        )
    if not to_principals:
        raise UnsupportedPolicyError("Policy has no TO principals")
    if not policy.match_columns:
        raise UnsupportedPolicyError("Policy has no MATCH COLUMNS conditions")

    function_name = _quote_dotted(
        map_function_name(policy_function_name(policy), catalog_map)
    )
    lines = [
        f"CREATE OR REPLACE POLICY {quote_identifier(policy.name)} "
        f"ON {on_type} {target_on_securable}"
    ]
    if policy.comment:
        lines.append(f"COMMENT {quote_string(policy.comment)}")

    if policy_type == ROW_FILTER:
        options = policy.row_filter
        if options is None:
            raise UnsupportedPolicyError("Row filter policy has no row_filter options")
        lines.append(f"ROW FILTER {function_name}")
    elif policy_type == COLUMN_MASK:
        options = policy.column_mask
        if options is None or not options.on_column:
            raise UnsupportedPolicyError("Column mask policy has no ON COLUMN alias")
        lines.append(f"COLUMN MASK {function_name}")
    else:
        raise UnsupportedPolicyError(f"Unsupported policy type: {policy_type or None}")

    lines.append(f"TO {_principal_list(to_principals)}")
    if except_principals:
        lines.append(f"EXCEPT {_principal_list(except_principals)}")
    lines.append("FOR TABLES")
    if policy.when_condition:
        lines.append(
            f"WHEN {rewrite_catalog_references(policy.when_condition, catalog_map)}"
        )
    lines.append(f"MATCH COLUMNS {_match_columns(policy.match_columns, catalog_map)}")
    if policy_type == COLUMN_MASK:
        lines.append(f"ON COLUMN {quote_identifier(options.on_column)}")
    if options.using:
        lines.append(f"USING COLUMNS ({_using_arguments(options.using)})")
    return "\n".join(lines)


def policy_fingerprint(
    policy: PolicyInfo,
    to_principals: Sequence[str],
    except_principals: Sequence[str],
    function_name: str,
) -> Tuple:
    """
    Comparable view of a policy definition that ignores ids, timestamps and
    owners. Used to tell an unchanged target policy from a changed one.

    ``to_principals`` / ``except_principals`` / ``function_name`` are passed in
    so the caller can compare a mapped source definition with a target one.
    """
    options = (
        policy.row_filter
        if _text(policy.policy_type) == ROW_FILTER
        else policy.column_mask
    )
    using = tuple(
        (arg.alias, arg.constant)
        for arg in ((options.using if options else None) or [])
    )
    return (
        _text(policy.policy_type),
        _text(policy.on_securable_type),
        _text(policy.for_securable_type),
        tuple(sorted(to_principals)),
        tuple(sorted(except_principals or [])),
        policy.comment or "",
        policy.when_condition or "",
        function_name,
        getattr(options, "on_column", None),
        using,
        tuple((m.alias, m.condition) for m in (policy.match_columns or [])),
    )
