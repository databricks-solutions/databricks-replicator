"""
Reconstruct CREATE FUNCTION DDL for Unity Catalog functions.

Databricks SQL has no ``SHOW CREATE FUNCTION``, so the DDL is rebuilt from the
function's metadata as returned by the Databricks SDK
(``WorkspaceClient.functions.get`` -> ``FunctionInfo``): ordered input
parameters, ``return_params`` for table-valued functions, and typed fields for
determinism, data access, body and language.

The function is still created with SQL (``CREATE [OR REPLACE] FUNCTION``)
rather than ``functions.create``: SQL replaces atomically and keeps existing
grants, and ``functions.create`` is an experimental API.

Everything in this module is pure (``FunctionInfo`` in, string out) so it can
be unit-tested without Spark or a workspace.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from databricks.sdk.service.catalog import FunctionInfo, FunctionParameterInfo

from .sql_utils import quote_identifier, quote_string, rewrite_catalog_references


class UnsupportedFunctionError(ValueError):
    """Raised when a function cannot be reconstructed into replayable DDL."""


# FunctionInfo.sql_data_access -> CREATE FUNCTION clause. NO_SQL has no clause
# (it is only valid for non-SQL bodies, where data access is not declared).
_DATA_ACCESS_CLAUSES = {
    "CONTAINS_SQL": "CONTAINS SQL",
    "READS_SQL_DATA": "READS SQL DATA",
}


def _enum_text(value: Any) -> str:
    """Upper-case text of an SDK enum (or plain string) value."""
    if value is None:
        return ""
    if isinstance(value, Enum):
        value = value.value
    return str(value).strip().upper()


def _params(container: Any) -> List[FunctionParameterInfo]:
    """Parameters of a FunctionParameterInfos, sorted by position."""
    params = list(getattr(container, "parameters", None) or [])
    return sorted(params, key=lambda p: p.position if p.position is not None else 0)


def _type_text(param: FunctionParameterInfo) -> str:
    if not param.type_text:
        raise UnsupportedFunctionError(f"Missing type for parameter {param.name!r}")
    return param.type_text.strip()


def _column_list(params: List[FunctionParameterInfo], with_default: bool) -> str:
    parts = []
    for p in params:
        if not p.name:
            raise UnsupportedFunctionError("Function parameter has no name")
        part = f"{quote_identifier(p.name)} {_type_text(p)}"
        if with_default and p.parameter_default not in (None, ""):
            part += f" DEFAULT {str(p.parameter_default).strip()}"
        if p.comment:
            part += f" COMMENT {quote_string(p.comment)}"
        parts.append(part)
    return ", ".join(parts)


def _strip_return_keyword(body: str) -> str:
    """routine_definition holds the body without RETURN, but tolerate a
    leading RETURN in case a runtime includes it."""
    stripped = body.strip()
    if stripped[:6].upper() == "RETURN" and (
        len(stripped) == 6 or not (stripped[6].isalnum() or stripped[6] == "_")
    ):
        stripped = stripped[6:].lstrip()
    return stripped


def build_create_function_ddl(
    function: FunctionInfo,
    target_full_name: str,
    create_or_replace: bool = False,
    catalog_map: Optional[Dict[str, str]] = None,
) -> str:
    """
    Build a CREATE FUNCTION statement from SDK function metadata.

    Args:
        function: ``FunctionInfo`` from ``WorkspaceClient.functions.get``.
        target_full_name: already-quoted target name, e.g.
            ```cat`.`sch`.`fn```.
        create_or_replace: emit CREATE OR REPLACE FUNCTION instead of
            CREATE FUNCTION IF NOT EXISTS.
        catalog_map: source -> target catalog names; fully qualified
            references in SQL bodies are rewritten with
            ``sql_utils.rewrite_catalog_references``. Python bodies are not
            rewritten.

    Returns:
        The CREATE FUNCTION DDL string.

    Raises:
        UnsupportedFunctionError: if the function kind or body cannot be
            replayed (e.g. unknown language, missing body).
    """
    routine_body = _enum_text(function.routine_body) or "SQL"
    language = _enum_text(function.external_language)
    if routine_body == "SQL":
        language = "SQL"
    elif routine_body != "EXTERNAL" or language != "PYTHON":
        raise UnsupportedFunctionError(
            f"Unsupported function body/language: routine_body={routine_body}, "
            f"external_language={language or None}"
        )

    definition = function.routine_definition
    if definition is None or str(definition).strip() == "":
        raise UnsupportedFunctionError("Function has no routine_definition")
    definition = str(definition)

    if create_or_replace:
        header = f"CREATE OR REPLACE FUNCTION {target_full_name}"
    else:
        header = f"CREATE FUNCTION IF NOT EXISTS {target_full_name}"

    params = _params(function.input_params)
    lines = [f"{header}({_column_list(params, with_default=True)})"]

    if _enum_text(function.data_type) == "TABLE_TYPE":
        result_cols = _params(function.return_params)
        if not result_cols:
            raise UnsupportedFunctionError("Table-valued function has no return_params")
        lines.append(f"RETURNS TABLE ({_column_list(result_cols, with_default=False)})")
    else:
        return_type = function.full_data_type or _enum_text(function.data_type)
        if not return_type:
            raise UnsupportedFunctionError("Function has no return type")
        lines.append(f"RETURNS {str(return_type).strip()}")

    lines.append(f"LANGUAGE {language}")

    if function.is_deterministic is True:
        lines.append("DETERMINISTIC")
    elif function.is_deterministic is False:
        lines.append("NOT DETERMINISTIC")

    if function.comment:
        lines.append(f"COMMENT {quote_string(function.comment)}")

    if language == "SQL":
        data_access = _DATA_ACCESS_CLAUSES.get(_enum_text(function.sql_data_access))
        if data_access:
            lines.append(data_access)
        body = _strip_return_keyword(definition)
        body = rewrite_catalog_references(body, catalog_map)
        lines.append(f"RETURN {body}")
    else:
        if "$$" in definition:
            raise UnsupportedFunctionError(
                "Python function body contains '$$' and cannot be wrapped in $$ ... $$"
            )
        lines.append(f"AS $$\n{definition.strip(chr(10))}\n$$")

    return "\n".join(lines)
