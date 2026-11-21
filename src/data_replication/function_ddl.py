"""
Reconstruct CREATE FUNCTION DDL for Unity Catalog functions.

Databricks SQL has no ``SHOW CREATE FUNCTION``, so the DDL is rebuilt from
``information_schema.routines`` (one row per function),
``information_schema.parameters`` (input parameters) and
``information_schema.routine_columns`` (result columns of table-valued
functions).

Everything in this module is pure (plain dicts in, string out) so it can be
unit-tested without Spark.
"""

from typing import Any, Dict, Iterable, List, Optional


class UnsupportedFunctionError(ValueError):
    """Raised when a function cannot be reconstructed into replayable DDL."""


def quote_identifier(name: str) -> str:
    """Backtick-quote an identifier, escaping embedded backticks."""
    return "`" + str(name).replace("`", "``") + "`"


def quote_string(value: str) -> str:
    """Render a SQL single-quoted string literal."""
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _get(row: Dict[str, Any], key: str) -> Any:
    """Case-insensitive dict lookup (Spark Row.asDict keys are lower case,
    but be lenient for hand-built rows)."""
    if key in row:
        return row[key]
    upper = key.upper()
    if upper in row:
        return row[upper]
    return None


def _type_text(row: Dict[str, Any]) -> str:
    """Prefer full_data_type (e.g. DECIMAL(10,2), ARRAY<STRING>) over
    data_type (e.g. DECIMAL, ARRAY)."""
    type_text = _get(row, "full_data_type") or _get(row, "data_type")
    if not type_text:
        raise UnsupportedFunctionError(f"Missing data type in row: {row}")
    return str(type_text).strip()


def _sorted_by_position(rows: Optional[Iterable[Dict[str, Any]]]) -> List[Dict]:
    return sorted(rows or [], key=lambda r: int(_get(r, "ordinal_position") or 0))


def _column_list(rows: List[Dict[str, Any]], name_key: str, with_default: bool) -> str:
    parts = []
    for row in rows:
        name = _get(row, name_key)
        if not name:
            raise UnsupportedFunctionError(f"Missing {name_key} in row: {row}")
        part = f"{quote_identifier(name)} {_type_text(row)}"
        if with_default:
            default = _get(row, "parameter_default")
            if default is not None and str(default).strip() != "":
                part += f" DEFAULT {str(default).strip()}"
        comment = _get(row, "comment")
        if comment:
            part += f" COMMENT {quote_string(comment)}"
        parts.append(part)
    return ", ".join(parts)


def _is_table_function(routine: Dict[str, Any], result_columns: List[Dict]) -> bool:
    data_type = str(_get(routine, "data_type") or "").strip().upper()
    return data_type == "TABLE" or bool(result_columns)


def _yes(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().upper()
    if text in ("YES", "TRUE", "Y"):
        return True
    if text in ("NO", "FALSE", "N"):
        return False
    return None


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
    routine: Dict[str, Any],
    parameters: Optional[Iterable[Dict[str, Any]]],
    result_columns: Optional[Iterable[Dict[str, Any]]],
    target_full_name: str,
    create_or_replace: bool = False,
) -> str:
    """
    Build a CREATE FUNCTION statement.

    Args:
        routine: information_schema.routines row as a dict.
        parameters: information_schema.parameters rows for the function
            (input parameters; rows with is_result = 'YES' are ignored).
        result_columns: information_schema.routine_columns rows (result
            columns of a table-valued function); empty for scalar functions.
        target_full_name: already-quoted target name, e.g.
            ```cat`.`sch`.`fn```.
        create_or_replace: emit CREATE OR REPLACE FUNCTION instead of
            CREATE FUNCTION IF NOT EXISTS.

    Returns:
        The CREATE FUNCTION DDL string.

    Raises:
        UnsupportedFunctionError: if the function kind or body cannot be
            replayed (e.g. unknown language, missing body).
    """
    params = [
        p
        for p in _sorted_by_position(parameters)
        if _yes(_get(p, "is_result")) is not True
    ]
    result_cols = _sorted_by_position(result_columns)

    routine_body = str(_get(routine, "routine_body") or "SQL").strip().upper()
    language = str(_get(routine, "external_language") or "").strip().upper()
    if routine_body == "SQL":
        language = "SQL"
    elif routine_body != "EXTERNAL" or language != "PYTHON":
        raise UnsupportedFunctionError(
            f"Unsupported function body/language: routine_body={routine_body}, "
            f"external_language={language or None}"
        )

    definition = _get(routine, "routine_definition")
    if definition is None or str(definition).strip() == "":
        raise UnsupportedFunctionError("Function has no routine_definition")
    definition = str(definition)

    if create_or_replace:
        header = f"CREATE OR REPLACE FUNCTION {target_full_name}"
    else:
        header = f"CREATE FUNCTION IF NOT EXISTS {target_full_name}"

    lines = [f"{header}({_column_list(params, 'parameter_name', with_default=True)})"]

    if _is_table_function(routine, result_cols):
        if not result_cols:
            raise UnsupportedFunctionError(
                "Table-valued function has no result columns in routine_columns"
            )
        lines.append(
            f"RETURNS TABLE ({_column_list(result_cols, 'column_name', with_default=False)})"
        )
    else:
        lines.append(f"RETURNS {_type_text(routine)}")

    lines.append(f"LANGUAGE {language}")

    deterministic = _yes(_get(routine, "is_deterministic"))
    if deterministic is True:
        lines.append("DETERMINISTIC")
    elif deterministic is False:
        lines.append("NOT DETERMINISTIC")

    comment = _get(routine, "comment")
    if comment:
        lines.append(f"COMMENT {quote_string(comment)}")

    if language == "SQL":
        data_access = str(_get(routine, "sql_data_access") or "").strip().upper()
        # Only CONTAINS SQL and READS SQL DATA are valid in CREATE FUNCTION.
        if data_access in ("CONTAINS SQL", "READS SQL DATA"):
            lines.append(data_access)
        lines.append(f"RETURN {_strip_return_keyword(definition)}")
    else:
        if "$$" in definition:
            raise UnsupportedFunctionError(
                "Python function body contains '$$' and cannot be wrapped in $$ ... $$"
            )
        body = definition.strip("\n")
        lines.append(f"AS $$\n{body}\n$$")

    return "\n".join(lines)
