"""
Token-aware SQL rewriting helpers.

Used when replaying DDL from a source catalog into a target catalog with a
different name: object references inside view / materialized view / streaming
table bodies (and, later, function bodies) still point at the source catalog
and must be rewritten.
"""

import re
from typing import Dict, List, Optional, Tuple

_BARE_IDENT_RE = re.compile(r"[A-Za-z0-9_]+")
_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

# Token kinds
_IDENT = "ident"  # bare identifier / keyword / number
_QUOTED = "quoted"  # `backticked` identifier
_DOT = "dot"
_WS = "ws"  # whitespace or comment (insignificant for name resolution)
_OTHER = "other"  # string literals, operators, punctuation


def _scan_quoted(sql: str, i: int, quote: str) -> int:
    """Return the index just past a quoted token starting at ``sql[i]``.

    Handles backslash escapes and doubled quotes. An unterminated token runs
    to the end of the input.
    """
    n = len(sql)
    j = i + 1
    while j < n:
        ch = sql[j]
        if ch == "\\" and quote != "`":
            j += 2
            continue
        if ch == quote:
            if j + 1 < n and sql[j + 1] == quote:
                j += 2
                continue
            return j + 1
        j += 1
    return n


def _scan_block_comment(sql: str, i: int) -> int:
    """Return the index just past a (possibly nested) ``/* ... */`` comment."""
    n = len(sql)
    depth = 0
    j = i
    while j < n:
        if sql.startswith("/*", j):
            depth += 1
            j += 2
        elif sql.startswith("*/", j):
            depth -= 1
            j += 2
            if depth == 0:
                return j
        else:
            j += 1
    return n


def _tokenize(sql: str) -> List[Tuple[str, str]]:
    """Split ``sql`` into (kind, text) tokens. Concatenating texts yields ``sql``."""
    tokens: List[Tuple[str, str]] = []
    n = len(sql)
    i = 0
    while i < n:
        ch = sql[i]
        if ch.isspace():
            j = i + 1
            while j < n and sql[j].isspace():
                j += 1
            tokens.append((_WS, sql[i:j]))
        elif sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
            tokens.append((_WS, sql[i:j]))
        elif sql.startswith("/*", i):
            j = _scan_block_comment(sql, i)
            tokens.append((_WS, sql[i:j]))
        elif ch in ("'", '"'):
            j = _scan_quoted(sql, i, ch)
            tokens.append((_OTHER, sql[i:j]))
        elif ch == "`":
            j = _scan_quoted(sql, i, "`")
            tokens.append((_QUOTED, sql[i:j]))
        elif ch == ".":
            j = i + 1
            tokens.append((_DOT, ch))
        else:
            m = _BARE_IDENT_RE.match(sql, i)
            if m:
                j = m.end()
                tokens.append((_IDENT, sql[i:j]))
            else:
                j = i + 1
                tokens.append((_OTHER, ch))
        i = j
    return tokens


def _ident_value(kind: str, text: str) -> str:
    if kind == _QUOTED:
        inner = text[1:-1] if text.endswith("`") and len(text) >= 2 else text[1:]
        return inner.replace("``", "`")
    return text


def _quote_ident(name: str, force: bool) -> str:
    if not force and _SIMPLE_IDENT_RE.match(name):
        return name
    return "`" + name.replace("`", "``") + "`"


def _next_significant(tokens: List[Tuple[str, str]], i: int) -> int:
    while i < len(tokens) and tokens[i][0] == _WS:
        i += 1
    return i


def _prev_significant(tokens: List[Tuple[str, str]], i: int) -> int:
    while i >= 0 and tokens[i][0] == _WS:
        i -= 1
    return i


def _qualified_parts_after(tokens: List[Tuple[str, str]], i: int) -> int:
    """Count ``.part`` segments following the identifier at ``tokens[i]``."""
    count = 0
    j = i + 1
    while True:
        j = _next_significant(tokens, j)
        if j >= len(tokens) or tokens[j][0] != _DOT:
            return count
        j = _next_significant(tokens, j + 1)
        if j >= len(tokens) or tokens[j][0] not in (_IDENT, _QUOTED):
            return count
        count += 1
        j += 1


def rewrite_catalog_references(
    sql: str, catalog_map: Optional[Dict[str, str]]
) -> str:
    """Rewrite catalog names in fully qualified object references.

    An identifier is treated as a catalog reference only when it is the first
    part of a name with at least three parts (``catalog.schema.object``), i.e.
    it is not preceded by ``.`` and is followed by two or more ``.part``
    segments. Two-part names are left alone because they are usually
    ``alias.column`` or ``schema.table``.

    Matching is case-insensitive and works for bare and backticked
    identifiers. String literals and comments are never modified. Backticked
    references stay backticked; bare references are backticked only when the
    target name requires quoting.

    Args:
        sql: SQL text to rewrite.
        catalog_map: Mapping of source catalog name -> target catalog name.

    Returns:
        The rewritten SQL. Returned unchanged if ``catalog_map`` is empty or
        maps every catalog to itself.
    """
    if not sql or not catalog_map:
        return sql
    mapping = {
        str(src).lower(): str(tgt)
        for src, tgt in catalog_map.items()
        if src and tgt and str(src).lower() != str(tgt).lower()
    }
    if not mapping:
        return sql

    tokens = _tokenize(sql)
    out: List[str] = []
    for idx, (kind, text) in enumerate(tokens):
        if kind in (_IDENT, _QUOTED):
            target = mapping.get(_ident_value(kind, text).lower())
            if target is not None:
                prev = _prev_significant(tokens, idx - 1)
                if (prev < 0 or tokens[prev][0] != _DOT) and _qualified_parts_after(
                    tokens, idx
                ) >= 2:
                    out.append(_quote_ident(target, force=kind == _QUOTED))
                    continue
        out.append(text)
    return "".join(out)
