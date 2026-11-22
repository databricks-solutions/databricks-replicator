"""
Token-aware SQL helpers for replaying source DDL on a target workspace.

All helpers share one tokenizer, so string literals, backticked identifiers and
comments are never rewritten:

* ``strip_unreplayable_tblproperties``: drop engine-derived table properties
  (protocol versions, row-tracking materialized column names, ...) that are
  rejected when set explicitly.
* ``strip_inline_masks_and_row_filters``: drop inline column masks
  (``MASK fn``) and row filters (``WITH ROW FILTER fn ON (...)``) whose
  functions may not exist on the target.
* ``rewrite_catalog_references``: rewrite the catalog in fully qualified
  ``catalog.schema.object`` references (view / MV / ST bodies) when the target
  catalog has a different name.

It also holds the quoting helpers used when generating SQL:

* ``quote_identifier``: backtick-quote an identifier.
* ``quote_string``: render a single-quoted string literal.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

from .constants import UNREPLAYABLE_TBLPROPERTIES


def quote_identifier(name: str) -> str:
    """Backtick-quote an identifier, escaping embedded backticks."""
    return "`" + str(name).replace("`", "``") + "`"


def quote_string(value: str) -> str:
    """
    Render a single-quoted SQL string literal, backslash-escaping ``\\`` and
    ``'``.

    Use ``\\'`` rather than ``''``: Databricks SQL reads ``'it''s'`` as two
    adjacent literals and concatenates them to ``its`` in expressions (it is
    only accepted as ``it's`` in some DDL clauses such as COMMENT).
    """
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


@dataclass
class _Token:
    kind: str  # ws, comment, string, ident, word, punct
    text: str

    @property
    def significant(self) -> bool:
        return self.kind not in ("ws", "comment")

    def is_word(self, *words: str) -> bool:
        return self.kind == "word" and self.text.upper() in words


_WORD_RE = re.compile(r"[A-Za-z0-9_$]+")


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


def _tokenize(sql: str) -> List[_Token]:
    """Split SQL into tokens. Concatenating token texts returns the input."""
    tokens: List[_Token] = []
    i, n = 0, len(sql)
    while i < n:
        c = sql[i]
        if c.isspace():
            j = i
            while j < n and sql[j].isspace():
                j += 1
            tokens.append(_Token("ws", sql[i:j]))
        elif c == "-" and sql.startswith("--", i):
            j = sql.find("\n", i)
            j = n if j == -1 else j
            tokens.append(_Token("comment", sql[i:j]))
        elif c == "/" and sql.startswith("/*", i):
            j = _scan_block_comment(sql, i)
            tokens.append(_Token("comment", sql[i:j]))
        elif c in ("'", '"', "`"):
            j = i + 1
            while j < n:
                if sql[j] == "\\" and c != "`":
                    j += 2
                    continue
                if sql[j] == c:
                    # Doubled quote is an escaped quote.
                    if j + 1 < n and sql[j + 1] == c:
                        j += 2
                        continue
                    j += 1
                    break
                j += 1
            else:
                j = n
            tokens.append(_Token("ident" if c == "`" else "string", sql[i:j]))
        else:
            m = _WORD_RE.match(sql, i)
            if m:
                tokens.append(_Token("word", m.group(0)))
                j = m.end()
            else:
                tokens.append(_Token("punct", c))
                j = i + 1
        i = j
    return tokens


def _join(tokens: Iterable[_Token]) -> str:
    return "".join(t.text for t in tokens)


def _next_sig(tokens: List[_Token], i: int) -> Optional[int]:
    """Index of the first significant token at or after ``i``."""
    while i < len(tokens):
        if tokens[i].significant:
            return i
        i += 1
    return None


def _matching_paren(tokens: List[_Token], open_idx: int) -> Optional[int]:
    depth = 0
    for k in range(open_idx, len(tokens)):
        t = tokens[k]
        if t.kind == "punct" and t.text == "(":
            depth += 1
        elif t.kind == "punct" and t.text == ")":
            depth -= 1
            if depth == 0:
                return k
    return None


def _trim_leading_ws(tokens: List[_Token], start: int) -> int:
    """Extend a removal range start backwards over preceding whitespace."""
    while start > 0 and tokens[start - 1].kind == "ws":
        start -= 1
    return start


def _unquote(text: str) -> str:
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"', "`"):
        return text[1:-1]
    return text


def _is_unreplayable(key: str, reserved: Iterable[str]) -> bool:
    k = key.lower()
    for r in reserved:
        r = r.lower()
        if r.endswith("*"):
            if k.startswith(r[:-1]):
                return True
        elif k == r:
            return True
    return False


def _property_key(entry: List[_Token]) -> Optional[str]:
    """Return the key of a ``key = value`` TBLPROPERTIES entry."""
    sig = [t for t in entry if t.significant]
    if not sig:
        return None
    if sig[0].kind in ("string", "ident"):
        return _unquote(sig[0].text)
    parts = []
    for t in sig:
        if t.kind == "punct" and t.text == "=":
            break
        parts.append(_unquote(t.text))
    return "".join(parts)


def strip_unreplayable_tblproperties(
    ddl: str, reserved: Iterable[str] = UNREPLAYABLE_TBLPROPERTIES
) -> Tuple[str, List[str]]:
    """
    Remove engine-derived keys from every ``TBLPROPERTIES (...)`` clause.

    Other properties are preserved verbatim. If a clause ends up empty it is
    removed entirely.

    Returns:
        Tuple of (cleaned DDL, list of removed property keys).
    """
    reserved = tuple(reserved)
    tokens = _tokenize(ddl)
    out: List[_Token] = []
    removed: List[str] = []
    i = 0
    while i < len(tokens):
        t = tokens[i]
        if not t.is_word("TBLPROPERTIES"):
            out.append(t)
            i += 1
            continue
        open_idx = _next_sig(tokens, i + 1)
        if open_idx is None or tokens[open_idx].text != "(":
            out.append(t)
            i += 1
            continue
        close_idx = _matching_paren(tokens, open_idx)
        if close_idx is None:
            out.extend(tokens[i:])
            break

        # Split clause body on top-level commas.
        entries: List[List[_Token]] = [[]]
        depth = 0
        for tok in tokens[open_idx + 1 : close_idx]:
            if tok.kind == "punct" and tok.text in "([":
                depth += 1
            elif tok.kind == "punct" and tok.text in ")]":
                depth -= 1
            if tok.kind == "punct" and tok.text == "," and depth == 0:
                entries.append([])
            else:
                entries[-1].append(tok)

        kept = []
        for entry in entries:
            key = _property_key(entry)
            if key is not None and _is_unreplayable(key, reserved):
                removed.append(key)
            elif key is not None:
                kept.append(entry)

        if kept:
            out.extend(tokens[i : open_idx + 1])
            body = ",".join(_join(e) for e in kept)
            # Preserve the original whitespace before ")" even if the last
            # entry was dropped.
            last = _join(entries[-1])
            trailing = last[len(last.rstrip()) :]
            out.append(_Token("raw", body.rstrip() + trailing))
            out.append(tokens[close_idx])
        else:
            # Drop the clause together with the whitespace before it.
            while out and out[-1].kind == "ws":
                out.pop()
            if close_idx + 1 < len(tokens) and tokens[close_idx + 1].kind != "ws":
                out.append(_Token("ws", " "))
        i = close_idx + 1
    return _join(out), removed


def _skip_qualified_name(tokens: List[_Token], i: int) -> Optional[int]:
    """
    Consume a (possibly dotted, possibly backticked) name starting at the
    significant token at/after ``i``. Returns index after the name.
    """
    k = _next_sig(tokens, i)
    if k is None or tokens[k].kind not in ("word", "ident"):
        return None
    k += 1
    while True:
        dot = _next_sig(tokens, k)
        if dot is None or tokens[dot].text != ".":
            return k
        part = _next_sig(tokens, dot + 1)
        if part is None or tokens[part].kind not in ("word", "ident"):
            return k
        k = part + 1


def _skip_paren_group_after(
    tokens: List[_Token], i: int, *keywords: str
) -> Optional[int]:
    """
    If the significant tokens at/after ``i`` are ``keywords`` followed by a
    parenthesised group, return the index after the closing paren.
    """
    k = i
    for kw in keywords:
        k = _next_sig(tokens, k)
        if k is None or not tokens[k].is_word(kw):
            return None
        k += 1
    k = _next_sig(tokens, k)
    if k is None or tokens[k].text != "(":
        return None
    close = _matching_paren(tokens, k)
    return None if close is None else close + 1


def strip_inline_masks_and_row_filters(ddl: str) -> Tuple[str, List[str]]:
    """
    Remove inline column masks and table row filters from a CREATE TABLE DDL.

    * ``<col> <type> ... MASK <fn> [USING COLUMNS (...)]`` inside the column
      list loses its ``MASK`` clause.
    * ``WITH ROW FILTER <fn> ON (...)`` at statement level is removed.

    Returns:
        Tuple of (cleaned DDL, list of removed clause texts).
    """
    tokens = _tokenize(ddl)
    removals: List[Tuple[int, int]] = []

    # Locate the column list: the first top-level parenthesised group.
    col_open = next(
        (k for k, t in enumerate(tokens) if t.kind == "punct" and t.text == "("),
        None,
    )
    col_close = _matching_paren(tokens, col_open) if col_open is not None else None

    if col_open is not None and col_close is not None:
        depth = 0
        entry_start = True
        k = col_open + 1
        while k < col_close:
            t = tokens[k]
            if not t.significant:
                k += 1
                continue
            if t.kind == "punct" and t.text == "(":
                depth += 1
            elif t.kind == "punct" and t.text == ")":
                depth -= 1
            elif t.kind == "punct" and t.text == "," and depth == 0:
                entry_start = True
                k += 1
                continue
            elif depth == 0 and not entry_start and t.is_word("MASK"):
                nxt = _next_sig(tokens, k + 1)
                if nxt is not None and tokens[nxt].text != "(":
                    end = _skip_qualified_name(tokens, k + 1)
                    if end is not None:
                        using_end = _skip_paren_group_after(
                            tokens, end, "USING", "COLUMNS"
                        )
                        end = using_end if using_end is not None else end
                        removals.append((_trim_leading_ws(tokens, k), end))
                        k = end
                        entry_start = False
                        continue
            entry_start = False
            k += 1

    # Row filters live at statement level (depth 0).
    depth = 0
    k = 0
    while k < len(tokens):
        t = tokens[k]
        if t.kind == "punct" and t.text == "(":
            depth += 1
        elif t.kind == "punct" and t.text == ")":
            depth -= 1
        elif depth == 0 and t.is_word("WITH"):
            row = _next_sig(tokens, k + 1)
            flt = _next_sig(tokens, row + 1) if row is not None else None
            if (
                row is not None
                and flt is not None
                and tokens[row].is_word("ROW")
                and tokens[flt].is_word("FILTER")
            ):
                name_end = _skip_qualified_name(tokens, flt + 1)
                end = (
                    _skip_paren_group_after(tokens, name_end, "ON")
                    if name_end is not None
                    else None
                )
                if end is not None:
                    removals.append((_trim_leading_ws(tokens, k), end))
                    k = end
                    continue
        k += 1

    if not removals:
        return ddl, []

    removals.sort()
    out: List[str] = []
    removed: List[str] = []
    pos = 0
    for start, end in removals:
        out.append(_join(tokens[pos:start]))
        removed.append(_join(tokens[start:end]).strip())
        pos = end
    out.append(_join(tokens[pos:]))
    return "".join(out), removed


# ---------------------------------------------------------------------------
# Catalog reference rewriting
# ---------------------------------------------------------------------------

_SIMPLE_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _is_name(t: _Token) -> bool:
    return t.kind in ("word", "ident")


def _is_dot(t: _Token) -> bool:
    return t.kind == "punct" and t.text == "."


def _ident_value(t: _Token) -> str:
    if t.kind == "ident":
        text = t.text
        inner = text[1:-1] if text.endswith("`") and len(text) >= 2 else text[1:]
        return inner.replace("``", "`")
    return t.text


def _quote_ident(name: str, force: bool) -> str:
    if not force and _SIMPLE_IDENT_RE.match(name):
        return name
    return quote_identifier(name)


def _prev_sig(tokens: List[_Token], i: int) -> Optional[int]:
    """Index of the last significant token at or before ``i``."""
    while i >= 0:
        if tokens[i].significant:
            return i
        i -= 1
    return None


def _qualified_parts_after(tokens: List[_Token], i: int) -> int:
    """Count ``.part`` segments following the name at ``tokens[i]``."""
    count = 0
    j: Optional[int] = i + 1
    while True:
        j = _next_sig(tokens, j)
        if j is None or not _is_dot(tokens[j]):
            return count
        j = _next_sig(tokens, j + 1)
        if j is None or not _is_name(tokens[j]):
            return count
        count += 1
        j += 1


def rewrite_catalog_references(sql: str, catalog_map: Optional[Dict[str, str]]) -> str:
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
    for idx, tok in enumerate(tokens):
        if _is_name(tok):
            target = mapping.get(_ident_value(tok).lower())
            if target is not None:
                prev = _prev_sig(tokens, idx - 1)
                if (prev is None or not _is_dot(tokens[prev])) and (
                    _qualified_parts_after(tokens, idx) >= 2
                ):
                    out.append(_quote_ident(target, force=tok.kind == "ident"))
                    continue
        out.append(tok.text)
    return "".join(out)
