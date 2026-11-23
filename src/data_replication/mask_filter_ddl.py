"""
Classic column masks and table row filters (issue #39) for replay on target.

Source policies are read from table metadata (SDK ``tables.get``):
``ColumnInfo.mask`` (``function_name`` + ``using_column_names``) and
``TableInfo.row_filter`` (``function_name`` + ``input_column_names``). This
module holds the pure parts: metadata -> spec conversion, the plan that
compares source and target specs, and the ``ALTER TABLE`` builders.

Function names are fully qualified (``catalog.schema.function``); the catalog
part is mapped through the source -> target catalog map, the schema and name
are kept as they are.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from .sql_utils import quote_identifier


@dataclass(frozen=True)
class ColumnMaskSpec:
    column: str
    function: str
    using_columns: Tuple[str, ...] = ()


@dataclass(frozen=True)
class RowFilterSpec:
    function: str
    input_columns: Tuple[str, ...] = ()


@dataclass
class MaskPlan:
    """Column-level changes needed on target to match source."""

    to_set: List[ColumnMaskSpec] = field(default_factory=list)
    to_unset: List[str] = field(default_factory=list)
    unchanged: List[str] = field(default_factory=list)
    # Target-only masks kept because overwrite is off.
    kept: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class RowFilterPlan:
    """Table-level row filter change: set / drop / unchanged / kept / none."""

    action: str
    spec: Optional[RowFilterSpec] = None


def map_function_name(function: str, catalog_map: Optional[Mapping[str, str]]) -> str:
    """Map the catalog part of ``catalog.schema.function`` through catalog_map."""
    parts = function.split(".")
    if len(parts) != 3 or not catalog_map:
        return function
    lookup = {str(k).lower(): str(v) for k, v in catalog_map.items() if k and v}
    catalog = lookup.get(parts[0].lower(), parts[0])
    return ".".join([catalog, parts[1], parts[2]])


def column_mask_specs(
    columns: Optional[Iterable], catalog_map: Optional[Mapping[str, str]] = None
) -> Dict[str, ColumnMaskSpec]:
    """Masked columns of a table (keyed by column name, in column order)."""
    specs: Dict[str, ColumnMaskSpec] = {}
    for column in columns or []:
        mask = getattr(column, "mask", None)
        if mask is None or not mask.function_name:
            continue
        specs[column.name] = ColumnMaskSpec(
            column=column.name,
            function=map_function_name(mask.function_name, catalog_map),
            using_columns=tuple(mask.using_column_names or ()),
        )
    return specs


def row_filter_spec(
    row_filter, catalog_map: Optional[Mapping[str, str]] = None
) -> Optional[RowFilterSpec]:
    """The table's row filter, or None when it has none."""
    if row_filter is None or not row_filter.function_name:
        return None
    return RowFilterSpec(
        function=map_function_name(row_filter.function_name, catalog_map),
        input_columns=tuple(row_filter.input_column_names or ()),
    )


def _same_mask(a: ColumnMaskSpec, b: ColumnMaskSpec) -> bool:
    return a.function.lower() == b.function.lower() and [
        c.lower() for c in a.using_columns
    ] == [c.lower() for c in b.using_columns]


def _same_filter(a: RowFilterSpec, b: RowFilterSpec) -> bool:
    return a.function.lower() == b.function.lower() and [
        c.lower() for c in a.input_columns
    ] == [c.lower() for c in b.input_columns]


def plan_column_masks(
    source: Mapping[str, ColumnMaskSpec],
    target: Mapping[str, ColumnMaskSpec],
    overwrite: bool,
) -> MaskPlan:
    """Plan column mask changes. Identical masks are left alone (idempotent).

    Target-only masks are dropped only when ``overwrite`` is true; otherwise
    they are kept (a target-side mask is never weaker than a missing one).
    """
    src = {name.lower(): spec for name, spec in source.items()}
    tgt = {name.lower(): spec for name, spec in target.items()}
    plan = MaskPlan()
    for key, spec in src.items():
        current = tgt.get(key)
        if current is not None and _same_mask(current, spec):
            plan.unchanged.append(spec.column)
        else:
            plan.to_set.append(spec)
    for key, spec in tgt.items():
        if key not in src:
            (plan.to_unset if overwrite else plan.kept).append(spec.column)
    return plan


def plan_row_filter(
    source: Optional[RowFilterSpec],
    target: Optional[RowFilterSpec],
    overwrite: bool,
) -> RowFilterPlan:
    """Plan the table row filter change (set, drop, unchanged, kept or none)."""
    if source is not None:
        if target is not None and _same_filter(source, target):
            return RowFilterPlan("unchanged", source)
        return RowFilterPlan("set", source)
    if target is not None:
        return RowFilterPlan("drop" if overwrite else "kept", target)
    return RowFilterPlan("none")


def function_sql(function: str) -> str:
    """Backtick-quoted ``catalog.schema.function`` for SQL and existence checks."""
    parts = function.split(".")
    if len(parts) != 3 or not all(parts):
        raise ValueError(
            f"Expected a fully qualified function name (catalog.schema.function), "
            f"got: {function}"
        )
    return ".".join(quote_identifier(p) for p in parts)


def _columns_sql(columns: Sequence[str]) -> str:
    return ", ".join(quote_identifier(c) for c in columns)


def set_column_mask_sql(table: str, spec: ColumnMaskSpec) -> str:
    """Build ``ALTER ... SET MASK``. ``table`` must already be quoted."""
    stmt = (
        f"ALTER TABLE {table} ALTER COLUMN {quote_identifier(spec.column)} "
        f"SET MASK {function_sql(spec.function)}"
    )
    if spec.using_columns:
        stmt += f" USING COLUMNS ({_columns_sql(spec.using_columns)})"
    return stmt


def drop_column_mask_sql(table: str, column: str) -> str:
    return f"ALTER TABLE {table} ALTER COLUMN {quote_identifier(column)} DROP MASK"


def set_row_filter_sql(table: str, spec: RowFilterSpec) -> str:
    return (
        f"ALTER TABLE {table} SET ROW FILTER {function_sql(spec.function)} "
        f"ON ({_columns_sql(spec.input_columns)})"
    )


def drop_row_filter_sql(table: str) -> str:
    return f"ALTER TABLE {table} DROP ROW FILTER"
