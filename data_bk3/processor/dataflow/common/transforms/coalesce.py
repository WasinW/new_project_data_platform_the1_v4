"""
Coalesce helpers for the dataflow_common package.

When new and old records have been joined by key the
:func:`coalesce_by_mapping` function decides which value to keep for
each destination column based on a flag in the mapping rows.  If the
preferred side has ``None`` for a given column then the value from
the other side is used.  The primary key is always preserved even if
it is missing from one side.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Iterable, List, Tuple

LOGGER = logging.getLogger(__name__)

def coalesce_by_mapping(
    kv: Tuple[Any, Dict[str, List[Dict[str, Any]]]],
    *,
    columns: Iterable[Dict[str, Any]],
    flag_field: str,
    pk_field: str,
    dest_field: str = "dest_column_name",
) -> Dict[str, Any]:
    """Coalesce values from new/old rows based on a mapping.

    Parameters
    ----------
    kv: tuple
        A tuple ``(key, groups)`` produced by
        ``beam.CoGroupByKey`` where ``groups`` is a dictionary
        mapping alias names to lists of records.  Typically aliases
        ``'new'`` and ``'old'`` are used.
    columns: iterable of dict
        The mapping rows used to determine which flag field applies to
        each destination column.  Each row must contain the
        destination column name under ``dest_field`` and the
        coalescing flag under ``flag_field``.
    flag_field: str
        Name of the field in ``columns`` that determines whether to
        prefer the new row (flag is truthy) or the old row (flag is
        falsey).
    pk_field: str
        Name of the primary key field that must always be present in
        the output.  The value is taken from the new row if present
        otherwise from the old row.
    dest_field: str, optional
        Name of the field in ``columns`` containing the destination
        column name.  Defaults to ``'RECONCILE_COLUMN_NAME'``.

    Returns
    -------
    dict
        A dictionary containing the coalesced values for each
        destination column and the primary key.
    """
    try:
        key, groups = kv
        new_rows = groups.get("new") or []
        old_rows = groups.get("old") or []
        # new_row: Dict[str, Any] = new_rows[0] if new_rows else {}
        # ✅ สำคัญ: ถ้าไม่มี new row ให้ SKIP - ไม่ return อะไรเลย!
        if not new_rows:
            LOGGER.debug(f"No new rows for key {key}, skipping")
            return None  # หรือ return {} แล้วไป filter ทีหลัง
        
        new_row: Dict[str, Any] = new_rows[0]
        old_row: Dict[str, Any] = old_rows[0] if old_rows else {}

        out: Dict[str, Any] = {}
        column_count = 0

        # Merge columns ตาม mapping
        for row in columns:
            try:
                if not row:
                    continue
                tgt = row.get(dest_field)
                if not tgt:
                    continue
                
                prefer_new = bool(row.get(flag_field))
                column_count += 1
                
                # Get value from new or old based on flag
                if prefer_new and tgt in new_row:
                    # Prefer new if flag is true and new has the column
                    out[tgt] = new_row[tgt]
                elif prefer_new and tgt in old_row:
                    # Fallback to old if new doesn't have it
                    out[tgt] = old_row[tgt]
                elif not prefer_new and tgt in old_row:
                    # Prefer old if flag is false
                    out[tgt] = old_row[tgt]
                elif not prefer_new and tgt in new_row:
                    # Fallback to new if old doesn't have it
                    out[tgt] = new_row[tgt]
                # If neither has the column, skip it
                
            except Exception as e:
                LOGGER.warning(f"Error processing column mapping: {e}")
                continue

        # Add PK only if we have new data
        # Ensure PK is always present
        try:
            if pk_field and new_row.get(pk_field):
                out[pk_field] = new_row.get(pk_field)
            elif pk_field and old_row.get(pk_field):
                out[pk_field] = old_row.get(pk_field)
        except Exception as e:
            LOGGER.error(f"Error setting primary key '{pk_field}': {e}")
        
        LOGGER.info(f"Coalesced {column_count} columns for key {key}")
        return out
        
    except Exception as e:
        LOGGER.error(f"Failed to coalesce records: {e}")
        LOGGER.error(f"Key: {kv[0] if kv else 'Unknown'}")
        raise

__all__ = ["coalesce_by_mapping"]