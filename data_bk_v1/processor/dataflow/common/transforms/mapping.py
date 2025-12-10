"""
Mapping utilities for the dataflow_common package.

This module provides helper functions to work with column mapping
definitions.  A mapping is a list of dictionaries, each of which
describes how to extract a value from an input record and place it
into an output column.  The mapping row field names can be
customised per pipeline via the configuration; defaults follow the
naming convention used in the original `test_bq_read_simple4.py`.

The central functions are :func:`create_mapping_dict` and
:func:`map_record`.  Together they convert a list of mapping rows
into a look‑up table and then transform a raw input record into an
output record according to a selected mode (``reconcile`` or
``original``).  Paths in the mapping are expressed as dotted
expressions (e.g. ``profiles.memberId``); use
:func:`normalize_path` and :func:`extract_by_path` to traverse
nested dictionaries.
"""

from __future__ import annotations

import logging
import json
from typing import Any, Dict, Iterable, List, Tuple
import re

LOGGER = logging.getLogger(__name__)

def normalize_path(path: str) -> List[str]:
    """Normalise a JSON path string into a list of keys.

    Supports dot notation and bracket notation interchangeably.  For
    example, ``profiles.memberId``, ``profiles['memberId']`` and
    ``['profiles']['memberId']`` all return ``['profiles', 'memberId']``.
    Returns an empty list if ``path`` is falsy.
    """
    try:
        if not path:
            return []
        
        # First, remove leading/trailing brackets if they exist
        cleaned = path.strip()
        
        # Replace ['key'] with .key
        cleaned = re.sub(r"\['([^']+)'\]", r".\1", cleaned)
        # Replace ["key"] with .key  
        cleaned = re.sub(r'\["([^"]+)"\]', r".\1", cleaned)
        # Replace [key] with .key (for unquoted keys)
        cleaned = re.sub(r"\[([^\]]+)\]", r".\1", cleaned)
        
        # Remove leading dot if exists
        if cleaned.startswith('.'):
            cleaned = cleaned[1:]
        
        # Split by dots and filter out empty strings
        result = [p.strip() for p in cleaned.split('.') if p.strip()]

        LOGGER.debug(f"Normalized path '{path}' to {result}")
        return result

    except Exception as e:
        LOGGER.error(f"Error normalizing path '{path}': {e}")
        return []

def extract_by_path(record: Dict[str, Any], path: List[str]) -> Any:
    """Extract a nested value from a record given a path list.

    Returns ``None`` if any intermediate key is missing.  If ``path``
    is empty the original record is returned.
    """
    try:
        import json
        cur: Any = record
        for i, part in enumerate(path):
            if cur is None:
                return None
                
            # If cur is string and we have more path, try parsing as JSON
            if isinstance(cur, str) and i < len(path):
                try:
                    cur = json.loads(cur)
                    LOGGER.debug(f"Parsed JSON string at path element '{part}'")
                except (json.JSONDecodeError, TypeError):
                    LOGGER.debug(f"Could not parse as JSON at path element '{part}'")
                    return None
                    
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                return None
        
        return cur
        
    except Exception as e:
        LOGGER.warning(f"Error extracting path {path} from record: {e}")
        return None

def create_mapping_dict(
    rows: Iterable[Dict[str, Any]],
    *,
    src_field: str = "src_column_name",
    dest_field: str = "dest_column_name",
    retrieved_flag_field: str = "retrieved_flag",
    confirmed_flag_field: str = "confirmed_flag",
) -> Dict[str, Dict[str, Any]]:
    """Convert a sequence of mapping rows into a lookup dictionary.

    Each mapping row is expected to have at least the destination
    column name (``dest_field``) and optionally the source path
    (``src_field``) and flags indicating whether the mapping should
    apply in reconcile or original mode (``retrieved_flag_field`` and
    ``confirmed_flag_field`` respectively).  The returned mapping
    dictionary has the destination column as the key and a dictionary
    with keys ``src_path``, ``reconcile`` and ``original`` as the
    value.
    """
    try:
        mapping: Dict[str, Dict[str, Any]] = {}
        row_count = 0
        for row in rows:
            row_count += 1
            try:
                if not row:
                    continue
                tgt = row.get(dest_field)
                if not tgt:
                    LOGGER.debug(f"Row {row_count} missing destination field '{dest_field}'")
                    continue
                src = row.get(src_field)
                mapping[tgt] = {
                    "src_path": normalize_path(src) if src else [],
                    "reconcile": bool(row.get(retrieved_flag_field)),
                    "original": bool(row.get(confirmed_flag_field)),
                }
                
            except Exception as e:
                LOGGER.warning(f"Error processing mapping row {row_count}: {e}")
                continue
        
        LOGGER.info(f"Created mapping dictionary with {len(mapping)} entries from {row_count} rows")
        return mapping
        
    except Exception as e:
        LOGGER.error(f"Failed to create mapping dictionary: {e}")
        raise

def map_record(
    record: Dict[str, Any],
    mapping_dict: Dict[str, Dict[str, Any]],
    mode: str = "reconcile",
) -> Dict[str, Any]:
    """Apply a mapping dictionary to a single input record.

    Parameters
    ----------
    record: dict
        The raw input record, typically a row read from BigQuery.
    mapping_dict: dict
        A mapping dict created by :func:`create_mapping_dict`.
    mode: str, optional
        Either ``'reconcile'`` or ``'original'``.  Only mappings
        whose flag for the given mode is truthy will be applied.  Any
        destination columns whose mapping does not apply in the
        selected mode will be omitted from the output.

    Returns
    -------
    dict
        A new dictionary containing only the mapped destination
        columns for which the mode flag was set.  If ``src_path`` in
        the mapping dict is empty the destination column is skipped.
    """
    try:
        out: Dict[str, Any] = {}
        mapped_count = 0
        for dest_col, cfg in mapping_dict.items():
            try:
                if not cfg.get(mode, False):
                    continue
                src_path = cfg.get("src_path") or []
                if not src_path:
                    continue
                val = extract_by_path(record, src_path)
                out[dest_col] = val
                mapped_count += 1

            except Exception as e:
                LOGGER.warning(f"Error mapping column '{dest_col}': {e}")
                continue
        
        LOGGER.debug(f"Mapped {mapped_count} fields in mode '{mode}'")
        return out
        
    except Exception as e:
        LOGGER.error(f"Failed to map record: {e}")
        raise

__all__ = [
    "normalize_path",
    "extract_by_path",
    "create_mapping_dict",
    "map_record",
]