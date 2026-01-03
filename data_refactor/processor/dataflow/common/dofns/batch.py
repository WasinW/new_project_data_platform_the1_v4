"""
Batch processing DoFn classes and utility functions.

This module provides DoFn classes and helper functions for batch pipelines.
Functions are extracted from transforms/ modules (mapping.py, coalesce.py, schema.py).

Usage:
    from dataflow_common.dofns.batch import (
        # Mapping utilities
        create_mapping_dict,
        map_record,
        # Schema utilities
        build_pyarrow_schema,
        query_mapping_schema,
        # DoFns
        ParseJsonDoFn,
        MapRecordDoFn,
    )
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, date
from typing import Any, Dict, Iterable, List, Optional, Tuple

import apache_beam as beam
from apache_beam import DoFn
import pyarrow as pa


LOGGER = logging.getLogger(__name__)


# =============================================================================
# MAPPING UTILITIES (from transforms/mapping.py)
# =============================================================================

def normalize_path(path: str) -> List[str]:
    """Normalise a JSON path string into a list of keys.

    Supports dot notation and bracket notation interchangeably.
    Example: 'profiles.memberId' -> ['profiles', 'memberId']
    """
    try:
        if not path:
            return []

        cleaned = path.strip()
        cleaned = re.sub(r"\['([^']+)'\]", r".\1", cleaned)
        cleaned = re.sub(r'\["([^"]+)"\]', r".\1", cleaned)
        cleaned = re.sub(r"\[([^\]]+)\]", r".\1", cleaned)

        if cleaned.startswith('.'):
            cleaned = cleaned[1:]

        return [p.strip() for p in cleaned.split('.') if p.strip()]

    except Exception as e:
        LOGGER.error(f"Error normalizing path '{path}': {e}")
        return []


def extract_by_path(record: Dict[str, Any], path: List[str]) -> Any:
    """Extract a nested value from a record given a path list."""
    try:
        cur: Any = record
        for i, part in enumerate(path):
            if cur is None:
                return None

            if isinstance(cur, str) and i < len(path):
                try:
                    cur = json.loads(cur)
                except (json.JSONDecodeError, TypeError):
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
    """Convert a sequence of mapping rows into a lookup dictionary."""
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

        LOGGER.info(f"Created mapping dictionary with {len(mapping)} entries")
        return mapping

    except Exception as e:
        LOGGER.error(f"Failed to create mapping dictionary: {e}")
        raise


def map_record(
    record: Dict[str, Any],
    mapping_dict: Dict[str, Dict[str, Any]],
    mode: str = "reconcile",
) -> Dict[str, Any]:
    """Apply a mapping dictionary to a single input record."""
    try:
        out: Dict[str, Any] = {}
        for dest_col, cfg in mapping_dict.items():
            try:
                if not cfg.get(mode, False):
                    continue
                src_path = cfg.get("src_path") or []
                if not src_path:
                    continue
                val = extract_by_path(record, src_path)
                out[dest_col] = val

            except Exception as e:
                LOGGER.warning(f"Error mapping column '{dest_col}': {e}")
                continue

        return out

    except Exception as e:
        LOGGER.error(f"Failed to map record: {e}")
        raise


# =============================================================================
# COALESCE UTILITIES (from transforms/coalesce.py)
# =============================================================================

def coalesce_by_mapping(
    kv: Tuple[Any, Dict[str, List[Dict[str, Any]]]],
    *,
    columns: Iterable[Dict[str, Any]],
    flag_field: str,
    pk_field: str,
    dest_field: str = "dest_column_name",
) -> Optional[Dict[str, Any]]:
    """Coalesce values from new/old rows based on a mapping."""
    try:
        key, groups = kv
        new_rows = groups.get("new") or []
        old_rows = groups.get("old") or []

        if not new_rows:
            LOGGER.debug(f"No new rows for key {key}, skipping")
            return None

        new_row: Dict[str, Any] = new_rows[0]
        old_row: Dict[str, Any] = old_rows[0] if old_rows else {}

        out: Dict[str, Any] = {}

        for row in columns:
            try:
                if not row:
                    continue
                tgt = row.get(dest_field)
                if not tgt:
                    continue

                prefer_new = bool(row.get(flag_field))

                if prefer_new and tgt in new_row:
                    out[tgt] = new_row[tgt]
                elif prefer_new and tgt in old_row:
                    out[tgt] = old_row[tgt]
                elif not prefer_new and tgt in old_row:
                    out[tgt] = old_row[tgt]
                elif not prefer_new and tgt in new_row:
                    out[tgt] = new_row[tgt]

            except Exception as e:
                LOGGER.warning(f"Error processing column mapping: {e}")
                continue

        # Ensure PK is always present
        if pk_field and new_row.get(pk_field):
            out[pk_field] = new_row.get(pk_field)
        elif pk_field and old_row.get(pk_field):
            out[pk_field] = old_row.get(pk_field)

        return out

    except Exception as e:
        LOGGER.error(f"Failed to coalesce records: {e}")
        raise


# =============================================================================
# SCHEMA UTILITIES (from transforms/schema.py - refactored)
# =============================================================================

def query_mapping_schema(project: str, dataset: str, table_name: str) -> List[str]:
    """Query mapping_reconcile to get column names for building Parquet schema.

    Args:
        project: BigQuery project ID
        dataset: BigQuery dataset name
        table_name: Table name to filter (e.g., 'ms_member')

    Returns:
        List of column names (reconcile_column_name values)
    """
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project)
        query = f"""
            SELECT reconcile_column_name
            FROM `{project}.{dataset}.mapping_reconcile`
            WHERE table_name = '{table_name}'
            ORDER BY reconcile_column_name
        """

        LOGGER.info(f"[query_mapping_schema] Querying mapping for table: {table_name}")

        results = client.query(query).result()
        columns = [row.reconcile_column_name for row in results if row.reconcile_column_name]

        LOGGER.info(f"[query_mapping_schema] Found {len(columns)} columns")
        return columns

    except Exception as e:
        LOGGER.error(f"[query_mapping_schema] Failed to query mapping: {e}")
        raise


def build_pyarrow_schema(schema_def: List[Dict[str, Any]]) -> pa.Schema:
    """Construct a PyArrow schema from a JSON-like definition.

    Supported types: STRING, INT64, FLOAT64, NUMERIC, BOOLEAN, DATE, TIMESTAMP
    """
    try:
        fields = []
        for col in schema_def:
            name = col.get("name")
            if not name:
                continue

            typ = (col.get("type") or "STRING").upper()
            if typ in ("INT64", "INTEGER", "INT32"):
                pa_type = pa.int64()
            elif typ in ("FLOAT64", "FLOAT", "DOUBLE"):
                pa_type = pa.float64()
            elif typ in ("NUMERIC",):
                pa_type = pa.decimal128(38, 9)
            elif typ in ("BOOLEAN", "BOOL"):
                pa_type = pa.bool_()
            elif typ in ("DATE",):
                pa_type = pa.date32()
            elif typ in ("TIMESTAMP",):
                pa_type = pa.timestamp("us")
            else:
                pa_type = pa.string()
            fields.append(pa.field(name, pa_type))

        return pa.schema(fields)

    except Exception as e:
        LOGGER.error(f"Failed to build PyArrow schema: {e}")
        raise


def build_pyarrow_schema_all_strings(columns: List[str]) -> pa.Schema:
    """Build PyArrow schema with all STRING types."""
    return pa.schema([pa.field(col, pa.string()) for col in columns])


def _parse_date(value: Any, formats: List[str]) -> Optional[date]:
    """Parse date from value using provided formats."""
    if value is None:
        return None
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    s = str(value).strip()
    if not s:
        return None
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _parse_timestamp(value: Any, formats: List[str]) -> Optional[datetime]:
    """Parse timestamp from value using provided formats."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    s = str(value).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    try:
        iv = int(s)
        if len(s) > 10:
            return datetime.fromtimestamp(iv / 1000.0)
        return datetime.fromtimestamp(iv)
    except (ValueError, OSError):
        pass
    return None


def normalize_row_to_schema(
    row: Dict[str, Any],
    schema: pa.Schema,
    date_formats: Optional[List[str]] = None,
    timestamp_formats: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Normalise a single row to match the provided PyArrow schema."""
    date_formats = date_formats or ["%Y-%m-%d", "%d/%m/%Y"]
    timestamp_formats = timestamp_formats or ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"]

    out: Dict[str, Any] = {}
    for field in schema:
        name = field.name
        val = row.get(name)

        if pa.types.is_string(field.type):
            out[name] = "" if val is None else str(val)
        elif pa.types.is_date(field.type):
            out[name] = None if val in (None, "") else _parse_date(val, date_formats)
        elif pa.types.is_timestamp(field.type):
            out[name] = None if val in (None, "") else _parse_timestamp(val, timestamp_formats)
        elif pa.types.is_boolean(field.type):
            if val in (None, ""):
                out[name] = None
            elif isinstance(val, bool):
                out[name] = val
            else:
                out[name] = str(val).lower() in ("true", "1", "yes")
        elif pa.types.is_integer(field.type):
            try:
                out[name] = None if val in (None, "") else int(val)
            except (ValueError, TypeError):
                out[name] = None
        elif pa.types.is_floating(field.type):
            try:
                out[name] = None if val in (None, "") else float(val)
            except (ValueError, TypeError):
                out[name] = None
        else:
            out[name] = val

    return out


# =============================================================================
# BATCH DoFn CLASSES
# =============================================================================

class ParseJsonDoFn(DoFn):
    """Parse JSON string fields into Python dictionaries."""

    def __init__(self, json_fields: Optional[List[str]] = None):
        self.json_fields = json_fields or ["profiles"]

    def process(self, element):
        try:
            rec = dict(element)
            for field in self.json_fields:
                if field in rec and isinstance(rec[field], str):
                    try:
                        rec[field] = json.loads(rec[field])
                    except json.JSONDecodeError as e:
                        LOGGER.warning(f"[ParseJsonDoFn] Failed to parse '{field}': {e}")
            yield rec
        except Exception as e:
            LOGGER.error(f"[ParseJsonDoFn] Error: {e}")
            raise


class MapRecordDoFn(DoFn):
    """Apply a mapping dictionary to each record."""

    def __init__(self, mode: str = "reconcile"):
        self.mode = mode

    def process(self, element, mapping_dict):
        try:
            result = map_record(element, mapping_dict, self.mode)
            yield result
        except Exception as e:
            LOGGER.error(f"[MapRecordDoFn] Error: {e}")
            raise


class EnsureColumnsDoFn(DoFn):
    """Ensure records have all specified columns, converting to string."""

    def __init__(self, columns: List[str]):
        self.columns = columns

    def process(self, element):
        result = {}
        for col in self.columns:
            val = element.get(col)
            result[col] = None if val is None else str(val)
        yield result


class NormalizeToSchemaDoFn(DoFn):
    """Normalise rows to schema using configurable formats."""

    def __init__(
        self,
        schema: pa.Schema,
        date_formats: Optional[List[str]] = None,
        timestamp_formats: Optional[List[str]] = None
    ):
        self.schema = schema
        self.date_formats = date_formats
        self.timestamp_formats = timestamp_formats

    def process(self, element):
        try:
            result = normalize_row_to_schema(
                element,
                self.schema,
                self.date_formats,
                self.timestamp_formats
            )
            yield result
        except Exception as e:
            LOGGER.error(f"[NormalizeToSchemaDoFn] Error: {e}")
            raise


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    # Mapping utilities
    'normalize_path',
    'extract_by_path',
    'create_mapping_dict',
    'map_record',
    # Coalesce utilities
    'coalesce_by_mapping',
    # Schema utilities
    'query_mapping_schema',
    'build_pyarrow_schema',
    'build_pyarrow_schema_all_strings',
    'normalize_row_to_schema',
    # DoFns
    'ParseJsonDoFn',
    'MapRecordDoFn',
    'EnsureColumnsDoFn',
    'NormalizeToSchemaDoFn',
]
