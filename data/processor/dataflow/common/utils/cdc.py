"""
CDC (Change Data Capture) utility functions for BigQuery writes.

This module contains helper functions for formatting records for CDC writes
to BigQuery using the Storage Write API.
"""

from __future__ import annotations

import logging
from typing import Any, Dict

LOGGER = logging.getLogger(__name__)


def format_for_cdc(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format a record for CDC write by adding _CHANGE_TYPE and _CHANGE_SEQUENCE_NUMBER fields.

    This is a lightweight helper that adds CDC metadata fields to records before
    they are processed by MapToCdcTableRow DoFn.

    Args:
        row: Input record dictionary

    Returns:
        Record with _CHANGE_TYPE and _CHANGE_SEQUENCE_NUMBER fields added

    Example:
        >>> record = {'memberId': '123', 'name': 'John', 'is_delete': False, 'timestamp': 1234567890}
        >>> cdc_record = format_for_cdc(record)
        >>> cdc_record['_CHANGE_TYPE']
        'UPSERT'
    """
    # Add _CHANGE_TYPE ('UPSERT' or 'DELETE')
    row['_CHANGE_TYPE'] = 'DELETE' if row.get('is_delete') else 'UPSERT'

    # Add _CHANGE_SEQUENCE_NUMBER (timestamp helps BigQuery order changes correctly)
    # Using a high-resolution timestamp or sequence ID is crucial for correctness
    row['_CHANGE_SEQUENCE_NUMBER'] = str(row.get('timestamp', ''))

    LOGGER.debug(f"[format_for_cdc] Formatted record: {row.get('_CHANGE_TYPE')}")

    return row


__all__ = [
    'format_for_cdc',
]
