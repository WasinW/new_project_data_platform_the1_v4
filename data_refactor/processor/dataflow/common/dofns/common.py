"""
Common DoFn classes shared between batch and streaming pipelines.

This module contains DoFn classes that can be used by both batch and streaming
pipelines. Move DoFns here when they are needed by multiple pipeline types.

Currently empty - add common DoFns as needed.
"""
from __future__ import annotations

import logging

from apache_beam import DoFn


LOGGER = logging.getLogger(__name__)


# Placeholder for common DoFns
# Example:
# class ParseJsonDoFn(DoFn):
#     """Parse JSON fields in records."""
#     pass


__all__ = [
    # Add common DoFn names here as they are created
]
