"""
dataflow_common.transforms
==========================

Pure functions used by the step implementations.  These helpers are
intentionally stateless so that they can be easily tested in
isolation and reused across steps.
"""

from dataflow_common.transforms.mapping import (
    normalize_path,
    extract_by_path,
    create_mapping_dict,
    map_record,
)
from dataflow_common.transforms.coalesce import coalesce_by_mapping
from dataflow_common.transforms.schema import (
    load_schema_from_spec,
    build_pyarrow_schema,
    normalize_row_to_schema,
)

__all__ = [
    "normalize_path",
    "extract_by_path",
    "create_mapping_dict",
    "map_record",
    "coalesce_by_mapping",
    "load_schema_from_spec",
    "build_pyarrow_schema",
    "normalize_row_to_schema",
]