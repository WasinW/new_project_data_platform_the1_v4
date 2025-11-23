"""
dataflow_common
===============

This package defines a set of reusable abstractions and helpers for
building Apache Beam pipelines in a configuration‑driven manner.  It
follows a modular design consisting of four main pieces:

* **config** – dataclass models and functions for loading and
  validating YAML configuration files.
* **orchestrator** – constructs a Beam pipeline based on a plan
  defined in the configuration.  Each plan step references a class
  in the registry.
* **registry** – a central registry mapping the string names used
  in your configuration plan to their corresponding Python classes.
* **steps** – generic pipeline steps that implement common
  operations such as reading from BigQuery, mapping fields, joining
  collections by key, coalescing values, normalising to a schema
  and writing Parquet files.  New steps can be added by creating
  subclasses of :class:`steps.BaseStep` and registering them in
  `registry.py`.

Nothing in this package is tied to a specific dataset or table;
schema definitions, field mappings, date/time parsing formats and
other business rules must all be provided at run‑time via the
configuration plan.

The package version is stored in :data:`__version__`.  See
``README.md`` for an overview of the design.
"""

__all__ = [
    "config",
    "orchestrator",
    "registry",
    "steps",
    "transforms",
]

# Updated version to reflect the new generic implementation.
__version__ = "1.0.0"