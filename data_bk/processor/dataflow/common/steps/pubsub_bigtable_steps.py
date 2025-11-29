"""
Step definitions for consuming Pub/Sub notifications, extracting record
identifiers and performing Bigtable lookups.

These classes encapsulate common realtime ingestion patterns.  They are
deliberately kept generic so that they can be reused across multiple
pipelines.  All configuration values can be supplied either via the
step `spec` or by falling back to values in the global pipeline
configuration (`self.config`).  See the YAML example at the end of this
file for usage details.

The three primary steps are:

1. ``ConsumePubSubSubscriptionStep`` – reads messages from a Pub/Sub
   subscription or topic.  The messages are returned as Beam
   ``PubsubMessage`` objects so that attributes (e.g. ``message_id`` or
   ``publish_time``) are available to downstream transforms.  At
   runtime this uses Beam's native ``ReadFromPubSub`` transform.

2. ``ExtractIdStep`` – extracts an identifier from a JSON-encoded
   Pub/Sub payload.  The identifier can be specified via a dotted
   path (``field_path``) to allow nested structures.  Elements that
   do not contain the specified path or cannot be parsed are filtered
   out.  This mirrors the behaviour of ``ExtractKeysStep`` used in
   batch pipelines but is generalised to any identifier.

3. ``ReadBigTableByIdStep`` – looks up records in Cloud Bigtable
   using the extracted identifiers.  It supports adding optional
   prefixes or suffixes to the row key and can limit the columns
   returned via ``column_families``.  A simple in-memory cache is
   implemented to reduce the number of round trips to Bigtable when
   the same key is requested repeatedly over a short period.  Cache
   settings can be tuned via ``cache_config``.

These steps deliberately avoid batching requests; each identifier is
looked up individually to minimise latency.  If throughput becomes a
bottleneck you can layer Beam's ``BatchElements`` transform in front
of ``ReadBigTableByIdStep``.  See the README for further guidance.

Example YAML configuration:

.. code-block:: yaml

   steps:
     - step: read_pubsub
       class: ConsumePubSubSubscriptionStep
       spec:
         subscription: "projects/my-project/subscriptions/member-updates"

     - step: extract_id
       class: ExtractIdStep
       spec:
         in: read_pubsub
         field_path: "profiles.memberId"

     - step: lookup_bt
       class: ReadBigTableByIdStep
       spec:
         in: extract_id
         prefix: "#1-"
         suffix: "#"
         column_families: ["profiles"]
         cache_config:
           ttl: 60
           max_size: 10000

"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, Iterable, Optional

import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub

from dataflow_common.core import BaseStep
from dataflow_common.connectors.bigtable import BigTableConnector  # noqa: F401  # retained for type hints

LOGGER = logging.getLogger(__name__)


class ConsumePubSubSubscriptionStep(BaseStep):
    """Consume messages from a Pub/Sub subscription or topic.

    The subscription or topic may be provided via the step ``spec``
    or resolved from the pipeline configuration under ``io.pubsub``.
    Messages are returned with attributes enabled (``with_attributes=True``)
    to allow downstream steps to access metadata such as ``message_id``
    and ``publish_time``.

    Required configuration in ``spec`` or ``self.config.io.pubsub``:

    - ``subscription`` (str): Full name of the Pub/Sub subscription.
    - ``topic`` (str, optional): Pub/Sub topic.  Only used if
      ``subscription`` is not provided.
    - ``with_attributes`` (bool, optional): Whether to return
      ``PubsubMessage`` objects with attributes.  Defaults to ``True``.
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        subscription: Optional[str] = self.spec.get("subscription")
        topic: Optional[str] = self.spec.get("topic")
        with_attributes: bool = self.spec.get("with_attributes", True)

        # Fall back to global configuration
        if not subscription and not topic:
            pubsub_cfg = getattr(self.config, "io", {}).get("pubsub", {}) if hasattr(self.config, "io") else {}
            subscription = subscription or pubsub_cfg.get("subscription")
            topic = topic or pubsub_cfg.get("topic")

        if not subscription and not topic:
            raise ValueError(f"Step {self.step_id}: either 'subscription' or 'topic' must be provided")

        # Read messages from Pub/Sub.  We include message attributes and set
        # id_label/timestamp_attribute for deduplication and watermarking.
        return pipeline | f"{self.step_id}_ReadPubSub" >> ReadFromPubSub(
            subscription=subscription,
            topic=topic,
            with_attributes=with_attributes,
            id_label="message_id",
            timestamp_attribute="publish_time",
        )


class ExtractIdStep(BaseStep):
    """Extract an identifier from JSON messages.

    This step reads a :class:`~apache_beam.PCollection` of messages (strings,
    bytes, :class:`~apache_beam.io.gcp.pubsub.PubsubMessage` or Python
    dictionaries) and extracts a value from a nested path.  The path
    segments are dot-separated (e.g. ``profiles.memberId``).  Elements
    that cannot be parsed as JSON or do not contain the specified path
    are filtered out.

    Configuration options:

    - ``in`` (str): The upstream step id producing the input PCollection.
    - ``field_path`` (str): Dot-separated path to the identifier in the
      JSON object.  Defaults to ``id`` if not provided.
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key: str = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")

        messages: beam.PCollection = self.state[input_key]

        # Determine field path either from spec or from streaming config
        default_field = "id"
        field_path: str
        if "field_path" in self.spec:
            field_path = self.spec["field_path"]
        elif hasattr(self.config, "streaming") and getattr(self.config.streaming, "extract_key", None):
            field_path = getattr(self.config.streaming.extract_key, "field_path", default_field)
        else:
            field_path = default_field
        parts = field_path.split(".")

        def extract_id(element: Any) -> Optional[Any]:
            """Parse the message and extract the identifier value."""
            try:
                # Unpack PubsubMessage or raw JSON
                if hasattr(element, "data"):
                    payload = element.data
                else:
                    payload = element
                if isinstance(payload, bytes):
                    data = json.loads(payload.decode("utf-8"))
                elif isinstance(payload, str):
                    data = json.loads(payload)
                elif isinstance(payload, dict):
                    data = payload
                else:
                    # Unsupported type
                    return None

                # Navigate nested path
                value: Any = data
                for part in parts:
                    if isinstance(value, dict):
                        value = value.get(part)
                    else:
                        return None
                return value
            except Exception as e:
                LOGGER.warning(f"{self.step_id}: failed to extract id: {e}")
                return None

        return (
            messages
            | f"{self.step_id}_ExtractId" >> beam.Map(extract_id)
            | f"{self.step_id}_FilterNone" >> beam.Filter(lambda x: x is not None)
        )


class ReadBigTableByIdStep(BaseStep):
    """Read rows from Cloud Bigtable using identifiers.

    The identifiers emitted by the upstream step are used to build
    Bigtable row keys.  Optional ``prefix`` and ``suffix`` strings may
    be specified in the step ``spec`` to construct the full row key
    (e.g. ``prefix='customer-'`` and ``suffix=''``).  The step then
    performs a per-element lookup using the Bigtable client.  A
    lightweight in-memory cache reduces the number of round trips when
    repeated keys are encountered.  Cache size and TTL can be tuned via
    ``cache_config`` in the step spec.

    Configuration options:

    - ``in`` (str): The upstream step id supplying the identifiers.
    - ``project`` (str, optional): GCP project id.  Falls back to
      ``config.io.bigtable.project``.
    - ``instance`` (str, optional): Bigtable instance id.  Falls back to
      ``config.io.bigtable.instance``.
    - ``table`` (str, optional): Bigtable table id.  Falls back to
      ``config.io.bigtable.table``.
    - ``prefix`` (str, optional): String to prepend to each identifier
      when forming the row key.  Default is empty.
    - ``suffix`` (str, optional): String to append to each identifier
      when forming the row key.  Default is empty.
    - ``column_families`` (list[str], optional): List of column family
      names to include.  If not specified, all families are returned.
    - ``cache_config`` (dict, optional): Dictionary with ``ttl`` (seconds)
      and ``max_size`` (int) controlling the in-memory cache.
    """

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key: str = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")

        identifiers: beam.PCollection = self.state[input_key]

        # Extract Bigtable connection parameters from spec or config
        bt_cfg: Dict[str, Any] = getattr(self.config, "io", {}).get("bigtable", {}) if hasattr(self.config, "io") else {}
        project_id: str = self.spec.get("project") or bt_cfg.get("project")
        instance_id: str = self.spec.get("instance") or bt_cfg.get("instance")
        table_id: str = self.spec.get("table") or bt_cfg.get("table")
        column_families: Iterable[str] = self.spec.get("column_families") or bt_cfg.get("column_families", [])
        prefix: str = self.spec.get("prefix", "")
        suffix: str = self.spec.get("suffix", "")
        cache_conf: Dict[str, Any] = self.spec.get("cache_config", {})
        cache_ttl: int = int(cache_conf.get("ttl", 60))
        cache_max_size: int = int(cache_conf.get("max_size", 1000))

        class _ReadBigtableRow(beam.DoFn):
            """Internal DoFn for per-row Bigtable lookup with caching."""

            # Shared cache across workers (per process).  Keys are row keys,
            # values are tuples of (timestamp, result_dict).
            _cache: Dict[str, Any] = {}

            def __init__(self, proj: str, inst: str, table: str,
                         families: Iterable[str], prefix: str, suffix: str,
                         ttl: int, max_size: int) -> None:
                self._proj = proj
                self._inst = inst
                self._table_id = table
                self._families = list(families) if families else []
                self._prefix = prefix
                self._suffix = suffix
                self._ttl = ttl
                self._max_size = max_size
                self._client = None
                self._table = None

            def setup(self) -> None:
                from google.cloud import bigtable
                # Create Bigtable client only once per worker
                self._client = bigtable.Client(project=self._proj)
                instance = self._client.instance(self._inst)
                self._table = instance.table(self._table_id)

            def process(self, identifier: Any) -> Iterable[Dict[str, Any]]:
                import time

                # Compose row key with optional prefix/suffix
                row_key = f"{self._prefix}{identifier}{self._suffix}"

                # Check cache first
                now = time.time()
                if row_key in self._cache:
                    ts, value = self._cache[row_key]
                    if now - ts < self._ttl:
                        yield value
                        return

                try:
                    row = self._table.read_row(row_key.encode())
                    if row:
                        result: Dict[str, Any] = {"row_key": row_key}
                        for family_id, columns in row.cells.items():
                            family_name = family_id.decode("utf-8")
                            # If column families specified, skip others
                            if self._families and family_name not in self._families:
                                continue
                            for col, cells in columns.items():
                                col_name = col.decode("utf-8")
                                cell_val = cells[0].value
                                try:
                                    # Try to decode bytes
                                    decoded = cell_val.decode("utf-8")
                                    # Attempt JSON parse if value appears to be JSON
                                    if decoded and decoded[0] in ("{", "["):
                                        try:
                                            decoded = json.loads(decoded)
                                        except Exception:
                                            pass
                                    value_out: Any = decoded
                                except Exception:
                                    # Leave as raw bytes if decoding fails
                                    value_out = cell_val
                                result[f"{family_name}:{col_name}"] = value_out
                        # Store in cache
                        self._cache[row_key] = (now, result)
                        # Enforce cache max size using FIFO eviction
                        if len(self._cache) > self._max_size:
                            oldest_key = next(iter(self._cache))
                            del self._cache[oldest_key]
                        yield result
                        return
                    else:
                        # Not found; cache negative result to avoid repeated hits
                        not_found = {"row_key": row_key, "found": False}
                        self._cache[row_key] = (now, not_found)
                        yield not_found
                        return
                except Exception as e:
                    LOGGER.error(f"{self._proj}/{self._inst}/{self._table_id}: failed to read {row_key}: {e}")
                    yield {"row_key": row_key, "error": str(e)}

        return identifiers | f"{self.step_id}_ReadBigtable" >> beam.ParDo(
            _ReadBigtableRow(
                proj=project_id,
                inst=instance_id,
                table=table_id,
                families=column_families,
                prefix=prefix,
                suffix=suffix,
                ttl=cache_ttl,
                max_size=cache_max_size,
            )
        )