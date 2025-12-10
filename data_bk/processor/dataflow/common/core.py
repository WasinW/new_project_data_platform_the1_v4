"""
Core abstractions for the ``dataflow_common`` package.

This module defines a minimal set of base classes that other parts of
``dataflow_common`` build upon.  The goal of these classes is to
provide a consistent interface for Beam pipeline steps and
connectors without enforcing any table‑specific behaviour.  By
keeping these abstractions small and generic, we allow new
pipeline components to be added without modifying the orchestrator
or other framework code.

Currently, only the :class:`BaseStep` abstract class is defined
here.  Additional base classes could be added in the future if
common behaviour emerges across connectors or other utilities.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict

import apache_beam as beam

from dataflow_common.config import PipelineConfig


LOGGER = logging.getLogger(__name__)


class BaseStep(ABC):
    """Abstract base class for all pipeline steps.

    A step encapsulates a single unit of work in a Beam pipeline.
    Subclasses must implement the :meth:`execute` method which
    receives a Beam :class:`~apache_beam.Pipeline` object, reads
    its inputs from the shared state dictionary and writes its
    outputs back into the state.  The orchestrator is responsible
    for instantiating each step with the correct specification,
    configuration and state.

    Attributes
    ----------
    spec: dict
        The raw plan dictionary describing this step.  Custom
        parameters should be accessed through this attribute.
    config: :class:`PipelineConfig`
        The top‑level pipeline configuration.  Global values such
        as :code:`io` and :code:`params` are defined here.
    state: dict
        A mapping from output identifiers to intermediate
        :class:`~apache_beam.PCollection` objects.  Steps must
        retrieve their inputs from this dict using the key specified
        in the step specification and store their outputs using
        either ``spec['out']`` or ``spec['id']``.
    """

    def __init__(self, *, spec: Dict[str, Any], config: PipelineConfig, state: Dict[str, Any]) -> None:
        self.spec = spec
        self.config = config
        self.state = state
        # Use a descriptive identifier for logging if provided
        # self.step_id = spec.get("id") or spec.get("out") or spec.get("step")
        step_type = spec.get("step", "Step")
        identifier = spec.get("id") or spec.get("out") or spec.get("in") or "unnamed"
        self.step_id = f"{step_type}_{identifier}"

    @abstractmethod
    def execute(self, pipeline: beam.Pipeline) -> Any:
        """Execute this step and return its output.

        Implementations must read input PCollections from
        :attr:`state` and return either a PCollection (for data
        transformations) or ``None`` (for sink steps).  The
        orchestrator will store the return value in ``state`` under
        the key given by ``spec['out']`` or ``spec['id']``.
        """
        raise NotImplementedError


__all__ = ["BaseStep"]