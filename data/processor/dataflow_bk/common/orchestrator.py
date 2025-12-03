"""
Orchestrate a Beam pipeline based on a configuration plan.

The :class:`Orchestrator` class walks through a pipeline plan defined
in a :class:`~dataflow_common.config.PipelineConfig` instance,
instantiates the corresponding step classes from the registry and
executes them in order.  Each step consumes input PCollections from
the orchestration state and stores its output back into the state.

Before a pipeline run string values in the plan are formatted with
values from the config using ``str.format`` syntax.  Placeholders
can reference nested fields in the config via dot notation, e.g.
``{io.bq.project}`` or ``{params.run_dt}``.
"""

from __future__ import annotations

import re
import logging
import traceback
from typing import Any, Dict

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from dataflow_common.config import PipelineConfig
from dataflow_common.registry import STEP_REGISTRY

LOGGER = logging.getLogger(__name__)

_TOKEN = re.compile(r"\{([a-zA-Z0-9_\.]+)\}")


def _get_by_path(obj: Any, path: str) -> Any:
    """Get a nested attribute or key from a config object.

    Supports dot‑separated lookups through dataclasses (attributes)
    and dictionaries (keys).  Returns ``None`` if the path is not
    found.
    """
    try:
        parts = path.split(".")
        cur = obj
        for part in parts:
            if cur is None:
                return None
            # support dataclass attributes and dict keys
            if isinstance(cur, dict):
                cur = cur.get(part)
            else:
                cur = getattr(cur, part, None)
        return cur
    except Exception as e:
        LOGGER.warning(f"Error accessing path '{path}': {e}")
        return None


def _format_value(value: str, cfg: PipelineConfig) -> str:
    """Format a string using config fields for placeholders.

    Placeholders of the form ``{io.bq.project}`` will be replaced
    with the corresponding attribute or key from ``cfg``.  Missing
    placeholders are replaced with an empty string.
    """
    try:
        def repl(match: re.Match[str]) -> str:
            key = match.group(1)
            val = _get_by_path(cfg, key)
            if val is None:
                LOGGER.warning(f"Config key '{key}' not found, using empty string")
                return ""
            return str(val)
        return _TOKEN.sub(repl, value)
    except Exception as e:
        LOGGER.error(f"Error formatting value '{value}': {e}")
        raise


class Orchestrator:
    """Construct and run a Beam pipeline from a :class:`PipelineConfig`.

    Parameters
    ----------
    config: :class:`PipelineConfig`
        The parsed pipeline configuration.
    """

    def __init__(self, config: PipelineConfig) -> None:
        self.config = config
        # state holds intermediate PCollections keyed by the step's
        # output identifier.  It also stores the pipeline object
        # under the special key ``__pipeline__``.
        self.state: Dict[str, Any] = {}
        LOGGER.info(f"Orchestrator initialized for pipeline: {config.name}")

    def run(self, pipeline_options: PipelineOptions | None = None) -> Dict[str, Any]:
        """Execute the pipeline according to the configured plan.

        Parameters
        ----------
        pipeline_options: :class:`~apache_beam.options.pipeline_options.PipelineOptions`, optional
            Custom pipeline options to pass to the Beam pipeline.  If
            ``None`` a default set of options will be used.

        Returns
        -------
        dict
            A mapping of step outputs and internal state accumulated
            during the run.
        """
        try:
            cfg = self.config
            plan = cfg.plan or []
            
            LOGGER.info(f"Starting pipeline execution with {len(plan)} steps")
        
            # Format schema fields if they exist
            try:
                if cfg.schema and cfg.schema.bq:
                    if cfg.schema.bq.project:
                        cfg.schema.bq.project = _format_value(cfg.schema.bq.project, cfg)
                    if cfg.schema.bq.dataset:
                        cfg.schema.bq.dataset = _format_value(cfg.schema.bq.dataset, cfg)
                    if cfg.schema.bq.table:
                        cfg.schema.bq.table = _format_value(cfg.schema.bq.table, cfg)
                    if cfg.schema.bq.query:
                        cfg.schema.bq.query = _format_value(cfg.schema.bq.query, cfg)
                    LOGGER.debug("Schema fields formatted successfully")
            except Exception as e:
                LOGGER.error(f"Error formatting schema fields: {e}")
                raise

            # Format string fields in the plan prior to execution
            for idx, spec in enumerate(plan):
                try:
                    for key, val in list(spec.items()):
                        if isinstance(val, str):
                            spec[key] = _format_value(val, cfg)
                    LOGGER.debug(f"Step {idx} spec formatted successfully")
                except Exception as e:
                    LOGGER.error(f"Error formatting step {idx} spec: {e}")
                    LOGGER.error(f"Step spec: {spec}")
                    raise
            
            # Run the pipeline
            with beam.Pipeline(options=pipeline_options) as p:
                self.state["__pipeline__"] = p
                
                for idx, spec in enumerate(plan):
                    step_name = spec.get("step")
                    step_id = spec.get("id") or spec.get("out") or f"step_{idx}"
                    
                    try:
                        LOGGER.info(f"Executing step {idx}: {step_name} (id: {step_id})")
                        
                        if not step_name:
                            raise ValueError(f"Plan step #{idx} is missing the 'step' field: {spec}")
                        
                        cls = STEP_REGISTRY.get(step_name)
                        if cls is None:
                            available_steps = list(STEP_REGISTRY.keys())
                            raise ValueError(
                                f"Unknown step type '{step_name}' at plan index {idx}. "
                                f"Available steps: {available_steps}"
                            )
                        
                        step = cls(spec=spec, config=cfg, state=self.state)
                        output = step.execute(p)
                        
                        out_key = spec.get("out") or spec.get("id")
                        if out_key:
                            self.state[out_key] = output
                            LOGGER.info(f"Step {idx} output stored as '{out_key}'")
                        else:
                            LOGGER.debug(f"Step {idx} has no output key")
                        
                        LOGGER.info(f"Step {idx}: {step_name} completed successfully")
                        
                    except Exception as e:
                        LOGGER.error(f"Failed at step {idx}: {step_name}")
                        LOGGER.error(f"Step spec: {spec}")
                        LOGGER.error(f"Error: {str(e)}")
                        LOGGER.error(f"Stack trace: {traceback.format_exc()}")
                        raise
            
            LOGGER.info("Pipeline execution completed successfully")
            return self.state
            
        except Exception as e:
            LOGGER.error(f"Pipeline execution failed: {e}")
            LOGGER.error(f"Stack trace: {traceback.format_exc()}")
            raise