# steps/streaming.py
import json
import logging
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.transforms import window
from dataflow_common.core import BaseStep
from dataflow_common.connectors.pubsub import PubSubConnector
from typing import Dict, Any, Optional
from dataflow_common.connectors.bigtable import BigTableConnector
import json
import logging

LOGGER = logging.getLogger(__name__)

class WindowStep(BaseStep):
    """Apply windowing to streaming data"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        pcoll = self.state[input_key]
        
        # Window configuration
        window_type = self.spec.get("type", "fixed")  # fixed, sliding, session
        size = self.spec.get("size", 60)  # seconds
        
        if window_type == "fixed":
            windowing = window.FixedWindows(size)
        elif window_type == "sliding":
            period = self.spec.get("period", size // 2)
            windowing = window.SlidingWindows(size, period)
        # elif window_type == "session":
        #     gap = self.spec.get("gap", 10)
        #     windowing = window.Sessions(gap)
        else:
            windowing = window.FixedWindows(size)
            # raise ValueError(f"Unknown window type: {window_type}")
        
        # Apply trigger if specified
        trigger_spec = self.spec.get("trigger")
        if trigger_spec:
            trigger = self._build_trigger(trigger_spec)
            return pcoll | f"{self.step_id}_Window" >> beam.WindowInto(
                windowing,
                trigger=trigger,
                accumulation_mode=beam.transforms.trigger.AccumulationMode.DISCARDING
            )
        
        return pcoll | f"{self.step_id}_Window" >> beam.WindowInto(windowing)
    
    def _build_trigger(self, trigger_spec):
        """Build trigger from spec"""
        trigger_type = trigger_spec.get("type", "default")
        
        if trigger_type == "after_watermark":
            early = trigger_spec.get("early_firing_minutes")
            late = trigger_spec.get("late_firing_minutes")
            
            trigger = beam.transforms.trigger.AfterWatermark()
            if early:
                trigger = trigger.with_early_firings(
                    beam.transforms.trigger.AfterProcessingTime(delay=early * 60)
                )
            if late:
                trigger = trigger.with_late_firings(
                    beam.transforms.trigger.AfterCount(late)
                )
            return trigger
        
        return beam.transforms.trigger.Default()


class CreateFixedMappingStep(BaseStep):
    """Create fixed mapping dict for streaming pipeline"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # # Fixed mapping for MS Member streaming
        # mapping_dict = {
        #     "member_id": {
        #         "src_path": ["memberId"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "email": {
        #         "src_path": ["email"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "phone": {
        #         "src_path": ["phone"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "first_name": {
        #         "src_path": ["firstName"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     "last_name": {
        #         "src_path": ["lastName"],
        #         "reconcile": True,
        #         "original": True
        #     },
        #     # Add more mappings as needed
        # }
        # ดึง mapping จาก config แทน hardcode
        mapping_dict = {}
        
        # Priority: spec > streaming config > default
        if "mapping" in self.spec:
            mapping_dict = self.spec["mapping"]
        elif hasattr(self.config, 'streaming') and self.config.streaming:
            mapping_dict = self.config.streaming.fixed_mapping
        else:
            # Default minimal mapping
            mapping_dict = {
                "member_id": {
                    "src_path": ["member_id"],
                    "reconcile": True,
                    "original": True
                }
            }
        # Return as single element PCollection
        return (
            pipeline
            | f"{self.step_id}_Create" >> beam.Create([mapping_dict])
        )

class CreateEmptyStep(BaseStep):
    """Create empty PCollection for streaming when no reconciliation needed"""

    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        # Return empty PCollection with correct KV format
        return (
            pipeline
            | f"{self.step_id}_Empty" >> beam.Create([])
            | f"{self.step_id}_KV" >> beam.Map(lambda x: (None, x))
        )

class WriteToBigQueryStep(BaseStep):
    """Write to BigQuery table"""
    
    def execute(self, pipeline: beam.Pipeline) -> None:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
        
        pcoll = self.state[input_key]
        table = self.spec.get("table")
        
        if not table:
            raise ValueError(f"Step {self.step_id}: 'table' must be provided")
        
        write_disposition = self.spec.get("write_disposition", "WRITE_APPEND")
        create_disposition = self.spec.get("create_disposition", "CREATE_IF_NEEDED")
        
        pcoll | f"{self.step_id}_WriteBQ" >> WriteToBigQuery(
            table=table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            schema="SCHEMA_AUTODETECT"
        )
        
        return None

class ProcessWithDLQStep(BaseStep):
    """Process messages with Dead Letter Queue handling"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from apache_beam.io import WriteToPubSub
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing or unknown input '{input_key}'")
        
        messages = self.state[input_key]
        dlq_topic = self.spec.get("dlq_topic") or self.config.io.get("pubsub", {}).get("dlq_topic")
        max_retries = self.spec.get("max_retries", 3)
        
        if not dlq_topic:
            # raise ValueError(f"Step {self.step_id}: 'dlq_topic' must be provided")
            return messages
        
        class ProcessWithRetry(beam.DoFn):
            def __init__(self, max_retries):
                self.max_retries = max_retries
            
            def process(self, element):
                # Simple pass-through with error handling
                try:
                    yield beam.pvalue.TaggedOutput('success', element)
                except Exception as e:
                    LOGGER.error(f"Processing failed: {e}")
                    yield beam.pvalue.TaggedOutput('dlq', {
                        'data': element,
                        'error': str(e)
                    })
        
        processed = messages | f"{self.step_id}_Process" >> beam.ParDo(
            ProcessWithRetry(max_retries)
        ).with_outputs('success', 'dlq')
        
        # Send failures to DLQ
        _ = (processed.dlq 
             | f"{self.step_id}_SerializeDLQ" >> beam.Map(json.dumps)
             | f"{self.step_id}_WriteDLQ" >> WriteToPubSub(topic=dlq_topic))
        
        return processed.success
