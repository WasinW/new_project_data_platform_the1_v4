# dataflow_common/src/dataflow_common/steps/streaming_midterm.py

from __future__ import annotations
import json
import logging
import time
from typing import Dict, Any, List
from datetime import datetime
import apache_beam as beam
from apache_beam.transforms import window
# from apache_beam.io.gcp.bigquery import WriteToBigQuery

from dataflow_common.core import BaseStep
from dataflow_common.connectors import BigQueryConnector
from dataflow_common.transforms.mapping import create_mapping_dict, map_record

LOGGER = logging.getLogger(__name__)

# 1. ConsumeMessagesWithDLQStep - Duplicate จาก ConsumePubSubSubscriptionStep + เพิ่ม DLQ
class ConsumeMessagesWithDLQStep(BaseStep):
    """Enhanced ConsumePubSubSubscription with DLQ and sequence handling"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        from apache_beam.io import ReadFromPubSub, WriteToPubSub
        
        # Get config from spec (ไม่ hardcode)
        subscription = self.spec.get("subscription")
        dlq_topic = self.spec.get("dlq_topic")
        max_retries = self.spec.get("max_retries", 3)
        with_attributes = self.spec.get("with_attributes", True)
        
        if not subscription:
            raise ValueError(f"Step {self.step_id}: subscription required")
        
        class HandleDLQ(beam.DoFn):
            def process(self, element):
                try:
                    # Parse message
                    if hasattr(element, 'data'):
                        data = element.data
                        attributes = element.attributes or {}
                    else:
                        data = element
                        attributes = {}
                    
                    retry_count = int(attributes.get('retry_count', 0))
                    
                    # Decode JSON
                    if isinstance(data, bytes):
                        decoded = json.loads(data.decode('utf-8'))
                    else:
                        decoded = json.loads(data)
                    
                    yield beam.pvalue.TaggedOutput('success', decoded)
                    
                except Exception as e:
                    if retry_count >= max_retries:
                        yield beam.pvalue.TaggedOutput('dlq', {
                            'data': element,
                            'error': str(e),
                            'retry_count': retry_count
                        })
                    else:
                        yield beam.pvalue.TaggedOutput('retry', element)
        
        # Read messages (เหมือน ConsumePubSubSubscriptionStep)
        messages = pipeline | f"{self.step_id}_Read" >> ReadFromPubSub(
            subscription=subscription,
            with_attributes=with_attributes,
            id_label='message_id',
            timestamp_attribute='publish_time'
        )
        
        # Process with DLQ
        processed = messages | f"{self.step_id}_Process" >> beam.ParDo(
            HandleDLQ()
        ).with_outputs('success', 'retry', 'dlq')
        
        # Write to DLQ if configured
        if dlq_topic:
            _ = (processed.dlq 
                 | f"{self.step_id}_SerializeDLQ" >> beam.Map(json.dumps)
                 | f"{self.step_id}_WriteDLQ" >> WriteToPubSub(topic=dlq_topic))
        
        return processed.success

# 2. ParseNestedJsonStep - Duplicate จาก ParseJsonStep แต่ handle nested structure
# dataflow_common/src/dataflow_common/steps/streaming_midterm.py

class ParseNestedJsonStep(BaseStep):
    """Parse and auto-flatten nested JSON fields"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
            
        pcoll = self.state[input_key]
        
        # Fields to auto-flatten (e.g., ["profiles"])
        json_fields = self.spec.get("json_fields", [])
        preserve_fields = self.spec.get("preserve_fields", [])
        field_mappings = self.spec.get("field_mappings", {})  # รับ mapping จาก config
        extract_from = self.spec.get("extract_from", None)  # e.g., "payload"
        
        def parse_and_flatten(record):
            """Auto-flatten specified nested fields"""
            result = {}
            
            # Handle both dict and Pub/Sub message
            if hasattr(record, 'data'):
                # It's a Pub/Sub message
                data = record.data
                if isinstance(data, bytes):
                    rec = json.loads(data.decode('utf-8'))
                else:
                    rec = json.loads(data) if isinstance(data, str) else data
            else:
                rec = record
            
            # Extract from payload if exists
            if extract_from and extract_from in rec:
                rec = rec[extract_from]
            
            # Preserve top-level fields
            for field in preserve_fields:
                if field in rec:
                    result[field] = rec[field]
            
            # Auto-flatten specified nested fields
            for field_name in json_fields:
                if field_name in rec:
                    nested_data = rec[field_name]
                    
                    # Parse if it's a JSON string
                    if isinstance(nested_data, str):
                        try:
                            nested_data = json.loads(nested_data)
                        except:
                            pass
                    
                    # Flatten all fields from nested object
                    if isinstance(nested_data, dict):
                        for key, value in nested_data.items():
                            # Apply field mappings if configured
                            output_key = field_mappings.get(key, key)
                            result[output_key] = value
            
            return result
        
        return pcoll | f"{self.step_id}_ParseFlatten" >> beam.Map(parse_and_flatten)
# 3. WindowedMappingQueryStep - Query mapping table periodically
class WindowedMappingQueryStep(BaseStep):
    """Query mapping table with windowing and cache (ไม่ hardcode)"""
    
    def execute(self, pipeline: beam.Pipeline) -> beam.PCollection:
        window_size = self.spec.get("window_size", 3600)
        query = self.spec.get("query")  # รับ query จาก config
        
        # Mapping field names from config
        src_field = self.spec.get("src_field", "PERSONAS_MAPPING_COLUMN_NAME")
        dest_field = self.spec.get("dest_field", "RECONCILE_COLUMN_NAME")
        retrieved_flag = self.spec.get("retrieved_flag_field", "RECONCILE_RETRIEVED")
        confirmed_flag = self.spec.get("confirmed_flag_field", "RECONCILE_CONFIRMED")
        
        if not query:
            raise ValueError(f"Step {self.step_id}: query required")
        
        class QueryAndBuildMapping(beam.DoFn):
            """Query and build mapping dict with cache"""
            
            _cache = None
            _cache_time = 0
            
            def __init__(self, query, config, window_size, field_names):
                self.query = query
                self.config = config
                self.window_size = window_size
                self.field_names = field_names
                
            def process(self, element):
                current_time = time.time()
                
                # Check cache
                if (self._cache is not None and 
                    current_time - self._cache_time < self.window_size):
                    LOGGER.info("Using cached mapping")
                    yield self._cache
                    return
                
                # Query new mapping
                LOGGER.info("Querying mapping from BigQuery")
                
                from google.cloud import bigquery
                client = bigquery.Client(project=self.config.io.bq.get("project"))
                
                try:
                    query_job = client.query(self.query)
                    mapping_rows = [dict(row) for row in query_job.result()]
                    
                    # Build mapping dict using create_mapping_dict
                    mapping_dict = create_mapping_dict(
                        mapping_rows,
                        src_field=self.field_names['src_field'],
                        dest_field=self.field_names['dest_field'],
                        retrieved_flag_field=self.field_names['retrieved_flag'],
                        confirmed_flag_field=self.field_names['confirmed_flag']
                    )
                    
                    # Update cache
                    QueryAndBuildMapping._cache = mapping_dict
                    QueryAndBuildMapping._cache_time = current_time
                    
                    yield mapping_dict
                    
                except Exception as e:
                    LOGGER.error(f"Query failed: {e}")
                    yield self._cache or {}
        
        field_names = {
            'src_field': src_field,
            'dest_field': dest_field,
            'retrieved_flag': retrieved_flag,
            'confirmed_flag': confirmed_flag
        }
        
        return (
            pipeline
            | f"{self.step_id}_Trigger" >> beam.transforms.PeriodicImpulse(
                fire_interval=window_size
            )
            | f"{self.step_id}_Window" >> beam.WindowInto(
                window.FixedWindows(window_size)
            )
            | f"{self.step_id}_Query" >> beam.ParDo(
                QueryAndBuildMapping(query, self.config, window_size, field_names)
            )
        )

# 4. ใช้ MapRecordStep เดิมได้เลย! แค่เรียกจาก existing
# from dataflow_common.steps import MapRecordStep  

# 5. ใช้ WriteParquetDynamicStep จากที่สร้างไว้ก่อนหน้า
# Update in streaming_additions.py

# class WriteParquetDynamicStep(BaseStep):
#     """Enhanced Write Parquet with streaming optimizations"""
    
#     def execute(self, pipeline: beam.Pipeline) -> None:
#         input_key = self.spec.get("in")
#         if not input_key or input_key not in self.state:
#             raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
            
#         pcoll = self.state[input_key]
        
#         # Get config
#         batch_size = self.spec.get("batch_size", 1000)  # Batch records
#         file_size_mb = self.spec.get("file_size_mb", 128)  # Target file size
        
#         class BatchAndWriteParquet(beam.DoFn):
#             """Batch records and write to Parquet"""
            
#             def __init__(self, schema_spec, batch_size):
#                 self.schema_spec = schema_spec
#                 self.batch_size = batch_size
#                 self._schema = None
#                 self._buffer = []
                
#             def setup(self):
#                 from dataflow_common.transforms.schema import load_schema_from_spec
#                 self._schema = load_schema_from_spec(self.schema_spec)
                
#             def start_bundle(self):
#                 """Reset buffer at bundle start"""
#                 self._buffer = []
                
#             def process(self, element):
#                 """Buffer records"""
#                 # Get partition path
#                 partition_path = element.get("_partition_path")
#                 if not partition_path:
#                     # Generate default path
#                     now = datetime.now()
#                     partition_path = (
#                         f"{self.config.io.s3.get('refined_prefix')}/ms_personas/"
#                         f"par_year={now.year}/par_month={now.month:02d}/"
#                         f"par_day={now.day:02d}/par_hour={now.hour:02d}"
#                     )
                    
#                 # Clean element
#                 clean_element = {
#                     k: v for k, v in element.items()
#                     if not k.startswith("_")
#                 }
                
#                 self._buffer.append((partition_path, clean_element))
                
#                 # Write when buffer is full
#                 if len(self._buffer) >= self.batch_size:
#                     yield from self._write_batch()
#                     self._buffer = []
                    
#             def finish_bundle(self):
#                 """Write remaining records"""
#                 if self._buffer:
#                     yield from self._write_batch()
                    
#             def _write_batch(self):
#                 """Write buffered records to Parquet"""
#                 import pyarrow as pa
#                 import pyarrow.parquet as pq
#                 from apache_beam.io.filesystems import FileSystems
#                 import uuid
#                 from collections import defaultdict
                
#                 # Group by partition
#                 partitions = defaultdict(list)
#                 for path, record in self._buffer:
#                     partitions[path].append(record)
                
#                 # Write each partition
#                 for path, records in partitions.items():
#                     filename = f"data_{uuid.uuid4().hex}.snappy.parquet"
#                     full_path = f"{path}/{filename}"
                    
#                     try:
#                         # Create PyArrow table
#                         table = pa.Table.from_pylist(records, schema=self._schema)
                        
#                         # Write to S3/GCS
#                         with FileSystems.create(full_path) as f:
#                             pq.write_table(table, f, compression='snappy')
                            
#                         LOGGER.info(f"Wrote {len(records)} records to {full_path}")
                        
#                         yield beam.pvalue.TaggedOutput('success', {
#                             'path': full_path,
#                             'count': len(records),
#                             'timestamp': datetime.now().isoformat()
#                         })
                        
#                     except Exception as e:
#                         LOGGER.error(f"Failed to write {full_path}: {e}")
#                         yield beam.pvalue.TaggedOutput('failed', {
#                             'error': str(e),
#                             'count': len(records)
#                         })
        
#         result = pcoll | f"{self.step_id}_Write" >> beam.ParDo(
#             BatchAndWriteParquet(self.config.schema, batch_size)
#         ).with_outputs('success', 'failed')
        
#         # Log metrics
#         _ = (result.success 
#              | f"{self.step_id}_CountSuccess" >> beam.Map(
#                  lambda x: Metrics.counter('parquet_write', 'success').inc(x['count'])
#              ))
        
#         _ = (result.failed
#              | f"{self.step_id}_CountFailed" >> beam.Map(
#                  lambda x: Metrics.counter('parquet_write', 'failed').inc(x['count'])
#              ))
        
#         return None

# 6. EnhancedWriteToBigQueryStep - ปรับจาก WriteToBigQueryStep เดิม
class EnhancedWriteToBigQueryStep(BaseStep):
    """Enhanced BQ write for streaming (based on WriteToBigQueryStep)"""
    
    def execute(self, pipeline: beam.Pipeline) -> None:
        
        input_key = self.spec.get("in")
        if not input_key or input_key not in self.state:
            raise KeyError(f"Step {self.step_id}: missing input '{input_key}'")
            
        pcoll = self.state[input_key]
        
        # Configuration
        table = self.spec.get("table")
        method = self.spec.get("method", "STREAMING_INSERTS")
        write_disposition = self.spec.get("write_disposition", "WRITE_APPEND")
        enable_upsert = self.spec.get("enable_upsert", False)
        primary_key = self.spec.get("primary_key", ["member_number"])
        
        if not table:
            raise ValueError(f"Step {self.step_id}: table required")
        
        # Clean metadata fields
        def clean_for_bq(element):
            return {k: v for k, v in element.items() if not k.startswith("_")}
        
        cleaned = pcoll | f"{self.step_id}_Clean" >> beam.Map(clean_for_bq)
        
        # Write to BQ
        # สำหรับ Upsert - ใช้ Storage Write API + CDC
        if enable_upsert:
            # Transform to CDC format
            def to_cdc_format(record):
                """แปลงเป็น format สำหรับ CDC"""
                record_with_type = dict(record)
                record_with_type["_CHANGE_TYPE"] = "UPSERT"
                
                return {
                    "record": record_with_type,
                    "row_mutation_info": {
                        "change_type": "UPSERT",
                        "timestamp": beam.utils.timestamp.Timestamp.now().to_rfc3339()
                    }
                }
            
            cdc_records = cleaned | f"{self.step_id}_ToCDC" >> beam.Map(to_cdc_format)
            
            # Use BigQueryConnector with CDC
            BigQueryConnector.write(
                pcoll=cdc_records,
                table=table,
                cfg=self.config,
                method="STORAGE_WRITE_API",
                use_cdc=True,
                primary_key=primary_key,
                label=f"{self.step_id}_WriteCDC"
            )
            
        else:
            # Standard write (non-upsert)
            BigQueryConnector.write(
                pcoll=cleaned,
                table=table,
                cfg=self.config,
                method=method,
                write_disposition=write_disposition,
                label=f"{self.step_id}_Write"
            )
        
        return None
