# connectors/pubsub.py
import json
from typing import Dict  # ✅ เพิ่มบรรทัดนี้
import apache_beam as beam
from apache_beam.io import ReadFromPubSub, WriteToPubSub
from apache_beam.transforms.window import FixedWindows

class PubSubConnector:
    """Connector for Pub/Sub operations with DLQ handling"""
    
    @staticmethod
    def consume_messages(
        pipeline: beam.Pipeline,
        subscription: str = None,
        topic: str = None,
        with_attributes: bool = True,
        label: str = "ConsumePubSub"
    ) -> beam.PCollection:
        """Consume messages from Pub/Sub topic or subscription"""
        
        return pipeline | label >> ReadFromPubSub(
            subscription=subscription,
            topic=topic,
            with_attributes=with_attributes,
            id_label='message_id',  # For deduplication
            timestamp_attribute='publish_time'  # For event time
        )
    
    @staticmethod
    def process_with_dlq(
        messages: beam.PCollection,
        process_fn: callable,
        dlq_topic: str,
        max_retries: int = 3,
        label: str = "ProcessWithDLQ"
    ) -> Dict[str, beam.PCollection]:
        """Process messages with Dead Letter Queue handling"""
        
        class ProcessWithRetry(beam.DoFn):
            def __init__(self, process_fn, max_retries):
                self.process_fn = process_fn
                self.max_retries = max_retries
            
            def process(self, element):
                retry_count = element.attributes.get('retry_count', 0) if hasattr(element, 'attributes') else 0
                
                try:
                    # Try processing
                    result = self.process_fn(element)
                    yield beam.pvalue.TaggedOutput('success', result)
                    
                except Exception as e:
                    # Handle failure
                    if retry_count < self.max_retries:
                        # Add retry metadata
                        failed_msg = {
                            'data': element,
                            'error': str(e),
                            'retry_count': retry_count + 1,
                            'timestamp': beam.utils.timestamp.Timestamp.now()
                        }
                        yield beam.pvalue.TaggedOutput('retry', failed_msg)
                    else:
                        # Send to DLQ after max retries
                        dlq_msg = {
                            'data': element,
                            'error': str(e),
                            'retry_count': retry_count,
                            'final_failure': True
                        }
                        yield beam.pvalue.TaggedOutput('dlq', dlq_msg)
        
        # Process with multiple outputs
        processed = messages | label >> beam.ParDo(
            ProcessWithRetry(process_fn, max_retries)
        ).with_outputs('success', 'retry', 'dlq')
        
        # Send failures to DLQ topic
        _ = (processed.dlq 
             | f"{label}_SerializeDLQ" >> beam.Map(json.dumps)
             | f"{label}_WriteDLQ" >> WriteToPubSub(topic=dlq_topic))
        
        # Retry messages (could re-publish to main topic)
        # processed.retry | "Republish" >> WriteToPubSub(topic=main_topic)
        
        return {
            'success': processed.success,
            'retry': processed.retry,
            'dlq': processed.dlq
        }