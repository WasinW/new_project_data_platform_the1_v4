# connectors/bigtable.py
from typing import List, Dict, Any
import apache_beam as beam
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
from dataflow_common.config import PipelineConfig  # ✅ เพิ่มบรรทัดนี้

class BigTableConnector:
    """Connector for Cloud Bigtable operations"""
    
    @staticmethod
    def read_by_keys(
        pipeline: beam.Pipeline, 
        keys: beam.PCollection,
        project_id: str,
        instance_id: str,
        table_id: str,
        column_family: str = None,
        label: str = "ReadBigTable"
    ) -> beam.PCollection:
        """Read rows from Bigtable by keys"""
        
        class ReadFromBigTable(beam.DoFn):
            def __init__(self, project_id, instance_id, table_id, column_family):
                self.project_id = project_id
                self.instance_id = instance_id
                self.table_id = table_id
                self.column_family = column_family
                self._client = None
                self._table = None
            
            def setup(self):
                # Initialize client once per worker
                self._client = bigtable.Client(project=self.project_id)
                instance = self._client.instance(self.instance_id)
                self._table = instance.table(self.table_id)
            
            def process(self, key):
                # Read single row by key
                row = self._table.read_row(key.encode())
                if row:
                    result = {"row_key": key}
                    for family_id, family in row.cells.items():
                        if not self.column_family or family_id == self.column_family:
                            for column, cells in family.items():
                                col_name = f"{family_id}:{column.decode()}"
                                # Get latest cell value
                                result[col_name] = cells[0].value.decode()
                    yield result
        
        return keys | label >> beam.ParDo(
            ReadFromBigTable(project_id, instance_id, table_id, column_family)
        )
    
    @staticmethod
    def batch_read(
        pipeline: beam.Pipeline,
        row_set: List[str],  # List of row keys or ranges
        cfg: PipelineConfig,
        label: str = "BatchReadBigTable"
    ) -> beam.PCollection:
        """Batch read from BigTable using row set"""
        from apache_beam.io.gcp.bigtableio import ReadFromBigtable
        
        bt_cfg = cfg.io.get("bigtable", {})
        
        return pipeline | label >> ReadFromBigtable(
            project_id=bt_cfg.get("project"),
            instance_id=bt_cfg.get("instance"),
            table_id=bt_cfg.get("table"),
            row_set=row_set
        )