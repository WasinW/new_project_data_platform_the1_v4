# dataflow_common/src/dataflow_common/transforms/cdc.py

def prepare_cdc_record(record: Dict[str, Any], change_type: str = "UPSERT") -> Dict:
    """
    Prepare record for CDC write to BigQuery
    
    ตัวอย่าง:
    Input: {"member_number": "123", "gender": "M"}
    Output: {
        "record": {"member_number": "123", "gender": "M", "_CHANGE_TYPE": "UPSERT"},
        "row_mutation_info": {"change_type": "UPSERT"}
    }
    """
    # Add _CHANGE_TYPE field required by BigQuery CDC
    record_with_type = dict(record)
    record_with_type["_CHANGE_TYPE"] = change_type
    
    # Create row mutation info
    row_mutation_info = {
        "change_type": change_type,
        "timestamp": beam.utils.timestamp.Timestamp.now().to_rfc3339()
    }
    
    # Return in the format expected by CDC writes
    return {
        "record": record_with_type,
        "row_mutation_info": row_mutation_info
    }