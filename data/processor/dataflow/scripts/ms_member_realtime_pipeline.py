import apache_beam as beam
from apache_beam import DoFn, ParDo, Map, FlatMap
from apache_beam.transforms.combiners import Latest
from apache_beam.io import ReadFromPubSub, WriteToBigQuery
from apache_beam.io.gcp.bigquery import BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms import window, trigger
from apache_beam.transforms.periodicsequence import PeriodicImpulse
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec
from apache_beam.transforms.window import IntervalWindow
from apache_beam.io.parquetio import WriteToParquet
from apache_beam.io.fileio import FileNaming
from apache_beam.utils.windowed_value import PaneInfo
from datetime import datetime, timedelta, timezone, date
from functools import reduce
import operator
import json
import logging
from typing import Dict, Any, Optional, List, Iterable
from google.cloud import bigtable
from google.cloud.bigtable import row_filters
import pyarrow as pa
import io
import fastavro
from apache_beam import Row  # ⬅️ นี่คือ path ที่ถูกต้อง
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
# import s3fs
from google.cloud import bigquery

# Configuration
PROJECT_ID = "the1-insight-stg"
BT_PROJECT_ID = "the1-insight-stg"
SUBSCRIPTION_NAME = "projects/the1-insight-stg/subscriptions/ms-personas-datapipeline-dataflow-subscription"
MAPPING_TABLE = f"{PROJECT_ID}.insight.mapping_reconcile"
NATIVE_TABLE = f"{PROJECT_ID}.insight.ms_personas"
ICEBERG_TABLE = f"{PROJECT_ID}.insight.ms_personas_iceberg"  # Iceberg historical table

# === SYNC CONFIGURATION ===
SYNC_WINDOW_SECONDS = 10      # 10 seconds - sync interval
SYNC_LOOKBACK_MINUTES = 30     # Query lookback buffer for late data

# Legacy alias (for backward compatibility)
BIGLAKE_TABLE = NATIVE_TABLE

BT_INSTANCE = "t1-insight-bt"
BT_TABLE = "personas"


# Parse schema นี้ไว้ล่วงหน้า (ทำครั้งเดียวตอน worker เริ่มทำงาน)
# try:
#     PARSED_PERSONAS_SCHEMA = fastavro.parse_schema(PERSONAS_AVRO_SCHEMA_DEFINITION)
#     logging.info("Successfully parsed Pub/Sub Avro schema.")
# except Exception as e:
#     logging.error(f"FATAL: Could not parse Pub/Sub Avro schema: {e}")
#     PARSED_PERSONAS_SCHEMA = None
# --- S3 Configuration ---
# GCS Staging Bucket สำหรับ Parquet (จะถูก Sync ไป S3 ในภายหลัง)
# ⚠️ เปลี่ยนเป็น GCS path ของคุณ
S3_PARQUET_BUCKET = f"s3://t1-analytics/refined/insights/ms_personas_realtime_dev" 
TZ_BANGKOK = timezone(timedelta(hours=7))
# --- Parquet Schema Definition ---
# ⚠️ คุณต้องกำหนด Schema นี้ให้ตรงกับ output ของ TransformSchemasDoFn
MS_PERSONAS_PARQUET_SCHEMA = pa.schema([
    pa.field('member_id', pa.string()),
    pa.field('member_number', pa.string()),
    pa.field('nationality', pa.string()),
    pa.field('country', pa.string()),
    pa.field('passport_exp', pa.date32()),
    pa.field('birth_date', pa.date32()),
    pa.field('age', pa.string()),
    pa.field('mobile_country_code', pa.string()),
    pa.field('home_ph_country_code', pa.string()),
    pa.field('type_of_housing', pa.string()),
    pa.field('sub_district', pa.string()),
    pa.field('district', pa.string()),
    pa.field('city', pa.string()),
    pa.field('postal_code', pa.string()),
    pa.field('member_type', pa.string()),
    pa.field('status_code', pa.string()),
    pa.field('hold_reason', pa.string()),
    pa.field('register_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('member_ref_by_name', pa.string()),
    pa.field('member_ref_by_id', pa.string()),
    pa.field('register_channel', pa.string()),
    pa.field('register_partner', pa.string()),
    pa.field('gender', pa.string()),
    pa.field('marital_status', pa.string()),
    pa.field('job_title', pa.string()),
    pa.field('education', pa.string()),
    pa.field('monthly_income', pa.string()),
    pa.field('prefer_lang', pa.string()),
    pa.field('customer_type', pa.string()),
    pa.field('register_staff', pa.string()),
    pa.field('register_branch', pa.string()),
    pa.field('privacy_flag', pa.string()),
    pa.field('data_invalid_flag', pa.string()),
    pa.field('dummy_flag', pa.string()),
    pa.field('employee_bu_group', pa.string()),
    pa.field('employee_bu', pa.string()),
    pa.field('employee_resign_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('employee_join_date', pa.date32()),
    pa.field('employee_id', pa.string()),
    pa.field('created_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('member_number_merged', pa.string()),
    pa.field('register_partner_code', pa.string()),
    pa.field('register_branch_code', pa.string()),
    pa.field('is_address', pa.string()),
    pa.field('is_mobile', pa.string()),
    pa.field('is_email', pa.string()),
    pa.field('is_send_sms_eng', pa.string()),
    pa.field('is_send_sms_thai', pa.string()),
    pa.field('is_expate', pa.string()),
    pa.field('is_cds_line', pa.string()),
    pa.field('is_rbs_line', pa.string()),
    pa.field('is_ssp_line', pa.string()),
    pa.field('is_cpn_line', pa.string()),
    pa.field('is_cfr_line', pa.string()),
    pa.field('is_cfm_line', pa.string()),
    pa.field('is_twd_line', pa.string()),
    pa.field('is_the1_line', pa.string()),
    pa.field('updated_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('insurance_not_send', pa.string()),
    pa.field('consent_flag', pa.string()),
    pa.field('consent_channel', pa.string()),
    pa.field('consent_version', pa.string()),
    pa.field('consent_date', pa.timestamp('us', tz='Asia/Bangkok')),
    pa.field('iscall', pa.string()),
    pa.field('is_call_cds', pa.string()),
    pa.field('is_email_cds', pa.string()),
    pa.field('is_address_cds', pa.string()),
    pa.field('is_send_sms_eng_cds', pa.string()),
    pa.field('is_send_sms_thai_cds', pa.string()),
    pa.field('is_call_rbs', pa.string()),
    pa.field('is_email_rbs', pa.string()),
    pa.field('is_address_rbs', pa.string()),
    pa.field('is_send_sms_eng_rbs', pa.string()),
    pa.field('is_send_sms_thai_rbs', pa.string()),
    pa.field('is_call_b2s', pa.string()),
    pa.field('is_email_b2s', pa.string()),
    pa.field('is_address_b2s', pa.string()),
    pa.field('is_send_sms_eng_b2s', pa.string()),
    pa.field('is_send_sms_thai_b2s', pa.string()),
    pa.field('is_call_hws', pa.string()),
    pa.field('is_email_hws', pa.string()),
    pa.field('is_address_hws', pa.string()),
    pa.field('is_send_sms_eng_hws', pa.string()),
    pa.field('is_send_sms_thai_hws', pa.string()),
    pa.field('is_call_twd', pa.string()),
    pa.field('is_email_twd', pa.string()),
    pa.field('is_address_twd', pa.string()),
    pa.field('is_send_sms_eng_twd', pa.string()),
    pa.field('is_send_sms_thai_twd', pa.string()),
    pa.field('is_call_ssp', pa.string()),
    pa.field('is_email_ssp', pa.string()),
    pa.field('is_address_ssp', pa.string()),
    pa.field('is_send_sms_eng_ssp', pa.string()),
    pa.field('is_send_sms_thai_ssp', pa.string()),
    pa.field('is_call_pwb', pa.string()),
    pa.field('is_email_pwb', pa.string()),
    pa.field('is_address_pwb', pa.string()),
    pa.field('is_send_sms_eng_pwb', pa.string()),
    pa.field('is_send_sms_thai_pwb', pa.string()),
    pa.field('is_call_ofm', pa.string()),
    pa.field('is_email_ofm', pa.string()),
    pa.field('is_address_ofm', pa.string()),
    pa.field('is_send_sms_eng_ofm', pa.string()),
    pa.field('is_send_sms_thai_ofm', pa.string()),
    pa.field('is_call_cfm', pa.string()),
    pa.field('is_email_cfm', pa.string()),
    pa.field('is_address_cfm', pa.string()),
    pa.field('is_send_sms_eng_cfm', pa.string()),
    pa.field('is_send_sms_thai_cfm', pa.string()),
    pa.field('is_call_cfr', pa.string()),
    pa.field('is_email_cfr', pa.string()),
    pa.field('is_address_cfr', pa.string()),
    pa.field('is_send_sms_eng_cfr', pa.string()),
    pa.field('is_send_sms_thai_cfr', pa.string()),
    pa.field('is_call_cmg', pa.string()),
    pa.field('is_email_cmg', pa.string()),
    pa.field('is_address_cmg', pa.string()),
    pa.field('is_send_sms_eng_cmg', pa.string()),
    pa.field('is_send_sms_thai_cmg', pa.string()),
    pa.field('th_title', pa.string()),
    pa.field('eng_title', pa.string()),
    pa.field('ever_consent_partner', pa.string()),
    pa.field('is_consent_the1', pa.string()),
    pa.field('etl_created_by', pa.string()),
    pa.field('etl_created_tms', pa.string()),
    pa.field('invalid_member_flag', pa.string()),
    pa.field('invalid_type', pa.string()),
])


# MS_PERSONAS_BIGQUERY_SCHEMA = {
#     'fields': [
#     {"name": "accountId","mode": "NULLABLE","type": "STRING"},
#     {"name": "dateOfBirth","mode": "NULLABLE","type": "STRING"},
#     {"name": "gender","mode": "NULLABLE","type": "STRING"},
#     {"name": "hasEmail","mode": "NULLABLE","type": "STRING"},
#     {"name": "hasMobile","mode": "NULLABLE","type": "STRING"},
#     {"name": "languagePrefer","mode": "NULLABLE","type": "STRING"},
#     {"name": "memberId","mode": "REQUIRED","type": "STRING","description": "Primary key","fields": []},
#     {"name": "nationalityId","mode": "NULLABLE","type": "STRING"},
#     {"name": "profileId","mode": "NULLABLE","type": "STRING"},
#     {"name": "updated_date","mode": "NULLABLE","type": "STRING"},
#     # 
#     ]
# }

# CDC Schema for Storage Write API with use_cdc_writes=True
# Must have "row_mutation_info" and "record" nested structure
MS_PERSONAS_CDC_SCHEMA = {
    'fields': [
        {
            "name": "row_mutation_info",
            "type": "RECORD",
            "mode": "REQUIRED",
            "fields": [
                {"name": "mutation_type", "type": "STRING", "mode": "REQUIRED"},
                {"name": "change_sequence_number", "type": "STRING", "mode": "REQUIRED"}
            ]
        },
        {
            "name": "record",
            "type": "RECORD",
            "mode": "REQUIRED",
            "fields": [
                {"name": "accountId", "type": "STRING", "mode": "NULLABLE"},
                {"name": "dateOfBirth", "type": "STRING", "mode": "NULLABLE"},
                {"name": "gender", "type": "STRING", "mode": "NULLABLE"},
                {"name": "hasEmail", "type": "STRING", "mode": "NULLABLE"},
                {"name": "hasMobile", "type": "STRING", "mode": "NULLABLE"},
                {"name": "languagePrefer", "type": "STRING", "mode": "NULLABLE"},
                {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
                {"name": "nationalityId", "type": "STRING", "mode": "NULLABLE"},
                {"name": "profileId", "type": "STRING", "mode": "NULLABLE"},
                {"name": "updated_date", "type": "STRING", "mode": "NULLABLE"},
            ]
        }
    ]
}

CDC_ROW_TYPE = beam.Row(
    accountId=str,
    # 💡 ใช้ date object
    dateOfBirth=date, 
    gender=str,
    hasEmail=str,
    hasMobile=str,
    languagePrefer=str,
    memberId=str,
    nationalityId=str,
    profileId=str,
    # 💡 ใช้ datetime object
    updated_date=datetime, 
)

# MS_PERSONAS_BIGQUERY_SCHEMA_TEXT = 'accountId:STRING,dateOfBirth:STRING'
# MS_PERSONAS_BIGQUERY_SCHEMA_TEXT = ','.join([f"{field['name']}:{field['type']}" for field in MS_PERSONAS_BIGQUERY_SCHEMA['fields']])


# DoFn สำหรับเพิ่ม window info
# ============================================
# SYNC TO ICEBERG DoFn (NEW)
# ============================================
class SyncToIcebergDoFn(beam.DoFn):
    """
    Sync data from Native CDC table to Iceberg Historical table.
    Uses MERGE to upsert only changed records.
    
    Triggered by window closing (e.g., every 5 minutes).
    """
    
    def __init__(self, project_id: str, native_table: str, iceberg_table: str, lookback_minutes: int = 30):
        self.project_id = project_id
        self.native_table = native_table
        self.iceberg_table = iceberg_table
        self.lookback_minutes = lookback_minutes
        self._client = None
    
    def setup(self):
        """Initialize BigQuery client once per worker."""
        from google.cloud import bigquery
        self._client = bigquery.Client(project=self.project_id)
        logging.info(f"SyncToIcebergDoFn initialized: {self.native_table} → {self.iceberg_table}")
    
    def process(self, trigger_element, window=beam.DoFn.WindowParam):
        """Execute MERGE query to sync data to Iceberg."""
        window_start = datetime.fromtimestamp(window.start.micros / 1e6, tz=timezone.utc)
        window_end = datetime.fromtimestamp(window.end.micros / 1e6, tz=timezone.utc)
        
        logging.info(f"SyncToIceberg triggered: window {window_start.isoformat()} - {window_end.isoformat()}")
        logging.info(f"SyncToIceberg trigger_element: {trigger_element}")
        
        # MERGE query: Get latest version per memberId, upsert to Iceberg
        merge_query = f"""
        MERGE `{self.iceberg_table}` AS T
        USING (
            -- Get latest version of each member updated in lookback window
            SELECT * EXCEPT(rn)
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY memberId 
                        ORDER BY updated_date DESC
                    ) AS rn
                FROM `{self.native_table}`
                WHERE COALESCE(updated_date,CURRENT_TIMESTAMP()) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {self.lookback_minutes} MINUTE)
            )
            WHERE rn = 1
        ) AS S
        ON T.memberId = S.memberId
        
        -- Update if source is newer
        WHEN MATCHED AND S.updated_date > T.updated_date THEN
            UPDATE SET
                accountId = S.accountId,
                dateOfBirth = S.dateOfBirth,
                gender = S.gender,
                hasEmail = S.hasEmail,
                hasMobile = S.hasMobile,
                languagePrefer = S.languagePrefer,
                nationalityId = S.nationalityId,
                profileId = S.profileId,
                updated_date = S.updated_date
        
        -- Insert new records
        WHEN NOT MATCHED THEN
            INSERT (accountId, dateOfBirth, gender, hasEmail, hasMobile, 
                    languagePrefer, memberId, nationalityId, profileId, updated_date)
            VALUES (S.accountId, S.dateOfBirth, S.gender, S.hasEmail, S.hasMobile,
                    S.languagePrefer, S.memberId, S.nationalityId, S.profileId, S.updated_date)
        """
        
        try:
            job = self._client.query(merge_query)
            result = job.result()  # Wait for completion
            logging.info(f"SyncToIceberg result: {result}")
            
            rows_affected = job.num_dml_affected_rows or 0
            bytes_processed = job.total_bytes_processed or 0
            slot_ms = job.slot_millis or 0
            
            logging.info(
                f"SyncToIceberg SUCCESS: "
                f"window={window_end.isoformat()}, "
                f"rows_affected={rows_affected}, "
                f"bytes={bytes_processed / (1024*1024):.2f}MB, "
                f"slot_ms={slot_ms}"
            )
            
            yield {
                'window_end': window_end.isoformat(),
                'rows_affected': rows_affected,
                'bytes_processed_mb': round(bytes_processed / (1024*1024), 2),
                'slot_ms': slot_ms,
                'status': 'success'
            }
            
        except Exception as e:
            logging.error(f"SyncToIceberg FAILED: {e}")
            # Don't fail pipeline, just log error
            yield {
                'window_end': window_end.isoformat(),
                'status': 'failed',
                'error': str(e)
            }


# ============================================
# EXISTING DoFn CLASSES (unchanged)
# ============================================
class AddWindowInfoFn(beam.DoFn):
    def process(self, element, window=beam.DoFn.WindowParam):
        # Thai timezone
        tz_bangkok = timezone(timedelta(hours=7))
        window_end = datetime.fromtimestamp(
            window.end.micros / 10**6, 
            tz=timezone.utc
        ).astimezone(tz_bangkok)
        
        # สร้าง path
        path = window_end.strftime('par_month=%m/par_day=%d/par_hour=%H/run_dt=%Y%m%d%H')
        logging.info(f"records path: {path}")
        
        yield {
            **element,
            '_window_path': path,
            '_window_timestamp': window_end
        }

# DoFn สำหรับเขียน Parquet
class WriteParquetByWindowFn(beam.DoFn):
    def __init__(self, base_path, schema):
        self.base_path = base_path
        self.schema = schema
    
    def process(self, group):
        logging.info(f"Init class WriteParquetByWindowFn(beam.DoFn)")
        window_path, records = group
        
        # สร้าง full path
        output_path = f"{self.base_path}/{window_path}/ms-member.parquet"
        logging.info(f"Output path: {output_path}")
        logging.info(f"records path: {records}")
        
        # Convert to pandas และเขียน parquet
        df = pd.DataFrame(list(records))

        # Convert date columns
        date_columns = ['birth_date', 'consent_date', 'created_date', 'register_date', 
                        'employee_join_date', 'employee_resign_date', 'passport_exp', 'updated_date']
        
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date


        df.drop(columns=['_window_path', '_window_timestamp'], inplace=True, errors='ignore')
        
        # เขียนไป S3 ผ่าน pyarrow
        # import pyarrow.parquet as pq
        # import pyarrow as pa
        table = pa.Table.from_pandas(df, schema=self.schema)
        
        # ใช้ s3fs สำหรับเขียนไป S3
        import s3fs
        fs = s3fs.S3FileSystem()
        
        with fs.open(output_path, 'wb') as f:
            # pq.write_table(table, f)
            pq.write_table(
                table, 
                f,
                compression='snappy',  # ← เพิ่มบรรทัดนี้
                use_dictionary=True    # ← compression ดีขึ้น
            )

        yield f"Written {len(records)} records to {output_path}"


class MappingRefreshDoFn(DoFn):
    """Refresh mapping table every hour"""
    
    def __init__(self, mapping_table):
        self.mapping_table = mapping_table
        logging.info(f"Init MappingRefreshDoFn")
        
    def process(self, element):
        from google.cloud import bigquery
        try:
            client = bigquery.Client(project=PROJECT_ID)
            logging.info(f"Init bigquery Client")
            
            # Query mapping table
            query = f"""
            SELECT * EXCEPT(row_num) FROM (
                SELECT 
                    reconcile_column_name,
                    mapping_column_name,
                    reconcile_retrieved,
                    reconcile_confirmed,
                    table_name,
                    ROW_NUMBER() OVER (PARTITION BY reconcile_column_name ORDER BY updated_date DESC) AS row_num
                FROM `{self.mapping_table}`
            )
            WHERE row_num = 1
            """
            
            try:
                results = client.query(query).result()
            except Exception as e:
                logging.error(f"Failed to query mapping table {self.mapping_table}: {e}")
                return
            # mapping_dict: Dict[str, Any] = {}
            # schemas_dict: List[str] = []
            mapping_dict = {}
            schemas_dict = []
            for row in results:
                org_name = row['reconcile_column_name']
                
                # mapping_dict[org_name] = new_name.split('.')[-1]  # Use last part for flat mapping
                if row['reconcile_retrieved'] == True:
                    new_name = row['mapping_column_name'].split('.')[-1]   # Use last part only
                    # mapping_dict[org_name] = new_name
                    # # mapping_dict : {'is_mobile': {'profile':{'consent':'has_mobile'}} }
                    # # message structure : {'profile':{'consent':{'has_mobile':'Y'}}}
                    if row['table_name'] not in mapping_dict:
                        mapping_dict[row['table_name']] = {'gcp':{},'aws':{}}
                    mapping_dict[row['table_name']]['gcp'][new_name] = row['mapping_column_name']
                    mapping_dict[row['table_name']]['aws'][org_name] = row['mapping_column_name']
                
                # mapping_dict[org_name][]
                schemas_dict.append(org_name)

            
            logging.info(f"Refreshed mapping with {len(mapping_dict)} entries")
            logging.info(f"Refreshed mapping with {mapping_dict}")
            logging.info(f"Refreshed schemas_dict with {schemas_dict}")
        except Exception as exc:
            # Log exception and continue with an empty mapping to avoid crashes
            logging.error(f"Error refreshing mapping: {exc}")

        yield {
            'mapping_dict': mapping_dict,
            'schemas_dict': schemas_dict
        }


class ExtractPersonasDoFn(DoFn):
    """
    Extract personasId from PubSub message
    (Decodes Avro binary message)
    """

    def setup(self):
        """ตรวจสอบว่า Schema ถูก Parse สำเร็จตอนเริ่มต้น"""

    def process(self, element):
        # 'element' คือ BINARY AVRO BYTES (ไม่ใช่ JSON string)
        try:
            json_reader = json.loads(element.decode('utf-8'))
            logging.info(f"Received JSON message: {json_reader}")
            payload = json_reader.get('payload')
            
            if payload:
                personas_id = payload.get('personaId')
                if personas_id:
                    yield {'personas_id': personas_id} 
                    logging.info(f"Extracted personasId: {personas_id}")
                else:
                    logging.warning(f"No personasId in Avro payload: {json_reader}")
            else:
                logging.warning(f"No payload in Avro message: {json_reader}")

        except Exception as e:
            logging.error(f"Error parsing Avro message: {e} | Data (first 50 bytes): {element[:500]}...")
            
class FetchFromBigtableDoFn(DoFn):
    """Fetch data from BigTable using personasId"""
    
    def __init__(self, instance_id, table_id, parent_field=['profiles']):
        self.instance_id = instance_id
        self.table_id = table_id
        self.parent_field = parent_field
        self._client = None
        self._table = None
        self._instance = None

    def setup(self):
        """Initializes Bigtable Client once per worker."""
        try:

            self._client = bigtable.Client(project=PROJECT_ID)
            self._instance = self._client.instance(self.instance_id)
            self._table = self._instance.table(self.table_id)
            logging.info("Bigtable client and table initialized.")
        except Exception as e:
            logging.error(f"Failed to initialize Bigtable client: {e}")
            self._client = None
            self._table = None


    def process(self, element):
        if not self._table:
            logging.error("Bigtable table not available. Skipping record.")
            return

        try:
            personas_id = element.get('personas_id')
            if not personas_id:
                logging.warning("Missing personas_id in element.")
                return
            
            logging.info(f"personas_id : {personas_id}")
            row_key = personas_id.encode()
            logging.info(f"row_key : {row_key}")
            row = self._table.read_row(personas_id)
            
            if row:
                # profile_data = self.transform_message(row.cells, mapping_dict)
                # Extract profile data from BigTable
                logging.info(f"Existing ROW in BT.")
                logging.info(f"BT row : {row}")
                logging.info(f"BT row.cells : {row.cells}")
                logging.info(f"BT row.cells.items() : {row.cells.items()}")
                logging.info(f"BT row.cells : {row.cells['profiles']}")
                # logging.info(f"BT row.cells.items() : {row.cells.items()['profiles']}")
                # family_data = row.cells.get(self.parent_field, {})
                result = {
                    'personas_id': personas_id
                }
                # for fam_col in self.parent_field:
                #     if fam_col in row.cells:  # ✅ ตรวจสอบว่ามี column family นี้หรือไม่
                #         result[fam_col] = row.cells[fam_col] # ✅ ใช้ row.cells โดยตรง


                # Extract data from each selected family column
                for family_name in self.parent_field:
                    if family_name in row.cells:
                        logging.info(f"Processing family column: {family_name}")
                        
                        # Extract all columns within this family
                        family_dict = {}
                        family_cells = row.cells[family_name]
                        
                        # Check if this family has only one column named 'value' with JSON
                        if len(family_cells) == 1 and b'value' in family_cells:
                            # Single 'value' column case - parse JSON
                            cells = family_cells[b'value']
                            if cells:
                                latest_cell = cells[0]
                                try:
                                    # Decode the value
                                    cell_value = latest_cell.value.decode('utf-8') if isinstance(latest_cell.value, bytes) else latest_cell.value
                                    
                                    # Try to parse as JSON
                                    if isinstance(cell_value, str) and (cell_value.startswith('{') or cell_value.startswith('[')):
                                        parsed_value = json.loads(cell_value)
                                        # If it's a dict, use it directly as the family data
                                        if isinstance(parsed_value, dict):
                                            result[family_name] = parsed_value
                                            logging.info(f"Parsed JSON from {family_name}.value: {len(parsed_value)} fields")
                                        else:
                                            # If it's not a dict (maybe a list), wrap it
                                            result[family_name] = {'data': parsed_value}
                                    else:
                                        # Not JSON, store as is
                                        result[family_name] = {'value': cell_value}
                                        
                                except json.JSONDecodeError as e:
                                    logging.warning(f"Failed to parse JSON in {family_name}.value: {e}")
                                    # If JSON parsing fails, store as string
                                    result[family_name] = {'value': cell_value}
                                except UnicodeDecodeError:
                                    # If decode fails, store as hex
                                    cell_value = latest_cell.value.hex() if isinstance(latest_cell.value, bytes) else str(latest_cell.value)
                                    result[family_name] = {'value': cell_value}
                        else:
                            # Multiple columns case - extract all columns
                            family_dict = {}
                            for column_qualifier, cells in family_cells.items():
                                if cells:
                                    latest_cell = cells[0]
                                    column_name = column_qualifier.decode('utf-8') if isinstance(column_qualifier, bytes) else column_qualifier
                                    
                                    try:
                                        cell_value = latest_cell.value.decode('utf-8') if isinstance(latest_cell.value, bytes) else latest_cell.value
                                        
                                        # Try to parse as JSON if it looks like JSON
                                        if isinstance(cell_value, str) and (cell_value.startswith('{') or cell_value.startswith('[')):
                                            try:
                                                cell_value = json.loads(cell_value)
                                            except json.JSONDecodeError:
                                                pass  # Keep as string
                                        
                                        family_dict[column_name] = cell_value
                                        
                                    except UnicodeDecodeError:
                                        family_dict[column_name] = latest_cell.value.hex() if isinstance(latest_cell.value, bytes) else str(latest_cell.value)
                            
                            result[family_name] = family_dict
                            logging.info(f"Extracted {len(family_dict)} columns from family '{family_name}'")
                    else:
                        logging.warning(f"Family column '{family_name}' not found in row")
                        result[family_name] = {}
                
                
                # Yield the properly structured result
                logging.info(f"Fetched data for personas_id {personas_id}: {result}")
                yield result
            else:
                logging.warning(f"Row not found for personas_id: {personas_id}")

        except Exception as e:
            # Log error แต่ไม่ให้ pipeline fail
            logging.error(f"Error processing personas_id {element.get('personas_id')}: {str(e)}")
            # อาจจะ yield error record สำหรับ dead letter queue
            yield {
                'personas_id': element.get('personas_id'),
                'error': str(e),
                'error_type': 'processing_error'
            }


class FilterEmptyMemberIdDoFn(DoFn):
    """
    Filter out records that don't have memberId
    This helps reduce noise in the pipeline and prevents errors in downstream processing
    """
    
    def process(self, element):
        """Check if element has memberId in profiles"""
        try:
            # Check if profiles exists and has memberId
            profiles = element.get('profiles', {})
            member_id = profiles.get('memberId')
            
            if member_id and str(member_id).strip():
                # Valid memberId found
                logging.debug(f"Valid record with memberId: {member_id}")
                yield element
            else:
                # No memberId or empty - filter out
                personas_id = element.get('personas_id', 'unknown')
                logging.warning(f"Filtering out record without memberId. personas_id: {personas_id}")
                
        except Exception as e:
            logging.error(f"Error in FilterEmptyMemberIdDoFn: {str(e)}", exc_info=True)

class TransformSchemasDoFn(DoFn):
    """Transform data according to mapping dictionary"""
    def get_nested_value(self , data, path):
        """ดึงค่าจาก nested dict ด้วย dot notation path"""
        try:
            return reduce(operator.getitem, path.split('.'), data)
        except (KeyError, TypeError):
            return None

    def transform_message(self , message_dict, mapping_dict,target='gcp',table_name='ms_member'):
        """แปลง message ตาม mapping"""
        result = {}
        # mapping_dict[row['table_name']]['gcp'][new_name] = row['mapping_column_name']
        # mapping_dict[row['table_name']]['aws'][org_name] = row['mapping_column_name']
        logging.info(f"TransformSchemasDoFn.transform_message called with org mapping_dict: {mapping_dict}")
        mapping_dict = mapping_dict.get(table_name,{}).get(target,{})
        logging.info(f"TransformSchemasDoFn.transform_message called with specific mapping_dict: {mapping_dict}")
        # logging.info(f"TransformSchemasDoFn.transform_message called with mapping_dict: {mapping_dict}")
        for new_key, path in mapping_dict.items():
            logging.info(f"TransformSchemasDoFn.transform_message called mapping_dict.items with new_key :{new_key} , path: {path}")
            value = self.get_nested_value(message_dict, path)
            if value is not None:
                result[new_key] = value
            else:
                result[new_key] = None
        return result


    def process(self, element, mapping_info, table_name='ms_personas'):
        logging.info(f"TransformSchemasDoFn.process called with element: {element}")
        mapping_dict = mapping_info.get('mapping_dict', {})
        logging.info(f"TransformSchemasDoFn.process called with mapping_dict: {mapping_dict}")
        aws_output = self.transform_message(element, mapping_dict=mapping_dict,target='aws',table_name=table_name)
        gcp_output = self.transform_message(element, mapping_dict=mapping_dict,target='gcp',table_name=table_name)

        logging.info(f"aws_output : {aws_output}")
        logging.info(f"gcp_output : {gcp_output}")
        yield beam.pvalue.TaggedOutput('aws', aws_output)
        yield beam.pvalue.TaggedOutput('gcp', gcp_output)

class FullfillSchemasDoFn(DoFn):
    """Full fill schemas data according to mapping dictionary"""

    def process(self, element, mapping_info):
        logging.info(f"FullfillSchemasDoFn.process called with element: {element}")
        schemas_dict = mapping_info.get('schemas_dict', [])
        new_dict = {}
        for i in schemas_dict:
            new_dict[i] = element.get(i, None)
        logging.info(f"Refreshed element to : {new_dict} ")
        yield new_dict


class WriteToBigLakeDoFn(DoFn):
    """Custom write to BigLake with partitioning"""
    
    def __init__(self, table_name):
        self.table_name = table_name
        
    def process(self, element):
        # Prepare for BigLake write with proper data types
        output = {}
        for key, value in element.items():
            # Convert None to appropriate BigQuery NULL
            if value is None:
                output[key] = None
            elif isinstance(value, dict):
                output[key] = json.dumps(value)
            else:
                output[key] = value
        
        # Add timestamp for partitioning
        # output['_insert_timestamp'] = datetime.utcnow().isoformat()
        
        yield output

# ----------------------------------------
# 💡 แนะนำให้เปลี่ยนชื่อ DoFn นี้เป็น PrepareForBigQueryFn
# ----------------------------------------
# class PrepareForBigQueryFn(DoFn):
#     """
#     Flatten the 'profiles' dict from Bigtable 
#     to match the BigQuery schema.
#     """
    
#     def __init__(self, bq_schema_fields):
#         # รับรายชื่อ field มาจาก schema
#         self.schema_fields = [f['name'] for f in bq_schema_fields]
        
#     def process(self, element):
#         # element คือ: {'personas_id': '...', 'profiles': {'accountId': 'a', ...}}
        
#         profile_data = element.get('profiles')
        
#         # if not profile_data or not isinstance(profile_data, dict):
#         #     logging.warning(f"Skipping record, missing or invalid 'profiles' data for {element.get('personas_id')}")
#         #     return

#         # สร้าง record ใหม่ โดยดึงค่าจาก 'profiles'
#         # output_record = {}
#         # for field in self.schema_fields:
#         #     # ดึงค่าจาก profile_data, ถ้าไม่มีให้เป็น None
#         #     output_record[field] = profile_data.get(field)
            
#         # ⚠️ ตรวจสอบ Primary Key (memberId)
#         # ถ้าไม่มี memberId การ Upsert จะล้มเหลว
#         if not output_record.get('memberId'):
#             logging.warning(f"Skipping record, 'memberId' (PK) is missing in profiles: {profile_data}")
#             return
            
#         # Yield flat dict ที่พร้อมสำหรับ BQ
#         # {'accountId': 'a', 'memberId': 'b', ...}
#         yield output_record

class MapToCdcTableRow(beam.DoFn):
    """
    Format data for BigQuery CDC write using Storage Write API.
    
    Required schema for CDC:
    {
        "row_mutation_info": {"type": "UPSERT" | "DELETE"},
        "record": { actual data fields }
    }
    """
    def process(self, element):
        import time
        # Get CDC operation type
        cdc_type = element.get('cdc_type', 'UPSERT')
        is_delete = element.get('is_delete', False)
        
        # Determine mutation type
        if is_delete:
            mutation_type = 'DELETE'
        elif cdc_type == 'DELETE':
            mutation_type = 'DELETE'
        else:
            mutation_type = 'UPSERT'  # INSERT or UPDATE both use UPSERT
        
        # Generate sequence number (timestamp-based for ordering)
        # Use updated_date if available, otherwise current time
        if element.get('updated_date'):
            if isinstance(element['updated_date'], datetime):
                seq_num = str(int(element['updated_date'].timestamp() * 1000000))
            else:
                seq_num = str(int(time.time() * 1000000))
        else:
            seq_num = str(int(time.time() * 1000000))
        
        # Clean up internal fields from record
        record = dict(element)
        record.pop('cdc_type', None)
        record.pop('is_delete', None)
        record.pop('_CHANGE_TYPE', None)
        record.pop('_CHANGE_SEQUENCE_NUMBER', None)
        
        logging.info(f"MapToCdcTableRow record: {record}")
        # Convert dateOfBirth to proper format if exists
        if record.get('dateOfBirth'):
            try:
                if isinstance(record['dateOfBirth'], str):
                    dt = datetime.strptime(record['dateOfBirth'], '%Y-%m-%d').date()
                    record['dateOfBirth'] = dt.isoformat()
            except:
                pass
        
        # Format for CDC API: must have "row_mutation_info" and "record" fields
        cdc_row = {
            'row_mutation_info': {
                'mutation_type': mutation_type,  # NOT "type"!
                'change_sequence_number': seq_num  # REQUIRED!
            },
            'record': record
        }
        
        logging.info(f"MapToCdcTableRow : {cdc_row}")
        yield cdc_row

# ใน Pipeline:
def format_for_cdc(row):
    # Example: assume 'id', 'data', 'timestamp' are in the input 'row'
    # and a field 'is_delete' indicates deletion
    
    # Add _CHANGE_TYPE ('UPSERT' or 'DELETE')
    row['_CHANGE_TYPE'] = 'DELETE' if row.get('is_delete') else 'UPSERT'
    
    # Add _CHANGE_SEQUENCE_NUMBER (timestamp helps BigQuery order changes correctly)
    # Using a high-resolution timestamp or sequence ID is crucial for correctness
    row['_CHANGE_SEQUENCE_NUMBER'] = str(row['timestamp'])
    logging.info(f"format_for_cdc : {row}")
    return row

def create_pipeline():
    """Create the main pipeline"""
    
    pipeline_options = PipelineOptions(
        streaming=True,
        runner='DataflowRunner',
        project=PROJECT_ID,
        job_name='ms-member-realtime-pipeline',
        temp_location='t1-insight-audit-bucket/audit_log/dataflow/temp',
        region='asia-southeast1',
        autoscaling_algorithm='THROUGHPUT_BASED',
        max_num_workers=10,
        experiments=['use_runner_v2']
    )
    
    pipeline = beam.Pipeline(options=pipeline_options)
    # with beam.Pipeline(options=pipeline_options) as pipeline:
        
    # Step 0: Cache mapping table (refresh every hour)
    mapping_refresh = (
        pipeline
        | 'PeriodicTrigger' >> PeriodicImpulse(
            start_timestamp=0,
            # stop_timestamp=float('inf'),
            # fire_interval=3600  # 1 hour in seconds
            fire_interval=60
        )
        | 'RefreshMapping' >> ParDo(MappingRefreshDoFn(MAPPING_TABLE))
        | 'WindowMapping' >> beam.WindowInto(
            window.GlobalWindows(),
            trigger=trigger.Repeatedly(trigger.AfterCount(1)),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
    )
    
    # Step 1-2: Consume from PubSub and extract personasId
    messages = (
        pipeline
        | 'ReadFromPubSub' >> ReadFromPubSub(subscription=SUBSCRIPTION_NAME)
        | 'ExtractPersonasId' >> ParDo(ExtractPersonasDoFn())
    )
    
    # Step 3: Fetch from BigTable
    bigtable_data = (
        messages
        | 'FetchFromBigTable' >> ParDo(
            FetchFromBigtableDoFn(BT_INSTANCE, BT_TABLE,parent_field=['profiles'])
        )
    )
    
    # Step 4.5: Filter records with missing memberId
    filtered_data = (
        bigtable_data
        | 'FilterEmptyMemberId' >> ParDo(FilterEmptyMemberIdDoFn())
    )

    # Step 5: Transform schemas
    ms_personas_transformed = (
        filtered_data
        | 'TransformSchemas' >> ParDo(
            TransformSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
            # family_field='profiles',
            table_name='ms_member'
            # pk_key='personas_id'
        ).with_outputs('aws', 'gcp')
    )
    
    # FullfillSchemasDoFn
    ms_personas_full_aws = (
        ms_personas_transformed.aws
        | 'FullfillSchemas' >> ParDo(
            FullfillSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
        )
    )
    
    # ใน Pipeline:
    gcp_data = ms_personas_transformed.gcp
    gcp_cdc_data = gcp_data | 'FormatForCDC' >> beam.Map(format_for_cdc)

    (
        gcp_cdc_data 
        # ... Deduplicate / GetLatestChange ...
        | "MapToCDCFormat" >> beam.ParDo(MapToCdcTableRow())
        | 'CDCWriteToNativeTable' >> WriteToBigQuery(
            table=NATIVE_TABLE,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            # method='STORAGE_WRITE_API',
            method=WriteToBigQuery.Method.STORAGE_WRITE_API,
            schema=MS_PERSONAS_CDC_SCHEMA,
            # schema='accountId:STRING,dateOfBirth:STRING,gender:STRING,hasEmail:STRING,hasMobile:STRING,languagePrefer:STRING,memberId:STRING,nationalityId:STRING,profileId:STRING,updated_date:STRING',
            # schema = 'accountId:STRING,dateOfBirth:DATE,gender:STRING,hasEmail:STRING,hasMobile:STRING,languagePrefer:STRING,memberId:STRING,nationalityId:STRING,profileId:STRING,updated_date:TIMESTAMP',
            # schema = output_schema,
            use_cdc_writes=True,  
            primary_key=['memberId'],
            triggering_frequency=5,
            # triggering_frequency=timedelta(seconds=5),
            num_storage_api_streams=5,
            use_at_least_once=True,
        )
    )
    # ========================================
    # WRITE 2: Sync to Iceberg (every 5 min)
    # ========================================
    (
        gcp_cdc_data
        # Window: Group into 5-minute windows
        | 'SyncWindow' >> beam.WindowInto(
            window.FixedWindows(SYNC_WINDOW_SECONDS),
            trigger=trigger.AfterWatermark(),
            accumulation_mode=trigger.AccumulationMode.DISCARDING
        )
        # Trigger sync when window closes
        | 'CountForSync' >> beam.CombineGlobally(
            beam.combiners.CountCombineFn()
        ).without_defaults()
        # Execute MERGE to Iceberg
        | 'SyncToIceberg' >> beam.ParDo(
            SyncToIcebergDoFn(
                project_id=PROJECT_ID,
                native_table=NATIVE_TABLE,
                iceberg_table=ICEBERG_TABLE,
                lookback_minutes=SYNC_LOOKBACK_MINUTES
            )
        )
        # Log sync results
        | 'LogSyncResult' >> beam.Map(
            lambda x: logging.info(f"Iceberg sync result: {x}")
        )
    )

    # (
    #     gcp_data
    #     # 💡 1. ใช้ DoFn ใหม่ที่เราสร้าง
    #     # | 'PrepareForBigQuery' >> ParDo(
    #     #     PrepareForBigQueryFn(
    #     #         bq_schema_fields=MS_PERSONAS_BIGQUERY_SCHEMA['fields']
    #     #     )
    #     # )
    #     | 'CDCWriteToBigLakeIceberg' >> WriteToBigQuery(
    #         table=BIGLAKE_TABLE,
    #         method=WriteToBigQuery.Method.STORAGE_WRITE_API,
    #         write_disposition=BigQueryDisposition.WRITE_APPEND,
    #         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
    #         # schema=MS_PERSONAS_BIGQUERY_SCHEMA,
    #         schema='accountId:STRING,dateOfBirth:STRING,gender:STRING,hasEmail:STRING,hasMobile:STRING,languagePrefer:STRING,memberId:STRING,nationalityId:STRING,profileId:STRING,updated_date:STRING',
    #         # schema='SCHEMA_AUTODETECT',
    #         # method='STORAGE_WRITE_API',
    #         # เปิดใช้งาน CDC และระบุ Primary Key
    #         use_cdc_writes=True,  
    #         primary_key=['memberId'],   # ← ต้องระบุ PK
    #         # สำหรับ Streaming Pipeline
    #         triggering_frequency=timedelta(seconds=5), # เช่น commit ทุก 5 วินาที
    #         num_storage_api_streams=5, # จำนวน Stream ที่ใช้เขียน
            
    #         # with_auto_sharding=True,
    #         # ignore_unknown_columns=True,
    #         # use_at_least_once=True,  # ← Enable CDC/UPSERT for BigLake Iceberg

    #     )
    # )

    # Step 6.2: Write to S3 (AWS)
    # aws_data = transformed.aws
    # (
    #     aws_data
    #     | 'WriteToS3' >> ParDo(
    #         WriteToS3DoFn(
    #             s3_bucket='your-s3-bucket',
    #             s3_prefix='ms-personas-cdc'
    #         )
    #     )
    # )
    # aws_data = transformed.aws
    # (
    #     aws_data
    #     # 5.2.1. Windowing: Batch data into 5-minute fixed windows (Near-Real-time batching)
    #     | 'ApplyFixedWindow' >> beam.WindowInto(
    #         window.FixedWindows(300) # 5 minutes = 300 seconds
    #     )
    #     # 5.2.2. Write to Parquet using Custom Filename Policy
    #     | 'WriteToParquetS3' >> WriteToParquet(
    #         file_path_prefix=HourlyPartitioningPolicy(
    #             base_path=S3_PARQUET_BUCKET,
    #             prefix='ms-pesonas'
    #         ),
    #         # file_path_prefix=S3_PARQUET_BUCKET, # ⚠️ S3 URI
    #         schema=MS_PERSONAS_PARQUET_SCHEMA,
    #         file_name_suffix=".parquet",
    #         # ตั้งเป็น 1 เพื่อให้ได้ไฟล์ต่อ partition น้อยที่สุด
    #         num_shards=1 
    #     )
    # )
    ms_personas_aws_data = ms_personas_full_aws
    (
        ms_personas_aws_data
        # Window 5 นาที (เหมือนเดิม)
        | 'ApplyFixedWindow' >> beam.WindowInto(
            window.FixedWindows(300)
        )
        
        # เพิ่ม window info เข้าไปใน record
        | 'AddWindowInfo' >> beam.ParDo(AddWindowInfoFn())
        
        # Group by window timestamp เพื่อเขียนแยก path
        | 'GroupByWindow' >> beam.GroupBy(lambda x: x['_window_path'])
        
        # เขียน Parquet แยกตาม window
        | 'WriteParquetPerWindow' >> beam.ParDo(
            WriteParquetByWindowFn(
                base_path=S3_PARQUET_BUCKET,
                schema=MS_PERSONAS_PARQUET_SCHEMA
            )
        )
    )
    # aws_data = transformed.aws
    # (
    #     aws_data
    #     | 'Window5Min' >> beam.WindowInto(window.FixedWindows(300))
    #     | 'WriteToParquetS3' >> WriteToParquet(
    #         file_path_prefix=f"{S3_PARQUET_BUCKET}/ms-personas",
    #         schema=MS_PERSONAS_PARQUET_SCHEMA,
    #         file_name_suffix=".snappy.parquet",
    #         codec='snappy',  # ← Native snappy
    #         num_shards=1     # 1 file/window
    #     )
    # )

    return pipeline

# Infrastructure setup script
# def setup_infrastructure():
#     """Setup required infrastructure"""
    
#     from google.cloud import bigquery
#     from google.cloud import pubsub_v1
    
#     # Create BigLake table
#     bq_client = bigquery.Client()
    
#     # BigLake external table DDL
#     biglake_ddl = """
#     CREATE OR REPLACE EXTERNAL TABLE `{}`
#     OPTIONS (
#         format = 'PARQUET',
#         uris = ['gs://your-bucket/biglake-data/*'],
#         max_staleness = INTERVAL 1 HOUR
#     )
#     """.format(BIGLAKE_TABLE)
    
#     # Create Pub/Sub subscription
#     publisher = pubsub_v1.PublisherClient()
#     subscriber = pubsub_v1.SubscriberClient()
    
#     topic_path = publisher.topic_path(PROJECT_ID, 'ms-member-realtime-topic')
#     subscription_path = subscriber.subscription_path(PROJECT_ID, 'ms-member-realtime-sub')
    
#     try:
#         subscriber.create_subscription(
#             request={
#                 "name": subscription_path,
#                 "topic": topic_path,
#                 "ack_deadline_seconds": 60,
#                 "message_retention_duration": {"seconds": 86400}  # 1 day
#             }
#         )
#         print(f"Subscription created: {subscription_path}")
#     except Exception as e:
#         print(f"Subscription might already exist: {e}")

if __name__ == "__main__":
    # Setup infrastructure first (run once)
    # setup_infrastructure()
    
    # Create and run pipeline
    pipeline = create_pipeline()
    result = pipeline.run()
    
    # if isinstance(pipeline.options, PipelineOptions):
    #     standard_options = pipeline.options.view_as(StandardOptions)
    #     if standard_options.streaming:
    #         print("Pipeline is running in streaming mode. Press Ctrl+C to stop.")
    #         result.wait_until_finish()