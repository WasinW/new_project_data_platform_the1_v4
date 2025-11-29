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
from datetime import datetime, timedelta, timezone
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
import s3fs
from google.cloud import bigquery

# Configuration
PROJECT_ID = "the1-insight-stg"
BT_PROJECT_ID = "the1-insight-stg"
SUBSCRIPTION_NAME = "projects/the1-insight-stg/subscriptions/ms-personas-datapipeline-dataflow-subscription"
MAPPING_TABLE = f"{PROJECT_ID}.insight.mapping_reconcile"
BIGLAKE_TABLE = f"{PROJECT_ID}.insight.ms_personas"
BT_INSTANCE = "t1-insight-bt"
BT_TABLE = "personas"

# --- Avro Schema Definition (สำหรับ Message ที่เข้ามา) ---
# นี่คือ Schema ที่คุณให้ผมมาในแชท
# PERSONAS_AVRO_SCHEMA_DEFINITION = {
#   "name": "personas_collector",
#   "type": "record",
#   "namespace": "th.co.the1.insight.personas_collector.avro",
#   "fields": [
#     {
#       "name": "type",
#       "type": {
#         "type": "enum",
#         "name": "type",
#         "symbols": [
#           "UPSERT",
#           "DELETE"
#         ]
#       }
#     },
#     {
#       "name": "payload",
#       "type": {
#         "type": "record",
#         "name": "Payload",
#         "fields": [
#           {
#             "name": "personaId",
#             "type": "string"
#           },
#           {
#             "name": "personaLegacyId",
#             "type": "string"
#           },
#           {
#             "name": "profiles",
#             "type": [
#               "null",
#               {
#                 "type": "record",
#                 "name": "Profile",
#                 "fields": [
#                   {"name": "profileId","type": ["null","string"],"default": None},
#                   {"name": "accountId","type": ["null","string"],"default": None},
#                   {"name": "memberId","type": ["null","string"],"default": None},
#                   {"name": "dateOfBirth","type": ["null","string"],"default": None},
#                   {"name": "gender","type": ["null","string"],"default": None},
#                   {"name": "nationalityId","type": ["null","string"],"default": None},
#                   {"name": "hasMobile","type": ["null","boolean"],"default": None},
#                   {"name": "hasEmail","type": ["null","boolean"],"default": None},
#                   {"name": "languagePrefer","type": ["null","string"],"default": None}
#                 ]
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "members",
#             "type": [
#               "null",
#               {
#                 "type": "record",
#                 "name": "Members",
#                 "fields": [
#                   {
#                     "name": "tiers",
#                     "type": [
#                       "null",
#                       {
#                         "type": "map",
#                         "values": {
#                           "type": "record",
#                           "name": "Tiers",
#                           "fields": [
#                             {"name": "code","type": "string"},
#                             {"name": "expiryDate","type": "long","logicalType": "timestamp-millis"},
#                             {"name": "startDate","type": ["null","long"],"logicalType": "timestamp-millis","default": None}
#                           ]
#                         }
#                       }
#                     ],
#                     "default": None
#                   }
#                 ]
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "purchases",
#             "type": [
#               "null",
#               {
#                 "type": "map",
#                 "values": {
#                   "type": "record",
#                   "name": "Purchases",
#                   "fields": [
#                     {"name": "lastPurchasedTime","type": "long","logicalType": "timestamp-millis"},
#                     {"name": "lastPurchasedStoreCode","type": ["null","string"],"default": None}
#                   ]
#                 }
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "status",
#             "type": [
#               "null",
#               {
#                 "type": "map",
#                 "values": {
#                   "type": "boolean",
#                   "name": "isActive"
#                 }
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "rankings",
#             "type": [
#               "null",
#               {
#                 "type": "map",
#                 "values": {
#                   "type": "map",
#                   "values": "double"
#                 }
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "the1app",
#             "type": [
#               "null",
#               {
#                 "type": "record",
#                 "name": "The1App",
#                 "fields": [
#                   {"name": "lastActive","type": "long","logicalType": "timestamp-millis"}
#                 ]
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "wealth",
#             "type": [
#               "null",
#               {
#                 "type": "record",
#                 "name": "Wealth",
#                 "fields": [
#                   {"name": "value","type": ["null","string"],"default": None}
#                 ]
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "family",
#             "type": [
#               "null",
#               {
#                 "type": "record",
#                 "name": "Family",
#                 "fields": [
#                   {"name": "hasKids","type": ["null","boolean"],"default": None},
#                   {
#                     "name": "members",
#                     "type": [
#                       "null",
#                       {
#                         "type": "map",
#                         "values": {
#                           "type": "record",
#                           "name": "FamilyMembers",
#                           "fields": [
#                             {"name": "lifeStage","type": ["null","string"],"default": None}
#                           ]
#                         }
#                       }
#                     ],
#                     "default": None
#                   }
#                 ]
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "locations",
#             "type": [
#               "null",
#               {
#                 "type": "record",
#                 "name": "Locations",
#                 "fields": [
#                   {"name": "mostVisitedStoreLocation","type": ["null","string"],"default": None},
#                   {"name": "mostVisitedProvince","type": ["null","string"],"default": None},
#                   {"name": "mostVisitedSubregion","type": ["null","string"],"default": None},
#                   {"name": "lastVisitedStoreCode","type": ["null","string"],"default": None},
#                   {"name": "lastVisitedDate","type": ["null","long"],"logicalType": "timestamp-millis","default": None}
#                 ]
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "categories",
#             "type": [
#               "null",
#               {
#                 "type": "map",
#                 "values": {
#                   "type": "record",
#                   "name": "Categories",
#                   "fields": [
#                     {"name": "segment","type": ["null","string"],"default": None},
#                     {"name": "relevanceScore","type": ["null","double"],"default": None},
#                     {"name": "luxuryScore","type": ["null","double"],"default": None},
#                     {"name": "propensityScore","type": ["null","double"],"default": None}
#                   ]
#                 }
#               }
#             ],
#             "default": None
#           },
#           {
#             "name": "channels",
#             "type": [
#               "null",
#               {
#                 "type": "map",
#                 "values": {
#                   "type": "record",
#                   "name": "Channels",
#                   "fields": [
#                     {"name": "lastActive","type": ["null","long"],"logicalType": "timestamp-millis","default": None}
#                   ]
#                 }
#               }
#             ],
#             "default": None
#           }
#         ]
#       }
#     }
#   ]
# }

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


MS_PERSONAS_BIGQUERY_SCHEMA = {
    'fields': [
    {"name": "accountId","mode": "","type": "STRING"},
    {"name": "dateOfBirth","mode": "","type": "STRING"},
    {"name": "gender","mode": "","type": "STRING"},
    {"name": "hasEmail","mode": "","type": "STRING"},
    {"name": "hasMobile","mode": "","type": "STRING"},
    {"name": "languagePrefer","mode": "","type": "STRING"},
    {"name": "memberId","mode": "REQUIRED","type": "STRING","description": "Primary key","fields": []},
    {"name": "nationalityId","mode": "","type": "STRING"},
    {"name": "profileId","mode": "","type": "STRING"},
    {"name": "updated_date","mode": "","type": "STRING"},
    # 
    ]
}
# --- BQ Schema (String Format) ---
# นี่คือวิธีแก้ปัญหา "ValueError: Converting BigQuery type [DATE]"

# MS_PERSONAS_BIGQUERY_SCHEMA = (
#     "accountId:STRING, "
#     "dateOfBirth:STRING, "     # ⬅️ การระบุ "DATE" ใน string format นี้ จะทำงานได้
#     "gender:STRING, "
#     "hasEmail:STRING, "
#     "hasMobile:STRING, "
#     "languagePrefer:STRING, "
#     "memberId:STRING, "      # ⬅️ ไม่ต้องระบุ REQUIRED ที่นี่ 
#     "nationalityId:STRING, "
#     "profileId:STRING"
# )

# --- Custom Naming Policy สำหรับ Hourly Partitioning (ใช้ได้ทั้ง S3 และ GCS) ---

# class HourlyPartitioningPolicy(WindowedFilenamePolicy):
#     """
#     สร้าง Path ตาม Partition hourly:
#     .../par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH/
#     """
#     def __init__(self, base_path, prefix='data'):
#         # base_path คือ S3_PARQUET_BUCKET
#         self.base_path = base_path
#         self.prefix = prefix
#         # ใช้ ShardNameTemplate มาตรฐานสำหรับ Parquet (.parquet)
#         self.shard_template = FileNaming.default(
#             prefix='shard', shard_template='-SSSSS-of-NNNNN', extension='.parquet'
#         )

#     def get_filename(self, window, shard_id, num_shards, pane_info):
#         # 1. ใช้ End Time ของ Window ในการกำหนด Partition
#         # window.end เป็น Beam timestamp (Microseconds)
#         window_end_micros = window.end.micros
#         window_end = datetime.fromtimestamp(window_end_micros / 10**6) 

#         # 2. สร้าง Partition Folders: par_month=MM/par_day=DD/par_hour=HH
#         par_month = window_end.strftime('%m')
#         par_day = window_end.strftime('%d')
#         par_hour = window_end.strftime('%H')
        
#         # 3. สร้าง Sub-folder: run_dt=YYYYMMDDHH
#         run_dt = window_end.strftime('%Y%m%d%H')
        
#         partition_path = (
#             f"par_month={par_month}/"
#             f"par_day={par_day}/"
#             f"par_hour={par_hour}/"
#             f"run_dt={run_dt}"
#         )
        
#         # 4. สร้าง Filename 
#         file_name = self.shard_template.get_filename(shard_id, num_shards, pane_info)
        
#         # คืนค่าเป็น Full Path: base_path/par_month=.../run_dt=.../cdc-data-shard-00000-of-00001.parquet
#         return f"{self.base_path}/{partition_path}/{self.prefix}-{file_name}"
# Fixed version using FileNaming instead of WindowedFilenamePolicy
# class HourlyPartitioningPolicy(FileNaming):
#     """
#     Custom file naming for hourly partitioning in S3/GCS
#     Creates path: .../par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH/
#     """

#     def __init__(self, base_path, prefix='ms-member'):
#         # Don't call super().__init__() with parameters
#         super().__init__()
#         self.base_path = base_path
#         self.prefix = prefix
        
#     def __call__(self, window, pane, shard_index, total_shards, compression, destination):
#         """
#         REQUIRED: Main method called by WriteToParquet
#         Returns the full file path for the given shard
#         """
#         if window:
#             # Use window end time for partitioning
#             window_end_micros = window.end.micros
#             # Convert to Thai timezone
#             window_end_utc = datetime.fromtimestamp(window_end_micros / 10**6, tz=timezone.utc)
#             timestamp = window_end_utc.astimezone(TZ_BANGKOK)
#         else:
#             # No window, use current Thai time
#             timestamp = datetime.now(TZ_BANGKOK)
        
#         # Create partition path
#         par_month = timestamp.strftime('%m')
#         par_day = timestamp.strftime('%d')
#         par_hour = timestamp.strftime('%H')
#         run_dt = timestamp.strftime('%Y%m%d%H')
        
#         partition_path = (
#             f"par_month={par_month}/"
#             f"par_day={par_day}/"
#             f"par_hour={par_hour}/"
#             f"run_dt={run_dt}"
#         )
        
#         # Create filename with shard info
#         filename = f"{self.prefix}-{shard_index:05d}-of-{total_shards:05d}.parquet"
        
#         # Return full path
#         return f"{self.base_path}/{partition_path}/{filename}"
    
#     # These methods are still required but can be simple
#     def unwindowed_filename(self, shard=None, num_shards=None, window=None, pane=None):
#         """For backwards compatibility"""
#         timestamp = datetime.now(TZ_BANGKOK)
#         return self._create_filename(timestamp, shard, num_shards)
    
#     def windowed_filename(self, shard=None, num_shards=None, window=None, pane=None):
#         """For backwards compatibility"""
#         if window:
#             # Use window end time for partitioning
#             window_end_micros = window.end.micros
#             window_end_utc = datetime.fromtimestamp(window_end_micros / 10**6, tz=timezone.utc)
#             timestamp = window_end_utc.astimezone(TZ_BANGKOK)
#         else:
#             timestamp = datetime.now(TZ_BANGKOK)
            
#         return self._create_filename(timestamp, shard, num_shards)
    
#     def _create_filename(self, timestamp, shard, num_shards):
#         """Create the actual filename with partition structure."""
#         # Create partition folders: par_month=MM/par_day=DD/par_hour=HH
#         par_month = timestamp.strftime('%m')
#         par_day = timestamp.strftime('%d')
#         par_hour = timestamp.strftime('%H')
        
#         # Create sub-folder: run_dt=YYYYMMDDHH
#         run_dt = timestamp.strftime('%Y%m%d%H')
        
#         partition_path = (
#             f"par_month={par_month}/"
#             f"par_day={par_day}/"
#             f"par_hour={par_hour}/"
#             f"run_dt={run_dt}"
#         )
        
#         # Create shard suffix
#         if shard is not None and num_shards is not None:
#             shard_suffix = f"-{shard:05d}-of-{num_shards:05d}"
#         else:
#             shard_suffix = ""
        
#         # Return full path
#         return f"{self.base_path}/{partition_path}/{self.prefix_name}-data{shard_suffix}.parquet"

# DoFn สำหรับเพิ่ม window info
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
        window_path, records = group
        
        # สร้าง full path
        output_path = f"{self.base_path}/{window_path}/ms-member.parquet"
        
        # Convert to pandas และเขียน parquet
        df = pd.DataFrame(list(records))
        df.drop(columns=['_window_path', '_window_timestamp'], inplace=True, errors='ignore')
        
        # เขียนไป S3 ผ่าน pyarrow
        # import pyarrow.parquet as pq
        # import pyarrow as pa
        table = pa.Table.from_pandas(df, schema=self.schema)
        
        # ใช้ s3fs สำหรับเขียนไป S3
        # import s3fs
        fs = s3fs.S3FileSystem()
        
        with fs.open(output_path, 'wb') as f:
            pq.write_table(table, f)
        
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
                    ROW_NUMBER() OVER (PARTITION BY reconcile_column_name ORDER BY updated_date DESC) AS row_num
                FROM `{self.mapping_table}`
                WHERE reconcile_retrieved 
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
                old_name = row['reconcile_column_name']
                new_name = row['personas_mapping_column_name']
                
                # mapping_dict[old_name] = new_name.split('.')[-1]  # Use last part for flat mapping
                if row['reconcile_retrieved'] == True:
                    mapping_dict[old_name] = new_name
                # mapping_dict : {'is_mobile': {'profile':{'consent':'has_mobile'}} }
                # message structure : {'profile':{'consent':{'has_mobile':'Y'}}}
                
                # mapping_dict[old_name][]
                schemas_dict.append(old_name)
                # Parse nested column names (e.g., 'profile.memberId')
                # parts = new_name.split('.')
                # if len(parts) == 2:
                #     parent, field = parts
                #     if parent not in mapping_dict:
                #         mapping_dict[parent] = {}
                #         schemas_dict[parent] = []
                #     mapping_dict[parent][field] = old_name
                #     schemas_dict[parent].append(field)
                # else:
                #     # Handle non-nested columns if any
                #     mapping_dict[new_name] = old_name
            
            logging.info(f"Refreshed mapping with {len(mapping_dict)} entries")
            logging.info(f"Refreshed mapping with {mapping_dict}")
            logging.info(f"Refreshed schemas_dict with {schemas_dict}")
        except Exception as exc:
            # Log exception and continue with an empty mapping to avoid crashes
            logging.error(f"Error refreshing mapping: {exc}")
        # Yield a plain dictionary; do not wrap in AsSingleton here. The
        # surrounding pipeline will combine these periodically and expose as
        # a side input via AsSingleton.

        yield {
            'mapping_dict': mapping_dict,
            'schemas_dict': schemas_dict
        }

# class ExtractPersonasDoFn(DoFn):
#     """Extract personasId from PubSub message"""
    
#     def process(self, element):
#         try:
#             message = json.loads(element.decode('utf-8'))
#             personas_id = message.get('payload.personaId')
            
#             if personas_id:
#                 yield personas_id
#                 logging.info(f"Extracted personasId: {personas_id}")
#             else:
#                 logging.warning(f"No personasId in message: {message}")
#         except Exception as e:
#             logging.error(f"Error parsing message: {e}")

class ExtractPersonasDoFn(DoFn):
    """
    Extract personasId from PubSub message
    (Decodes Avro binary message)
    """

    def setup(self):
        """ตรวจสอบว่า Schema ถูก Parse สำเร็จตอนเริ่มต้น"""
        # if PARSED_PERSONAS_SCHEMA is None:
        #     # ถ้า Schema ไม่พร้อม, pipeline นี้ทำงานต่อไม่ได้
        #     raise RuntimeError("Personas Avro schema (PARSED_PERSONAS_SCHEMA) was not parsed successfully.")

    def process(self, element):
        # 'element' คือ BINARY AVRO BYTES (ไม่ใช่ JSON string)
        try:
            json_reader = json.loads(element.decode('utf-8'))
            logging.info(f"Received JSON message: {json_reader}")
            # 1. สร้าง "ไฟล์ในหน่วยความจำ" จาก bytes ที่เข้ามา
            # bytes_reader = io.BytesIO(json_reader)
            # logging.info(f"Extracted bytes_reader: {bytes_reader}")
            
            # 2. ถอดรหัส Binary Avro โดยใช้ schema ที่เรา parse ไว้
            # เราใช้ 'schemaless_reader' เพราะ Pub/Sub ไม่ได้แนบ schema มากับทุก message
            # message = fastavro.schemaless_reader(json_reader, PARSED_PERSONAS_SCHEMA)
            # logging.info(f"Extracted message: {message}")
            
            # 3. 'message' ตอนนี้เป็น Python Dict ที่ถูกต้องแล้ว
            #    (เช่น {'type': 'UPSERT', 'payload': {...}})
            
            # --- โค้ดเดิมของคุณ (ตอนนี้ทำงานได้แล้ว) ---
            payload = json_reader.get('payload')
            
            if payload:
                personas_id = payload.get('personaId')
                if personas_id:
                    # ที่นี่เรา yield `personas_id` ซึ่งเป็น string
                    #1-959178136#5700c7a8-fd3f-4ae6-8482-91e0f246acd1
                    # accountId#memberId#profileId
                    personas_key = '#'.join(personas_id.split('#')[1:]) 
                    yield {'personas_id': personas_key} 
                    logging.info(f"Extracted personasId: {personas_id}")
                else:
                    logging.warning(f"No personasId in Avro payload: {json_reader}")
            else:
                logging.warning(f"No payload in Avro message: {json_reader}")
            # --- จบโค้ดเดิม ---

        except Exception as e:
            # Error นี้จะจับการถอดรหัส Avro ที่ล้มเหลว
            logging.error(f"Error parsing Avro message: {e} | Data (first 50 bytes): {element[:500]}...")
            # logging.error(f"Error parsing Avro message: {e} | Data json_reader: {json_reader}")
            
class FetchFromBigtableDoFn(DoFn):
    """Fetch data from BigTable using personasId"""
    
    def __init__(self, instance_id, table_id, parent_field='profiles'):
        self.instance_id = instance_id
        self.table_id = table_id
        self.parent_field = parent_field
        self._client = None
        self._table = None
        
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
            
            row_key = personas_id.encode()
            row = self._table.read_row(row_key)
            
            if row:
                # profile_data = self.transform_message(row.cells, mapping_dict)
                # Extract profile data from BigTable
                family_data = {}
                # family_data = row.cells.get(self.parent_field, {})

                for column_family_id, columns in row.cells.items():
                    if column_family_id == self.parent_field:
                        for column, cells in columns.items():
                            # Get the latest value
                            value = cells[0].value.decode('utf-8')
                            try:
                                # Try to parse as JSON if applicable
                                family_data[column.decode('utf-8')] = json.loads(value)
                            except:
                                family_data[column.decode('utf-8')] = value
                
                yield {
                    'personas_id': personas_id,
                    self.parent_field: family_data,
                    # 'original_message': element['message']
                }
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

class TransformSchemasDoFn(DoFn):
    """Transform data according to mapping dictionary"""
    def get_nested_value(self , data, path):
        """ดึงค่าจาก nested dict ด้วย dot notation path"""
        try:
            return reduce(operator.getitem, path.split('.'), data)
        except (KeyError, TypeError):
            return None

    def transform_message(self , message_dict, mapping_dict):
        """แปลง message ตาม mapping"""
        result = {}
        for new_key, path in mapping_dict.items():
            value = self.get_nested_value(message_dict, path)
            if value is not None:
                result[new_key] = value
            else:
                result[new_key] = None
        return result


    def process(self, element, mapping_info, family_field='profile',pk_key='personas_id'):
        mapping_dict = mapping_info.get('mapping_dict', {})
        # schemas_dict = mapping_info.get('schemas_dict', {})
        
        personas_id = element[pk_key]
        family_data = element.get(family_field, {})
        
        # Prepare outputs
        # aws_output = {}  # For old schema
        # gcp_output = {'personasId': personas_id}  # For new schema
        # aws_output = self.transform_message(element, mapping_dict)

        # Process family data
        # if family_field in mapping_dict and family_data:
        if family_data:
        #     family_mapping = mapping_dict[family_field]
        #     family_schemas = schemas_dict.get(family_field, [])
            full_data = {
                'personas_id': personas_id,
                family_field: family_data,
                # เพิ่ม fields อื่นๆ จาก original_message ถ้าต้องการ
                # 'original_message': element.get('original_message', {}) 
            }
            
            aws_output = self.transform_message(full_data, mapping_dict)

        #     # For AWS (old schema)
        #     for new_field, old_field in family_mapping.items():
        #         value = family_data.get(new_field)
        #         aws_output[old_field] = value if value is not None else None
            
        #     # Add missing columns as None
        #     for field in family_schemas:
        #         if field in family_mapping:
        #             old_name = family_mapping[field]
        #             if old_name not in aws_output:
        #                 aws_output[old_name] = None
            
            # For GCP (new schema)
            # gcp_output = {
            #     'personasId': personas_id,
            #     family_field: family_data
            #     # BigQuery Auto-detect Schema จะทำงานได้ดีกับ nested dict
            # }
        
        yield beam.pvalue.TaggedOutput('aws', aws_output)
        # yield beam.pvalue.TaggedOutput('gcp', gcp_output)

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

# class WriteToS3DoFn(DoFn):
#     """Write to S3 with CDC pattern"""
    
#     def __init__(self, s3_bucket, s3_prefix):
#         self.s3_bucket = s3_bucket
#         self.s3_prefix = s3_prefix
        
#     def setup(self):
#         import boto3
#         self.s3_client = boto3.client('s3')
        
#     def process(self, element):
#         # Prepare CDC format
#         cdc_record = {
#             'operation': 'INSERT',  # or UPDATE based on logic
#             'timestamp': datetime.utcnow().isoformat(),
#             'data': element
#         }
        
#         # Generate partition path
#         now = datetime.utcnow()
#         partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}/hour={now.hour:02d}"
        
#         # Write to S3 (batch for better performance in production)
#         key = f"{self.s3_prefix}/{partition_path}/{element.get('member_number', 'unknown')}_{now.timestamp()}.json"
        
#         self.s3_client.put_object(
#             Bucket=self.s3_bucket,
#             Key=key,
#             Body=json.dumps(cdc_record)
#         )
        
#         yield element
# --- Custom Naming Policy สำหรับ Hourly Partitioning ---

# class HourlyPartitioningPolicy(WindowedFilenamePolicy):
#     """
#     สร้าง Path ตาม Partition hourly:
#     .../par_month=MM/par_day=DD/par_hour=HH/run_dt=YYYYMMDDHH/
#     """
#     def __init__(self, base_path, prefix='data'):
#         # base_path คือ S3_PARQUET_BUCKET
#         self.base_path = base_path
#         self.prefix = prefix
#         # ใช้ ShardNameTemplate มาตรฐานสำหรับ Parquet (.parquet)
#         self.shard_template = FileNaming.default(
#             prefix='shard', shard_template='-SSSSS-of-NNNNN', extension='.parquet'
#         )

#     def get_filename(self, window, shard_id, num_shards, pane_info):
#         # 1. ใช้ End Time ของ Window ในการกำหนด Partition
#         # window.end เป็น Beam timestamp (Microseconds)
#         window_end_micros = window.end.micros
#         window_end = datetime.fromtimestamp(window_end_micros / 10**6) 

#         # 2. สร้าง Partition Folders: par_month=MM/par_day=DD/par_hour=HH
#         par_month = window_end.strftime('%m')
#         par_day = window_end.strftime('%d')
#         par_hour = window_end.strftime('%H')
        
#         # 3. สร้าง Sub-folder: run_dt=YYYYMMDDHH
#         run_dt = window_end.strftime('%Y%m%d%H')
        
#         partition_path = (
#             f"par_month={par_month}/"
#             f"par_day={par_day}/"
#             f"par_hour={par_hour}/"
#             f"run_dt={run_dt}"
#         )
        
#         # 4. สร้าง Filename 
#         file_name = self.shard_template.get_filename(shard_id, num_shards, pane_info)
        
#         # คืนค่าเป็น Full Path: base_path/par_month=.../run_dt=.../cdc-data-shard-00000-of-00001.parquet
#         return f"{self.base_path}/{partition_path}/{self.prefix}-{file_name}"
# ----------------------------------------
# 💡 แนะนำให้เปลี่ยนชื่อ DoFn นี้เป็น PrepareForBigQueryFn
# ----------------------------------------
class PrepareForBigQueryFn(DoFn):
    """
    Flatten the 'profiles' dict from Bigtable 
    to match the BigQuery schema.
    """
    
    def __init__(self, bq_schema_fields):
        # รับรายชื่อ field มาจาก schema
        self.schema_fields = [f['name'] for f in bq_schema_fields]
        
    def process(self, element):
        # element คือ: {'personas_id': '...', 'profiles': {'accountId': 'a', ...}}
        
        profile_data = element.get('profiles')
        
        if not profile_data or not isinstance(profile_data, dict):
            logging.warning(f"Skipping record, missing or invalid 'profiles' data for {element.get('personas_id')}")
            return

        # สร้าง record ใหม่ โดยดึงค่าจาก 'profiles'
        output_record = {}
        for field in self.schema_fields:
            # ดึงค่าจาก profile_data, ถ้าไม่มีให้เป็น None
            output_record[field] = profile_data.get(field)
            
        # ⚠️ ตรวจสอบ Primary Key (memberId)
        # ถ้าไม่มี memberId การ Upsert จะล้มเหลว
        if not output_record.get('memberId'):
            logging.warning(f"Skipping record, 'memberId' (PK) is missing in profiles: {profile_data}")
            return
            
        # Yield flat dict ที่พร้อมสำหรับ BQ
        # {'accountId': 'a', 'memberId': 'b', ...}
        yield output_record
class FormatForCdcDoFn(DoFn):
    """
    จัด Format Python dict ธรรมดา
    ให้เป็น beam.Row ที่ sink (use_cdc_writes=True) คาดหวัง
    ตาม Error: "expect Row Schema with a 'row_mutation_info' Row field"
   
    """
    def process(self, element):
        # element คือ dict {'accountId': '...', 'memberId': ...}
        
        # เราต้องสร้าง Row ที่มี 2 field: 'record' และ 'row_mutation_info'
        yield Row(
            # 1. record: ใส่ dict ข้อมูลของคุณลงไป
            record=element,
            
            # 2. row_mutation_info: ระบุประเภทการเปลี่ยนแปลง
            #    (ต้องเป็น bytes b'UPSERT')
            row_mutation_info=Row(
                mutation_type=b'UPSERT'
            )
        )
        
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
            FetchFromBigtableDoFn(BT_INSTANCE, BT_TABLE,parent_field='profiles')
        )
    )
    
    # Step 5: Transform schemas
    transformed = (
        bigtable_data
        | 'TransformSchemas' >> ParDo(
            TransformSchemasDoFn(),
            mapping_info=beam.pvalue.AsSingleton(mapping_refresh),
            family_field='profiles',
            pk_key='personas_id'
        ).with_outputs('aws', 'gcp')
    )
    
    # Step 6.1: Write to BigLake (GCP)
    # gcp_data = transformed.gcp
    # gcp_data = bigtable_data
    # (
    #     gcp_data
    #     # | 'PrepareForBigLake' >> ParDo(WriteToBigLakeDoFn(BIGLAKE_TABLE))
    # #     | 'WriteToBigQuery' >> WriteToBigQuery(
    # #         table=BIGLAKE_TABLE,
    # #         # schema='SCHEMA_AUTODETECT',
    # #         schema=MS_PERSONAS_BIGQUERY_SCHEMA,
    # #         write_disposition=BigQueryDisposition.WRITE_APPEND,
    # #         create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
    # #         method='STREAMING_INSERTS',  # Explicitly use streaming inserts
    # #         insert_retry_strategy='RETRY_ON_TRANSIENT_ERROR',
    # #         validate=False  # Skip validation for better performance
    # #     )
    #     # 💡 1. ใช้ DoFn ใหม่ที่เราสร้าง
    #     | 'PrepareForBigQuery' >> ParDo(
    #         PrepareForBigQueryFn(
    #             bq_schema_fields=MS_PERSONAS_BIGQUERY_SCHEMA['fields']
    #         )
    #     )
    #     # # --- 6.1.1 (เพิ่ม Step นี้) ---
    #     # # "ห่อ" dict ของเราให้อยู่ใน Schema ที่ CDC ต้องการ
    #     # | 'FormatForCDC' >> beam.ParDo(FormatForCdcDoFn())
    #     # # --- 6.1.2 (Sink เดิม) ---
    #     # | 'WriteToBigQuery (CDC/Upsert)' >> WriteToBigQuery(
    #     | 'CDCWriteToBigLakeIceberg' >> WriteToBigQuery(
    #         table=BIGLAKE_TABLE,
    #         # schema=MS_PERSONAS_BIGQUERY_SCHEMA,
    #         schema='SCHEMA_AUTODETECT',
    #         write_disposition=BigQueryDisposition.WRITE_APPEND,
    #         create_disposition=BigQueryDisposition.CREATE_NEVER,
    #         method='STORAGE_WRITE_API',
    #         # use_cdc_writes=True,  
    #         use_at_least_once=True,  # ← Enable CDC/UPSERT for BigLake Iceberg
    #         # triggering_frequency=10,  # ← Trigger every 10 seconds
    #         with_auto_sharding=True,
    #         ignore_unknown_columns=True
    #     )
    # )
    gcp_data = bigtable_data
    (
        gcp_data
        # 💡 1. ใช้ DoFn ใหม่ที่เราสร้าง
        | 'PrepareForBigQuery' >> ParDo(
            PrepareForBigQueryFn(
                bq_schema_fields=MS_PERSONAS_BIGQUERY_SCHEMA['fields']
            )
        )
        | 'CDCWriteToBigLakeIceberg' >> WriteToBigQuery(
            table=BIGLAKE_TABLE,
            # schema=MS_PERSONAS_BIGQUERY_SCHEMA,
            schema='accountId:STRING,dateOfBirth:STRING,gender:STRING,hasEmail:STRING,hasMobile:STRING,languagePrefer:STRING,memberId:STRING,nationalityId:STRING,profileId:STRING,updated_date:STRING',
            # schema='SCHEMA_AUTODETECT',
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
            method='STORAGE_WRITE_API',
            # use_cdc_writes=True,  
            use_at_least_once=True,  # ← Enable CDC/UPSERT for BigLake Iceberg
            # triggering_frequency=10,  # ← Trigger every 10 seconds
            with_auto_sharding=True,
            ignore_unknown_columns=True
        )
    )

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
    aws_data = transformed.aws
    (
        aws_data
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