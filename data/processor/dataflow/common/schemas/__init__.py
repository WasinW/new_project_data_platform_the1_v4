"""
Schema definitions for dataflow pipelines.

This module contains static schema definitions for BigQuery and Parquet writes,
including CDC schemas and PyArrow schemas for various data models.
"""

from __future__ import annotations

from datetime import datetime, date
import pyarrow as pa
import apache_beam as beam


# ============================================
# MS PERSONAS SCHEMAS
# ============================================

# PyArrow Schema for MS Personas Parquet writes (AWS S3)
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


# BigQuery CDC Schema for Storage Write API with use_cdc_writes=True
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


# Beam Row type for CDC records (optional - used for type checking)
CDC_ROW_TYPE = beam.Row(
    accountId=str,
    dateOfBirth=date,  # date object
    gender=str,
    hasEmail=str,
    hasMobile=str,
    languagePrefer=str,
    memberId=str,
    nationalityId=str,
    profileId=str,
    updated_date=datetime,  # datetime object
)


__all__ = [
    'MS_PERSONAS_PARQUET_SCHEMA',
    'MS_PERSONAS_CDC_SCHEMA',
    'CDC_ROW_TYPE',
]
