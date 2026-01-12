"""
Member Tiers Kafka Consumer Pipeline

Consumes member tier events (upgraded/downgraded) from Confluent Kafka
and writes to GCS Parquet files (BigQuery external tables).

Pipeline Flow:
1. Consume from Kafka topics (upgraded, downgraded)
2. Extract and parse messages
3. Map to RAW schemas and write to Parquet:
   - member_tier_upgraded_raw
   - member_tier_downgraded_raw
4. Merge both topics, call Member API, and write:
   - member_tier_raw

Author: Data Engineering Team
Date: 2025-01-07

IMPORTANT: Kafka config follows kafkaGCS.py pattern (proven working):
  - sasl.mechanism (SINGULAR, not sasl.mechanisms)
  - NO serviceName in JAAS config (only for GSSAPI/Kerberos)
  - with_metadata=True in ReadFromKafka
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timezone
from io import BytesIO
from typing import Any, Dict, Iterator, List, Optional, Tuple, NamedTuple

import apache_beam as beam
from apache_beam import DoFn, PTransform
from apache_beam.io.kafka import ReadFromKafka
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.metrics import Metrics

import pyarrow as pa
import pyarrow.parquet as pq
import requests
from dataclasses import dataclass
from fastavro import schemaless_reader

# logger = logging.getlogger(__name__)
# =============================================================================
# LOGGING SETUP
# =============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# logger = logging.getlogger(__name__)

# Module-level logger name prefix for consistency
MODULE_LOGGER_NAME = "member_tiers"

# Module-level logger for non-DoFn code (main, helper functions)
LOGGER = logging.getLogger(MODULE_LOGGER_NAME)


def get_logger(class_name: str) -> logging.Logger:
    """Get a logger with hierarchical naming for DoFn classes.
    
    This creates loggers like: member_tiers.CallMemberTierInfoAPIDoFn
    Which allows filtering by prefix 'member_tiers' in Log Explorer
    while still identifying which DoFn produced the log.
    """
    return logging.getLogger(f"{MODULE_LOGGER_NAME}.{class_name}")


# =============================================================================
# CONFIGURATION
# =============================================================================

# Topic configurations
TOPIC_CONFIGS = {
    "upgraded": {
        "topic": "loyalty.members.upgraded",
        # "consumer_group": "datamart_loyalty.members.upgraded-consumer-group",
        "consumer_group": "loyalty-sem-members-tier-loyalty-members-tier",
    },
    "downgraded": {
        "topic": "loyalty.members.downgraded",
        # "consumer_group": "datamart_loyalty.members.downgraded-consumer-group",
        "consumer_group": "loyalty-sem-members-tier-loyalty-members-tier",
    },
}

# Default Secret Manager secret name
DEFAULT_KAFKA_SECRET_NAME = "loyalty-data-kafka-connect-members"
DEFAULT_API_SECRET_NAME = "loyalty-data-kafka-connect-members"

# GCS output configuration
DEFAULT_GCS_OUTPUT_BUCKET = "gs://the1-loyalty-data-stg-pipeline-data-raw"

# Iceberg Catalog Configuration (Hadoop/File-based)
ICEBERG_CATALOG_NAME = "loyalty_raw_gcs"
ICEBERG_DATABASE = "data_raw"

# Table names (will be created under ICEBERG_DATABASE)
TABLE_UPGRADED = "raw_member_upgraded"
TABLE_DOWNGRADED = "raw_member_downgraded"
TABLE_MEMBER_TIER = "raw_member_tier"

# API configuration
API_BASE_URL = "https://vpce-09b5b0e1633f97118-9fc5jzij.execute-api.ap-southeast-1.vpce.amazonaws.com/prod"
API_GATEWAY_ID = "ee2j2kx5tb"

# Windowing configuration (seconds)
WINDOW_SIZE_SECONDS = 60  # 1 minute micro-batch

# Retry configuration
API_MAX_RETRIES = 2
API_RETRY_DELAY_SECONDS = 1

# Payload format: 'json' or 'avro'
PAYLOAD_FORMAT = "json"

# If using Confluent Schema Registry for Avro, set this URL and set PAYLOAD_FORMAT='avro'
USE_SCHEMA_REGISTRY = False
SCHEMA_REGISTRY_URL = "http://schema-registry:8081"

# Simple cache for schema id -> parsed schema
_SCHEMA_CACHE: Dict[int, Dict] = {}
DEFAULT_CONFLUENT_SECRET_NAME = "loyalty-data-transaction"

# Flag to track if Schema Registry credentials have been loaded on worker
_WORKER_SR_CREDENTIALS_LOADED = False

# =============================================================================
# SCHEMAS - PyArrow for Parquet
# =============================================================================

# Schema: member_tier_upgraded_raw
@dataclass
class MemberTierUpgradedRow(NamedTuple):
    """Schema for raw_member_upgraded table."""
    eventId: str
    source: Optional[str]
    eventName: Optional[str]
    timestamp: Optional[datetime]
    accountId: Optional[str]
    memberId: Optional[str]
    tierEventId: Optional[str]
    tierCode: Optional[str]
    isExistingTier: Optional[bool]
    triggerType: Optional[str]
    processedAt: Optional[str]
    ingested_at: datetime
    source_topic: str


@dataclass
class MemberTierDowngradedRow(NamedTuple):
    """Schema for raw_member_downgraded table."""
    eventId: str
    source: Optional[str]
    eventName: Optional[str]
    timestamp: Optional[datetime]
    accountId: Optional[str]
    memberId: Optional[str]
    tierEventId: Optional[str]
    tierCode: Optional[str]
    triggerType: Optional[str]
    processedAt: Optional[str]
    ingested_at: datetime
    source_topic: str


@dataclass
class MemberTierRawRow(NamedTuple):
    """Schema for raw_member_tier table (from API)."""
    id: Optional[str]
    member_id: Optional[str]
    tier_code: Optional[str]
    program_code: Optional[str]
    partner_code: Optional[str]
    start_date: Optional[str]
    expiry_date: Optional[str]
    name: Optional[str]
    level_name: Optional[str]
    ingested_at: datetime
    source_event_id: Optional[str]
    source_topic: Optional[str]


# =============================================================================
# ICEBERG SCHEMAS - PyArrow for PyIceberg
# =============================================================================

SCHEMA_UPGRADED_RAW = pa.schema([
    pa.field("eventId", pa.string()),
    pa.field("source", pa.string()),
    pa.field("eventName", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("accountId", pa.string()),
    pa.field("memberId", pa.string()),
    pa.field("tierEventId", pa.string()),
    pa.field("tierCode", pa.string()),
    pa.field("isExistingTier", pa.bool_()),
    pa.field("triggerType", pa.string()),
    pa.field("processedAt", pa.string()),
    # Pipeline metadata
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("source_topic", pa.string(), nullable=False),
])

# Schema: member_tier_downgraded_raw
SCHEMA_DOWNGRADED_RAW = pa.schema([
    pa.field("eventId", pa.string()),
    pa.field("source", pa.string()),
    pa.field("eventName", pa.string()),
    pa.field("timestamp", pa.timestamp("us", tz="UTC")),
    pa.field("accountId", pa.string()),
    pa.field("memberId", pa.string()),
    pa.field("tierEventId", pa.string()),
    pa.field("tierCode", pa.string()),
    pa.field("triggerType", pa.string()),
    pa.field("processedAt", pa.string()),
    # Pipeline metadata
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("source_topic", pa.string(), nullable=False),
])

# Schema: member_tier_raw (from API)
SCHEMA_MEMBER_TIER_RAW = pa.schema([
    pa.field("id", pa.string()),
    pa.field("member_id", pa.string()),
    pa.field("tier_code", pa.string()),
    pa.field("program_code", pa.string()),
    pa.field("partner_code", pa.string()),
    pa.field("start_date", pa.string()),
    pa.field("expiry_date", pa.string()),
    pa.field("name", pa.string()),
    pa.field("level_name", pa.string()),
    # Pipeline metadata
    pa.field("ingested_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("source_event_id", pa.string()),
    pa.field("source_topic", pa.string()),
])


# =============================================================================
# SECRET MANAGER HELPERS (from kafkaGCS.py pattern)
# =============================================================================

def get_secret_value(secret_id: str, project_id: str) -> dict:
    """Get secret value from Secret Manager."""
    if not secret_id:
        raise ValueError("secret_id is required")
    if not project_id:
        raise ValueError("project_id is required")
    
    try:
        from google.cloud import secretmanager
    except Exception as exc:
        raise RuntimeError("google-cloud-secret-manager not installed") from exc

    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
    
    LOGGER.info("Accessing Secret Manager: secret_id=%s project=%s", secret_id, project_id)
    
    try:
        response = client.access_secret_version(request={"name": name})
        payload = response.payload.data.decode("UTF-8")
        value = json.loads(payload)
        
        if not isinstance(value, dict):
            raise ValueError("Secret payload JSON must be an object")
        
        LOGGER.info("✓ Secret Manager access successful: secret_id=%s keys_found=%d",
                    secret_id, len(value))
        return value
    except Exception:
        LOGGER.exception("Failed to access Secret Manager secret %s in project %s",
                        secret_id, project_id)
        raise


def load_confluent_env_from_secret_manager(
    secret_id: str,
    project_id: str,
    overwrite: bool = False,
) -> bool:
    """Load Confluent/Kafka config from Secret Manager into env vars.
    
    Follows kafkaGCS.py pattern for key mapping.
    """
    data = get_secret_value(secret_id, project_id)

    def _get_any(*keys):
        for k in keys:
            if k in data and data.get(k) is not None:
                return data.get(k)
        lower_map = {str(k).lower(): v for k, v in data.items()}
        for k in keys:
            lk = str(k).lower()
            if lk in lower_map and lower_map.get(lk) is not None:
                return lower_map.get(lk)
        return None

    # Full mapping from kafkaGCS.py
    mapping = {
        "CONFLUENT_TOPIC": _get_any(
            "confluent-topic", "confluent_topic", "topic", "CONFLUENT_TOPIC"),
        "CONFLUENT_GROUP_ID": _get_any(
            "confluent-groupid", "confluent_groupid", "group_id", "groupId", "CONFLUENT_GROUP_ID"),
        "CONFLUENT_BOOTSTRAP_SERVERS": _get_any(
            "confluent-bootstrapserver", "confluent_bootstrapserver", "bootstrap",
            "bootstrap_servers", "bootstrap.servers", "bootstrapServerEndpoint",
            "CONFLUENT_BOOTSTRAP_SERVERS"),
        "CONFLUENT_SECURITY_PROTOCOL": _get_any(
            "confluent-securityprotocol", "confluent_securityprotocol",
            "security_protocol", "security.protocol", "CONFLUENT_SECURITY_PROTOCOL"),
        "CONFLUENT_SASL_MECHANISM": _get_any(
            "confluent-saslmechanism", "confluent_saslmechanism",
            "sasl_mechanism", "sasl.mechanism", "CONFLUENT_SASL_MECHANISM"),
        "CONFLUENT_SASL_USERNAME": _get_any(
            "confluent-saslusername", "confluent_saslusername", "sasl_username",
            "sasl.username", "confluentId", "CONFLUENT_SASL_USERNAME"),
        "CONFLUENT_SASL_PASSWORD": _get_any(
            "confluent-saslpassword", "confluent_saslpassword", "sasl_password",
            "sasl.password", "confluentSecret", "CONFLUENT_SASL_PASSWORD"),
        "CONFLUENT_SR_URL": _get_any(
            "confluent-srurl", "confluent_srurl", "sr_url",
            "schema_registry_url", "schemaRegistryURL", "CONFLUENT_SR_URL"),
        "CONFLUENT_SR_API_KEY": _get_any(
            "confluent-srapikey", "confluent_srapikey", "sr_api_key",
            "schema_registry_api_key", "confluentRegistryApiKey", "CONFLUENT_SR_API_KEY"),
        "CONFLUENT_SR_API_SECRET": _get_any(
            "confluent-srapisecret", "confluent_srapisecret", "sr_api_secret",
            "schema_registry_api_secret", "confluentRegistrySecret", "CONFLUENT_SR_API_SECRET"),
    }

    # Check required fields
    required_env = ["CONFLUENT_BOOTSTRAP_SERVERS", "CONFLUENT_SASL_USERNAME", "CONFLUENT_SASL_PASSWORD"]
    missing = [env for env in required_env if mapping.get(env) is None]
    if missing:
        raise RuntimeError(f"Secret Manager missing required settings: {', '.join(missing)}")

    # Apply to environment
    applied_env_names = []
    for env_name, value in mapping.items():
        if value is None:
            continue
        if overwrite or not os.getenv(env_name):
            os.environ[env_name] = str(value)
            applied_env_names.append(env_name)

    if applied_env_names:
        LOGGER.info(
            "Loaded Confluent config from Secret Manager: applied env vars: %s",
            ", ".join(sorted(applied_env_names)),
        )
    return True


def build_consumer_config(group_id: str, auto_offset_reset: str = "latest") -> Dict[str, str]:
    """Build Kafka consumer config dict for ReadFromKafka.
    
    CRITICAL: This follows kafkaGCS.py pattern EXACTLY:
    - Uses 'sasl.mechanism' (SINGULAR, not 'sasl.mechanisms')
    - NO serviceName in JAAS config (only for GSSAPI/Kerberos)
    """
    bootstrap = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
    security_protocol = os.getenv("CONFLUENT_SECURITY_PROTOCOL", "SASL_SSL")
    sasl_mechanism = os.getenv("CONFLUENT_SASL_MECHANISM", "PLAIN")
    sasl_username = os.getenv("CONFLUENT_SASL_USERNAME")
    sasl_password = os.getenv("CONFLUENT_SASL_PASSWORD")

    if not bootstrap or not sasl_username or not sasl_password:
        raise RuntimeError("Kafka credentials not set in environment")

    # Base Kafka client configuration - EXACTLY from kafkaGCS.py
    config = {
        "bootstrap.servers": bootstrap,
        "group.id": group_id,
        "security.protocol": security_protocol,
        # CRITICAL: Use 'sasl.mechanism' (SINGULAR) - NOT 'sasl.mechanisms'
        "sasl.mechanism": sasl_mechanism,
        "enable.auto.commit": "false",
        "auto.offset.reset": auto_offset_reset,
        # Timeout settings
        "session.timeout.ms": "45000",       # Default 45s (was 120s - too long)
        "heartbeat.interval.ms": "15000",    # Must be < session.timeout.ms / 3
        "request.timeout.ms": "60000",       # Default 60s
        "connections.max.idle.ms": "540000",
        # SSL settings
        "ssl.endpoint.identification.algorithm": "https",
        # Throughput optimization settings (from kafkaGCS.py)
        "fetch.min.bytes": "1",              # Don't wait for batch
        "fetch.max.wait.ms": "500",          # CORRECT key name (not fetch.wait.max.ms)
        "max.partition.fetch.bytes": "1048576",
        "max.poll.records": "500",           # Limit records per poll
        "isolation.level": "read_committed",
    }

    # SASL credentials
    if sasl_username and sasl_password:
        # config["sasl.username"] = sasl_username
        # config["sasl.password"] = sasl_password
        # # CRITICAL: NO serviceName for PLAIN mechanism (only for GSSAPI/Kerberos)
        # # This is the exact format from kafkaGCS.py that WORKS
        config["sasl.jaas.config"] = (
            f"org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{sasl_username}" password="{sasl_password}";'
        )

    # Log config summary (mask sensitive fields)
    safe_config = {
        k: ("***" if "jaas" in k.lower() else v)
        for k, v in config.items()
    }
    LOGGER.info(
        "Kafka consumer config built: %s",
        ", ".join(f"{k}={v}" for k, v in safe_config.items()),
    )
    return config

def get_schema_from_registry(schema_id: int):
    """Fetch schema from Confluent Schema Registry.
    
    Automatically loads credentials from Secret Manager on first call
    within each worker process if not already available.
    """
    global _WORKER_SR_CREDENTIALS_LOADED
    
    if schema_id in _SCHEMA_CACHE:
        return _SCHEMA_CACHE[schema_id]
    
    # Lazy-load credentials on worker if not available
    sr_api_key = os.getenv("CONFLUENT_SR_API_KEY")
    sr_api_secret = os.getenv("CONFLUENT_SR_API_SECRET")
    
    if (not sr_api_key or not sr_api_secret) and not _WORKER_SR_CREDENTIALS_LOADED:
        LOGGER.info("Schema Registry credentials not in worker env; loading from Secret Manager")
        project = os.getenv("GOOGLE_CLOUD_PROJECT")
        # Fallback: query GCE metadata server (available on Dataflow workers)
        if not project:
            try:
                import urllib.request
                req = urllib.request.Request(
                    "http://metadata.google.internal/computeMetadata/v1/project/project-id",
                    headers={"Metadata-Flavor": "Google"},
                )
                with urllib.request.urlopen(req, timeout=2) as resp:
                    project = resp.read().decode("utf-8").strip()
                if project:
                    LOGGER.info("Resolved GCP project from metadata server: %s", project)
            except Exception as meta_err:
                LOGGER.warning("Failed to query GCE metadata for project: %s", meta_err)
        if project:
            try:
                load_confluent_env_from_secret_manager(
                    DEFAULT_CONFLUENT_SECRET_NAME,
                    project,
                    overwrite=False
                )
                _WORKER_SR_CREDENTIALS_LOADED = True
                sr_api_key = os.getenv("CONFLUENT_SR_API_KEY")
                sr_api_secret = os.getenv("CONFLUENT_SR_API_SECRET")
                LOGGER.info("✓ Schema Registry credentials loaded on worker")
            except Exception as e:
                LOGGER.error("Worker failed to load Secret Manager credentials: %s", e)
                _WORKER_SR_CREDENTIALS_LOADED = True  # Don't retry repeatedly
        else:
            LOGGER.error("Cannot load Schema Registry credentials: GCP project not set (GOOGLE_CLOUD_PROJECT or metadata server)")
            _WORKER_SR_CREDENTIALS_LOADED = True
    
    url = f"{SCHEMA_REGISTRY_URL}/schemas/ids/{schema_id}"
    auth = None
    if sr_api_key and sr_api_secret:
        auth = (sr_api_key, sr_api_secret)
    else:
        raise RuntimeError(
            f"Schema Registry credentials not available for schema {schema_id}. "
            "Check Secret Manager access and ensure GOOGLE_CLOUD_PROJECT is set."
        )
    
    LOGGER.debug("Fetching schema %d from %s", schema_id, url)
    resp = requests.get(url, timeout=10, auth=auth)
    resp.raise_for_status()
    schema_json = resp.json().get("schema")
    schema = json.loads(schema_json)
    _SCHEMA_CACHE[schema_id] = schema
    LOGGER.info("✓ Schema %d fetched and cached", schema_id)
    return schema


def decode_confluent_avro(b: bytes):
    if not b or len(b) < 5:
        return None
    if b[0] != 0:
        try:
            forced_id = os.getenv("CONFLUENT_SCHEMA_ID")
            schema = None
            if forced_id:
                fid = int(forced_id)
                schema = _SCHEMA_CACHE.get(fid) or (get_schema_from_registry(fid) if USE_SCHEMA_REGISTRY else None)
                if schema:
                    return schemaless_reader(BytesIO(b), schema)
            return schemaless_reader(BytesIO(b), _SCHEMA_CACHE.get(0))
        except Exception:  # pragma: no cover - defensive
            return None
    schema_id = int.from_bytes(b[1:5], "big")
    schema = (
        get_schema_from_registry(schema_id)
        if USE_SCHEMA_REGISTRY
        else _SCHEMA_CACHE.get(schema_id)
    )
    if schema is None:
        raise RuntimeError(
            f"Schema {schema_id} not available in cache and schema registry disabled"
        )
    try:
        return schemaless_reader(BytesIO(b[5:]), schema)
    except Exception:  # pragma: no cover - defensive
        return None

def decode_kafka_value(raw):
    """Decode Kafka message value according to `PAYLOAD_FORMAT`.

    Supports JSON (UTF-8 text) and Avro (Confluent wire format). Non-bytes
    are returned unchanged to keep behavior identical to the prior inline
    lambda.
    """
    if PAYLOAD_FORMAT == "json":
        if isinstance(raw, (bytes, bytearray)):
            return raw.decode("utf-8")
        return raw
    if PAYLOAD_FORMAT == "avro":
        if isinstance(raw, (bytes, bytearray)):
            return decode_confluent_avro(bytes(raw))
        return raw
    return raw

# =============================================================================
# NETWORK DIAGNOSTIC HELPERS (from kafkaGCS.py)
# =============================================================================

def extract_kafka_value(record):
    """Extract value from Kafka record.
    
    CRITICAL: Must be a module-level named function (not lambda) for proper 
    pickling/serialization in Dataflow distributed workers.
    """
    try:
        # Log for debugging
        LOGGER.info(f"extract_kafka_value called with type: {type(record)} , record: {record}")
        
        # KafkaRecord has .value attribute when with_metadata=True
        if hasattr(record, 'value'):
            value = record.value
            LOGGER.info(f"Extracted value from record.value, type: {type(value)}, len: {len(value) if value else 0}")
            return value
        # Fallback: if it's a tuple (key, value) when with_metadata=False
        elif isinstance(record, tuple) and len(record) >= 2:
            value = record[1]
            LOGGER.info(f"Extracted value from tuple[1], type: {type(value)}")
            return value
        else:
            LOGGER.warning(f"Unexpected record format: {type(record)}, returning as-is")
            return record
    except Exception as e:
        LOGGER.error(f"Error extracting Kafka value: {e}, record type: {type(record)}")
        return None


def _log_dns_resolution(bootstrap_servers: str) -> None:
    """Log DNS resolution for bootstrap servers (helps debug connectivity)."""
    if not bootstrap_servers:
        return
    for server in bootstrap_servers.split(","):
        host = server.split(":")[0].strip()
        try:
            ips = socket.gethostbyname_ex(host)
            LOGGER.info("DNS resolution for %s: %s", host, ips)
        except socket.gaierror as e:
            LOGGER.warning("DNS resolution failed for %s: %s", host, e)


def _preflight_kafka_connectivity(bootstrap_servers: str, timeout: float = 5.0) -> None:
    """Test TCP connectivity to Kafka brokers before starting pipeline."""
    if not bootstrap_servers:
        return
    for server in bootstrap_servers.split(","):
        parts = server.strip().split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9092
        try:
            sock = socket.create_connection((host, port), timeout=timeout)
            sock.close()
            LOGGER.info("✓ TCP connectivity OK: %s:%d", host, port)
        except Exception as e:
            LOGGER.warning("✗ TCP connectivity FAILED: %s:%d - %s", host, port, e)


def parse_timestamp(ts_str: Any) -> Optional[datetime]:
    """Parse timestamp string to datetime object."""
    if not ts_str:
        return None
    try:
        if isinstance(ts_str, datetime):
            return ts_str
        ts = str(ts_str).replace('Z', '+00:00')
        return datetime.fromisoformat(ts)
    except Exception:
        return datetime.now(timezone.utc)


# =============================================================================
# API CLIENT HELPER
# =============================================================================

class MemberTierAPIClient:
    """Client for Member Tier API with token caching."""
    
    def __init__(self, api_secret_project: str, api_secret_id: str):
        self.api_secret_project = api_secret_project
        self.api_secret_id = api_secret_id
        self._token: Optional[str] = None
        self._token_expires_at: float = 0
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")
    
    def _get_credentials(self) -> Tuple[str, str]:
        """Get API credentials from Secret Manager."""
        secret = get_secret_value(self.api_secret_id, self.api_secret_project)
        # client_id = secret.get("clientId", secret.get("client_id", "data-engineer"))
        # client_secret = secret.get("clientSecret", secret.get("client_secret"))
        client_id = secret.get("confluent-srapikey",  "data-engineer")
        client_secret = secret.get("confluent-srapisecret", None)
        if not client_secret:
            raise ValueError("clientSecret not found in API secret")
        return client_id, client_secret
    
    def _refresh_token(self) -> str:
        """Get new authentication token from API."""
        client_id, client_secret = self._get_credentials()
        
        url = f"{API_BASE_URL}/authentications/tokens"
        headers = {
            "Content-Type": "application/json",
            "x-apigw-api-id": API_GATEWAY_ID,
        }
        payload = {
            "clientId": client_id,
            "clientSecret": client_secret,
        }
        
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        response.raise_for_status()
        
        data = response.json()
        token = data.get("access_token") or data.get("token") or data.get("accessToken")
        if not token:
            raise ValueError(f"No token in response: {data.keys()}")
        
        # Cache token for 25 minutes (assuming 30 min expiry)
        self._token = token
        self._token_expires_at = time.time() + 25 * 60
        
        self._logger.info("✓ API token refreshed successfully")
        return token
    
    def get_token(self) -> str:
        """Get valid token (refresh if needed)."""
        if not self._token or time.time() >= self._token_expires_at:
            return self._refresh_token()
        return self._token
    
    def get_member_accounts(self, member_id: str, max_retries: int = API_MAX_RETRIES) -> Optional[Dict]:
        """Get member account info from API."""
        url = f"{API_BASE_URL}/loyalty/v2/members/{member_id}/accounts"
        
        for attempt in range(max_retries + 1):
            try:
                token = self.get_token()
                headers = {
                    "Authorization": f"Bearer {token}",
                    "x-apigw-api-id": API_GATEWAY_ID,
                }
                
                response = requests.get(url, headers=headers, timeout=30)
                
                if response.status_code == 401:
                    self._token = None
                    continue
                
                response.raise_for_status()
                return response.json()
                
            except Exception as e:
                self._logger.warning(f"API call failed (attempt {attempt + 1}/{max_retries + 1}): {e}")
                if attempt < max_retries:
                    time.sleep(API_RETRY_DELAY_SECONDS)
                else:
                    self._logger.error(f"API call failed after {max_retries + 1} attempts for member_id={member_id}")
                    return None
        
        return None


# =============================================================================
# DoFn CLASSES - KAFKA DECODE (from kafkaGCS.py pattern)
# =============================================================================

class DecodeKafkaValueDoFn(DoFn):
    """Decode Kafka message value bytes to dict.

    Following kafkaGCS.py pattern:
    - Handles bytes decoding
    - Parses JSON to dict
    - Error tracking via metrics
    """

    def __init__(self, topic_name: str, debug_mode: bool = True):
        self.topic_name = topic_name
        self.debug_mode = debug_mode
        self._decode_errors = 0
        self._message_count = 0
        self._logger = None
        # Initialize counters that are used in process()
        self._success_count = 0
        self._errors_seen = 0
        self._logs_emitted = 0
        self._max_logs = 10
        self._every_n_errors = 100
        self._log_every_n_success = 1000

    def setup(self):  # pragma: no cover
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")
        self._seen = Metrics.counter("kafka", "messages_seen")
        self._ok = Metrics.counter("kafka", "decode_ok")
        self._errors = Metrics.counter("kafka", "decode_errors")
        self._decode_latency = Metrics.distribution("kafka", "decode_latency_ms")
        self._payload_size = Metrics.distribution("kafka", "payload_bytes")

    def start_bundle(self):
        """Initialize per-bundle state for batch processing tracking."""
        self._bundle_count = 0

    def finish_bundle(self):
        """Log per-bundle metrics summary."""
        if self._bundle_count > 0:
            self._logger.debug("Kafka decode bundle finished: %d elements processed", self._bundle_count)

    def teardown(self):
        """Cleanup resources when worker shuts down."""
        pass

    # def process(self, element) -> Iterator[Dict[str, Any]]:
    #     self._message_count += 1
    #     timestamp = datetime.now(timezone.utc)
    #     self._logger.info(f"[{self.topic_name}] DecodeKafkaValueDoFn received element type: {type(element)}")
    #     self._logger.info(f"[{self.topic_name}] Decoding message #{self._message_count} , element={element}")
    #     try:
    #         # element is bytes (from record.value after Extract Value step)
    #         if element is None:
    #             self._logger.warning(f"[{self.topic_name}] Received None element, skipping")
    #             return
            
    #         if isinstance(element, bytes):
    #             decoded = element.decode("utf-8")
    #             self._logger.info(f"[{self.topic_name}] Decoded bytes to string, length: {len(decoded)}")
    #         else:
    #             decoded = str(element) if element else "{}"
    #             self._logger.info(f"[{self.topic_name}] Element was not bytes, converted to string")
            
    #         # Parse JSON
    #         try:
    #             parsed = json.loads(decoded) if decoded else {}
    #             self._logger.info(f"[{self.topic_name}] Parsed JSON successfully, keys: {list(parsed.keys()) if isinstance(parsed, dict) else 'not a dict'}")
    #         except json.JSONDecodeError as e:
    #             self._logger.warning(f"[{self.topic_name}] JSON parse error: {e}")
    #             parsed = {"_raw": decoded, "_parse_error": str(e)}
            
    #         result = {
    #             "_topic": self.topic_name,
    #             "_ingested_at": timestamp,
    #             "_message_number": self._message_count,
    #             **parsed,
    #         }
            
    #         if self.debug_mode and self._message_count <= 10:
    #             self._logger.info(f"[{self.topic_name}] Message #{self._message_count}: {json.dumps(parsed, default=str)[:500]}")
            
    #         yield result
            
    #     except Exception as e:
    #         self._decode_errors += 1
    #         self._logger.error(f"[{self.topic_name}] Decode error #{self._decode_errors}: {e}")
    #         if self._decode_errors <= 5:
    #             self._logger.error(f"[{self.topic_name}] Raw element type: {type(element)}, repr: {repr(element)[:200]}")
    def process(self, element):
        import time
        self._seen.inc()
        self._bundle_count += 1
        self._message_count += 1
        timestamp = datetime.now(timezone.utc)

        # Track payload size
        payload_len = len(element) if isinstance(element, (bytes, bytearray, str)) else 0
        if self._payload_size and payload_len > 0:
            self._payload_size.update(payload_len)

        start_time = time.time()
        try:
            # Decode bytes to string
            decoded_str = decode_kafka_value(element)

            # Track decode latency
            if self._decode_latency:
                latency_ms = int((time.time() - start_time) * 1000)
                self._decode_latency.update(latency_ms)

            if decoded_str is None:
                self._logger.warning(f"[{self.topic_name}] Received None after decode, skipping")
                return

            # Parse JSON string to dict
            try:
                if isinstance(decoded_str, dict):
                    parsed = decoded_str  # Already a dict (from Avro)
                elif isinstance(decoded_str, str):
                    parsed = json.loads(decoded_str) if decoded_str else {}
                else:
                    parsed = {"_raw": str(decoded_str)}
            except json.JSONDecodeError as e:
                self._logger.warning(f"[{self.topic_name}] JSON parse error: {e}")
                parsed = {"_raw": decoded_str, "_parse_error": str(e)}

            self._ok.inc()
            self._success_count += 1

            # Build result dict with metadata
            result = {
                "_topic": self.topic_name,
                "_ingested_at": timestamp,
                "_message_number": self._message_count,
                **parsed,
            }

            # Debug logging for first N messages
            if self.debug_mode and self._message_count <= 10:
                self._logger.info(f"[{self.topic_name}] Message #{self._message_count}: {json.dumps(parsed, default=str)[:500]}")

            # Log every Nth successful decode to confirm messages are flowing
            if self._log_every_n_success > 0 and self._success_count % self._log_every_n_success == 0:
                self._logger.info(
                    "✓ Kafka messages flowing: %d messages decoded successfully (latest payload_len=%s)",
                    self._success_count,
                    payload_len if payload_len > 0 else "n/a",
                )

            yield result

        except Exception as exc:  # defensive: avoid worker crashes on bad payloads
            self._errors.inc()
            self._errors_seen += 1
            should_log = (
                self._logs_emitted < self._max_logs
                or (self._every_n_errors > 0 and self._errors_seen % self._every_n_errors == 0)
            )
            if should_log:
                self._logs_emitted += 1
                self._logger.warning(
                    "Kafka value decode failed (%s: %s). payload_type=%s payload_len=%s",
                    type(exc).__name__,
                    exc,
                    type(element).__name__,
                    (len(element) if isinstance(element, (bytes, bytearray, str)) else "n/a"),
                )


# =============================================================================
# DoFn CLASSES - TRANSFORM
# =============================================================================

# class MapMemberTierUpgradeRawDoFn(DoFn):
#     """Map message to member_tier_upgraded_raw schema."""
    
#     def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
#         try:
#             payload = element.get("payload", {})
#             self._logger.info(f"Mapping upgraded message payload: {json.dumps(payload, default=str)[:200]}")
#             yield {
#                 "eventId": element.get("eventId"),
#                 "source": element.get("source"),
#                 "eventName": element.get("eventName"),
#                 "timestamp": element.get("timestamp"),
#                 "accountId": payload.get("accountId"),
#                 "memberId": payload.get("memberId"),
#                 "tierEventId": payload.get("tierEventId"),
#                 "tierCode": payload.get("tierCode"),
#                 "isExistingTier": payload.get("isExistingTier"),
#                 "triggerType": payload.get("triggerType"),
#                 "processedAt": payload.get("processedAt"),
#                 "_ingested_at": element.get("_ingested_at"),
#                 "_source_topic": element.get("_topic"),
#             }
#         except Exception as e:
#             self._logger.error(f"Error mapping upgraded message: {e}")


# class MapMemberTierDowngradeRawDoFn(DoFn):
#     """Map message to member_tier_downgraded_raw schema."""
    
#     def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
#         try:
#             payload = element.get("payload", {})
#             self._logger.info(f"Mapping downgraded message payload: {json.dumps(payload, default=str)[:200]}")
#             yield {
#                 "eventId": element.get("eventId"),
#                 "source": element.get("source"),
#                 "eventName": element.get("eventName"),
#                 "timestamp": element.get("timestamp"),
#                 "accountId": payload.get("accountId"),
#                 "memberId": payload.get("memberId"),
#                 "tierEventId": payload.get("tierEventId"),
#                 "tierCode": payload.get("tierCode"),
#                 "triggerType": payload.get("triggerType"),
#                 "processedAt": payload.get("processedAt"),
#                 "_ingested_at": element.get("_ingested_at"),
#                 "_source_topic": element.get("_topic"),
#             }
#         except Exception as e:
#             self._logger.error(f"Error mapping downgraded message: {e}")

# =============================================================================
# DoFn CLASSES - TRANSFORM TO ICEBERG ROWS (for Beam IcebergIO)
# =============================================================================

class ToUpgradedRowDoFn(DoFn):
    """Convert dict to MemberTierUpgradedRow (NamedTuple)."""
    
    def __init__(self):
        self._logger = None
    
    def setup(self):
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")

    def process(self, element: Dict[str, Any]) -> Iterator[MemberTierUpgradedRow]:
        try:
            payload = element.get("payload", {})
            yield MemberTierUpgradedRow(
                eventId=str(element.get("eventId", "")),
                source=element.get("source"),
                eventName=element.get("eventName"),
                timestamp=parse_timestamp(element.get("timestamp")),
                accountId=payload.get("accountId"),
                memberId=payload.get("memberId"),
                tierEventId=payload.get("tierEventId"),
                tierCode=payload.get("tierCode"),
                isExistingTier=bool(payload.get("isExistingTier")) if payload.get("isExistingTier") is not None else None,
                triggerType=payload.get("triggerType"),
                processedAt=payload.get("processedAt"),
                ingested_at=element.get("_ingested_at", datetime.now(timezone.utc)),
                source_topic=element.get("_topic", "upgraded"),
            )
        except Exception as e:
            self._logger.error(f"Error converting to UpgradedRow: {e}")


class ToDowngradedRowDoFn(DoFn):
    """Convert dict to MemberTierDowngradedRow (NamedTuple)."""
    
    def __init__(self):
        self._logger = None
    
    def setup(self):
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")
    
    def process(self, element: Dict[str, Any]) -> Iterator[MemberTierDowngradedRow]:
        try:
            payload = element.get("payload", {})
            yield MemberTierDowngradedRow(
                eventId=str(element.get("eventId", "")),
                source=element.get("source"),
                eventName=element.get("eventName"),
                timestamp=parse_timestamp(element.get("timestamp")),
                accountId=payload.get("accountId"),
                memberId=payload.get("memberId"),
                tierEventId=payload.get("tierEventId"),
                tierCode=payload.get("tierCode"),
                triggerType=payload.get("triggerType"),
                processedAt=payload.get("processedAt"),
                ingested_at=element.get("_ingested_at", datetime.now(timezone.utc)),
                source_topic=element.get("_topic", "downgraded"),
            )
        except Exception as e:
            self._logger.error(f"Error converting to DowngradedRow: {e}")


class ToMemberTierRawRowDoFn(DoFn):
    """Convert API response to MemberTierRawRow (NamedTuple)."""
        
    def __init__(self):
        self._logger = None
    
    def setup(self):
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")

    def process(self, element: Dict[str, Any]) -> Iterator[MemberTierRawRow]:
        try:
            api_response = element.get("api_response", {})
            member = api_response.get("member", api_response)
            yield MemberTierRawRow(
                id=member.get("accountId"),
                member_id=member.get("memberId"),
                tier_code=member.get("code"),
                program_code=member.get("programCode"),
                partner_code=member.get("partnerCode"),
                start_date=member.get("startDate"),
                expiry_date=member.get("expiryDate"),
                name=member.get("name"),
                level_name=member.get("levelName"),
                ingested_at=element.get("_ingested_at", datetime.now(timezone.utc)),
                source_event_id=element.get("_source_event_id"),
                source_topic=element.get("_source_topic"),
            )
        except Exception as e:
            self._logger.error(f"Error converting to MemberTierRawRow: {e}")


# =============================================================================
# DoFn CLASSES - TRANSFORM TO DICT (for PyIceberg)
# =============================================================================

class ToUpgradedDictDoFn(DoFn):
    """Convert to dict format for PyIceberg."""
        
    def __init__(self):
        self._logger = None
    
    def setup(self):
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")

    def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        try:
            payload = element.get("payload", {})
            yield {
                "eventId": str(element.get("eventId", "")),
                "source": element.get("source"),
                "eventName": element.get("eventName"),
                "timestamp": parse_timestamp(element.get("timestamp")),
                "accountId": payload.get("accountId"),
                "memberId": payload.get("memberId"),
                "tierEventId": payload.get("tierEventId"),
                "tierCode": payload.get("tierCode"),
                "isExistingTier": bool(payload.get("isExistingTier")) if payload.get("isExistingTier") is not None else None,
                "triggerType": payload.get("triggerType"),
                "processedAt": payload.get("processedAt"),
                "ingested_at": element.get("_ingested_at", datetime.now(timezone.utc)),
                "source_topic": element.get("_topic", "upgraded"),
            }
        except Exception as e:
            self._logger.error(f"Error converting to dict: {e}")


class ToDowngradedDictDoFn(DoFn):
    """Convert to dict format for PyIceberg."""
        
    def __init__(self):
        self._logger = None
    
    def setup(self):
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")

    def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        try:
            payload = element.get("payload", {})
            yield {
                "eventId": str(element.get("eventId", "")),
                "source": element.get("source"),
                "eventName": element.get("eventName"),
                "timestamp": parse_timestamp(element.get("timestamp")),
                "accountId": payload.get("accountId"),
                "memberId": payload.get("memberId"),
                "tierEventId": payload.get("tierEventId"),
                "tierCode": payload.get("tierCode"),
                "triggerType": payload.get("triggerType"),
                "processedAt": payload.get("processedAt"),
                "ingested_at": element.get("_ingested_at", datetime.now(timezone.utc)),
                "source_topic": element.get("_topic", "downgraded"),
            }
        except Exception as e:
            self._logger.error(f"Error converting to dict: {e}")


class ToMemberTierDictDoFn(DoFn):
    """Convert API response to dict for PyIceberg."""
        
    def __init__(self):
        self._logger = None
    
    def setup(self):
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")

    def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        try:
            api_response = element.get("api_response", {})
            member = api_response.get("member", api_response)
            yield {
                "id": member.get("accountId"),
                "member_id": member.get("memberId"),
                "tier_code": member.get("code"),
                "program_code": member.get("programCode"),
                "partner_code": member.get("partnerCode"),
                "start_date": member.get("startDate"),
                "expiry_date": member.get("expiryDate"),
                "name": member.get("name"),
                "level_name": member.get("levelName"),
                "ingested_at": element.get("_ingested_at", datetime.now(timezone.utc)),
                "source_event_id": element.get("_source_event_id"),
                "source_topic": element.get("_source_topic"),
            }
        except Exception as e:
            self._logger.error(f"Error converting to dict: {e}")


# =============================================================================
# DoFn CLASSES - API CALL
# =============================================================================

class CallMemberTierInfoAPIDoFn(DoFn):
    """Call Member Tier API to get member info."""
    
    def __init__(self, api_secret_project: str, api_secret_id: str):
        self.api_secret_project = api_secret_project
        self.api_secret_id = api_secret_id
        # self._client: Optional[MemberTierAPIClient] = None
        self._client = None  # Will be initialized in setup()
        self._logger = None
    
    def setup(self):
        """Initialize API client (called once per worker)."""
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")
        self._logger.info("CallMemberTierInfoAPIDoFn.setup() called")
        self._client = MemberTierAPIClient(self.api_secret_project, self.api_secret_id)
        self._logger.info("MemberTierAPIClient initialized successfully")
    
    def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        payload = element.get("payload", {})
        member_id = payload.get("memberId")
        self._logger.info(f"CallMemberTierInfoAPIDoFn payload={payload}")
        self._logger.info(f"Calling API for member_id={member_id}")
        if not member_id:
            self._logger.warning("No memberId in message, skipping API call")
            return
        
        api_response = self._client.get_member_accounts(member_id)
        
        if api_response:
            self._logger.info(f"Calling API for api_response={api_response}")
            yield {
                "_source_event_id": element.get("eventId"),
                "_source_topic": element.get("_topic"),
                "_ingested_at": element.get("_ingested_at"),
                "api_response": api_response,
            }
        else:
            self._logger.warning(f"API call failed for member_id={member_id}, skipping")


# class MapMemberTierRawDoFn(DoFn):
#     """Map API response to member_tier_raw schema."""
    
#     def process(self, element: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
#         try:
#             api_response = element.get("api_response", {})
#             self._logger.info(f"MapMemberTierRawDoFn element={element}")
#             member = api_response.get("member", api_response)
#             self._logger.info(f"Mapping member tier raw for member_id={member.get('memberId')}")
            
#             yield {
#                 "id": member.get("accountId"),
#                 "member_id": member.get("memberId"),
#                 "tier_code": member.get("code"),
#                 "program_code": member.get("programCode"),
#                 "partner_code": member.get("partnerCode"),
#                 "start_date": member.get("startDate"),
#                 "expiry_date": member.get("expiryDate"),
#                 "name": member.get("name"),
#                 "level_name": member.get("levelName"),
#                 "_ingested_at": element.get("_ingested_at"),
#                 "_source_event_id": element.get("_source_event_id"),
#                 "_source_topic": element.get("_source_topic"),
#             }
#         except Exception as e:
#             self._logger.error(f"Error mapping member tier raw: {e}")


# # =============================================================================
# # DoFn CLASSES - WRITE TO GCS PARQUET
# # =============================================================================

# class WriteParquetToGCSDoFn(DoFn):
#     """Write records to GCS as Parquet files."""
    
#     def __init__(self, output_path: str, table_name: str, schema: pa.Schema):
#         self.output_path = output_path.rstrip("/")
#         self.table_name = table_name
#         self.schema = schema
    
#     def process(self, element: Tuple[str, list]) -> Iterator[str]:
#         window_key, records = element
#         records_list = list(records)
        
#         if not records_list:
#             self._logger.info(f"WriteParquetToGCSDoFn: No records to write for {self.table_name}")
#             return
        
#         now = datetime.now(timezone.utc)
#         partition_path = f"year={now.year}/month={now.month:02d}/day={now.day:02d}"
#         file_name = f"{self.table_name}_{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
#         full_path = f"{self.output_path}/{self.table_name}/{partition_path}/{file_name}"
        
#         try:
#             self._logger.info(f"Writing {len(records_list)} records to {full_path}")
#             table = pa.Table.from_pylist(records_list, schema=self.schema)
            
#             buffer = BytesIO()
#             pq.write_table(table, buffer, compression="snappy")
#             buffer.seek(0)
            
#             with FileSystems.create(full_path) as f:
#                 f.write(buffer.getvalue())
            
#             self._logger.info(f"✓ Written {len(records_list)} records to {full_path}")
#             yield full_path
            
#         except Exception as e:
#             self._logger.error(f"Error writing Parquet to {full_path}: {e}")
# =============================================================================
# DoFn CLASSES - PYICEBERG WRITER
# =============================================================================

class WriteToIcebergWithPyIcebergDoFn(DoFn):
    """Write records to Iceberg table using PyIceberg.
    
    This DoFn batches records per window and commits to Iceberg.
    Uses optimistic concurrency for safe concurrent writes.
    """
    
    def __init__(self, warehouse_location: str, database: str, table_name: str, 
                 schema: pa.Schema, project_id: str):
        self.warehouse_location = warehouse_location
        self.database = database
        self.table_name = table_name
        self.schema = schema
        self.project_id = project_id
        self._catalog = None
        self._table = None
        self._logger = None
    
    def setup(self):
        """Initialize PyIceberg catalog and table."""
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")
        try:
            from pyiceberg.catalog import load_catalog

            # Use HadoopCatalog - stores metadata in GCS alongside data
            # This allows all workers to see the same table state
            # No external metastore needed - metadata is in warehouse path
            self._catalog = load_catalog(
                ICEBERG_CATALOG_NAME,
                **{
                    "type": "hadoop",
                    "warehouse": self.warehouse_location,
                    # PyIceberg uses pyarrow.fs or gcsfs for GCS
                    # GCS credentials come from ADC (Application Default Credentials)
                    "py-io-impl": "pyiceberg.io.pyarrow.PyArrowFileIO",
                }
            )

            self._logger.info(f"✓ PyIceberg HadoopCatalog initialized: warehouse={self.warehouse_location}")

            # Try to load existing table
            table_identifier = f"{self.database}.{self.table_name}"
            try:
                self._table = self._catalog.load_table(table_identifier)
                self._logger.info(f"✓ Loaded existing Iceberg table: {table_identifier}")
            except Exception as e:
                self._logger.info(f"Table {table_identifier} not found ({e}), will create on first write")
                self._table = None

        except ImportError as e:
            self._logger.error(f"PyIceberg not installed. Run: pip install 'pyiceberg[gcsfs]' - {e}")
            raise
        except Exception as e:
            self._logger.error(f"Failed to setup PyIceberg: {e}")
            raise
    
    def _create_table_if_not_exists(self):
        """Create Iceberg table if it doesn't exist."""
        if self._table is not None:
            return
        
        try:
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField

            # Convert PyArrow schema to Iceberg schema
            iceberg_fields = []
            for i, field in enumerate(self.schema):
                iceberg_type = self._pyarrow_to_iceberg_type(field.type)
                iceberg_fields.append(
                    NestedField(
                        field_id=i + 1,
                        name=field.name,
                        field_type=iceberg_type,
                        required=not field.nullable
                    )
                )
            
            iceberg_schema = Schema(*iceberg_fields)
            
            # Create table
            table_identifier = f"{self.database}.{self.table_name}"
            table_location = f"{self.warehouse_location}/{self.database}/{self.table_name}"
            
            # Ensure namespace exists
            try:
                self._catalog.create_namespace(self.database)
            except Exception:
                pass  # Namespace may already exist
            
            self._table = self._catalog.create_table(
                identifier=table_identifier,
                schema=iceberg_schema,
                location=table_location,
            )
            self._logger.info(f"✓ Created Iceberg table: {table_identifier} at {table_location}")
            
        except Exception as e:
            self._logger.error(f"Failed to create Iceberg table: {e}")
            raise
    
    def _pyarrow_to_iceberg_type(self, pa_type):
        """Convert PyArrow type to Iceberg type."""
        from pyiceberg.types import StringType, BooleanType, TimestamptzType
        
        if pa.types.is_string(pa_type):
            return StringType()
        elif pa.types.is_boolean(pa_type):
            return BooleanType()
        elif pa.types.is_timestamp(pa_type):
            return TimestamptzType()
        else:
            return StringType()  # Default to string
    
    def process(self, element: Tuple[str, List[Dict[str, Any]]]) -> Iterator[str]:
        """Write batched records to Iceberg."""
        window_key, records = element
        records_list = list(records)
        
        if not records_list:
            self._logger.info(f"PyIceberg: No records to write for {self.table_name}")
            return
        
        try:
            # Ensure table exists
            self._create_table_if_not_exists()
            
            # Convert to PyArrow table
            arrow_table = pa.Table.from_pylist(records_list, schema=self.schema)
            
            # Append data with retry for concurrent write conflicts
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    self._table.append(arrow_table)
                    break
                except Exception as e:
                    if "CommitFailedException" in str(e) and attempt < max_retries - 1:
                        self._logger.warning(f"Concurrent write conflict, retrying ({attempt + 1}/{max_retries})")
                        time.sleep(0.5 * (attempt + 1))
                        # Reload table to get latest metadata
                        table_identifier = f"{self.database}.{self.table_name}"
                        self._table = self._catalog.load_table(table_identifier)
                    else:
                        raise
            
            table_identifier = f"{self.database}.{self.table_name}"
            self._logger.info(f"✓ PyIceberg: Written {len(records_list)} records to {table_identifier}")
            yield f"{table_identifier}:{window_key}:{len(records_list)}"
            
        except Exception as e:
            self._logger.error(f"PyIceberg write error for {self.table_name}: {e}")
            import traceback
            self._logger.error(traceback.format_exc())





class AddWindowKeyDoFn(DoFn):
    """Add window key for grouping."""
    def __init__(self):
        self._logger = None
    
    def setup(self):
        self._logger = logging.getLogger(f"{MODULE_LOGGER_NAME}.{self.__class__.__name__}")

    def process(self, element, window=DoFn.WindowParam):
        window_key = f"{window.start.to_utc_datetime().isoformat()}"
        yield (window_key, element)


# # =============================================================================
# # PIPELINE HELPER TRANSFORMS
# # =============================================================================

# class WriteToParquet(PTransform):
#     """Composite transform to write records to GCS Parquet with windowing."""
    
#     def __init__(self, output_path: str, table_name: str, schema: pa.Schema):
#         self.output_path = output_path
#         self.table_name = table_name
#         self.schema = schema
    
#     def expand(self, pcoll):
#         return (
#             pcoll
#             | f"AddWindowKey_{self.table_name}" >> beam.ParDo(AddWindowKeyDoFn())
#             | f"GroupByWindow_{self.table_name}" >> beam.GroupByKey()
#             | f"WriteParquet_{self.table_name}" >> beam.ParDo(
#                 WriteParquetToGCSDoFn(self.output_path, self.table_name, self.schema)
#             )
#         )

# =============================================================================
# COMPOSITE TRANSFORMS 
# =============================================================================

class WriteToIcebergPyIceberg(PTransform):
    """Write to Iceberg using PyIceberg (batch append per window)."""

    def __init__(self, warehouse_location: str, database: str, table_name: str,
                 schema: pa.Schema, project_id: str):
        self.warehouse_location = warehouse_location
        self.database = database
        self.table_name = table_name
        self.schema = schema
        self.project_id = project_id
    
    def expand(self, pcoll):
        return (
            pcoll
            | f"AddWindowKey_{self.table_name}" >> beam.ParDo(AddWindowKeyDoFn())
            | f"GroupByWindow_{self.table_name}" >> beam.GroupByKey()
            | f"WriteIceberg_{self.table_name}" >> beam.ParDo(
                WriteToIcebergWithPyIcebergDoFn(
                    self.warehouse_location, self.database, self.table_name,
                    self.schema, self.project_id
                )
            )
        )

# =============================================================================
# MAIN PIPELINE
# =============================================================================

def run():
    """Main pipeline entry point."""
    parser = argparse.ArgumentParser(description="Member Tiers Kafka Consumer Pipeline")

    # Kafka arguments
    parser.add_argument("--secret_project", required=True,
                        help="GCP Project ID where Kafka secret is stored")
    parser.add_argument("--secret_id", default=DEFAULT_KAFKA_SECRET_NAME,
                        help="Secret Manager secret ID for Kafka credentials")
    parser.add_argument("--topic_type", default="both", choices=["upgraded", "downgraded", "both"],
                        help="Which topic(s) to consume")
    parser.add_argument("--auto_offset_reset", default="latest", choices=["latest", "earliest"],
                        help="Where to start reading")
    
    # API arguments
    parser.add_argument("--api_secret_project", default=None,
                        help="GCP Project ID where API secret is stored")
    parser.add_argument("--api_secret_id", default=DEFAULT_API_SECRET_NAME,
                        help="Secret Manager secret ID for API credentials")
    
    # Output arguments
    parser.add_argument("--output_path", default=DEFAULT_GCS_OUTPUT_BUCKET,
                        help="GCS path for output Parquet files")
    parser.add_argument("--window_size", type=int, default=WINDOW_SIZE_SECONDS,
                        help="Window size in seconds for micro-batching")
    
    # Writer mode
    parser.add_argument("--writer_mode", default="beam_iceberg",
                        choices=["beam_iceberg", "pyiceberg"],
                        help="Iceberg writer mode: beam_iceberg (X-Lang) or pyiceberg (Pure Python)")
    
    # Debug arguments
    parser.add_argument("--debug_mode", action="store_true", default=True)
    parser.add_argument("--no_debug", action="store_true")
    parser.add_argument("--skip_api_call", action="store_true",
                        help="Skip API calls (for testing Kafka consume only)")

    known_args, pipeline_args = parser.parse_known_args()

    # # Configure logging
    # logging.basicConfig(
    #     level=logging.INFO,
    #     format="%(asctime)s %(levelname)s %(name)s %(message)s",
    #     stream=sys.stderr,
    # )

    debug_mode = known_args.debug_mode and not known_args.no_debug
    api_secret_project = known_args.api_secret_project or known_args.secret_project

    LOGGER.info("=" * 60)
    LOGGER.info("Member Tiers Kafka Consumer Pipeline (kafkaGCS pattern)")
    LOGGER.info(f"Writer Mode: {known_args.writer_mode}")
    LOGGER.info(f"Secret Project: {known_args.secret_project}")
    LOGGER.info(f"Topic Type: {known_args.topic_type}")
    LOGGER.info(f"Output Path: {known_args.output_path}")
    LOGGER.info(f"Window Size: {known_args.window_size}s")
    LOGGER.info(f"Skip API Call: {known_args.skip_api_call}")
    LOGGER.info("=" * 60)

    # Load Kafka credentials
    load_confluent_env_from_secret_manager(
        known_args.secret_id,
        known_args.secret_project,
        overwrite=True,
    )

    # Pre-flight connectivity check (from kafkaGCS.py)
    bootstrap_servers = os.getenv("CONFLUENT_BOOTSTRAP_SERVERS")
    _log_dns_resolution(bootstrap_servers)
    _preflight_kafka_connectivity(bootstrap_servers)

    # Determine topics
    topics_to_consume = []
    if known_args.topic_type in ("upgraded", "both"):
        topics_to_consume.append(("upgraded", TOPIC_CONFIGS["upgraded"]))
    if known_args.topic_type in ("downgraded", "both"):
        topics_to_consume.append(("downgraded", TOPIC_CONFIGS["downgraded"]))

    # Pipeline options
    options = PipelineOptions(pipeline_args)
    options.view_as(StandardOptions).streaming = True
    
    # For beam_iceberg mode: Enable cross-language transforms
    # Dataflow Runner handles this automatically
    if known_args.writer_mode == "beam_iceberg":
        LOGGER.info("beam_iceberg mode: Using Beam ManagedIO with cross-language transforms")
        LOGGER.info("NOTE: Requires Dataflow Runner or manual expansion service for DirectRunner")

    # ==========================================================================
    # BUILD PIPELINE
    # ==========================================================================

    with beam.Pipeline(options=options) as p:
        extracted_collections = {}
        
        # =====================================================================
        # STEP 1-2: Consume from Kafka and Extract (kafkaGCS.py pattern)
        # =====================================================================
        for topic_type, topic_config in topics_to_consume:
            topic = topic_config["topic"]
            consumer_group = topic_config["consumer_group"]
            safe_name = topic.replace(".", "_")

            consumer_config = build_consumer_config(
                group_id=consumer_group,
                auto_offset_reset=known_args.auto_offset_reset,
            )

            LOGGER.info(
                "Starting Kafka consumer: topic=%s group_id=%s",
                topic, consumer_group,
            )

            # Read from Kafka - EXACTLY like kafkaGCS.py
            # with_metadata=True then Extract Value
            raw_messages = (
                p
                | f"ReadFromKafka_{safe_name}"
                >> ReadFromKafka(
                    consumer_config=consumer_config,
                    topics=[topic],
                    with_metadata=True,  # CRITICAL: Same as kafkaGCS.py
                )
                # | f"ExtractValue_{safe_name}" >> beam.Map(lambda record: record.value)
                | f"ExtractValue_{safe_name}" >> beam.Map(extract_kafka_value)
            )

            # Filter out None values (from failed extractions)
            # filtered_messages = (
            #     raw_messages
            #     | f"FilterNone_{safe_name}" >> beam.Filter(lambda x: x is not None)
            # )

            # Decode Kafka value (following kafkaGCS.py pattern)
            decoded = (
                raw_messages
                | f"DecodeKafkaValue_{safe_name}"
                >> beam.ParDo(DecodeKafkaValueDoFn(topic, debug_mode=debug_mode))
            )
            
            extracted_collections[topic_type] = decoded

            # =================================================================
            # STEP 3-4: Map to RAW schema and Write Parquet
            # =================================================================
            windowed = (
                decoded
                | f"Window_{safe_name}" >> beam.WindowInto(
                    FixedWindows(known_args.window_size),
                    trigger=beam.trigger.Repeatedly(
                        beam.trigger.AfterProcessingTime(known_args.window_size)
                    ),
                    accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
                    allowed_lateness=300,
                )
            )
            # ==========================================
            # WRITE TO PARQUET (OPTIONAL)
            # ==========================================

            # if topic_type == "upgraded":
            #     mapped = windowed | "MapUpgradedRaw" >> beam.ParDo(MapMemberTierUpgradeRawDoFn())
            #     _ = mapped | "WriteUpgradedRaw" >> WriteToParquet(
            #         known_args.output_path, "member_tier_upgraded_raw", SCHEMA_UPGRADED_RAW
            #     )
            # else:  # downgraded
            #     mapped = windowed | "MapDowngradedRaw" >> beam.ParDo(MapMemberTierDowngradeRawDoFn())
            #     _ = mapped | "WriteDowngradedRaw" >> WriteToParquet(
            #         known_args.output_path, "member_tier_downgraded_raw", SCHEMA_DOWNGRADED_RAW
            #     )
            # ==========================================
            # WRITE TO ICEBERG
            # ==========================================
            if known_args.writer_mode == "beam_iceberg":
                # =====================================
                # OPTION 1: Beam IcebergIO (X-Lang)
                # =====================================
                try:
                    from apache_beam.transforms.managed import Managed
                    
                    if topic_type == "upgraded":
                        rows = windowed | f"ToDict_{safe_name}" >> beam.ParDo(ToUpgradedDictDoFn())
                        table_id = f"{ICEBERG_DATABASE}.{TABLE_UPGRADED}"
                    else:
                        rows = windowed | f"ToDict_{safe_name}" >> beam.ParDo(ToDowngradedDictDoFn())
                        table_id = f"{ICEBERG_DATABASE}.{TABLE_DOWNGRADED}"
                    
                    # Beam Managed Iceberg config
                    iceberg_config = {
                        "table": table_id,
                        "catalog_name": ICEBERG_CATALOG_NAME,
                        "catalog_properties": {
                            "type": "hadoop",
                            "warehouse": known_args.output_path,
                        },
                        "triggering_frequency_seconds": known_args.window_size,
                    }
                    
                    _ = rows | f"WriteIceberg_{safe_name}" >> Managed.write(
                        Managed.ICEBERG
                    ).with_config(iceberg_config)
                    
                    LOGGER.info(f"Using Beam ManagedIO (Iceberg) for {table_id}")
                    
                except ImportError as e:
                    LOGGER.warning(f"Beam ManagedIO not available: {e}")
                    LOGGER.warning("Auto-switching to PyIceberg mode...")
                    known_args.writer_mode = "pyiceberg"
                except Exception as e:
                    LOGGER.warning(f"Beam ManagedIO error: {e}")
                    LOGGER.warning("Auto-switching to PyIceberg mode...")
                    known_args.writer_mode = "pyiceberg"
            
            # PyIceberg mode (default and fallback)
            if known_args.writer_mode == "pyiceberg":
                # =====================================
                # OPTION 2: PyIceberg (Pure Python)
                # =====================================
                if topic_type == "upgraded":
                    dicts = windowed | f"ToDict_{safe_name}" >> beam.ParDo(ToUpgradedDictDoFn())
                    _ = dicts | f"WritePyIceberg_{safe_name}" >> WriteToIcebergPyIceberg(
                        known_args.output_path, ICEBERG_DATABASE, TABLE_UPGRADED,
                        SCHEMA_UPGRADED_RAW, known_args.secret_project
                    )
                else:
                    dicts = windowed | f"ToDict_{safe_name}" >> beam.ParDo(ToDowngradedDictDoFn())
                    _ = dicts | f"WritePyIceberg_{safe_name}" >> WriteToIcebergPyIceberg(
                        known_args.output_path, ICEBERG_DATABASE, TABLE_DOWNGRADED,
                        SCHEMA_DOWNGRADED_RAW, known_args.secret_project
                    )
                LOGGER.info(f"Using PyIceberg for {ICEBERG_DATABASE}.{TABLE_UPGRADED if topic_type == 'upgraded' else TABLE_DOWNGRADED}")


        # =====================================================================
        # STEP 5-8: Merge, Call API, Map, Write member_tier_raw
        # =====================================================================
        if not known_args.skip_api_call and len(extracted_collections) > 0:
            if len(extracted_collections) == 2:
                merged = (
                    (extracted_collections["upgraded"], extracted_collections["downgraded"])
                    | "MergeTopics" >> beam.Flatten()
                )
            else:
                merged = list(extracted_collections.values())[0]

            merged_windowed = (
                merged
                | "WindowMerged" >> beam.WindowInto(
                    FixedWindows(known_args.window_size),
                    trigger=beam.trigger.Repeatedly(
                        beam.trigger.AfterProcessingTime(known_args.window_size)
                    ),
                    accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
                    allowed_lateness=300,
                )
            )

            api_results = (
                merged_windowed
                | "CallMemberTierAPI" >> beam.ParDo(
                    CallMemberTierInfoAPIDoFn(api_secret_project, known_args.api_secret_id)
                )
            )
            # ==========================================
            # WRITE TO PARQUET (OPTIONAL)
            # ==========================================
            # member_tier_raw = (
            #     api_results
            #     | "MapMemberTierRaw" >> beam.ParDo(MapMemberTierRawDoFn())
            # )

            # _ = member_tier_raw | "WriteMemberTierRaw" >> WriteToParquet(
            #     known_args.output_path, "member_tier_raw", SCHEMA_MEMBER_TIER_RAW
            # )
            # ==========================================
            # WRITE TO ICEBERG
            # ==========================================
            # Write API results to Iceberg
            if known_args.writer_mode == "beam_iceberg":
                # =====================================
                # OPTION 1: Beam IcebergIO (X-Lang)
                # =====================================
                try:
                    from apache_beam.transforms.managed import Managed
                    
                    member_dicts = api_results | "ToMemberTierDict" >> beam.ParDo(ToMemberTierDictDoFn())
                    
                    iceberg_config = {
                        "table": f"{ICEBERG_DATABASE}.{TABLE_MEMBER_TIER}",
                        "catalog_name": ICEBERG_CATALOG_NAME,
                        "catalog_properties": {
                            "type": "hadoop",
                            "warehouse": known_args.output_path,
                        },
                        "triggering_frequency_seconds": known_args.window_size,
                    }
                    
                    _ = member_dicts | "WriteIcebergMemberTier" >> Managed.write(
                        Managed.ICEBERG
                    ).with_config(iceberg_config)
                    
                except (ImportError, Exception) as e:
                    LOGGER.warning(f"Beam ManagedIO not available for member_tier: {e}")
                    LOGGER.warning("Using PyIceberg for member_tier...")
                    known_args.writer_mode = "pyiceberg"

                # =====================================
                # OPTION 2: PyIceberg (Pure Python)
                # =====================================
            if known_args.writer_mode == "pyiceberg":
                member_dicts = api_results | "ToMemberTierDict" >> beam.ParDo(ToMemberTierDictDoFn())
                _ = member_dicts | "WritePyIcebergMemberTier" >> WriteToIcebergPyIceberg(
                    known_args.output_path, ICEBERG_DATABASE, TABLE_MEMBER_TIER,
                    SCHEMA_MEMBER_TIER_RAW, known_args.secret_project
                )

    LOGGER.info("✓ Pipeline created successfully!")


if __name__ == "__main__":
    run()
