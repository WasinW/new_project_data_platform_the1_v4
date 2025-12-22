"""
Shared test fixtures and sample data.

This module contains sample data and mock objects used across different pipeline tests.
"""
import json
from unittest.mock import MagicMock


# =============================================================================
# Sample Mapping Data
# =============================================================================

SAMPLE_MAPPING_ROWS = [
    {
        "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
        "RECONCILE_COLUMN_NAME": "MEMBER_NUMBER",
        "RECONCILE_RETRIEVED": "Y",
        "RECONCILE_CONFIRMED": "Y"
    },
    {
        "PERSONAS_MAPPING_COLUMN_NAME": "profiles.email",
        "RECONCILE_COLUMN_NAME": "EMAIL_ADDRESS",
        "RECONCILE_RETRIEVED": "Y",
        "RECONCILE_CONFIRMED": "Y"
    },
    {
        "PERSONAS_MAPPING_COLUMN_NAME": "profiles.phone",
        "RECONCILE_COLUMN_NAME": "PHONE_NUMBER",
        "RECONCILE_RETRIEVED": "N",
        "RECONCILE_CONFIRMED": "N"
    },
]


# =============================================================================
# Sample Personas Data
# =============================================================================

SAMPLE_PERSONAS_RAW = [
    {
        "personaId": "P001",
        "profiles": '{"memberId": "M123", "email": "alice@example.com"}',
        "timestamp": "2025-01-01 10:00:00"
    },
    {
        "personaId": "P002",
        "profiles": '{"memberId": "M456", "email": "bob@example.com"}',
        "timestamp": "2025-01-01 11:00:00"
    },
]

SAMPLE_PERSONAS_PARSED = [
    {
        "personaId": "P001",
        "profiles": {"memberId": "M123", "email": "alice@example.com"},
        "timestamp": "2025-01-01 10:00:00"
    },
    {
        "personaId": "P002",
        "profiles": {"memberId": "M456", "email": "bob@example.com"},
        "timestamp": "2025-01-01 11:00:00"
    },
]


# =============================================================================
# Sample MS Member Data
# =============================================================================

SAMPLE_MS_MEMBER_ROWS = [
    {"MEMBER_NUMBER": "M123", "EMAIL_ADDRESS": "alice_old@example.com", "PHONE": "111"},
    {"MEMBER_NUMBER": "M789", "EMAIL_ADDRESS": "charlie@example.com", "PHONE": "333"},
]


# =============================================================================
# Sample PubSub Messages
# =============================================================================

SAMPLE_PUBSUB_MESSAGES = [
    b'{"personaId": "P001", "action": "update"}',
    b'{"personaId": "P002", "action": "create"}',
    b'invalid json',
    b'{"action": "delete"}',  # Missing personaId
]


# =============================================================================
# Mock Factories
# =============================================================================

def create_mock_config(name="test_pipeline", mode="batch", term="initial"):
    """Create a mock PipelineConfig object."""
    mock_config = MagicMock()
    mock_config.name = name
    mock_config.mode = mode
    mock_config.term = term
    mock_config.plan = [{"step": "test"}]
    mock_config.params = MagicMock()
    mock_config.params.run_dt = None
    mock_config.params.run_par_month = None
    mock_config.params.run_par_day = None
    mock_config.params.run_par_hour = None
    return mock_config


def create_mock_state():
    """Create a mock state dictionary with pipeline."""
    return {"__pipeline__": MagicMock()}
