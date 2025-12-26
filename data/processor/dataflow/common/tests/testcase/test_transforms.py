"""
Test cases for transform functions.

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest

from dataflow_common.transforms import (
    normalize_path,
    extract_by_path,
    create_mapping_dict,
    map_record,
    coalesce_by_mapping
)


# =============================================================================
# Tests: normalize_path
# =============================================================================

class TestNormalizePath:
    """Tests for normalize_path function."""

    @pytest.mark.parametrize("input_path,expected", [
        ("profiles.memberId", ["profiles", "memberId"]),
        ("profiles['memberId']", ["profiles", "memberId"]),
        ("['profiles']['memberId']", ["profiles", "memberId"]),
        ("profiles['member'].data", ["profiles", "member", "data"]),
        ("a.b.c.d", ["a", "b", "c", "d"]),
        ("", []),
        (None, []),
        ('profiles["memberId"]', ["profiles", "memberId"]),
        ("profiles[memberId]", ["profiles", "memberId"]),
    ])
    def test_normalize_path(self, input_path, expected):
        result = normalize_path(input_path)
        assert result == expected


# =============================================================================
# Tests: extract_by_path
# =============================================================================

class TestExtractByPath:
    """Tests for extract_by_path function."""

    @pytest.fixture
    def sample_record(self):
        return {
            "level1": {"level2": {"value": "nested_value"}},
            "profiles": '{"memberId": "12345", "email": "test@example.com"}'
        }

    def test_extract_nested_value(self, sample_record):
        path = ["level1", "level2", "value"]
        result = extract_by_path(sample_record, path)
        assert result == "nested_value"

    def test_extract_from_json_string(self, sample_record):
        path = ["profiles", "memberId"]
        result = extract_by_path(sample_record, path)
        assert result == "12345"

    def test_extract_missing_path(self, sample_record):
        path = ["missing", "path"]
        result = extract_by_path(sample_record, path)
        assert result is None


# =============================================================================
# Tests: create_mapping_dict
# =============================================================================

class TestCreateMappingDict:
    """Tests for create_mapping_dict function."""

    def test_create_mapping_dict(self):
        mapping_rows = [
            {
                "RECONCILE_COLUMN_NAME": "MEMBER_NUMBER",
                "MAPPING_COLUMN_NAME": "profiles.memberId",
                "RECONCILE_RETRIEVED": True,
                "RECONCILE_CONFIRMED": False
            },
            {
                "RECONCILE_COLUMN_NAME": "EMAIL",
                "MAPPING_COLUMN_NAME": "profiles.email",
                "RECONCILE_RETRIEVED": True,
                "RECONCILE_CONFIRMED": True
            }
        ]

        mapping_dict = create_mapping_dict(
            mapping_rows,
            src_field="MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )

        assert "MEMBER_NUMBER" in mapping_dict
        assert mapping_dict["MEMBER_NUMBER"]["src_path"] == ["profiles", "memberId"]
        assert mapping_dict["EMAIL"]["reconcile"] is True
        assert mapping_dict["EMAIL"]["original"] is True


# =============================================================================
# Tests: map_record
# =============================================================================

class TestMapRecord:
    """Tests for map_record function."""

    @pytest.fixture
    def sample_data(self):
        record = {"profiles": {"memberId": "123", "email": "test@example.com"}}
        mapping_dict = {
            "MEMBER_NUMBER": {"src_path": ["profiles", "memberId"], "reconcile": True, "original": False},
            "EMAIL": {"src_path": ["profiles", "email"], "reconcile": True, "original": True}
        }
        return record, mapping_dict

    def test_map_record_reconcile_mode(self, sample_data):
        record, mapping_dict = sample_data
        result = map_record(record, mapping_dict, mode="reconcile")

        assert result["MEMBER_NUMBER"] == "123"
        assert result["EMAIL"] == "test@example.com"

    def test_map_record_original_mode(self, sample_data):
        record, mapping_dict = sample_data
        result = map_record(record, mapping_dict, mode="original")

        assert "MEMBER_NUMBER" not in result
        assert "EMAIL" in result


# =============================================================================
# Tests: coalesce_by_mapping
# =============================================================================

class TestCoalesceByMapping:
    """Tests for coalesce_by_mapping function."""

    def test_coalesce_records(self):
        kv = (
            "key123",
            {
                "new": [{"MEMBER_NUMBER": "123", "EMAIL": "new@example.com"}],
                "old": [{"MEMBER_NUMBER": "123", "EMAIL": "old@example.com", "PHONE": "555-1234"}]
            }
        )

        columns = [
            {"RECONCILE_COLUMN_NAME": "EMAIL", "RECONCILE_RETRIEVED": True},
            {"RECONCILE_COLUMN_NAME": "PHONE", "RECONCILE_RETRIEVED": False}
        ]

        result = coalesce_by_mapping(
            kv,
            columns=columns,
            flag_field="RECONCILE_RETRIEVED",
            pk_field="MEMBER_NUMBER",
            dest_field="RECONCILE_COLUMN_NAME"
        )

        assert result["EMAIL"] == "new@example.com"
        assert result["PHONE"] == "555-1234"
        assert result["MEMBER_NUMBER"] == "123"
