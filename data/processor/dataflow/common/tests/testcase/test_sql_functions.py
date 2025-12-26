"""
Test cases for SQL function mapping and data type conversion.

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest
from datetime import datetime, date

from dataflow_common.dofns.stream import (
    SQL_FUNCTION_MAPPING,
    DATA_TYPE_CONVERTERS,
    convert_value_to_type,
    _convert_to_date_string,
    _convert_to_timestamp_string,
)


# =============================================================================
# Tests: SQL_FUNCTION_MAPPING
# =============================================================================

class TestSQLFunctionMapping:
    """Tests for SQL_FUNCTION_MAPPING constant and functions."""

    @pytest.mark.parametrize("func_name", [
        "CURRENT_DATE()",
        "CURRENT_TIMESTAMP()",
        "NOW()",
        "UUID()",
    ])
    def test_contains_expected_function(self, func_name):
        assert func_name in SQL_FUNCTION_MAPPING

    def test_current_date_returns_valid_format(self):
        result = SQL_FUNCTION_MAPPING["CURRENT_DATE()"]()

        # YYYY-MM-DD format
        assert len(result) == 10
        assert result[4] == "-"
        assert result[7] == "-"

    def test_current_timestamp_returns_valid_format(self):
        result = SQL_FUNCTION_MAPPING["CURRENT_TIMESTAMP()"]()

        # Should start with YYYY-MM-DD HH:MM:SS
        assert result[:4].isdigit()  # Year
        assert "-" in result
        assert ":" in result

    def test_now_returns_valid_format(self):
        result = SQL_FUNCTION_MAPPING["NOW()"]()

        # YYYY-MM-DD HH:MM:SS format
        assert len(result) == 19
        assert result[4] == "-"
        assert result[10] == " "
        assert result[13] == ":"

    def test_uuid_returns_valid_format(self):
        result = SQL_FUNCTION_MAPPING["UUID()"]()

        # UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
        parts = result.split("-")
        assert len(parts) == 5
        assert len(parts[0]) == 8
        assert len(parts[4]) == 12

    def test_uuid_returns_unique_values(self):
        result1 = SQL_FUNCTION_MAPPING["UUID()"]()
        result2 = SQL_FUNCTION_MAPPING["UUID()"]()

        assert result1 != result2


# =============================================================================
# Tests: DATA_TYPE_CONVERTERS
# =============================================================================

class TestDataTypeConverters:
    """Tests for DATA_TYPE_CONVERTERS and convert_value_to_type function."""

    @pytest.mark.parametrize("type_name", [
        "STRING", "INT64", "INTEGER", "FLOAT64", "FLOAT",
        "BOOLEAN", "BOOL", "DATE", "TIMESTAMP", "DATETIME",
    ])
    def test_contains_expected_type(self, type_name):
        assert type_name in DATA_TYPE_CONVERTERS

    @pytest.mark.parametrize("input_val,expected", [
        (123, "123"),
        (123.45, "123.45"),
        (True, "True"),
        ("hello", "hello"),
    ])
    def test_convert_to_string(self, input_val, expected):
        result = convert_value_to_type(input_val, "STRING")
        assert result == expected

    @pytest.mark.parametrize("input_val,expected", [
        ("123", 123),
        (123.7, 123),
        (123, 123),
    ])
    def test_convert_to_int64(self, input_val, expected):
        result = convert_value_to_type(input_val, "INT64")
        assert result == expected

    def test_convert_to_integer_alias(self):
        result = convert_value_to_type("456", "INTEGER")
        assert result == 456

    @pytest.mark.parametrize("input_val,expected", [
        ("123.45", 123.45),
        (123, 123.0),
        ("100", 100.0),
    ])
    def test_convert_to_float64(self, input_val, expected):
        result = convert_value_to_type(input_val, "FLOAT64")
        assert abs(result - expected) < 0.01

    @pytest.mark.parametrize("input_val,expected", [
        (True, True),
        (False, False),
        (1, True),
        (0, False),
        ("true", True),
    ])
    def test_convert_to_boolean(self, input_val, expected):
        result = convert_value_to_type(input_val, "BOOLEAN")
        assert result == expected

    def test_convert_to_date_from_string(self):
        result = convert_value_to_type("2025-12-10", "DATE")
        assert result == "2025-12-10"

    def test_convert_to_date_from_date_object(self):
        date_obj = date(2025, 12, 10)
        result = convert_value_to_type(date_obj, "DATE")
        assert result == "2025-12-10"

    def test_convert_to_timestamp_from_string(self):
        result = convert_value_to_type("2025-12-10 14:30:00", "TIMESTAMP")
        assert result.startswith("2025-12-10 14:30:00")

    def test_convert_to_timestamp_from_datetime_object(self):
        dt_obj = datetime(2025, 12, 10, 14, 30, 0)
        result = convert_value_to_type(dt_obj, "TIMESTAMP")
        assert result.startswith("2025-12-10 14:30:00")

    @pytest.mark.parametrize("data_type", ["STRING", "INT64", "DATE", "TIMESTAMP"])
    def test_convert_none_returns_none(self, data_type):
        result = convert_value_to_type(None, data_type)
        assert result is None

    def test_convert_with_none_data_type_returns_original(self):
        result = convert_value_to_type("hello", None)
        assert result == "hello"

    def test_convert_invalid_raises_error(self):
        with pytest.raises(ValueError):
            convert_value_to_type("not_a_number", "INT64")


# =============================================================================
# Tests: Date/Timestamp Helper Functions
# =============================================================================

class TestDateStringHelpers:
    """Tests for _convert_to_date_string and _convert_to_timestamp_string."""

    def test_convert_to_date_string_iso_format(self):
        result = _convert_to_date_string("2025-12-10")
        assert result == "2025-12-10"

    def test_convert_to_date_string_slash_format(self):
        result = _convert_to_date_string("2025/12/10")
        assert result == "2025-12-10"

    def test_convert_to_date_string_ambiguous_format(self):
        """Ambiguous d/m/Y format - just verify valid output."""
        result = _convert_to_date_string("10/12/2025")
        # Should produce valid YYYY-MM-DD format
        assert len(result) == 10
        assert result[4] == "-"
        assert result[7] == "-"

    @pytest.mark.parametrize("input_val", [
        "2025-12-10 14:30:00",
        "2025-12-10T14:30:00",
    ])
    def test_convert_to_timestamp_string_formats(self, input_val):
        result = _convert_to_timestamp_string(input_val)
        assert result.startswith("2025-12-10 14:30:00")
