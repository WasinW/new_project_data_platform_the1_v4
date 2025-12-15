"""
Test cases for SQL function mapping and data type conversion.

These tests verify the SQL_FUNCTION_MAPPING, DATA_TYPE_CONVERTERS,
and convert_value_to_type functions added in the enhancement.
"""
import unittest
from datetime import datetime, date
from unittest.mock import patch

from dataflow_common.dofns.stream import (
    SQL_FUNCTION_MAPPING,
    DATA_TYPE_CONVERTERS,
    convert_value_to_type,
    _convert_to_date_string,
    _convert_to_timestamp_string,
)


class TestSQLFunctionMapping(unittest.TestCase):
    """Test SQL_FUNCTION_MAPPING constant and functions."""

    def test_sql_function_mapping_contains_expected_functions(self):
        """Verify all expected SQL functions are present."""
        print("\n[Test] SQL_FUNCTION_MAPPING contains expected functions")

        expected_functions = [
            'CURRENT_DATE()',
            'CURRENT_TIMESTAMP()',
            'NOW()',
            'UUID()',
        ]

        for func_name in expected_functions:
            self.assertIn(func_name, SQL_FUNCTION_MAPPING,
                         f"Missing SQL function: {func_name}")
            print(f"   OK: {func_name}")

    def test_current_date_returns_valid_date_string(self):
        """Test CURRENT_DATE() returns date in YYYY-MM-DD format."""
        print("\n[Test] CURRENT_DATE() returns valid date string")

        result = SQL_FUNCTION_MAPPING['CURRENT_DATE()']()

        # Should be in YYYY-MM-DD format
        self.assertRegex(result, r'^\d{4}-\d{2}-\d{2}$')

        # Should be parseable as date
        parsed = datetime.strptime(result, '%Y-%m-%d')
        self.assertIsNotNone(parsed)

        print(f"   OK: {result}")

    def test_current_timestamp_returns_valid_timestamp_string(self):
        """Test CURRENT_TIMESTAMP() returns timestamp in expected format."""
        print("\n[Test] CURRENT_TIMESTAMP() returns valid timestamp")

        result = SQL_FUNCTION_MAPPING['CURRENT_TIMESTAMP()']()

        # Should be in YYYY-MM-DD HH:MM:SS format
        self.assertRegex(result, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')

        # Should be parseable
        parsed = datetime.strptime(result, '%Y-%m-%d %H:%M:%S')
        self.assertIsNotNone(parsed)

        print(f"   OK: {result}")

    def test_now_returns_valid_timestamp_string(self):
        """Test NOW() returns timestamp (same as CURRENT_TIMESTAMP)."""
        print("\n[Test] NOW() returns valid timestamp")

        result = SQL_FUNCTION_MAPPING['NOW()']()

        self.assertRegex(result, r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$')
        print(f"   OK: {result}")

    def test_uuid_returns_valid_uuid_string(self):
        """Test UUID() returns valid UUID format."""
        print("\n[Test] UUID() returns valid UUID string")

        result = SQL_FUNCTION_MAPPING['UUID()']()

        # Should match UUID format
        self.assertRegex(result,
            r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$')

        # Each call should return unique UUID
        result2 = SQL_FUNCTION_MAPPING['UUID()']()
        self.assertNotEqual(result, result2)

        print(f"   OK: {result}")


class TestDataTypeConverters(unittest.TestCase):
    """Test DATA_TYPE_CONVERTERS and convert_value_to_type function."""

    def test_data_type_converters_contains_expected_types(self):
        """Verify all expected data types are supported."""
        print("\n[Test] DATA_TYPE_CONVERTERS contains expected types")

        expected_types = [
            'STRING', 'INT64', 'INTEGER', 'FLOAT64', 'FLOAT',
            'BOOLEAN', 'BOOL', 'DATE', 'TIMESTAMP', 'DATETIME',
        ]

        for type_name in expected_types:
            self.assertIn(type_name, DATA_TYPE_CONVERTERS,
                         f"Missing data type: {type_name}")
            print(f"   OK: {type_name}")

    def test_convert_to_string(self):
        """Test STRING conversion."""
        print("\n[Test] STRING conversion")

        test_cases = [
            (123, "123"),
            (123.45, "123.45"),
            (True, "True"),
            ("hello", "hello"),
        ]

        for input_val, expected in test_cases:
            result = convert_value_to_type(input_val, 'STRING')
            self.assertEqual(result, expected)
            print(f"   OK: {input_val} -> {result}")

    def test_convert_to_int64(self):
        """Test INT64/INTEGER conversion."""
        print("\n[Test] INT64 conversion")

        test_cases = [
            ("123", 123),
            (123.7, 123),
            (123, 123),
        ]

        for input_val, expected in test_cases:
            result = convert_value_to_type(input_val, 'INT64')
            self.assertEqual(result, expected)
            print(f"   OK: {input_val} -> {result}")

        # Test INTEGER alias
        result = convert_value_to_type("456", 'INTEGER')
        self.assertEqual(result, 456)
        print(f"   OK: INTEGER alias works")

    def test_convert_to_float64(self):
        """Test FLOAT64/FLOAT conversion."""
        print("\n[Test] FLOAT64 conversion")

        test_cases = [
            ("123.45", 123.45),
            (123, 123.0),
            ("100", 100.0),
        ]

        for input_val, expected in test_cases:
            result = convert_value_to_type(input_val, 'FLOAT64')
            self.assertAlmostEqual(result, expected)
            print(f"   OK: {input_val} -> {result}")

    def test_convert_to_boolean(self):
        """Test BOOLEAN/BOOL conversion."""
        print("\n[Test] BOOLEAN conversion")

        test_cases = [
            (True, True),
            (False, False),
            (1, True),
            (0, False),
            ("true", True),  # Non-empty string is truthy
        ]

        for input_val, expected in test_cases:
            result = convert_value_to_type(input_val, 'BOOLEAN')
            self.assertEqual(result, expected)
            print(f"   OK: {input_val} -> {result}")

    def test_convert_to_date_string(self):
        """Test DATE conversion to string."""
        print("\n[Test] DATE conversion")

        # String input
        result = convert_value_to_type("2025-12-10", 'DATE')
        self.assertEqual(result, "2025-12-10")
        print(f"   OK: string -> {result}")

        # Date object input
        date_obj = date(2025, 12, 10)
        result = convert_value_to_type(date_obj, 'DATE')
        self.assertEqual(result, "2025-12-10")
        print(f"   OK: date object -> {result}")

    def test_convert_to_timestamp_string(self):
        """Test TIMESTAMP conversion to string."""
        print("\n[Test] TIMESTAMP conversion")

        # String input
        result = convert_value_to_type("2025-12-10 14:30:00", 'TIMESTAMP')
        self.assertEqual(result, "2025-12-10 14:30:00")
        print(f"   OK: string -> {result}")

        # Datetime object input
        dt_obj = datetime(2025, 12, 10, 14, 30, 0)
        result = convert_value_to_type(dt_obj, 'TIMESTAMP')
        self.assertEqual(result, "2025-12-10 14:30:00")
        print(f"   OK: datetime object -> {result}")

    def test_convert_none_returns_none(self):
        """Test that None input returns None."""
        print("\n[Test] None handling")

        for data_type in ['STRING', 'INT64', 'DATE', 'TIMESTAMP']:
            result = convert_value_to_type(None, data_type)
            self.assertIsNone(result)
            print(f"   OK: None -> None for {data_type}")

    def test_convert_with_none_data_type_returns_original(self):
        """Test that None data_type returns original value."""
        print("\n[Test] None data_type handling")

        result = convert_value_to_type("hello", None)
        self.assertEqual(result, "hello")
        print(f"   OK: None data_type returns original")

    def test_convert_invalid_raises_error(self):
        """Test that invalid conversion raises ValueError."""
        print("\n[Test] Invalid conversion raises ValueError")

        with self.assertRaises(ValueError):
            convert_value_to_type("not_a_number", 'INT64')

        print(f"   OK: ValueError raised for invalid conversion")


class TestDateStringHelpers(unittest.TestCase):
    """Test helper functions for date/timestamp conversion."""

    def test_convert_to_date_string_various_formats(self):
        """Test _convert_to_date_string with various input formats."""
        print("\n[Test] _convert_to_date_string with various formats")

        test_cases = [
            ("2025-12-10", "2025-12-10"),
            ("10/12/2025", "2025-12-10"),  # d/m/Y format
            ("2025/12/10", "2025-12-10"),  # Y/m/d format
        ]

        for input_val, expected in test_cases:
            result = _convert_to_date_string(input_val)
            self.assertEqual(result, expected)
            print(f"   OK: {input_val} -> {result}")

    def test_convert_to_timestamp_string_various_formats(self):
        """Test _convert_to_timestamp_string with various input formats."""
        print("\n[Test] _convert_to_timestamp_string with various formats")

        test_cases = [
            ("2025-12-10 14:30:00", "2025-12-10 14:30:00"),
            ("2025-12-10T14:30:00", "2025-12-10 14:30:00"),
        ]

        for input_val, expected in test_cases:
            result = _convert_to_timestamp_string(input_val)
            self.assertEqual(result, expected)
            print(f"   OK: {input_val} -> {result}")


if __name__ == "__main__":
    unittest.main()
