"""
Test cases for DoFn classes in dofns/stream.py.

These tests use mocks to avoid GCP dependencies.
"""
import unittest
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, timezone

from dataflow_common.dofns.stream import (
    TransformSchemasDoFn,
    MappingRefreshDoFn,
    SQL_FUNCTION_MAPPING,
)


class TestTransformSchemasDoFn(unittest.TestCase):
    """Test TransformSchemasDoFn class."""

    def setUp(self):
        """Set up test fixtures."""
        self.dofn = TransformSchemasDoFn()

    def test_get_nested_value_simple_path(self):
        """Test getting value from simple nested path."""
        print("\n[Test] get_nested_value with simple path")

        data = {
            "level1": {
                "level2": {
                    "value": "found_it"
                }
            }
        }

        result = self.dofn.get_nested_value(data, "level1.level2.value")
        self.assertEqual(result, "found_it")
        print(f"   OK: level1.level2.value -> {result}")

    def test_get_nested_value_missing_path(self):
        """Test getting value from missing path returns None."""
        print("\n[Test] get_nested_value with missing path")

        data = {"level1": {"level2": "value"}}

        result = self.dofn.get_nested_value(data, "level1.missing.path")
        self.assertIsNone(result)
        print(f"   OK: missing path returns None")

    def test_is_sql_function_true(self):
        """Test isSqlFunction returns True for SQL functions."""
        print("\n[Test] isSqlFunction returns True for SQL functions")

        sql_functions = ['CURRENT_DATE()', 'CURRENT_TIMESTAMP()', 'NOW()', 'UUID()']

        for func in sql_functions:
            result = self.dofn.isSqlFunction(func)
            self.assertTrue(result, f"Should be True for {func}")
            print(f"   OK: {func} -> True")

    def test_is_sql_function_false(self):
        """Test isSqlFunction returns False for non-SQL functions."""
        print("\n[Test] isSqlFunction returns False for non-SQL functions")

        non_functions = ['profiles.memberId', 'some_value', 'not_a_function']

        for val in non_functions:
            result = self.dofn.isSqlFunction(val)
            self.assertFalse(result, f"Should be False for {val}")
            print(f"   OK: {val} -> False")

    def test_is_sql_function_none(self):
        """Test isSqlFunction handles None input."""
        print("\n[Test] isSqlFunction handles None")

        result = self.dofn.isSqlFunction(None)
        self.assertFalse(result)
        print(f"   OK: None -> False")

    def test_sql_function_current_date(self):
        """Test sql_function returns value for CURRENT_DATE()."""
        print("\n[Test] sql_function for CURRENT_DATE()")

        result = self.dofn.sql_function('CURRENT_DATE()')

        self.assertIsNotNone(result)
        self.assertRegex(result, r'^\d{4}-\d{2}-\d{2}$')
        print(f"   OK: CURRENT_DATE() -> {result}")

    def test_sql_function_unknown(self):
        """Test sql_function returns None for unknown function."""
        print("\n[Test] sql_function for unknown function")

        result = self.dofn.sql_function('UNKNOWN_FUNC()')

        self.assertIsNone(result)
        print(f"   OK: UNKNOWN_FUNC() -> None")

    def test_transform_message_old_format(self):
        """Test transform_message with old string-based mapping format."""
        print("\n[Test] transform_message with old format (backward compatibility)")

        message_dict = {
            "profiles": {
                "memberId": "12345",
                "email": "test@example.com"
            }
        }

        # Old format: mapping values are just path strings
        mapping_dict = {
            "ms_member": {
                "gcp": {
                    "member_id": "profiles.memberId",
                    "email_address": "profiles.email"
                }
            }
        }

        result = self.dofn.transform_message(
            message_dict, mapping_dict,
            target='gcp', table_name='ms_member'
        )

        self.assertEqual(result['member_id'], '12345')
        self.assertEqual(result['email_address'], 'test@example.com')
        print(f"   OK: Old format works - {result}")

    def test_transform_message_new_format_path(self):
        """Test transform_message with new dict-based mapping format (path type)."""
        print("\n[Test] transform_message with new format - path type")

        message_dict = {
            "profiles": {
                "memberId": "12345"
            }
        }

        # New format: mapping values are dicts with type, value, data_type
        mapping_dict = {
            "ms_member": {
                "gcp": {
                    "member_id": {
                        "type": "path",
                        "value": "profiles.memberId",
                        "data_type": "STRING"
                    }
                }
            }
        }

        result = self.dofn.transform_message(
            message_dict, mapping_dict,
            target='gcp', table_name='ms_member'
        )

        self.assertEqual(result['member_id'], '12345')
        print(f"   OK: New format path type works - {result}")

    def test_transform_message_new_format_logic(self):
        """Test transform_message with new format (logic type for SQL function)."""
        print("\n[Test] transform_message with new format - logic type")

        message_dict = {}

        mapping_dict = {
            "ms_member": {
                "gcp": {
                    "created_date": {
                        "type": "logic",
                        "value": "CURRENT_DATE()",
                        "data_type": "DATE"
                    }
                }
            }
        }

        result = self.dofn.transform_message(
            message_dict, mapping_dict,
            target='gcp', table_name='ms_member'
        )

        self.assertIn('created_date', result)
        self.assertRegex(result['created_date'], r'^\d{4}-\d{2}-\d{2}$')
        print(f"   OK: Logic type works - {result}")

    def test_transform_message_new_format_constant(self):
        """Test transform_message with new format (constant type)."""
        print("\n[Test] transform_message with new format - constant type")

        message_dict = {}

        mapping_dict = {
            "ms_member": {
                "gcp": {
                    "source_system": {
                        "type": "constant",
                        "value": "THE1",
                        "data_type": "STRING"
                    }
                }
            }
        }

        result = self.dofn.transform_message(
            message_dict, mapping_dict,
            target='gcp', table_name='ms_member'
        )

        self.assertEqual(result['source_system'], 'THE1')
        print(f"   OK: Constant type works - {result}")

    def test_transform_message_data_type_conversion(self):
        """Test transform_message converts data types correctly."""
        print("\n[Test] transform_message with data type conversion")

        message_dict = {
            "age": "30",
            "score": "85.5",
            "active": "true"
        }

        mapping_dict = {
            "ms_member": {
                "gcp": {
                    "age_int": {
                        "type": "path",
                        "value": "age",
                        "data_type": "INT64"
                    },
                    "score_float": {
                        "type": "path",
                        "value": "score",
                        "data_type": "FLOAT64"
                    }
                }
            }
        }

        result = self.dofn.transform_message(
            message_dict, mapping_dict,
            target='gcp', table_name='ms_member'
        )

        self.assertEqual(result['age_int'], 30)
        self.assertIsInstance(result['age_int'], int)
        self.assertAlmostEqual(result['score_float'], 85.5)
        print(f"   OK: Data type conversion works - {result}")

    def test_transform_message_missing_table(self):
        """Test transform_message returns empty dict for missing table."""
        print("\n[Test] transform_message with missing table")

        message_dict = {"data": "value"}
        mapping_dict = {"other_table": {"gcp": {}}}

        result = self.dofn.transform_message(
            message_dict, mapping_dict,
            target='gcp', table_name='missing_table'
        )

        self.assertEqual(result, {})
        print(f"   OK: Missing table returns empty dict")


class TestMappingRefreshDoFn(unittest.TestCase):
    """Test MappingRefreshDoFn class."""

    def test_build_mapping_value_path_type(self):
        """Test _build_mapping_value returns path type correctly."""
        print("\n[Test] _build_mapping_value with path type")

        dofn = MappingRefreshDoFn(
            mapping_table="project.dataset.table",
            project_id="test-project"
        )

        row = {
            'mapping_column_name': 'profiles.memberId',
            'mapping_logic': None,
            'mapping_column_type': 'STRING'
        }

        result = dofn._build_mapping_value(row)

        self.assertEqual(result['type'], 'path')
        self.assertEqual(result['value'], 'profiles.memberId')
        self.assertEqual(result['data_type'], 'STRING')
        print(f"   OK: {result}")

    def test_build_mapping_value_logic_type(self):
        """Test _build_mapping_value returns logic type for SQL function."""
        print("\n[Test] _build_mapping_value with logic type")

        dofn = MappingRefreshDoFn(
            mapping_table="project.dataset.table",
            project_id="test-project"
        )

        row = {
            'mapping_column_name': None,
            'mapping_logic': 'CURRENT_DATE()',
            'mapping_column_type': 'DATE'
        }

        result = dofn._build_mapping_value(row)

        self.assertEqual(result['type'], 'logic')
        self.assertEqual(result['value'], 'CURRENT_DATE()')
        self.assertEqual(result['data_type'], 'DATE')
        print(f"   OK: {result}")

    def test_build_mapping_value_constant_type(self):
        """Test _build_mapping_value returns constant type for non-SQL logic."""
        print("\n[Test] _build_mapping_value with constant type")

        dofn = MappingRefreshDoFn(
            mapping_table="project.dataset.table",
            project_id="test-project"
        )

        row = {
            'mapping_column_name': None,
            'mapping_logic': 'DEFAULT_VALUE',
            'mapping_column_type': 'STRING'
        }

        result = dofn._build_mapping_value(row)

        self.assertEqual(result['type'], 'constant')
        self.assertEqual(result['value'], 'DEFAULT_VALUE')
        self.assertEqual(result['data_type'], 'STRING')
        print(f"   OK: {result}")

    def test_build_mapping_value_null_value(self):
        """Test _build_mapping_value handles null values correctly."""
        print("\n[Test] _build_mapping_value with null values")

        dofn = MappingRefreshDoFn(
            mapping_table="project.dataset.table",
            project_id="test-project"
        )

        row = {
            'mapping_column_name': None,
            'mapping_logic': None,
            'mapping_column_type': 'STRING'
        }

        result = dofn._build_mapping_value(row)

        self.assertEqual(result['type'], 'constant')
        self.assertIsNone(result['value'])
        self.assertEqual(result['data_type'], 'STRING')
        print(f"   OK: {result}")

    def test_build_mapping_value_empty_string(self):
        """Test _build_mapping_value handles empty strings correctly."""
        print("\n[Test] _build_mapping_value with empty strings")

        dofn = MappingRefreshDoFn(
            mapping_table="project.dataset.table",
            project_id="test-project"
        )

        row = {
            'mapping_column_name': '',
            'mapping_logic': '',
            'mapping_column_type': 'STRING'
        }

        result = dofn._build_mapping_value(row)

        # Empty strings should be treated as constant with empty value
        self.assertEqual(result['type'], 'constant')
        print(f"   OK: {result}")


if __name__ == "__main__":
    unittest.main()
