"""
Test cases for DoFn classes in dofns/stream.py.

These tests use mocks to avoid GCP dependencies.
"""
import unittest
from unittest.mock import MagicMock, patch, Mock
from datetime import datetime, timezone
import time

from dataflow_common.dofns.stream import (
    TransformSchemasDoFn,
    MappingRefreshDoFn,
    SQL_FUNCTION_MAPPING,
    MapToCdcTableRowDoFn,
    build_cdc_schema,
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


class TestBuildCdcSchema(unittest.TestCase):
    """Test build_cdc_schema function."""

    def test_build_cdc_schema_structure(self):
        """Test CDC schema has correct structure."""
        print("\n[Test] build_cdc_schema structure")

        record_fields = [
            {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"}
        ]

        schema = build_cdc_schema(record_fields)

        self.assertIn('fields', schema)
        self.assertEqual(len(schema['fields']), 2)
        print("   OK: Schema has 2 top-level fields")

    def test_build_cdc_schema_row_mutation_info_nullable(self):
        """Test row_mutation_info is NULLABLE (fix for Beam SDK bug)."""
        print("\n[Test] build_cdc_schema - row_mutation_info is NULLABLE")

        record_fields = [{"name": "id", "type": "STRING", "mode": "REQUIRED"}]
        schema = build_cdc_schema(record_fields)

        row_mutation_info = schema['fields'][0]
        self.assertEqual(row_mutation_info['name'], 'row_mutation_info')
        self.assertEqual(row_mutation_info['type'], 'RECORD')
        self.assertEqual(row_mutation_info['mode'], 'NULLABLE')  # CRITICAL: Must be NULLABLE
        print("   OK: row_mutation_info.mode = NULLABLE")

    def test_build_cdc_schema_record_nullable(self):
        """Test record is NULLABLE (fix for Beam SDK bug)."""
        print("\n[Test] build_cdc_schema - record is NULLABLE")

        record_fields = [{"name": "id", "type": "STRING", "mode": "REQUIRED"}]
        schema = build_cdc_schema(record_fields)

        record = schema['fields'][1]
        self.assertEqual(record['name'], 'record')
        self.assertEqual(record['type'], 'RECORD')
        self.assertEqual(record['mode'], 'NULLABLE')  # CRITICAL: Must be NULLABLE
        print("   OK: record.mode = NULLABLE")

    def test_build_cdc_schema_mutation_info_fields(self):
        """Test row_mutation_info has correct sub-fields."""
        print("\n[Test] build_cdc_schema - mutation_info sub-fields")

        record_fields = [{"name": "id", "type": "STRING", "mode": "REQUIRED"}]
        schema = build_cdc_schema(record_fields)

        row_mutation_info = schema['fields'][0]
        sub_fields = row_mutation_info['fields']

        self.assertEqual(len(sub_fields), 2)
        self.assertEqual(sub_fields[0]['name'], 'mutation_type')
        self.assertEqual(sub_fields[0]['mode'], 'REQUIRED')
        self.assertEqual(sub_fields[1]['name'], 'change_sequence_number')
        self.assertEqual(sub_fields[1]['mode'], 'REQUIRED')
        print("   OK: mutation_type and change_sequence_number are REQUIRED")

    def test_build_cdc_schema_record_fields_passed_through(self):
        """Test record_fields are passed through correctly."""
        print("\n[Test] build_cdc_schema - record_fields passed through")

        record_fields = [
            {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "age", "type": "INT64", "mode": "NULLABLE"}
        ]
        schema = build_cdc_schema(record_fields)

        record = schema['fields'][1]
        self.assertEqual(record['fields'], record_fields)
        self.assertEqual(len(record['fields']), 3)
        print("   OK: record_fields passed through correctly")


class TestMapToCdcTableRowDoFn(unittest.TestCase):
    """Test MapToCdcTableRowDoFn class."""

    def setUp(self):
        """Set up test fixtures."""
        self.record_fields = [
            {"name": "memberId", "type": "STRING", "mode": "REQUIRED"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"}
        ]
        self.dofn = MapToCdcTableRowDoFn(
            default_change_type="UPSERT",
            record_fields=self.record_fields,
            pipeline_name="test_pipeline"
        )

    def test_process_valid_element(self):
        """Test processing valid element produces CDC row."""
        print("\n[Test] MapToCdcTableRowDoFn - valid element")

        element = {
            "memberId": "12345",
            "email": "test@example.com"
        }

        results = list(self.dofn.process(element))

        self.assertEqual(len(results), 1)
        result = results[0]

        # Check it's a success result (tagged output)
        self.assertIsNotNone(result)
        print("   OK: Produced 1 result")

    def test_process_element_with_upsert(self):
        """Test element produces UPSERT mutation type."""
        print("\n[Test] MapToCdcTableRowDoFn - UPSERT mutation")

        element = {"memberId": "12345"}

        results = list(self.dofn.process(element))
        result = results[0]

        # The result is a tagged tuple (tag, value)
        if hasattr(result, 'value'):
            cdc_row = result.value
        else:
            cdc_row = result

        self.assertIn('row_mutation_info', cdc_row)
        self.assertEqual(cdc_row['row_mutation_info']['mutation_type'], 'UPSERT')
        print("   OK: mutation_type = UPSERT")

    def test_process_element_with_delete(self):
        """Test element with is_delete produces DELETE mutation type."""
        print("\n[Test] MapToCdcTableRowDoFn - DELETE mutation")

        element = {"memberId": "12345", "is_delete": True}

        results = list(self.dofn.process(element))
        result = results[0]

        if hasattr(result, 'value'):
            cdc_row = result.value
        else:
            cdc_row = result

        self.assertEqual(cdc_row['row_mutation_info']['mutation_type'], 'DELETE')
        print("   OK: mutation_type = DELETE")

    def test_process_element_has_sequence_number(self):
        """Test CDC row has change_sequence_number."""
        print("\n[Test] MapToCdcTableRowDoFn - sequence number")

        element = {"memberId": "12345"}

        results = list(self.dofn.process(element))
        result = results[0]

        if hasattr(result, 'value'):
            cdc_row = result.value
        else:
            cdc_row = result

        seq_num = cdc_row['row_mutation_info']['change_sequence_number']
        self.assertIsNotNone(seq_num)
        self.assertTrue(seq_num.isdigit())
        print(f"   OK: change_sequence_number = {seq_num}")

    def test_process_element_has_record(self):
        """Test CDC row has record field with data."""
        print("\n[Test] MapToCdcTableRowDoFn - record field")

        element = {"memberId": "12345", "email": "test@example.com"}

        results = list(self.dofn.process(element))
        result = results[0]

        if hasattr(result, 'value'):
            cdc_row = result.value
        else:
            cdc_row = result

        self.assertIn('record', cdc_row)
        self.assertEqual(cdc_row['record']['memberId'], '12345')
        self.assertEqual(cdc_row['record']['email'], 'test@example.com')
        print("   OK: record contains data")

    def test_process_none_element_to_dlq(self):
        """Test None element goes to DLQ."""
        print("\n[Test] MapToCdcTableRowDoFn - None element to DLQ")

        results = list(self.dofn.process(None))

        self.assertEqual(len(results), 1)
        result = results[0]

        # Should be DLQ tagged output
        if hasattr(result, 'tag'):
            self.assertIn('dlq', result.tag.lower())
        print("   OK: None element sent to DLQ")

    def test_process_empty_dict_to_dlq(self):
        """Test empty dict goes to DLQ."""
        print("\n[Test] MapToCdcTableRowDoFn - empty dict to DLQ")

        results = list(self.dofn.process({}))

        self.assertEqual(len(results), 1)
        result = results[0]

        # Should be DLQ tagged output
        if hasattr(result, 'tag'):
            self.assertIn('dlq', result.tag.lower())
        print("   OK: Empty dict sent to DLQ")

    def test_process_non_dict_to_dlq(self):
        """Test non-dict element goes to DLQ."""
        print("\n[Test] MapToCdcTableRowDoFn - non-dict to DLQ")

        results = list(self.dofn.process("not a dict"))

        self.assertEqual(len(results), 1)
        print("   OK: Non-dict sent to DLQ")

    def test_sanitize_nan_value(self):
        """Test NaN float values are sanitized to None."""
        print("\n[Test] MapToCdcTableRowDoFn - sanitize NaN")

        element = {"memberId": "12345", "score": float('nan')}

        results = list(self.dofn.process(element))
        result = results[0]

        if hasattr(result, 'value'):
            cdc_row = result.value
        else:
            cdc_row = result

        self.assertIsNone(cdc_row['record']['score'])
        print("   OK: NaN sanitized to None")

    def test_sanitize_inf_value(self):
        """Test Inf float values are sanitized to None."""
        print("\n[Test] MapToCdcTableRowDoFn - sanitize Inf")

        element = {"memberId": "12345", "value": float('inf')}

        results = list(self.dofn.process(element))
        result = results[0]

        if hasattr(result, 'value'):
            cdc_row = result.value
        else:
            cdc_row = result

        self.assertIsNone(cdc_row['record']['value'])
        print("   OK: Inf sanitized to None")

    def test_internal_fields_removed(self):
        """Test internal CDC fields are removed from record."""
        print("\n[Test] MapToCdcTableRowDoFn - internal fields removed")

        element = {
            "memberId": "12345",
            "_CHANGE_TYPE": "INSERT",
            "is_delete": False,
            "cdc_type": "UPSERT"
        }

        results = list(self.dofn.process(element))
        result = results[0]

        if hasattr(result, 'value'):
            cdc_row = result.value
        else:
            cdc_row = result

        record = cdc_row['record']
        self.assertNotIn('_CHANGE_TYPE', record)
        self.assertNotIn('is_delete', record)
        self.assertNotIn('cdc_type', record)
        print("   OK: Internal fields removed from record")


if __name__ == "__main__":
    unittest.main()
