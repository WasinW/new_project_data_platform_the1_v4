"""
Test cases for transform functions
"""
import unittest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts', 'dataflow_common', 'src'))
from dataflow_common.transforms import (
    normalize_path,
    extract_by_path,
    create_mapping_dict,
    map_record,
    coalesce_by_mapping
)

class TestTransformModule(unittest.TestCase):
    """Test transform utilities"""
    
    def test_normalize_path(self):
        """Test path normalization"""
        print("\n🔬 Test: Path normalization")
        
        test_cases = [
            ("profiles.memberId", ["profiles", "memberId"]),
            ("profiles['memberId']", ["profiles", "memberId"]),
            ("['profiles']['memberId']", ["profiles", "memberId"]),
            ("profiles['member'].data", ["profiles", "member", "data"]),
            ("a.b.c.d", ["a", "b", "c", "d"]),
            ("", []),
            (None, []),
            # Additional bracket variations
            ('profiles["memberId"]', ["profiles", "memberId"]),
            ("profiles[memberId]", ["profiles", "memberId"]),
        ]
        
        for input_path, expected in test_cases:
            result = normalize_path(input_path)
            self.assertEqual(result, expected, 
                           f"Failed for input: {input_path}")
            print(f"   ✅ {input_path} -> {result}")
    
    def test_extract_by_path(self):
        """Test nested value extraction"""
        print("\n🔬 Test: Extract nested values")
        
        # Test data
        record = {
            "level1": {
                "level2": {
                    "value": "nested_value"
                }
            },
            "profiles": '{"memberId": "12345", "email": "test@example.com"}'
        }
        
        # Test nested extraction
        path = ["level1", "level2", "value"]
        result = extract_by_path(record, path)
        self.assertEqual(result, "nested_value")
        print(f"   ✅ Extracted nested: {result}")
        
        # Test JSON string parsing
        path = ["profiles", "memberId"]
        result = extract_by_path(record, path)
        self.assertEqual(result, "12345")
        print(f"   ✅ Extracted from JSON: {result}")
        
        # Test missing path
        path = ["missing", "path"]
        result = extract_by_path(record, path)
        self.assertIsNone(result)
        print(f"   ✅ Missing path returns None")
    
    def test_create_mapping_dict(self):
        """Test mapping dictionary creation"""
        print("\n🔬 Test: Create mapping dictionary")
        
        mapping_rows = [
            {
                "RECONCILE_COLUMN_NAME": "MEMBER_NUMBER",
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.memberId",
                "RECONCILE_RETRIEVED": True,
                "RECONCILE_CONFIRMED": False
            },
            {
                "RECONCILE_COLUMN_NAME": "EMAIL",
                "PERSONAS_MAPPING_COLUMN_NAME": "profiles.email",
                "RECONCILE_RETRIEVED": True,
                "RECONCILE_CONFIRMED": True
            }
        ]
        
        mapping_dict = create_mapping_dict(
            mapping_rows,
            src_field="PERSONAS_MAPPING_COLUMN_NAME",
            dest_field="RECONCILE_COLUMN_NAME",
            retrieved_flag_field="RECONCILE_RETRIEVED",
            confirmed_flag_field="RECONCILE_CONFIRMED"
        )
        
        self.assertIn("MEMBER_NUMBER", mapping_dict)
        self.assertEqual(mapping_dict["MEMBER_NUMBER"]["src_path"], ["profiles", "memberId"])
        self.assertTrue(mapping_dict["EMAIL"]["reconcile"])
        self.assertTrue(mapping_dict["EMAIL"]["original"])
        
        print(f"   ✅ Created mapping with {len(mapping_dict)} entries")
    
    def test_map_record(self):
        """Test record mapping"""
        print("\n🔬 Test: Map record")
        
        # Input record
        record = {
            "profiles": {
                "memberId": "123",
                "email": "test@example.com"
            }
        }
        
        # Mapping dict
        mapping_dict = {
            "MEMBER_NUMBER": {
                "src_path": ["profiles", "memberId"],
                "reconcile": True,
                "original": False
            },
            "EMAIL": {
                "src_path": ["profiles", "email"],
                "reconcile": True,
                "original": True
            }
        }
        
        # Test reconcile mode
        result = map_record(record, mapping_dict, mode="reconcile")
        self.assertEqual(result["MEMBER_NUMBER"], "123")
        self.assertEqual(result["EMAIL"], "test@example.com")
        print(f"   ✅ Reconcile mode: {len(result)} fields mapped")
        
        # Test original mode
        result = map_record(record, mapping_dict, mode="original")
        self.assertNotIn("MEMBER_NUMBER", result)
        self.assertIn("EMAIL", result)
        print(f"   ✅ Original mode: {len(result)} fields mapped")
    
    def test_coalesce_by_mapping(self):
        """Test record coalescing"""
        print("\n🔬 Test: Coalesce records")
        
        # Test data
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
        
        self.assertEqual(result["EMAIL"], "new@example.com")
        self.assertEqual(result["PHONE"], "555-1234")
        self.assertEqual(result["MEMBER_NUMBER"], "123")
        
        print(f"   ✅ Coalesced {len(result)} fields")

if __name__ == "__main__":
    unittest.main()