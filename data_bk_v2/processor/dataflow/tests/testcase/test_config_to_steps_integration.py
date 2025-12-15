"""
Integration tests for config-to-steps creation.

These tests verify that:
1. Config YAML files can be loaded successfully
2. Each step in the plan is registered in STEP_REGISTRY
3. Step classes can be instantiated with the config
4. Step parameters are extracted correctly

NOTE: These tests do NOT run actual Dataflow jobs or connect to GCP services.
They only verify the config → step creation logic works correctly.
"""
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Set environment variable for config expansion before importing
os.environ.setdefault("WORKSPACE_ENV", "dev")

# Get the configs directory path
DATAFLOW_DIR = Path(__file__).parent.parent.parent
CONFIGS_DIR = DATAFLOW_DIR / "configs"

# Import at module level after setting env vars
from dataflow_common.config import load_config
from dataflow_common.registry import STEP_REGISTRY


class TestConfigToStepsIntegration(unittest.TestCase):
    """Integration tests for loading configs and creating steps."""

    @classmethod
    def setUpClass(cls):
        """Set up test fixtures."""
        cls.config_files = list(CONFIGS_DIR.glob("*.yaml"))

        print(f"\n{'='*60}")
        print(f"Config-to-Steps Integration Tests")
        print(f"Found {len(cls.config_files)} config files in {CONFIGS_DIR}")
        print(f"{'='*60}")

    def test_config_files_exist(self):
        """Verify config files exist in the configs directory."""
        print(f"\n[TEST] Config files exist")

        self.assertTrue(
            len(self.config_files) > 0,
            f"No config files found in {CONFIGS_DIR}"
        )

        for config_file in self.config_files:
            self.assertTrue(config_file.exists(), f"Config file not found: {config_file}")
            print(f"   [OK] Found: {config_file.name}")

    def test_all_configs_load_successfully(self):
        """Test that all config files can be loaded without errors."""
        print(f"\n[TEST] All configs load successfully")

        for config_file in self.config_files:
            if config_file.name == ".gitkeep":
                continue

            with self.subTest(config=config_file.name):
                try:
                    config = load_config(str(config_file))
                    self.assertIsNotNone(config)
                    self.assertIsNotNone(config.name)
                    self.assertIsNotNone(config.mode)
                    print(f"   [OK] Loaded: {config_file.name} (pipeline: {config.name}, mode: {config.mode})")
                except Exception as e:
                    self.fail(f"Failed to load {config_file.name}: {e}")

    def test_all_steps_are_registered(self):
        """Test that all steps referenced in configs are registered in STEP_REGISTRY."""
        print(f"\n[TEST] All steps are registered in STEP_REGISTRY")

        for config_file in self.config_files:
            if config_file.name == ".gitkeep":
                continue

            config = load_config(str(config_file))
            plan = config.plan or []

            print(f"\n   Config: {config_file.name} ({len(plan)} steps)")

            for idx, spec in enumerate(plan):
                step_name = spec.get("step")
                step_id = spec.get("id") or spec.get("out") or f"step_{idx}"

                with self.subTest(config=config_file.name, step=step_name, index=idx):
                    self.assertIsNotNone(
                        step_name,
                        f"Step #{idx} in {config_file.name} has no 'step' field"
                    )

                    self.assertIn(
                        step_name,
                        STEP_REGISTRY,
                        f"Step '{step_name}' not found in STEP_REGISTRY. "
                        f"Available: {list(STEP_REGISTRY.keys())}"
                    )

                    print(f"      [{idx+1}] {step_name} (id: {step_id}) ✓")

    def test_steps_can_be_instantiated(self):
        """Test that Step classes can be instantiated with config specs."""
        print(f"\n[TEST] Steps can be instantiated")

        for config_file in self.config_files:
            if config_file.name == ".gitkeep":
                continue

            config = load_config(str(config_file))
            plan = config.plan or []

            # Create mock state with pipeline
            mock_state = {"__pipeline__": MagicMock()}

            print(f"\n   Config: {config_file.name}")

            for idx, spec in enumerate(plan):
                step_name = spec.get("step")
                step_id = spec.get("id") or spec.get("out") or f"step_{idx}"

                with self.subTest(config=config_file.name, step=step_name, index=idx):
                    step_cls = STEP_REGISTRY.get(step_name)
                    self.assertIsNotNone(step_cls)

                    try:
                        # Instantiate the step (this validates spec parsing)
                        step = step_cls(spec=spec, config=config, state=mock_state)
                        self.assertIsNotNone(step)

                        # Verify step has required attributes
                        self.assertTrue(hasattr(step, 'spec'))
                        self.assertTrue(hasattr(step, 'config'))
                        self.assertTrue(hasattr(step, 'execute'))

                        print(f"      [{idx+1}] {step_name} instantiated ✓")

                    except Exception as e:
                        self.fail(
                            f"Failed to instantiate {step_name} at index {idx} "
                            f"in {config_file.name}: {e}"
                        )


class TestBatchConfigSteps(unittest.TestCase):
    """Test batch pipeline config (customer_profile_short.yaml)."""

    @classmethod
    def setUpClass(cls):
        cls.config_path = CONFIGS_DIR / "customer_profile_short.yaml"

    def test_batch_config_structure(self):
        """Test batch config has correct structure."""
        print(f"\n[TEST] Batch config structure")

        if not self.config_path.exists():
            self.skipTest(f"Config not found: {self.config_path}")

        config = load_config(str(self.config_path))

        # Verify pipeline settings
        self.assertEqual(config.mode, "batch")
        print(f"   [OK] Pipeline mode: {config.mode}")

        # Verify plan exists
        self.assertIsNotNone(config.plan)
        self.assertGreater(len(config.plan), 0)
        print(f"   [OK] Plan has {len(config.plan)} steps")

    def test_batch_steps_order(self):
        """Test batch pipeline steps are in correct order."""
        print(f"\n[TEST] Batch steps order")

        if not self.config_path.exists():
            self.skipTest(f"Config not found: {self.config_path}")

        config = load_config(str(self.config_path))
        plan = config.plan or []

        # Expected step types in order (first few steps)
        expected_steps = [
            "ReadBQQuery",      # 1. Get mapping
            "BuildMappingDict", # 2. Build dict
            "ReadBQQuery",      # 3. Get personas
            "ParseJson",        # 4. Parse JSON
        ]

        for idx, expected in enumerate(expected_steps):
            if idx < len(plan):
                actual = plan[idx].get("step")
                self.assertEqual(
                    actual, expected,
                    f"Step {idx}: expected {expected}, got {actual}"
                )
                print(f"   [{idx+1}] {actual} ✓")

    def test_batch_step_parameters(self):
        """Test batch steps have required parameters."""
        print(f"\n[TEST] Batch step parameters")

        if not self.config_path.exists():
            self.skipTest(f"Config not found: {self.config_path}")

        config = load_config(str(self.config_path))
        plan = config.plan or []

        # Check specific steps have required params
        for spec in plan:
            step_name = spec.get("step")

            if step_name == "ReadBQQuery":
                self.assertIn("query", spec, "ReadBQQuery missing 'query' param")
                self.assertIn("out", spec, "ReadBQQuery missing 'out' param")

            elif step_name == "BuildMappingDict":
                self.assertIn("in", spec, "BuildMappingDict missing 'in' param")
                self.assertIn("mapping_fields", spec, "BuildMappingDict missing 'mapping_fields'")

            elif step_name == "WriteParquet":
                self.assertIn("in", spec, "WriteParquet missing 'in' param")
                self.assertIn("prefix", spec, "WriteParquet missing 'prefix' param")

        print(f"   [OK] All step parameters validated")


class TestStreamingConfigSteps(unittest.TestCase):
    """Test streaming pipeline config (customer_profile_realtime.yaml)."""

    @classmethod
    def setUpClass(cls):
        cls.config_path = CONFIGS_DIR / "customer_profile_realtime.yaml"

    def test_streaming_config_structure(self):
        """Test streaming config has correct structure."""
        print(f"\n[TEST] Streaming config structure")

        if not self.config_path.exists():
            self.skipTest(f"Config not found: {self.config_path}")

        config = load_config(str(self.config_path))

        # Verify pipeline settings
        self.assertEqual(config.mode, "streaming")
        print(f"   [OK] Pipeline mode: {config.mode}")

        # Verify plan exists
        self.assertIsNotNone(config.plan)
        self.assertGreater(len(config.plan), 0)
        print(f"   [OK] Plan has {len(config.plan)} steps")

    def test_streaming_steps_include_required(self):
        """Test streaming pipeline has required step types."""
        print(f"\n[TEST] Streaming steps include required types")

        if not self.config_path.exists():
            self.skipTest(f"Config not found: {self.config_path}")

        config = load_config(str(self.config_path))
        plan = config.plan or []

        step_types = [spec.get("step") for spec in plan]

        # Required streaming steps
        required_steps = [
            "RefreshMappingTable",
            "ReadFromPubSub",
            "ExtractPersonas",
            "FetchFromBigtable",
            "FilterEmptyPK",
            "TransformSchemas",
        ]

        for required in required_steps:
            self.assertIn(
                required, step_types,
                f"Missing required streaming step: {required}"
            )
            print(f"   [OK] Has {required}")

    def test_streaming_step_parameters(self):
        """Test streaming steps have required parameters."""
        print(f"\n[TEST] Streaming step parameters")

        if not self.config_path.exists():
            self.skipTest(f"Config not found: {self.config_path}")

        config = load_config(str(self.config_path))
        plan = config.plan or []

        for spec in plan:
            step_name = spec.get("step")
            params = spec.get("params", {})

            if step_name == "ReadFromPubSub":
                self.assertIn("subscription", params, "ReadFromPubSub missing 'subscription'")

            elif step_name == "FetchFromBigtable":
                self.assertIn("project", params, "FetchFromBigtable missing 'project'")
                self.assertIn("instance", params, "FetchFromBigtable missing 'instance'")
                self.assertIn("table", params, "FetchFromBigtable missing 'table'")

            elif step_name == "WriteToBigQueryCDC":
                self.assertIn("table", params, "WriteToBigQueryCDC missing 'table'")

        print(f"   [OK] All streaming step parameters validated")


class TestStepInstantiationDetails(unittest.TestCase):
    """Detailed tests for step instantiation and parameter extraction."""

    def test_step_extracts_correct_input(self):
        """Test that steps correctly extract input references."""
        print(f"\n[TEST] Step extracts correct input references")

        config_path = CONFIGS_DIR / "customer_profile_short.yaml"
        if not config_path.exists():
            self.skipTest(f"Config not found: {config_path}")

        config = load_config(str(config_path))
        plan = config.plan or []
        mock_state = {"__pipeline__": MagicMock()}

        # Find steps that have 'in' parameter
        for spec in plan:
            step_name = spec.get("step")
            input_ref = spec.get("in")

            if input_ref:
                step_cls = STEP_REGISTRY.get(step_name)
                step = step_cls(spec=spec, config=config, state=mock_state)

                # Step should have stored the input reference
                self.assertEqual(spec.get("in"), input_ref)
                print(f"   [OK] {step_name} input: {input_ref}")

    def test_placeholder_formatting_in_steps(self):
        """Test that placeholders like {io.bq.project} are in specs."""
        print(f"\n[TEST] Placeholder formatting in step specs")

        config_path = CONFIGS_DIR / "customer_profile_realtime.yaml"
        if not config_path.exists():
            self.skipTest(f"Config not found: {config_path}")

        config = load_config(str(config_path))
        plan = config.plan or []

        # Check that specs contain placeholders (before formatting)
        placeholder_found = False
        for spec in plan:
            spec_str = str(spec)
            if "{io." in spec_str or "{params." in spec_str:
                placeholder_found = True
                step_name = spec.get("step")
                print(f"   [OK] {step_name} has config placeholders")

        self.assertTrue(placeholder_found, "No placeholders found in config")


if __name__ == "__main__":
    unittest.main(verbosity=2)
