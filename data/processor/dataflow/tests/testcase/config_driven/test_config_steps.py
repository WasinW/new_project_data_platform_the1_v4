"""
Integration tests for config-to-steps creation.

These tests verify that:
1. Config YAML files can be loaded successfully
2. Each step in the plan is registered in STEP_REGISTRY
3. Step classes can be instantiated with the config
4. Step parameters are extracted correctly

NOTE: These tests do NOT run actual Dataflow jobs or connect to GCP services.
They only verify the config -> step creation logic works correctly.

NOTE: This module may be removed when pipelines switch to direct import of steps.
"""
import os
import unittest
from pathlib import Path
from unittest.mock import MagicMock

# Set environment variable for config expansion before importing
os.environ.setdefault("WORKSPACE_ENV", "dev")

# Get the configs directory path
DATAFLOW_DIR = Path(__file__).parent.parent.parent.parent
CONFIGS_DIR = DATAFLOW_DIR / "configs"

# Try to import dataflow_common modules
try:
    from dataflow_common.config import load_config
    from dataflow_common.registry import STEP_REGISTRY
    CONFIG_AVAILABLE = True
except ImportError:
    CONFIG_AVAILABLE = False


@unittest.skipUnless(CONFIG_AVAILABLE, "dataflow_common not available")
class TestConfigLoading(unittest.TestCase):
    """Test config file loading."""

    @classmethod
    def setUpClass(cls):
        cls.config_files = list(CONFIGS_DIR.glob("*.yaml"))
        print(f"\n{'='*60}")
        print(f"Config Loading Tests")
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
            if config_file.name == ".gitkeep":
                continue
            self.assertTrue(config_file.exists())
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


@unittest.skipUnless(CONFIG_AVAILABLE, "dataflow_common not available")
class TestStepRegistration(unittest.TestCase):
    """Test step registration in STEP_REGISTRY."""

    @classmethod
    def setUpClass(cls):
        cls.config_files = list(CONFIGS_DIR.glob("*.yaml"))

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
                    self.assertIsNotNone(step_name)
                    self.assertIn(
                        step_name,
                        STEP_REGISTRY,
                        f"Step '{step_name}' not found in STEP_REGISTRY"
                    )
                    print(f"      [{idx+1}] {step_name} (id: {step_id}) OK")


@unittest.skipUnless(CONFIG_AVAILABLE, "dataflow_common not available")
class TestStepInstantiation(unittest.TestCase):
    """Test that Step classes can be instantiated."""

    @classmethod
    def setUpClass(cls):
        cls.config_files = list(CONFIGS_DIR.glob("*.yaml"))

    def test_steps_can_be_instantiated(self):
        """Test that Step classes can be instantiated with config specs."""
        print(f"\n[TEST] Steps can be instantiated")

        for config_file in self.config_files:
            if config_file.name == ".gitkeep":
                continue

            config = load_config(str(config_file))
            plan = config.plan or []
            mock_state = {"__pipeline__": MagicMock()}

            print(f"\n   Config: {config_file.name}")

            for idx, spec in enumerate(plan):
                step_name = spec.get("step")

                with self.subTest(config=config_file.name, step=step_name, index=idx):
                    step_cls = STEP_REGISTRY.get(step_name)
                    self.assertIsNotNone(step_cls)

                    try:
                        step = step_cls(spec=spec, config=config, state=mock_state)
                        self.assertIsNotNone(step)
                        self.assertTrue(hasattr(step, 'spec'))
                        self.assertTrue(hasattr(step, 'config'))
                        self.assertTrue(hasattr(step, 'execute'))
                        print(f"      [{idx+1}] {step_name} instantiated OK")
                    except Exception as e:
                        self.fail(f"Failed to instantiate {step_name}: {e}")


if __name__ == "__main__":
    unittest.main(verbosity=2)
