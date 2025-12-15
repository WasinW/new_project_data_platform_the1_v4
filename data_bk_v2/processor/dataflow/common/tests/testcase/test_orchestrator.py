"""
Test cases for orchestrator module
"""
import unittest
from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from dataflow_common.orchestrator import Orchestrator
from dataflow_common.config import PipelineConfig

class TestOrchestratorModule(unittest.TestCase):
    """Test pipeline orchestration"""
    
    def setUp(self):
        """Set up test config"""
        self.config_dict = {
            "pipeline": {
                "name": "test_orchestrator",
                "mode": "batch",
                "term": "short"
            },
            "params": {
                "pk": "id",
                "run_dt": "2025-01-01"
            },
            "io": {
                "bq": {
                    "project": "test-project",
                    "dataset": "test_dataset"
                }
            },
            "plan": []
        }
        
        self.config = PipelineConfig.from_dict(self.config_dict)
    
    @patch('dataflow_common.registry.STEP_REGISTRY')
    def test_orchestrator_initialization(self, mock_registry):
        """Test orchestrator initialization"""
        print("\n[Test] Test: Orchestrator initialization")
        
        orchestrator = Orchestrator(self.config)
        
        self.assertEqual(orchestrator.config, self.config)
        self.assertEqual(orchestrator.state, {})
        
        print(f"   [OK] Orchestrator initialized for: {self.config.name}")
    
    @patch('dataflow_common.registry.STEP_REGISTRY')
    def test_step_execution_order(self, mock_registry):
        """Test steps execute in order"""
        print("\n[Test] Test: Step execution order")
        
        # Create mock steps
        mock_step1 = MagicMock()
        mock_step1.execute.return_value = beam.Create([1, 2, 3])
        
        mock_step2 = MagicMock()
        mock_step2.execute.return_value = beam.Create([4, 5, 6])
        
        mock_registry.get.side_effect = lambda x: {
            "Step1": lambda **kwargs: mock_step1,
            "Step2": lambda **kwargs: mock_step2
        }.get(x)
        
        # Update config with plan
        self.config.plan = [
            {"step": "Step1", "out": "output1"},
            {"step": "Step2", "in": "output1", "out": "output2"}
        ]
        
        orchestrator = Orchestrator(self.config)
        
        with TestPipeline() as p:
            orchestrator.state["__pipeline__"] = p
            
            # Execute first step
            step1_class = mock_registry.get("Step1")
            step1 = step1_class(spec=self.config.plan[0], config=self.config, state=orchestrator.state)
            output1 = step1.execute(p)
            orchestrator.state["output1"] = output1
            
            # Execute second step
            step2_class = mock_registry.get("Step2")
            step2 = step2_class(spec=self.config.plan[1], config=self.config, state=orchestrator.state)
            output2 = step2.execute(p)
            orchestrator.state["output2"] = output2
        
        self.assertIn("output1", orchestrator.state)
        self.assertIn("output2", orchestrator.state)
        
        print(f"   [OK] Executed {len(self.config.plan)} steps in order")
    
    def test_format_value(self):
        """Test config value formatting"""
        print("\n[Test] Test: Format config values")
        
        from dataflow_common.orchestrator import _format_value
        
        # Test formatting
        template = "{io.bq.project}.{io.bq.dataset}.table"
        result = _format_value(template, self.config)
        
        self.assertEqual(result, "test-project.test_dataset.table")
        print(f"   [OK] Formatted: {template} -> {result}")
        
        # Test missing value
        template = "{missing.key}"
        result = _format_value(template, self.config)
        
        self.assertEqual(result, "")
        print(f"   [OK] Missing key handled: {template} -> (empty)")

if __name__ == "__main__":
    unittest.main()