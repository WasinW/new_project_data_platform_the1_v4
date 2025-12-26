"""
Test cases for orchestrator module.

Uses pytest style for cleaner, more maintainable tests.
"""
import pytest
from unittest.mock import MagicMock, patch
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline

from dataflow_common.orchestrator import Orchestrator
from dataflow_common.config import PipelineConfig


# =============================================================================
# Fixtures
# =============================================================================

@pytest.fixture
def config():
    """Create test config."""
    config_dict = {
        "pipeline": {"name": "test_orchestrator", "mode": "batch", "term": "short"},
        "params": {"pk": "id", "run_dt": "2025-01-01"},
        "io": {"bq": {"project": "test-project", "dataset": "test_dataset"}},
        "plan": []
    }
    return PipelineConfig.from_dict(config_dict)


# =============================================================================
# Tests: Orchestrator
# =============================================================================

class TestOrchestrator:
    """Tests for Orchestrator class."""

    @patch('dataflow_common.registry.STEP_REGISTRY')
    def test_orchestrator_initialization(self, mock_registry, config):
        orchestrator = Orchestrator(config)

        assert orchestrator.config == config
        assert orchestrator.state == {}

    @patch('dataflow_common.registry.STEP_REGISTRY')
    def test_step_execution_order(self, mock_registry, config):
        mock_step1 = MagicMock()
        mock_step1.execute.return_value = beam.Create([1, 2, 3])

        mock_step2 = MagicMock()
        mock_step2.execute.return_value = beam.Create([4, 5, 6])

        mock_registry.get.side_effect = lambda x: {
            "Step1": lambda **kwargs: mock_step1,
            "Step2": lambda **kwargs: mock_step2
        }.get(x)

        config.plan = [
            {"step": "Step1", "out": "output1"},
            {"step": "Step2", "in": "output1", "out": "output2"}
        ]

        orchestrator = Orchestrator(config)

        with TestPipeline() as p:
            orchestrator.state["__pipeline__"] = p

            step1_class = mock_registry.get("Step1")
            step1 = step1_class(spec=config.plan[0], config=config, state=orchestrator.state)
            output1 = step1.execute(p)
            orchestrator.state["output1"] = output1

            step2_class = mock_registry.get("Step2")
            step2 = step2_class(spec=config.plan[1], config=config, state=orchestrator.state)
            output2 = step2.execute(p)
            orchestrator.state["output2"] = output2

        assert "output1" in orchestrator.state
        assert "output2" in orchestrator.state


# =============================================================================
# Tests: _format_value
# =============================================================================

class TestFormatValue:
    """Tests for _format_value function."""

    def test_format_template(self, config):
        from dataflow_common.orchestrator import _format_value

        template = "{io.bq.project}.{io.bq.dataset}.table"
        result = _format_value(template, config)

        assert result == "test-project.test_dataset.table"

    def test_format_missing_key(self, config):
        from dataflow_common.orchestrator import _format_value

        template = "{missing.key}"
        result = _format_value(template, config)

        assert result == ""
