# 08 - Testing Guide

> Comprehensive testing strategy และ test implementation

## 📖 Table of Contents

- [Testing Strategy](#testing-strategy)
- [Unit Tests](#unit-tests)
- [Integration Tests](#integration-tests)
- [Running Tests](#running-tests)
- [Test Coverage](#test-coverage)
- [CI/CD Testing](#cicd-testing)

---

## Testing Strategy

### Test Pyramid

```
         ╱╲
        ╱  ╲
       ╱ E2E ╲          Few (Manual)
      ╱────────╲
     ╱Integration╲      Some (Automated)
    ╱──────────────╲
   ╱  Unit Tests    ╲   Many (Fast & Automated)
  ╱──────────────────╲
```

### Test Levels

| Level | Purpose | Count | Speed | Environment |
|-------|---------|-------|-------|-------------|
| **Unit** | Test components | Many | Fast | Local |
| **Integration** | Test workflows | Some | Medium | STG |
| **E2E** | Test full system | Few | Slow | UAT/PROD |

### Coverage Goals

- **Unit Tests**: > 80% code coverage
- **Integration Tests**: All critical workflows
- **E2E Tests**: Happy path + critical failures

---

## Unit Tests

### Location

```
data/processor/dataflow/common/tests/testcase/
├── __init__.py
├── test_config.py            # Config loading tests
├── test_connectors.py        # BigQuery, Parquet connectors
├── test_dofns.py             # DoFn class tests
├── test_orchestrator.py      # Orchestrator tests
├── test_steps.py             # Batch step tests
├── test_streaming_steps.py   # Streaming step tests
├── test_realtime_steps.py    # Realtime pipeline tests
├── test_sql_functions.py     # SQL function tests
└── test_transforms.py        # Transform function tests
```

### Testing DAGs

```python
# tests/unit/test_dags.py

import pytest
from airflow.models import DagBag


def test_dag_loaded():
    """Test that all DAGs load without errors."""
    dagbag = DagBag(dag_folder='data/processor/dags')

    assert dagbag.import_errors == {}, \
        f"DAG import errors: {dagbag.import_errors}"

    assert len(dagbag.dags) >= 3, \
        f"Expected at least 3 DAGs, found {len(dagbag.dags)}"


def test_ms_member_short_dag():
    """Test ms_member_short_dag structure."""
    dagbag = DagBag(dag_folder='data/processor/dags')
    dag = dagbag.get_dag('ms_member_short_dag')

    assert dag is not None
    assert dag.schedule_interval == '0 */2 * * *'
    assert 'member-data' in dag.tags
    assert len(dag.tasks) == 1


def test_dag_task_dependencies():
    """Test DAG task dependencies."""
    dagbag = DagBag(dag_folder='data/processor/dags')
    dag = dagbag.get_dag('ms_member_short_dag')

    task = dag.get_task('run_dataflow_job')
    assert task.retries == 2
```

### Testing Config Loading

```python
# tests/unit/test_config.py

import pytest
from dataflow_common.config import load_config


def test_load_batch_config():
    """Test loading batch config."""
    config = load_config('configs/ms_member_short.yaml')

    assert config.pipeline['name'] == 'ms_member_short'
    assert config.pipeline['mode'] == 'batch'
    assert config.params['pk'] == 'member_number'


def test_load_streaming_config():
    """Test loading streaming config."""
    config = load_config('configs/ms_member_realtime.yaml')

    assert config.pipeline['mode'] == 'streaming'
    assert len(config.plan) == 9  # 9 steps
    assert config.mapping['refresh_interval_sec'] == 60


def test_config_validation():
    """Test config validation fails on invalid YAML."""
    with pytest.raises(Exception):
        load_config('configs/invalid.yaml')
```

### Testing Orchestrator

```python
# tests/unit/test_orchestrator.py

import pytest
from dataflow_common.config import load_config
from dataflow_common.orchestrator import Orchestrator


def test_orchestrator_init():
    """Test orchestrator initialization."""
    config = load_config('configs/ms_member_short.yaml')
    orchestrator = Orchestrator(config)

    assert orchestrator.config == config
    assert orchestrator.state == {}


def test_format_placeholders():
    """Test config placeholder formatting."""
    config = load_config('configs/ms_member_short.yaml')
    orchestrator = Orchestrator(config)

    # Test formatting
    from dataflow_common.orchestrator import _format_value
    result = _format_value("{io.bq.project}", config)

    assert result == "the1-insight-stg"
```

### Testing Pipeline Steps

```python
# tests/unit/test_dataflow_pipeline.py

import pytest
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from dataflow_common.steps import ReadBQQueryStep


def test_read_bq_query_step():
    """Test ReadBQQueryStep execution."""
    with TestPipeline() as p:
        # Mock config and spec
        spec = {
            'step': 'ReadBQQuery',
            'query': 'SELECT 1 as id, "test" as name',
        }
        config = MockConfig()
        state = {}

        # Execute step
        step = ReadBQQueryStep(spec=spec, config=config, state=state)
        output = step.execute(p)

        # Assert output structure
        assert output is not None


@pytest.fixture
def mock_config():
    """Mock pipeline config."""
    return MockConfig(
        project_id='test-project',
        dataset='test_dataset'
    )
```

---

## Integration Tests

### Location

```
data/processor/dataflow/tests/integration/
├── __init__.py
├── README.md
├── run_integration_tests.sh
├── test_ms_member_short_stg.py
├── test_ms_member_daily_stg.py
└── test_ms_member_realtime_stg.py
```

### Running Integration Tests

```bash
# Run all integration tests
cd data/processor/dataflow/tests/integration
./run_integration_tests.sh

# Or run specific test
pytest test_ms_member_short_stg.py -v
```

### Integration Test Example

```python
# tests/integration/test_ms_member_short_stg.py

import pytest
import subprocess
import time
from google.cloud import bigquery, storage


class TestMsMemberShortSTG:
    """Integration tests for ms_member_short in STG."""

    @pytest.fixture(scope="class")
    def setup_environment(self):
        """Setup test environment."""
        self.project = "the1-insight-stg"
        self.dataset = "insight"
        self.temp_bucket = "test-temp-bucket"

        # Create temp resources
        yield

        # Cleanup
        self._cleanup()

    def test_pipeline_execution(self, setup_environment):
        """Test full pipeline execution."""
        # Run pipeline
        result = subprocess.run([
            'python',
            'scripts/ms_member_short_pipeline.py',
            '--config_path=configs/ms_member_short.yaml',
            '--runner=DataflowRunner',
            '--project=the1-insight-stg',
            '--region=asia-southeast1',
        ], capture_output=True, text=True)

        assert result.returncode == 0, f"Pipeline failed: {result.stderr}"

    def test_output_validation(self):
        """Test output data quality."""
        client = bigquery.Client(project=self.project)

        # Query output table
        query = f"""
            SELECT COUNT(*) as count
            FROM `{self.project}.{self.dataset}.ms_member_output`
            WHERE DATE(created_at) = CURRENT_DATE()
        """
        results = list(client.query(query).result())

        assert len(results) > 0
        assert results[0].count > 0

    def test_s3_output(self):
        """Test S3 Parquet output."""
        # Check S3 bucket for output files
        import boto3
        s3 = boto3.client('s3')

        response = s3.list_objects_v2(
            Bucket='t1-analytics',
            Prefix='refined/insights/ms_member_short/'
        )

        assert 'Contents' in response
        assert len(response['Contents']) > 0
```

---

## Running Tests

### Run All Tests

```bash
# Run all unit tests (from common directory)
cd data/processor/dataflow/common
python -m pytest tests/testcase/ -v

# Or run from project root
pytest data/processor/dataflow/common/tests/testcase/ -v
```

### Run Specific Tests

```bash
# Run single test file
pytest tests/testcase/test_config.py -v

# Run single test function
pytest tests/testcase/test_config.py::test_load_config -v

# Run tests matching pattern
pytest -k "streaming" -v

# Run DoFn tests only
pytest tests/testcase/test_dofns.py -v

# Run streaming step tests
pytest tests/testcase/test_streaming_steps.py -v
```

### Run with Options

```bash
# Run with coverage
pytest --cov=data/processor/dataflow --cov-report=html

# Run with detailed output
pytest -v -s

# Run and stop on first failure
pytest -x

# Run last failed tests only
pytest --lf
```

---

## Test Coverage

### Generate Coverage Report

```bash
# Run tests with coverage
pytest \
  --cov=data/processor/dataflow \
  --cov-report=html \
  --cov-report=term

# View HTML report
open htmlcov/index.html
```

### Coverage Configuration

```ini
# .coveragerc
[run]
source = data/processor/dataflow
omit =
    */tests/*
    */venv/*
    */__pycache__/*

[report]
precision = 2
show_missing = True
skip_covered = False

[html]
directory = htmlcov
```

### Coverage Goals

| Component | Current | Target |
|-----------|---------|--------|
| **DAGs** | 85% | > 80% |
| **Config** | 90% | > 80% |
| **Orchestrator** | 88% | > 80% |
| **Pipeline Steps** | 82% | > 80% |
| **Overall** | 85% | > 80% |

---

## CI/CD Testing

### GitLab CI Configuration

```yaml
# .gitlab-ci.yml (excerpt)

test:unit:
  stage: test
  script:
    - pip install -r requirements.txt
    - pip install -r requirements-dev.txt
    - pytest tests/unit/ -v --cov=data/processor/dataflow
  coverage: '/TOTAL.*\s+(\d+%)$/'

test:integration:nonprod:
  stage: test
  script:
    - pytest tests/integration/ -v
  only:
    - main
    - develop
  environment:
    name: STG
```

### Pre-commit Hooks

```yaml
# .pre-commit-config.yaml

repos:
  - repo: https://github.com/psf/black
    rev: 23.7.0
    hooks:
      - id: black
        language_version: python3.11

  - repo: https://github.com/PyCQA/flake8
    rev: 6.1.0
    hooks:
      - id: flake8

  - repo: local
    hooks:
      - id: pytest
        name: pytest
        entry: pytest
        language: system
        pass_filenames: false
        always_run: true
        args: [tests/unit/, -v]
```

---

## Best Practices

### 1. Test Naming

```python
# Good: Descriptive names
def test_dag_loads_without_errors():
    pass

def test_config_validates_required_fields():
    pass

# Bad: Vague names
def test1():
    pass

def test_stuff():
    pass
```

### 2. Test Organization

```python
# Use class to group related tests
class TestMemberPipeline:
    def test_read_step(self):
        pass

    def test_transform_step(self):
        pass

    def test_write_step(self):
        pass
```

### 3. Use Fixtures

```python
@pytest.fixture
def sample_config():
    """Provide sample config for tests."""
    return load_config('configs/test.yaml')

def test_with_fixture(sample_config):
    assert sample_config.pipeline['name'] == 'test'
```

### 4. Mock External Dependencies

```python
from unittest.mock import Mock, patch

@patch('google.cloud.bigquery.Client')
def test_bigquery_read(mock_client):
    """Test BigQuery read with mocked client."""
    mock_client.return_value.query.return_value = Mock()
    # Test logic
```

---

## Next Steps

📖 Continue reading:
- [09-DEPLOYMENT](./09-DEPLOYMENT.md) - Deployment procedures
- [10-TROUBLESHOOTING](./10-TROUBLESHOOTING.md) - Troubleshooting guide

---

**Document Version**: 1.0
**Last Updated**: 2024-01-15
**Author**: Data Engineering Team
