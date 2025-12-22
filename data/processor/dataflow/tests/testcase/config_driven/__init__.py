"""
Config-driven pipeline tests.

This module contains tests for pipelines that use the Orchestrator pattern
with config.py, orchestrator.py, and core.py.

NOTE: This module may be removed in the future when pipelines switch to
direct import of steps/DoFns without using the Orchestrator.

Used by:
- customer_profile_batch_initial (uses config + orchestrator)
- customer_profile_realtime (uses config + orchestrator)
"""
