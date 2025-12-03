# dataflow_common/src/dataflow_common/utils/logger.py
import logging
import sys

def get_dataflow_logger(name):
    """
    Get a logger configured for Dataflow (not Airflow)
    
    Returns:
        logging.Logger: Logger that outputs to Cloud Logging
    """
    logger = logging.getLogger(name)
    
    # Check if already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        
    return logger

# Convenience function for step classes
def get_step_logger(step_class):
    """Get logger for a step class"""
    return get_dataflow_logger(f"dataflow_common.steps.{step_class.__class__.__name__}")