"""
Utils module for dataflow_common - uses standard Python logging
"""
import logging

# สร้าง wrapper ง่ายๆ ที่แก้ปัญหา format 
class CompatibleLogger:
    """Logger wrapper ที่รองรับทั้ง format เดิมและใหม่"""
    
    def __init__(self, name):
        self._logger = logging.getLogger(name)
    
    def _format_message(self, *args):
        """แก้ปัญหา format ที่ใช้ 2 arguments"""
        if len(args) == 1:
            return str(args[0])
        elif len(args) == 2:
            # Handle: logger.info(step_id, message) -> "[step_id] message"
            return f"[{args[0]}] {args[1]}"
        else:
            return ' '.join(str(a) for a in args)
    
    def info(self, *args, **kwargs):
        self._logger.info(self._format_message(*args), **kwargs)
    
    def debug(self, *args, **kwargs):
        self._logger.debug(self._format_message(*args), **kwargs)
    
    def warning(self, *args, **kwargs):
        self._logger.warning(self._format_message(*args), **kwargs)
    
    def error(self, *args, **kwargs):
        self._logger.error(self._format_message(*args), **kwargs)
    
    def critical(self, *args, **kwargs):
        self._logger.critical(self._format_message(*args), **kwargs)
    
    # Forward other methods
    def __getattr__(self, name):
        return getattr(self._logger, name)

def get_dataflow_logger(name):
    """
    Returns a logger ที่:
    1. แก้ปัญหา format (รองรับทั้ง 1 และ 2 arguments)
    2. ใช้ standard Python logging
    3. Dataflow จะส่ง logs ไป Cloud Logging อัตโนมัติ
    """
    return CompatibleLogger(name)

def get_step_logger(step_class):
    """Get logger for a step class"""
    return get_dataflow_logger(f"dataflow_common.steps.{step_class.__class__.__name__}")

# Export
__all__ = ['get_dataflow_logger', 'get_step_logger', 'CompatibleLogger']