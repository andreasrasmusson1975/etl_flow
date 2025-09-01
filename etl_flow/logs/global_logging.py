
"""
Module: global_logging
----------------------

This module provides a decorator for logging function calls, return values, and exceptions
to a global log file for the ETL workflow system. It ensures that all decorated functions
write structured log entries to 'global.log', including function names, arguments,
return values, and error traces.

Features:
    - Logs entry and exit of decorated functions.
    - Logs exceptions with full traceback information.
    - Automatically creates ther log file if it doesn't exist.
    - Uses UTF-8 encoding to support Unicode characters in log messages.

Usage:
    Import and apply the @log_this decorator to any function whose execution you want to log.

Example:
    from logs.global_logging import log_this

    @log_this
    def my_function(x, y):
        return x + y
"""
import logging
import functools

def log_this(func):
    """
    Decorator that logs the entry, exit, and exceptions of the decorated function.

    This function sets up a logger (if not already configured) and writes log messages
    to 'global.log' each time the decorated function is called. It logs the function
    name, arguments, return value, and any exceptions raised during execution.

    Args:
        func (callable): The function to be decorated and logged.

    Returns:
        callable: The wrapped function with logging enabled.

    Side Effects:
        - Creates the 'global.log' file if it does not exist.
        - Writes log entries for function calls, return values, and exceptions.

    Example:
        @log_this
        def my_function(x, y):
            return x + y
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        logger = logging.getLogger("global_logger")
        logger.setLevel(logging.INFO)
        if not logger.hasHandlers():
            fh = logging.FileHandler("global.log",encoding='utf-8')
            fh.setLevel(logging.INFO)
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        logger.info(f"Calling function: {func.__name__} with args: {args} and kwargs: {kwargs}")
        try:
            result = func(*args, **kwargs)
            logger.info(f"Function {func.__name__} returned: {result}")
            return result
        except Exception as e:
            logger.error(f"Error in function {func.__name__}: {e}", exc_info=True)
            raise
    return wrapper