"""
Module: test_logging

This module contains unit tests for the logging functionality used in the ETL workflow.
It verifies that log messages are correctly generated, formatted, and recorded by the logging system.

Tests:
    - Log file creation and content verification

Requirements:
    - pytest
    - logging
"""

import pytest
import logging
from etl_flow.logs.global_logging import log_this


@pytest.fixture()
def tmpfile_path(tmp_path):
    """
    Pytest fixture that provides a temporary file path for a SQLite database.

    This fixture creates a unique temporary directory for each test invocation
    and returns the path to a 'local.db' file within that directory. It ensures
    that each test runs with an isolated, clean database file.

    Args:
        tmp_path: Built-in pytest fixture providing a temporary directory unique to the test.

    Returns:
        str: The file path to a temporary SQLite database.
    """
    path = tmp_path / "temp.log"
    return str(path)

def test_log_this(tmpfile_path):
    """
    Test that the log_this decorator writes function call and return value to the specified log file.

    This test:
        - Applies the log_this decorator to a sample function.
        - Configures the global_logger to use a temporary log file.
        - Calls the decorated function.
        - Checks that the log file contains entries for the function call and its return value.
    """

    # Reconfigure the logger to use the temporary log file
    logger = logging.getLogger("global_logger")
    logger.handlers.clear()
    fh = logging.FileHandler(tmpfile_path, encoding="utf-8")
    fh.setFormatter(logging.Formatter('%(message)s'))
    logger.addHandler(fh)

    @log_this
    def add(x, y):
        return x + y

    result = add(2, 3)
    assert result == 5

    fh.flush()
    fh.close()

    with open(tmpfile_path, encoding="utf-8") as f:
        log_content = f.read()
        assert "Calling function: add" in log_content
        assert "Function add returned: 5" in log_content
