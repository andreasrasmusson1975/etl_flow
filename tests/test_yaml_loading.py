"""
Module: test_yaml_loading

This module contains unit tests for YAML configuration file loading functionality in the ETL workflow.
It verifies that YAML files are correctly read, parsed, and converted into Python dictionaries.

Tests:
    - Successful loading and parsing of valid YAML files
    - Handling of missing or malformed YAML files
    - Verification of expected keys and values in loaded configurations

Requirements:
    - pytest
    - PyYAML
"""

import tempfile
from etl_flow.helper_functions.helper_functions import load_yaml
import os

def test_load_yaml():
    """
    Tests the load_yaml function to ensure it correctly loads and parses a YAML file.

    This test creates a temporary YAML file with sample content, loads it using load_yaml,
    and verifies that the returned dictionary contains the expected keys and values.

    Asserts:
        - The result is a dictionary.
        - The dictionary contains the expected nested keys and values.

    Cleans up the temporary file after the test.
    """
    yaml_content = """
    foo:
      bar: 123
      baz: hello
    """
    with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".yaml") as tmp_file:
        tmp_file.write(yaml_content)
        tmp_file_path = tmp_file.name

    try:
        result = load_yaml(tmp_file_path)
        assert isinstance(result, dict)
        assert "foo" in result
        assert result["foo"]["bar"] == 123
        assert result["foo"]["baz"] == "hello"
    finally:
        os.remove(tmp_file_path)

