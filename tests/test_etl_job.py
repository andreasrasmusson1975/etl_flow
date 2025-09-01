"""
Module: test_etl_job

This module contains unit and integration tests for the core ETL job functions in the ETL workflow.
It verifies the correct loading of YAML configuration files, connection to Azure Blob Storage,
downloading and validation of JSON data, and insertion of data into the SQLite database.

Tests:
    - Loading and parsing of YAML configuration files
    - Connecting to Azure Blob Storage
    - Downloading the latest JSON backup from storage
    - Validating JSON data against a schema
    - Inserting data into the database
    - Running the full ETL pipeline

Requirements:
    - pytest
    - PyYAML
    - azure-storage-blob
    - jsonschema
    - sqlite3
"""

import pytest
from pathlib import Path
import yaml
import tempfile
from etl_flow.helper_functions.helper_functions import *
from etl_flow.etl_job import *

@pytest.fixture()
def tmp_db_path(tmp_path):
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
    db_path = tmp_path / "local.db"
    return str(db_path)


def test_load_yaml():
    """
    Tests that the load_yaml function correctly loads and parses a valid YAML file.

    Asserts:
        - The returned result is a dictionary
    """
    config = load_yaml("config.yaml")
    assert isinstance(config, dict)


def test_connect_to_storage_account():
    """
    Test for connect_to_storage_account.

    This test loads the storage account configuration, checks that the required environment variables
    for the SAS token and container URL are set, and attempts to connect to Azure Blob Storage.
    It asserts that the returned container client has a 'list_blobs' method, indicating a successful connection.

    Asserts:
        - The required environment variables are set.
        - The returned container client has a 'list_blobs' method.

    Requirements:
        - Valid Azure Blob Storage credentials and config.yaml.
        - Environment variables for SAS token and container URL must be set.
    """
    # Load config to get expected env var names
    config = load_yaml("config.yaml")
    sa = config.get("storage_account")
    sas_token_env = sa["sas_token_env"]
    container_url_env = sa["container_url_env"]

    # Ensure the required environment variables are set
    assert os.getenv(sas_token_env), f"Environment variable {sas_token_env} must be set."
    assert os.getenv(container_url_env), f"Environment variable {container_url_env} must be set."

    # Attempt to connect
    container_client = connect_to_storage_account()
    # The container client should have a list_blobs method if connection succeeded
    assert hasattr(container_client, "list_blobs")

def test_download_latest_json():
    """
    Test for download_latest_json.

    This test connects to the Azure Blob Storage container, downloads the latest JSON backup file,
    and asserts that the file exists and is not empty.

    Asserts:
        - The returned file path is not None.
        - The downloaded file exists and contains data.

    Requirements:
        - The Azure Blob Storage container must be accessible and contain at least one valid 'backup_' JSON blob.
    """
    yaml_file_name = "sql.yaml"
    sql_statements = load_yaml(yaml_file_name)["sql_statements"]
    container_client = connect_to_storage_account()
    json_file_path = download_latest_json(container_client)
    file_path = download_latest_json(container_client)
    # The function should return a path to a file that exists and is not empty
    assert file_path is not None
    with open(file_path, "r") as f:
        content = f.read()
        assert content  # File should not be empty

def test_download_and_validate_json():
    """
    Test for download_and_validate_json.

    This test connects to the Azure Blob Storage container, downloads the latest JSON backup file,
    and validates its content against the expected SQL statements.

    Asserts:
        - The downloaded file exists.
        - That the validation is successful.

    Requirements:
        - The Azure Blob Storage container must be accessible and contain at least one valid 'backup_' JSON blob.
    """
    container_client = connect_to_storage_account()
    file_path = download_latest_json(container_client)
    assert file_path is not None
    result = validate_json_string(file_path)
    assert "validation successful" in result

def test_insert_to_db(tmp_path):
    """
    Test for insert_to_db.

    This test creates a temporary SQLite database, a table, and inserts data into the table
    using the insert_to_db function. It asserts that the data was inserted successfully.

    Asserts:
        - The function returns a success message.
        - The data is correctly inserted into the database.

    Requirements:
        - The insert_to_db function must be implemented correctly.
    """
    # Create a temporary SQLite database
    db_path = tmp_path / "test.db"
    table_name = "test_table"
    insert_statement = "INSERT INTO test_table (id, name) VALUES (?, ?)"

    # Create the table
    con = sqlite3.connect(db_path)
    con.execute(f"CREATE TABLE {table_name} (id INTEGER, name TEXT)")
    con.commit()
    con.close()

    # Create a JSON file with data to insert
    data = {
        table_name: [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]
    }
    json_file = tmp_path / "data.json"
    with open(json_file, "w") as f:
        json.dump(data, f)

    # Call the function
    result = insert_to_db(str(json_file), table_name, insert_statement, str(db_path))
    assert "successfully" in result

    # Verify data was inserted
    con = sqlite3.connect(db_path)
    rows = list(con.execute(f"SELECT * FROM {table_name}"))
    con.close()
    assert rows == [(1, "Alice"), (2, "Bob")]

def test_run_job(tmp_db_path):
    """
    Integration test for run_job.

    This test orchestrates the entire ETL process by calling the run_job function.
    It asserts that the function completes successfully and returns the expected output.

    Asserts:
        - The function returns a success message.

    Requirements:
        - The run_job function must be implemented correctly.
    """
    create_db(tmp_db_path, ensure_wal=True)
    result = run_job(tmp_db_path)
    assert "successfully" in result

