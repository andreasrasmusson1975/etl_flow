"""
Module: test_storage_account

This module contains unit tests for Azure Blob Storage account connectivity and operations
used in the ETL workflow. It verifies that the application can connect to the storage account,
access containers, and perform basic blob operations.

Tests:
    - Connection to Azure Blob Storage using provided credentials
    - Access to containers and listing of blobs

Requirements:
    - pytest
    - azure-storage-blob
"""

import os
import sqlite3
from etl_flow.helper_functions.helper_functions import create_db, insert_mock_data, to_storage_account
import pytest

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

def test_to_storage_account(tmp_db_path):
    """
    Test that to_storage_account uploads a backup file to Azure Blob Storage and returns the container name.

    This test:
        - Creates a temporary SQLite database and inserts mock data.
        - Calls to_storage_account to upload the backup to Azure Blob Storage using real credentials.
        - Asserts that the returned value is the expected container name.

    Requirements:
        - The environment variables CHAT_DB_CONTAINER_SAS_TOKEN and CHAT_DB_CONTAINER_URL must be set and valid.
        - Network access to Azure Blob Storage.

    Asserts:
        - The returned value matches the expected blob name.
    """
    # Setup: create a temporary database and insert mock data
    
    create_db(tmp_db_path)
    insert_mock_data(2, tmp_db_path)

    # The following environment variables must be set in your environment:
    # CHAT_DB_CONTAINER_SAS_TOKEN and CHAT_DB_CONTAINER_URL

    cname = to_storage_account(tmp_db_path)
    assert (cname == "chat-db-backups")



