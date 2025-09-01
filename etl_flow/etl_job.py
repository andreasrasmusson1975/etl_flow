"""
Module: etl_job

This module implements the ETL (Extract, Transform, Load) workflow.
It provides functions to load configuration, connect to Azure Blob Storage, download and validate
the latest JSON data, and insert the data into a SQLite database.

Functions:
    - load_yaml: Load YAML configuration files.
    - connect_to_storage_account: Connect to Azure Blob Storage using credentials.
    - download_latest_json: Download the latest JSON backup from storage.
    - validate_json_string: Validate a JSON file against a schema.
    - insert_to_db: Insert validated data into the database.
    - run_job: Execute the full ETL pipeline.
    - main: Command-line entry point for running the ETL job.

Requirements:
    - Azure Storage Blob SDK
    - PyYAML
    - jsonschema
    - SQLite3
"""

import sys
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from etl_flow.logs.global_logging import log_this
from pathlib import Path
import yaml
import os
from azure.storage.blob import BlobServiceClient
import tempfile
import json
import jsonschema
import sqlite3
from etl_flow.helper_functions.helper_functions import setup
from urllib.parse import urlparse, urlunparse

@log_this
def load_yaml(yaml_file_name: str) -> dict:
    """
    Loads a YAML file from the 'yaml' subdirectory and returns its contents as a dictionary.

    Args:
        yaml_file_name (str): The name of the YAML file to load (e.g., 'config.yaml').

    Returns:
        dict: The parsed YAML configuration as a Python dictionary.

    Example:
        config = load_yaml("config.yaml")
    """
    path = Path(__file__).parent / "yaml" / yaml_file_name
    with open(path, "r") as f:
        config = yaml.safe_load(f)
        print(f"   ✅ Successfully loaded configuration from {path}.")
        return config

@log_this
def connect_to_storage_account():
    """
    Connects to an Azure Blob Storage account using credentials from a YAML config file and environment variables.

    Loads the storage account configuration from 'config.yaml', retrieves the SAS token and container URL
    from environment variables, and establishes a connection to the specified Azure Blob Storage container.

    Returns:
        azure.storage.blob.ContainerClient: The connected container client for Azure Blob Storage.

    Example:
        container_client = connect_to_storage_account()
    """
    yaml_file_name = "config.yaml"
    config = load_yaml(yaml_file_name)
    sa = config.get("storage_account")
    sas_token = os.getenv(sa["sas_token_env"])
    container_url = os.getenv(sa["container_url_env"])
    u = urlparse(container_url)
    if not u.scheme or not u.netloc:
        raise ValueError(f"Invalid container URL: {container_url}")
    parts = [p for p in u.path.split('/') if p]
    if not parts:
        raise ValueError(f"Container name missing in URL path: {container_url}")
    container_name = parts[0]
    account_url = f"{u.scheme}://{u.netloc}"
    sas = u.query or sas_token
    blob_service_client = BlobServiceClient(account_url=account_url, container_name=container_name, credential=sas)
    container_client = blob_service_client.get_container_client(container_name)
    print(f"   ✅ Successfully connected to Azure Blob Storage account.")
    return container_client

@log_this
def download_latest_json(container_client):
    """
    Downloads the latest JSON blob from the specified Azure Blob Storage container.

    Finds the most recently created blob whose name starts with 'backup_', downloads its contents
    to a temporary file, and returns the path to that file.

    Args:
        container_client (azure.storage.blob.ContainerClient): The container client to use for listing and downloading blobs.

    Returns:
        str: The file path to the downloaded JSON file.

    Raises:
        Exception: If no matching blob is found or if the download fails.

    Example:
        tmp_file_path = download_latest_json(container_client)
    """
    try:
        blobs = container_client.list_blobs(name_starts_with="backup_")
        latest_blob = max(blobs, key=lambda b: b.creation_time, default=None)
        blob_client = container_client.get_blob_client(latest_blob.name)
        download_stream = blob_client.download_blob()
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            tmp_file.write(download_stream.readall())
            tmp_file_path = tmp_file.name
            print(f"   ✅ Successfully downloaded latest JSON file.")
        return tmp_file_path
    except Exception as e:
        raise e

@log_this
def validate_json_string(json_file_path: str) -> bool:
    """
    Validates a JSON file against a predefined JSON schema.

    Reads the JSON data from the specified file, loads the schema from 'json/schema.json',
    and checks if the data conforms to the schema using the jsonschema library.

    Args:
        json_file_path (str): The path to the JSON file to validate.

    Returns:
        bool: True if the JSON is valid according to the schema.

    Raises:
        jsonschema.ValidationError: If the JSON does not conform to the schema.

    Example:
        is_valid = validate_json_string("data.json")
    """
    with open(json_file_path, "r") as json_file:
        json_string = json_file.read()
    schema_path = Path(__file__).parent / "json" / "schema.json"
    with open(schema_path, "r") as schema_file:
        schema = json.load(schema_file)
    data = json.loads(json_string)
    try:
        jsonschema.validate(instance=data, schema=schema)
    except jsonschema.ValidationError as e:
        raise
    print(f"   ✅ Json validation successful.")
    return "✅ Json validation successful."

@log_this
def insert_to_db(
    json_file_path: str,
    table_name: str,
    insert_statement: str, 
    db_path: str = "local.db"
) -> str:
    """
    Inserts data from a JSON file into a SQLite database.

    Reads the JSON data from the specified file, connects to the SQLite database at the given path,
    and inserts the data into the appropriate table(s).

    Args:
        json_file_path (str): The path to the JSON file containing the data to insert.
        db_path (str): The path to the SQLite database file.

    Returns:
        None

    Example:
        insert_to_db("data.json", "database/mydb.sqlite")
    """
    con = sqlite3.connect(db_path)
    try:
        with open(json_file_path, "r") as f:
            json_data = f.read()
        data = json.loads(json_data)
        # Process and insert data into the database
        rows = data.get(table_name, [])
        for row in rows:
            con.execute(insert_statement, tuple(row.values()))
        con.commit()
        print(f"   ✅ Data inserted successfully to {table_name}.")
        return f"✅  Data inserted successfully to {table_name}."
    except Exception as e:
        con.rollback()
        raise e
    finally:
        con.close()

@log_this
def run_job(db_path: str = "local.db") -> str:
    """
    Executes the full ETL pipeline: loads SQL statements, connects to Azure Blob Storage,
    downloads and validates the latest JSON data, and inserts the data into the SQLite database.

    Steps performed:
        1. Loads SQL statements from 'sql.yaml'.
        2. Connects to the Azure Blob Storage container.
        3. Downloads the latest JSON backup file from the container.
        4. Validates the JSON file against the schema.
        5. Inserts the validated data into the 'sessions', 'events', and 'event_parents' tables in the database.

    Returns:
        str: Success message if the job completes successfully.

    Raises:
        Exception: If any step in the ETL process fails.

    Example:
        result = run_job()
    """
    try:
        yaml_file_name = "sql.yaml"
        sql_statements = load_yaml(yaml_file_name)["sql_statements"]
        container_client = connect_to_storage_account()
        json_file_path = download_latest_json(container_client)
        validation_result = validate_json_string(json_file_path)
        insert_to_db(json_file_path,"sessions", sql_statements["insert_sessions"], db_path)
        insert_to_db(json_file_path,"events", sql_statements["insert_events"], db_path)
        insert_to_db(json_file_path,"event_parents", sql_statements["insert_event_parents"], db_path)
        return "   ✅ Job ran successfully."
    except Exception as e:
        raise e
    
def main():
    """
    Entry point for running the ETL job from the command line.

    Sets up the database and tables, then executes the ETL pipeline by calling run_job().
    Prints status messages to indicate progress.

    This function is intended to be used as the main entry point for the ETL workflow,
    either when running etl_job.py directly or via a console script entry point.

    Example:
        python -m etl_flow.etl_job
        # or, if installed as a console script:
        etl-flow
    """
    # Ensure database and tables exist and that there is data in the storage account.
    # Not really part of the ETL job, but needed for this toy example to work.
    # In a real-world scenario, the database and tables would already exist and
    # there would be a steady stream of new data in the storage account.
    print( "ℹ️  Setting things up..." )
    setup()
    # Run the ETL job
    print( "\nℹ️ Running the ETL job..." )
    run_job()
if __name__ == "__main__":
    main()