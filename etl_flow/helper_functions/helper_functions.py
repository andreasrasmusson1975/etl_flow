"""
Module: helper_functions
------------------------

This module provides utility functions for setting up the ETL workflow's SQLite database
and Azure Blob Storage integration. It includes helpers for database creation, removal,
mock data insertion, data dumping, configuration loading, uploading backups to Azure,
and database truncation.

Features:
    - Create, remove, and truncate the SQLite database.
    - Insert mock session and event data for testing and development.
    - Dump database tables to a JSON file for backup or migration.
    - Load YAML configuration files for environment and storage settings.
    - Upload database backups to Azure Blob Storage using SAS tokens.
    - Clean up temporary files after upload.
    - Setup workflow for initializing, backing up, and resetting the database.

Dependencies:
    - sqlite3
    - os
    - uuid
    - json
    - datetime
    - yaml
    - azure-storage-blob
    - pathlib
    - tempfile

Example:
    from helper_functions.helper_functions import setup
    setup("local.db"),
"""

import sqlite3
import os
import uuid
import json
from datetime import datetime, timedelta
from etl_flow.logs.global_logging import log_this
import yaml
from azure.storage.blob import BlobServiceClient, ContainerClient, ContentSettings
from pathlib import Path
import tempfile
from urllib.parse import urlparse, urlunparse

def load_yaml(yaml_file_name: str) -> dict:
    """
    Loads a YAML file and returns its contents as a dictionary.

    Args:
        yaml_file_name (str or Path): The file path to the YAML configuration file.

    Returns:
        dict: The parsed configuration data.

    Example:
        >>> config = load_config("config.yaml")
        >>> print(config["storage_account"]["sas_token_env"])
    """
    path = Path(__file__).parent.parent / "yaml" / yaml_file_name
    with open(path, "r") as f:
        return yaml.safe_load(f)    

def remove_db(db_path: str = "local.db") -> None:
    """
    Removes the SQLite database file if it exists.

    Args:
        db_path (str): Path to the SQLite database file. Defaults to "local.db".

    Side Effects:
        - Deletes the database file if it exists.

    Example:
        >>> remove_db("database/local.db")
    """
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
            print(f"   ✅ Database file {db_path} removed.")
        else:
            print(f"   ⚠️ Database file {db_path} does not exist.")
    except Exception as e:
        raise e

def create_db(db_path: str = "local.db", *, ensure_wal: bool = True) -> None:
    """
    Creates and initializes the SQLite database for the ETL workflow system.

    This function ensures the database file and its parent directory exist, applies
    the schema for sessions, events, and event lineage tables, and configures SQLite
    pragmas for durability and concurrency (WAL mode by default).

    Args:
        db_path (str): Path to the SQLite database file. Defaults to "local.db".
        ensure_wal (bool): If True, sets SQLite to use Write-Ahead Logging (WAL) mode and
            other recommended settings for local concurrency and durability.

    Side Effects:
        - Creates the database file and parent directories if they do not exist.
        - Applies schema and index creation scripts.
        - Closes the connection after initialization.

    Example:
        >>> create_database("database/local.db")
    """
    sql_statements = load_yaml("sql.yaml")["sql_statements"]
    con = sqlite3.connect(db_path)
    con.execute("PRAGMA foreign_keys = ON;")
    try:
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)    
        if ensure_wal:
            # Good durability + concurrency for a local single-writer setup
            con.execute("PRAGMA journal_mode = WAL;")
            con.execute("PRAGMA synchronous = NORMAL;")
            con.execute("PRAGMA temp_store = MEMORY;")
        # Apply schema
        con.executescript(sql_statements["schema_sql"])
        con.commit()
        print("   ✅ Database created and initialized.")
    
    except Exception as e:
        raise e
    finally:
        con.close()
    


def insert_mock_data(n_rows: int, db_path: str = "local.db") -> None:
    """
    Inserts mock sessions and conversation events into the database.

    Args:
        n_rows (int): The number of mock rows to insert.
        db_path (str): Path to the SQLite database file.

    Side Effects:
        - Adds new rows to the tables sessions, events, and event_parents.

    Example:
        >>> insert_mock_data("database/local.db")
    """
    sql_statements = load_yaml("sql.yaml")["sql_statements"]
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()

        def new_id():
            return str(uuid.uuid4())

        base_time = datetime.now()

        # --- Create sessions ---
        session_ids = []
        for i in range(n_rows):
            sid = new_id()
            session_ids.append(sid)
            cur.execute(
                sql_statements["insert_sessions"],
                (sid, (base_time + timedelta(minutes=i*10)).isoformat(), json.dumps({"user": f"user{i+1}"}))
            )

        # --- Create events per session ---
        for s_idx, sid in enumerate(session_ids, start=1):
            # User prompt
            e_user = new_id()
            user_ts = (base_time + timedelta(minutes=s_idx*10 + 1)).isoformat()
            cur.execute(
                sql_statements["insert_events"],
                (
                    e_user,
                    user_ts,
                    sid,
                    1,
                    "user_prompt",
                    "User",
                    json.dumps({"text": f"Hello, this is session {s_idx}!"}),
                    f"Hello, this is session {s_idx}!",
                    json.dumps({"tokens": 5, "temp": 0.0}),
                ),
            )

            # Assistant reply
            e_assistant = new_id()
            asst_ts = (base_time + timedelta(minutes=s_idx*10 + 2)).isoformat()
            cur.execute(
                sql_statements["insert_events"],
                (
                    e_assistant,
                    asst_ts,
                    sid,
                    1,
                    "assistant_out",
                    "Assistant",
                    json.dumps({"text": f"Hi User{s_idx}, nice to meet you."}),
                    f"Hi User{s_idx}, nice to meet you.",
                    json.dumps({"tokens": 7, "temp": 0.2}),
                ),
            )

            # Link assistant reply to user prompt
            cur.execute(
                sql_statements["insert_event_parents"],
                (e_assistant, e_user)
            )

        con.commit()
        con.close()
        print(f"   ✅ Inserted {len(session_ids)} mock sessions with conversations into {db_path}.")
    except Exception as e:
        raise e
    finally:
        con.close()

def dump_json(con) -> str:
    """
    Dumps the tables of the database to a json string.

    Args:
        con: An open sqlite3.Connection object.

    Returns:
        str: A json string representing the database dump.
    """
    file_name = "dbdump.json"
    try:
        cur = con.cursor()
        tables = ["sessions", "events", "event_parents"]
        dump_data = {}
        for table in tables:
            cur.execute(f"SELECT * FROM {table};")
            columns = [description[0] for description in cur.description]
            rows = cur.fetchall()
            dump_data[table] = [dict(zip(columns, row)) for row in rows]
        json_string = json.dumps(dump_data, indent=2, default=str)
        with tempfile.NamedTemporaryFile(delete=False, mode="w", suffix=".json") as tmp_file:
            tmp_file.write(json_string)
        
        return tmp_file.name
    except Exception as e:
        raise 



def parse_container_url(container_url: str):
    """
    Parse an Azure Blob container URL (optionally with SAS) and return:
      account_url (e.g., https://acct.blob.core.windows.net)
      container_name (e.g., mycontainer)
      sas (query string or None)
    """
    if not container_url:
        raise ValueError("container_url is empty or not set")

    u = urlparse(container_url)
    if not u.scheme or not u.netloc:
        raise ValueError(f"Invalid container URL: {container_url}")

    # path like '/mycontainer' or '/mycontainer/...'
    parts = [p for p in u.path.split('/') if p]
    if not parts:
        raise ValueError(f"Container name missing in URL path: {container_url}")
    container_name = parts[0]

    account_url = f"{u.scheme}://{u.netloc}"
    sas = u.query or None
    return account_url, container_name, sas

def to_storage_account(database_path: str = "local.db") -> str:
    """
    Dump the SQLite database to a temporary JSON file and upload it to Azure Blob Storage.
    Returns the blob URL (without SAS).
    """
    # Paths & config
    yaml_file_name = "config.yaml"

    # Connect DB
    con = sqlite3.connect(database_path)

    # Dump DB to a temp JSON file (your dump_json should return a file path)
    tmp_json_path = dump_json(con)  # <- your existing function

    try:
        # Load storage account configuration
        config = load_yaml(yaml_file_name)
        sa_cfg = config["storage_account"]

        # Read env vars that hold the *values* we need
        sas_token_env = sa_cfg.get("sas_token_env")
        container_url_env = sa_cfg.get("container_url_env")

        sas_token = os.getenv(sas_token_env) if sas_token_env else None
        container_url = os.getenv(container_url_env)

        if not container_url:
            raise RuntimeError(
                f"{container_url_env} is not set. "
                "Ensure the task's user context has this environment variable."
            )

        # Parse container URL
        account_url, container_name, sas_in_url = parse_container_url(container_url)

        # Prefer SAS from URL if present, else fall back to separate env SAS
        effective_sas = sas_in_url or sas_token

        # Build a ContainerClient
        if effective_sas:
            # If you already have a full container URL including SAS, use from_container_url
            if sas_in_url:
                container_client = ContainerClient.from_container_url(container_url)
            else:
                # No SAS in URL, but SAS token present separately
                container_client = ContainerClient(account_url, container_name, credential=effective_sas)
        else:
            # No SAS at all
            raise RuntimeError(
                "No SAS provided in URL or environment."
            )

        # Ensure container exists (no-op if it already does)
        try:
            container_client.create_container()
        except Exception:
            # Likely already exists; ignore more specific errors to keep it simple
            pass

        # Create a unique blob name
        timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
        blob_name = f"backup_{timestamp}.json"

        # Upload
        content_settings = ContentSettings(content_type="application/json")
        with open(tmp_json_path, "rb") as f:
            container_client.upload_blob(
                name=blob_name,
                data=f,
                overwrite=True,
                content_settings=content_settings,
            )
        print("   ✅ Database backup uploaded to storage account.")
        return container_name
    finally:
        try:
            os.remove(tmp_json_path)
        except Exception:
            pass
        con.close()

def truncate_db(db_path: str = "local.db") -> None:
    """
    Truncates all data from the database tables: sessions, events, and event_parents.

    Args:
        db_path (str): Path to the SQLite database file. Defaults to "local.db".

    Side Effects:
        - Deletes all rows from the specified tables.

    Example:
        >>> truncate_db("database/local.db")
    """
    try:
        con = sqlite3.connect(db_path)
        cur = con.cursor()
        cur.execute("DELETE FROM event_parents;")
        cur.execute("DELETE FROM events;")
        cur.execute("DELETE FROM sessions;")
        con.commit()
        con.close()
        print(f"   ✅ Truncated all data from {db_path}.")
    except Exception as e:
        raise e
    finally:
        con.close()

def setup(db_path: str = "local.db", n_mock_sessions: int = 500) -> None:
    """
    Sets up the SQLite database by removing any existing database file,
    creating a new database, and inserting mock data.

    Args:
        db_path (str): Path to the SQLite database file. Defaults to "local.db".
        n_mock_sessions (int): Number of mock sessions to insert. Defaults to 500.

    Side Effects:
        - Removes existing database file if it exists.
        - Creates a new database and initializes it.
        - Inserts mock sessions and conversation events.

    Example:
        >>> setup("local.db", 10)
    """
    remove_db(db_path)
    create_db(db_path)
    insert_mock_data(n_mock_sessions, db_path)
    to_storage_account(db_path)
    truncate_db(db_path)