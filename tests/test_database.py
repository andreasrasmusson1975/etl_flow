"""
Module: test_database
----------------------------

This module contains unit tests for the database-related functions 
in this project.

Features Tested:
    - Creation of required tables and indices in the database schema.
    - Idempotency of schema creation (safe to call multiple times).
    - Enabling and verification of SQLite Write-Ahead Logging (WAL) mode.
    - Correct insertion of mock session and event data.
    - Enforcement of foreign key constraints and cascading deletes.

Test Utilities:
    - Uses pytest fixtures for temporary database paths.
    - Provides helper functions to check table and index existence.

Usage:
    Run with pytest to execute all tests:
        pytest test_create_database.py
"""

import json
import re
import os
import sqlite3
import uuid
import pytest

from etl_flow.helper_functions.helper_functions import *


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


def table_exists(con, name: str) -> bool:
    """
    Checks if a table with the given name exists in the SQLite database.

    Args:
        con: An open sqlite3.Connection object.
        name (str): The name of the table to check for existence.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    cur = con.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?;", (name,)
    )
    return cur.fetchone() is not None


def index_names(con):
    """
    Returns the set of user-defined index names in the SQLite database.

    Args:
        con: An open sqlite3.Connection object.

    Returns:
        set: A set containing the names of all non-auto-generated indices in the database.
    """
    cur = con.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_autoindex%';"
    )
    return {row[0] for row in cur.fetchall()}


def test_create_database(tmp_db_path):
    """
    Test that create_database creates all required tables in the SQLite database.

    This test verifies that after initializing the database, the 'sessions',
    'events', and 'event_parents' tables exist as expected.

    Args:
        tmp_db_path (str): Path to a temporary SQLite database file provided by the fixture.

    Asserts:
        - The 'sessions' table exists.
        - The 'events' table exists.
        - The 'event_parents' table exists.
    """
    create_db(tmp_db_path, ensure_wal=True)
    con = sqlite3.connect(tmp_db_path)
    try:
        assert table_exists(con, "sessions")
        assert table_exists(con, "events")
        assert table_exists(con, "event_parents")
    finally:
        con.close()


def test_schema_idempotent(tmp_db_path):
    """
    Test that create_database can be called multiple times without error or duplication.

    This test ensures that running the schema creation on an existing database
    does not raise errors and does not create duplicate tables or objects.

    Args:
        tmp_db_path (str): Path to a temporary SQLite database file provided by the fixture.
    """
    create_db(tmp_db_path, ensure_wal=True)
    # Run again — should not crash and no duplicate objects should be created
    create_db(tmp_db_path, ensure_wal=True)
    con = sqlite3.connect(tmp_db_path)
    try:
        assert table_exists(con, "sessions")
        assert table_exists(con, "events")
        assert table_exists(con, "event_parents")
    finally:
        con.close()


def test_wal_enabled_when_requested(tmp_db_path):
    """
    Test that create_database enables Write-Ahead Logging (WAL) mode when requested.

    This test verifies that the database is set to use WAL journal mode and that
    the synchronous pragma is set to NORMAL or a compatible value when ensure_wal=True.

    Args:
        tmp_db_path (str): Path to a temporary SQLite database file provided by the fixture.
    """
    create_db(tmp_db_path, ensure_wal=True)
    con = sqlite3.connect(tmp_db_path)
    try:
        mode = con.execute("PRAGMA journal_mode;").fetchone()[0].lower()
        # journal_mode can return 'wal' directly or after being set — we check that it is wal.
        assert mode == "wal"
        # synchronous NORMAL (2)
        sync = con.execute("PRAGMA synchronous;").fetchone()[0]
        assert sync in (1, 2)
    finally:
        con.close()


def test_indices_exist(tmp_db_path):
    """
    Test that create_database creates the required indices in the SQLite database.

    This test verifies that after initializing the database, the expected user-defined
    indices ('events_session_ts' and 'events_session_kind') exist as specified in the schema.

    Args:
        tmp_db_path (str): Path to a temporary SQLite database file provided by the fixture.

    Asserts:
        - The 'events_session_ts' index exists.
        - The 'events_session_kind' index exists.
    """
    create_db(tmp_db_path, ensure_wal=False)
    con = sqlite3.connect(tmp_db_path)
    try:
        idxs = index_names(con)
        # The names come from SCHEMA_SQL
        assert "events_session_ts" in idxs
        assert "events_session_kind" in idxs
    finally:
        con.close()


def test_insert_mock_data(tmp_db_path):
    """
    Test that insert_mock_data correctly inserts to sessions, events, and event_parents.

    This test verifies that after calling insert_mock_data, the expected number of
    sessions, events, and event_parents (edges) are present in the database.

    Args:
        tmp_db_path (str): Path to a temporary SQLite database file provided by the fixture.

    Asserts:
        - The 'sessions' table has 3 rows.
        - The 'events' table has 6 rows.
        - The 'event_parents' table has 3 rows.
    """
    create_db(tmp_db_path, ensure_wal=False)
    insert_mock_data(3, tmp_db_path)

    con = sqlite3.connect(tmp_db_path)
    try:
        cur = con.cursor()
        sessions = cur.execute("SELECT COUNT(*) FROM sessions;").fetchone()[0]
        events = cur.execute("SELECT COUNT(*) FROM events;").fetchone()[0]
        edges = cur.execute("SELECT COUNT(*) FROM event_parents;").fetchone()[0]

        # From insert_mock_data: 3 sessions, 2 events per session, 1 link per session
        assert sessions == 3
        assert events == 6
        assert edges == 3
    finally:
        con.close()


def test_event_parents_foreign_keys(tmp_db_path):
    """
    Test that foreign key constraints are enforced for event_parents relationships.

    This test verifies that:
      - Each child-parent relationship in event_parents refers to valid events.
      - Deleting a parent event cascades and removes the corresponding row in event_parents.

    Args:
        tmp_db_path (str): Path to a temporary SQLite database file provided by the fixture.

    Asserts:
        - The 'child_id' exists in the 'events' table.
        - The 'parent_id' exists in the 'events' table.
        - After deleting a parent event, the corresponding row in 'event_parents' is also deleted.
    """
    create_db(tmp_db_path, ensure_wal=False)
    insert_mock_data(3,tmp_db_path)

    con = sqlite3.connect(tmp_db_path)
    try:
        con.execute("PRAGMA foreign_keys = ON;")
        cur = con.cursor()

        # Fetch a child-parent pair
        row = cur.execute("SELECT child_id, parent_id FROM event_parents LIMIT 1;").fetchone()
        assert row is not None
        child_id, parent_id = row

        # Both should exist in events
        child_exists = cur.execute("SELECT 1 FROM events WHERE id=?;", (child_id,)).fetchone()
        parent_exists = cur.execute("SELECT 1 FROM events WHERE id=?;", (parent_id,)).fetchone()
        assert child_exists is not None
        assert parent_exists is not None

        # Test FK: delete parent event => the row in event_parents should be CASCADE deleted
        cur.execute("DELETE FROM events WHERE id=?;", (parent_id,))
        con.commit()

        still_linked = cur.execute(
            "SELECT 1 FROM event_parents WHERE parent_id=?;", (parent_id,)
        ).fetchone()
        assert still_linked is None  # Should be deleted
    finally:
        con.close()


def test_remove_db_(tmp_db_path):
    """
    Test that remove_db deletes an existing database file and does not raise an error for a non-existent file.

    This test creates a temporary file to simulate a database, verifies that remove_db removes it,
    and then calls remove_db on a non-existent file to ensure no exception is raised.

    Asserts:
        - The database file is deleted after calling remove_db.
        - No exception is raised when attempting to remove a non-existent file.
    """
    # Create a temporary file to act as a fake database
    create_db(tmp_db_path)
    # Ensure the file exists
    assert os.path.exists(tmp_db_path)

    # Remove the file using remove_db
    remove_db(tmp_db_path)

    # Now the file should not exist
    assert not os.path.exists(tmp_db_path)
    # Use a random temp file path that does not exist
    db_path = os.path.join(tempfile.gettempdir(), "nonexistent_db_file.db")
    # Ensure the file does not exist
    if os.path.exists(db_path):
        os.remove(db_path)
    # Should not raise any exception
    remove_db(db_path)

def test_truncate_db(tmp_db_path):
    """
    Test that truncate_db removes all data from sessions, events, and event_parents tables.

    This test:
        - Creates a temporary SQLite database and inserts mock data.
        - Calls truncate_db to remove all data.
        - Asserts that all relevant tables are empty after truncation.

    Asserts:
        - The 'sessions' table is empty.
        - The 'events' table is empty.
        - The 'event_parents' table is empty.
    """
    create_db(tmp_db_path)
    insert_mock_data(2, str(tmp_db_path))

    # Ensure tables have data before truncation
    con = sqlite3.connect(tmp_db_path)
    cur = con.cursor()
    assert cur.execute("SELECT COUNT(*) FROM sessions;").fetchone()[0] > 0
    assert cur.execute("SELECT COUNT(*) FROM events;").fetchone()[0] > 0
    assert cur.execute("SELECT COUNT(*) FROM event_parents;").fetchone()[0] > 0
    con.close()

    # Truncate the database
    truncate_db(str(tmp_db_path))

    # Check that all tables are empty
    con = sqlite3.connect(str(tmp_db_path))
    cur = con.cursor()
    assert cur.execute("SELECT COUNT(*) FROM sessions;").fetchone()[0] == 0
    assert cur.execute("SELECT COUNT(*) FROM events;").fetchone()[0] == 0
    assert cur.execute("SELECT COUNT(*) FROM event_parents;").fetchone()[0] == 0

