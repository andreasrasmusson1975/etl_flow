"""
Module: test_scheduling

This module contains tests for the scheduling functionality of the ETL workflow.
It verifies that scheduled tasks can be created, detected, and removed using the Windows Task Scheduler.

Tests:
    - Creation of scheduled ETL jobs
    - Detection of existing scheduled tasks
    - Removal of scheduled tasks

Requirements:
    - pytest
    - pywin32 (win32com)
    - Windows operating system
"""

import pytest
from datetime import datetime, timedelta
from etl_flow.scheduling.scheduling import schedule_job, remove_scheduled_job
import win32com.client

def task_exists(task_name: str) -> bool:
    """
    Checks if a scheduled task with the given name exists in the Windows Task Scheduler.

    Connects to the Windows Task Scheduler, searches for a task with the specified name
    in the root folder, and returns True if the task exists, otherwise False.

    Args:
        task_name (str): The name of the scheduled task to check.

    Returns:
        bool: True if the task exists, False otherwise.
    """
    service = win32com.client.Dispatch("Schedule.Service")
    service.Connect()
    root = service.GetFolder("\\")
    try:
        root.GetTask(task_name)
        return True
    except Exception:
        return False

def test_schedule_and_remove_job():
    """
    Test that schedules a Windows Task Scheduler job, verifies its existence,
    removes it, and verifies its removal.

    This test:
        - Checks if the task 'run_etl_job' exists.
        - Removes the task and verifies it no longer exists.
        - Schedules the task again.
        - Verifies that the task exists after scheduling.

    Note:
        This test requires administrator privileges and will modify the Windows Task Scheduler.
        Use with caution on production systems.
    """
    # Check that the task exists
    assert task_exists("run_etl_job")

    # Remove the job
    remove_scheduled_job()
    # Check that the task is gone
    assert not task_exists("run_etl_job")

    # Schedule the job
    schedule_job(
        start_time=datetime.now() + timedelta(minutes=2),
        repeat_every="5 minutes",
    )

    # Check that the task exists
    assert task_exists("run_etl_job")