"""
Module: scheduling
----------------------

This module provides functions to schedule and manage the ETL job using the Windows Task Scheduler.

It allows you to create, update, and remove scheduled tasks that execute the ETL workflow at specified intervals.
The module supports flexible scheduling options (e.g., every N minutes, hours, or days) and can be used as a
command-line tool to automate ETL job execution or cleanup.

Functions:
    - schedule_job: Schedule the ETL job at a given interval.
    - remove_scheduled_job: Remove the scheduled ETL job by name.
    - main: Command-line entry point for scheduling or removing the ETL job.

Requires:
    - Windows operating system
    - pywin32 (win32com) library

Example:
    >> from scheduling import schedule_job, remove_scheduled_job
    >> schedule_job(datetime.now(), "5 minutes")
    >> remove_scheduled_job()
"""

import win32com.client
from pathlib import Path
from datetime import datetime, timedelta
import re
import sys

# Task Scheduler constants


TASK_TRIGGER_TIME   = 1 # Run once (can be repeated)  
TASK_TRIGGER_DAILY  = 2 # Repeat daily
TASK_TRIGGER_HOURLY = 3 # Repeat hourly
TASK_CREATE_OR_UPDATE = 6 # Create or update task
TASK_LOGON_SERVICE_ACCOUNT = 3 # Use service account

def schedule_job(
    start_time: datetime,
    repeat_every: str,
):
    """
    Schedules a Windows Task Scheduler job to run the ETL process at a specified interval.

    Creates or updates a scheduled task named 'run_etl_job' that executes the 'etl-flow.exe' entry point
    in the current Python environment. The task can be configured to run once, daily, or at a custom
    interval (e.g., every N minutes or hours) based on the 'repeat_every' argument.

    Args:
        start_time (datetime): The date and time when the first run should occur.
        repeat_every (str): The interval for repetition, such as '5 minutes', '2 hours', or '1 day'.

    Raises:
        ValueError: If 'repeat_every' is not in a recognized format.
        pywintypes.com_error: If there is an error communicating with the Windows Task Scheduler.

    Example:
        schedule_job(
            start_time=datetime.now() + timedelta(minutes=5),
            repeat_every="5 minutes"
        )

    Notes:
        - Scheduling requires Administrator privileges.
        - Ensure that the ETL environment is properly configured before scheduling.
    """
    # Parse the repeat_every string
    m = re.match(r"(\d+)\s*(minute|hour|day|minutes|hours|days)$", repeat_every, re.IGNORECASE)
    if not m:
        raise ValueError("repeat_every must be like '5 minutes', '2 hours', or '1 day'")
    interval = int(m.group(1))
    unit = m.group(2).lower()

    # Create/Connect scheduler
    service = win32com.client.Dispatch("Schedule.Service")
    service.Connect()
    root = service.GetFolder("\\")
    task_def = service.NewTask(0)

    # Registration info
    task_def.RegistrationInfo.Description = "Toy example of ETL job scheduling"

    # Settings
    task_def.Settings.Enabled = True
    task_def.Settings.StartWhenAvailable = True
    task_def.Settings.MultipleInstances = 0
    task_def.Settings.DisallowStartIfOnBatteries = False
    # Trigger
    start_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")
    if unit.startswith("day"):
        # Every N days at start_time
        trig = task_def.Triggers.Create(TASK_TRIGGER_DAILY)
        trig.StartBoundary = start_str
        trig.DaysInterval = interval
    else:
        # One-time trigger that *repeats* every N minutes/hours indefinitely
        trig = task_def.Triggers.Create(TASK_TRIGGER_TIME)
        trig.StartBoundary = start_str
        rep = trig.Repetition
        if unit.startswith("hour"):
            rep.Interval = f"PT{interval}H"
        else:
            rep.Interval = f"PT{interval}M"
        rep.Duration = "P9999D"

    # Action

    env_scripts = Path(sys.executable).parent  # Points to env\Scripts
    etl_flow_exe = env_scripts / "etl-flow.exe"  # On Windows

    # Create an execute action (Create(0))
    action = task_def.Actions.Create(0)
    action.Path = str(etl_flow_exe)
    action.WorkingDirectory = "C:\\etl_job"

    # Register to run as SYSTEM (requires admin)
    root.RegisterTaskDefinition(
        "run_etl_job",
        task_def,
        TASK_CREATE_OR_UPDATE,
        "SYSTEM",
        None,
        TASK_LOGON_SERVICE_ACCOUNT,
    )

def remove_scheduled_job():
    """
    Removes a scheduled task from the Windows Task Scheduler by its name.

    This function connects to the Windows Task Scheduler, locates the task with the specified
    name in the root folder, and deletes it if it exists.

    Args:
        job_name (str): The name of the scheduled task to remove.

    Raises:
        Exception: If the task cannot be found or deleted.

    Example:
        >>> remove_scheduled_job("run_etl_job.bat")
    """
    service = win32com.client.Dispatch("Schedule.Service")
    service.Connect()
    root = service.GetFolder("\\")
    root.DeleteTask("run_etl_job", 0)

def main():
    """
    Entry point for scheduling or removing the ETL job via the command line.

    If called with the argument "remove", this function removes the scheduled task named 'run_etl_job'.
    Otherwise, it schedules the ETL job to run every 5 minutes, starting 5 minutes from now.

    Usage:
        python -m etl_flow.scheduling.scheduling           # Schedules the ETL job
        python -m etl_flow.scheduling.scheduling remove    # Removes the scheduled ETL job

    Example:
        main()
    """
    if len(sys.argv) > 1 and sys.argv[1] == "remove":
        remove_scheduled_job()
        print("Removed scheduled job: run_etl_job")
        return
    schedule_job(
        start_time=datetime.now() + timedelta(minutes=5),
        repeat_every="5 minutes",
    )


if __name__ == "__main__":
    main()

