# etl_flow

## Introduction

The etl_flow package can be used to simulate the following situation:

1. You have some chatbot application that writes session- and event logs in json format
   to some container in Azure blob storage.

2. You also have a sql database for storing session and event information

3. Every night the database is updated with the information in the last created json file
   in the container.

For simplicity, and since this is a toy example, a sqlite database is used but the principle is
similar for any sql database.

## Requirements
1. You must be on a windows machine
2. You must have an Azure storage account
3. You must have an active SAS-token for some container in that storage account
4. You must set the container url you want to use in a system environment variable named CHAT_DB_CONTAINER_URL
5. You must set the SAS-token in a system environment variable named CHAT_DB_CONTAINER_SAS_TOKEN

## Install
Simply run install.bat as Administrator. This will:
1. Create the folder C:\etl_job
2. Create a python environment in that folder and activate it
3. Install the etl_flow package into that environment
4. Schedule a Windows Task Scheduler job to run the etl flow every five minutes.
5. Run the etl job once.

After installation, you should see the following in the C:\etl_job folder:
1. A folder called env
2. A file called global.log (The log file for job runs)
3. A file called local.db (the toy example database)

## Uninstall
Simply run uninstall.bat as Administrator. This will remove the task scheduler job and the
etl_job folder.

## Package structure
The etl_flow package consists of 5 subpackages:
1. helper_functions - Contains a module also named helper_functions, which contains
mostly functions that help setting up a toy database of chat sessions/events and exporting
a json version of three tables in that database to blob storage.
2. json - A very simple package containing the json schema to be used when dumping database table
content to a json file.
3. logs - Contains a module named global_logging that defines a decorator function log_this, 
which can be used for logging the execution of functions. This way, you don't have to write
logg messages inside each and every function.
4. scheduling - Contains a module also named scheduling, which contains code for creating
and removing Windows Task Scheduler jobs.
5. yaml - A very simple package containing two yaml files. One for storing the names of
the environment variables used for the Azure storage account and one for storing the
set of sql statements that are used in the job.

There is also a stand-alone file etl_job.py in the root of the package. This is the script that
is executed when the job is run.

## What happens during execution of the job?
When the etl job is run, the following happens (and this is where we realize it's a toy example):
1. The database is removed (if it exists).
2. The database is recreated and filled with mock data.
3. Database contents are dumped to json and uploaded to blob storage.
4. All tables in the database are truncated.
5. The latest json in blob storage is downloaded.
6. The contents of the json file are inserted to the tables in the database.

## Running the job manually
If you want to run the job manually after installation, you can open up a terminal and type:

C:\etl_job\env\Scripts\etl-flow.exe

This will run the job, creating the database and the log in the CWD, so it won't interfere with C:\etl_job contents.

There is also a convenience bat script "run_manually.bat" in the project root that does exactly the same.

## Testing
There is a suite of test scripts in the test folder in the project root:
1. test-database.py - tests for the database-related functions
2. test_logging.py - tests for the logging functionality
3. test_scheduling.py - tests for the scheduling functionality of the ETL workflow
4. test_storage_account.py - tests for Azure Blob Storage account connectivity and operations
5. test_yaml_loading.py - tests for YAML configuration file loading functionality

### Runing the tests
Run the file run_tests.bat as Administrator. Administrator priviledges are needed to test the task
scheduling functionality.



