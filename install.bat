@echo off
if not exist "C:\etl_job" mkdir "C:\etl_job"
cd "C:\etl_job"
if not exist env (
    python -m venv env
)
call env\Scripts\activate
python -m pip install --upgrade pip
cd /d %~dp0
pip install .
etl-flow-schedule
cd "C:\etl_job"
etl-flow

echo "Installation complete."
pause
