cd C:\etl_job
call env\Scripts\activate
cd /d %~dp0
pytest -q
pause