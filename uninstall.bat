cd "C:\etl_job"
call env\Scripts\activate
etl-flow-schedule remove
cd /d %~dp0
rmdir /s /q "C:\etl_job"
echo "Uninstallation complete."
pause
