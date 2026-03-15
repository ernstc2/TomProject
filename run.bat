@echo off
REM PubLog Characteristics Importer -- Task Scheduler wrapper
REM
REM SETUP INSTRUCTIONS:
REM   1. Open a command prompt and run: where python
REM   2. Replace the python.exe path below with the output from step 1
REM   3. This file auto-detects its own directory -- no need to change the project path
REM   4. Create a Task Scheduler job pointing to this .bat file
REM      Example: schtasks /create /tn "PubLog Importer" /tr "C:\Users\Tom\TomProject\run.bat" /sc monthly /d 1 /st 06:00 /ru "DOMAIN\Tom" /f
REM
REM DO NOT use relative paths -- Task Scheduler runs from System32 by default.
REM DO NOT remove the "exit /b" line -- it propagates Python's exit code to Task Scheduler.

cd /d "%~dp0"
"C:\PATH\TO\python.exe" "%~dp0importer.py"
exit /b %ERRORLEVEL%
