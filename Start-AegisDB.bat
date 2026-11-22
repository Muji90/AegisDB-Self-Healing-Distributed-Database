@echo off
rem Double-click this file to start the AegisDB cluster and open the dashboard.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0start_cluster.ps1"
if errorlevel 1 pause
