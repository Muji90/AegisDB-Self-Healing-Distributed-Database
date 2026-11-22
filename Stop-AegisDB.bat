@echo off
rem Double-click this file to stop all five AegisDB nodes.
cd /d "%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0stop_cluster.ps1"
timeout /t 2 >nul
