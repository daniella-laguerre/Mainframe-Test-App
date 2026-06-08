@echo off
REM ============================================================
REM Trading Platform - Windows Installer (wrapper)
REM Right-click and "Run as administrator"
REM ============================================================

NET SESSION >nul 2>&1
if %errorLevel% NEQ 0 (
    echo ERROR: This script must be run as Administrator.
    echo Right-click install.bat and select "Run as administrator".
    pause
    exit /b 1
)

powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0install.ps1"
pause
