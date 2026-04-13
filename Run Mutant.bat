@echo off
title Mutant - Agentic Excel Processor
echo.
echo  ======================================
echo    Mutant - Starting up...
echo  ======================================
echo.

cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found.
    echo Run setup first: python -m venv .venv
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat
python desktop_app.py

if %ERRORLEVEL% neq 0 (
    echo.
    echo [ERROR] Something went wrong. Check the output above.
    pause
)
