@echo off
title Mutant - Agentic Excel Processor
echo.
echo  ======================================
echo    Mutant - Starting up...
echo  ======================================
echo.

cd /d "%~dp0"
echo Working directory: %CD%
echo.

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found at .venv\Scripts\python.exe
    echo.
    echo Please run these commands first:
    echo   python -m venv .venv
    echo   .venv\Scripts\activate.bat
    echo   pip install -e ".[desktop]"
    echo.
    pause
    exit /b 1
)

echo Found virtual environment. Activating...
call .venv\Scripts\activate.bat

echo Starting desktop app...
echo.
python desktop_app.py

echo.
echo ======================================
echo  App has stopped. Exit code: %ERRORLEVEL%
echo ======================================
echo.
pause
