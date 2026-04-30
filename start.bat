@echo off
chcp 65001 >nul
setlocal

cd /d "%~dp0"

echo ========================================
echo  Vault PRO - Local Backend Launcher
echo ========================================
echo.

where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python is not installed or not on PATH.
    echo         Please install Python 3.10+ from https://www.python.org/downloads/
    pause
    exit /b 1
)

if not exist ".venv\Scripts\python.exe" (
    echo [setup] Creating virtual environment .venv ...
    python -m venv .venv
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment.
        pause
        exit /b 1
    )
)

call ".venv\Scripts\activate.bat"

if not exist ".venv\.deps_installed" (
    echo [setup] Installing Python dependencies ...
    python -m pip install --upgrade pip
    python -m pip install -r backend\requirements.txt
    if errorlevel 1 (
        echo [ERROR] pip install failed.
        pause
        exit /b 1
    )
    echo [setup] Installing Playwright Chromium ...
    python -m playwright install chromium
    if errorlevel 1 (
        echo [ERROR] playwright install failed.
        pause
        exit /b 1
    )
    type nul > ".venv\.deps_installed"
)

echo.
echo [run] Starting FastAPI on http://0.0.0.0:8765
echo       LAN devices can connect via: http://<this-pc-ip>:8765
echo       Press Ctrl+C to stop.
echo.
python -m uvicorn backend.server:app --host 0.0.0.0 --port 8765
