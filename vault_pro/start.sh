#!/usr/bin/env bash
set -e
cd "$(dirname "$0")"

if ! command -v python3 >/dev/null 2>&1; then
    echo "[ERROR] python3 not found. Install Python 3.10+ first."
    exit 1
fi

if [ ! -d ".venv" ]; then
    echo "[setup] Creating virtual environment .venv ..."
    python3 -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate

if [ ! -f ".venv/.deps_installed" ]; then
    echo "[setup] Installing Python dependencies ..."
    python -m pip install --upgrade pip
    python -m pip install -r backend/requirements.txt
    echo "[setup] Installing Playwright Chromium ..."
    python -m playwright install chromium
    touch .venv/.deps_installed
fi

echo "[run] Starting FastAPI on http://127.0.0.1:8765"
exec python -m uvicorn backend.server:app --host 127.0.0.1 --port 8765
