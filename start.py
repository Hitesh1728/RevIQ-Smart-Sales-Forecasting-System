#!/usr/bin/env python3
"""
AI Sales Forecasting System - One-Command Startup
"""

import subprocess
import sys
import os
import time
import signal
import threading
import webbrowser
from pathlib import Path

BASE_DIR = Path(__file__).parent
processes = []


def load_env():
    """Load .env file into os.environ if present."""
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        with open(env_path, encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, _, value = line.partition('=')
                    key = key.strip()
                    value = value.strip()
                    if value and value != 'your_api_key_here':
                        os.environ.setdefault(key, value)


def check_and_install_deps():
    print("📦 Checking dependencies...")
    req_file = BASE_DIR / "requirements.txt"
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "-r", str(req_file), "-q"],
            capture_output=True, text=True
        )
        print("✅ Dependencies ready" if result.returncode == 0 else f"⚠️ {result.stderr[:200]}")
    except Exception as e:
        print(f"❌ Could not install deps: {e}")


def kill_port(port):
    """Kill any process already using a port so we start clean."""
    try:
        if sys.platform == "win32":
            result = subprocess.run(
                f"for /f \"tokens=5\" %a in ('netstat -ano ^| findstr :{port}') do taskkill /F /PID %a",
                shell=True, capture_output=True
            )
        else:
            subprocess.run(f"lsof -ti:{port} | xargs kill -9", shell=True, capture_output=True)
    except Exception:
        pass


def start_api():
    print("🚀 Starting FastAPI backend on http://localhost:8000 ...")
    api_dir = BASE_DIR / "backend"
    env = os.environ.copy()
    env['PYTHONPATH'] = str(BASE_DIR)
    proc = subprocess.Popen(
        [sys.executable, "main.py"],
        cwd=str(api_dir),
        env=env
    )
    processes.append(proc)
    return proc


def start_streamlit():
    print("🎨 Starting Streamlit frontend on http://localhost:8501 ...")
    frontend_dir = BASE_DIR / "frontend"
    env = os.environ.copy()
    env['PYTHONPATH'] = str(BASE_DIR)
    proc = subprocess.Popen(
        [sys.executable, "-m", "streamlit", "run", "app.py",
         "--server.port", "8501",
         "--server.headless", "true",
         "--browser.gatherUsageStats", "false"],
        cwd=str(frontend_dir),
        env=env
    )
    processes.append(proc)
    return proc


def wait_for_api(timeout=45):
    """Wait until the API responds to /health."""
    import urllib.request
    start = time.time()
    while time.time() - start < timeout:
        try:
            urllib.request.urlopen("http://localhost:8000/health", timeout=2)
            return True
        except Exception:
            time.sleep(1)
    return False


def cleanup(sig=None, frame=None):
    print("\n🛑 Shutting down...")
    for p in processes:
        try:
            p.terminate()
        except Exception:
            pass
    time.sleep(1)
    for p in processes:
        try:
            p.kill()
        except Exception:
            pass
    sys.exit(0)


def main():
    load_env()
    signal.signal(signal.SIGINT, cleanup)
    signal.signal(signal.SIGTERM, cleanup)

    print("\n" + "=" * 60)
    print("  ⚡ AI Sales Forecasting System")
    print("=" * 60)

    # Step 1: Install deps
    check_and_install_deps()
    print()

    # Step 2: Free up ports in case they're already in use
    kill_port(8000)
    kill_port(8501)
    time.sleep(1)

    # Step 3: Start API and wait for it to be ready
    api_proc = start_api()
    print("⏳ Waiting for API to start...")

    if wait_for_api(45):
        print("✅ API is running!")
    else:
        # Check if process died — print helpful message
        if api_proc.poll() is not None:
            print("❌ API failed to start. Check that all dependencies are installed:")
            print("   pip install -r requirements.txt")
        else:
            print("⚠️ API is slow to start, continuing anyway...")

    # Step 4: Start Streamlit
    time.sleep(1)
    st_proc = start_streamlit()

    print()
    print("=" * 60)
    print("  🟢 SYSTEM RUNNING")
    print("=" * 60)
    print("  Frontend:  http://localhost:8501")
    print("  API:       http://localhost:8000")
    print("=" * 60)
    print("  Press Ctrl+C to stop")
    print("=" * 60)
    print()

    # Open exactly ONE browser tab after short delay
    def open_browser():
        time.sleep(4)
        webbrowser.open("http://localhost:8501")
    threading.Thread(target=open_browser, daemon=True).start()

    # Keep alive — only restart API if it crashes unexpectedly
    try:
        while True:
            time.sleep(3)
            # Stop if streamlit exits
            if st_proc.poll() is not None:
                print("Streamlit stopped. Shutting down.")
                break
            # Restart API only if it crashed (not during clean shutdown)
            if api_proc.poll() is not None:
                if st_proc.poll() is None:
                    print("⚠️ API stopped, restarting...")
                    time.sleep(2)
                    api_proc = start_api()
    except KeyboardInterrupt:
        pass

    cleanup()


if __name__ == "__main__":
    main()